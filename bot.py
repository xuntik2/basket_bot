#!/usr/bin/env python3
"""
Telegram Bot для учета посещаемости тренировок
Поздравления с ДР, проф. праздники, новости баскетбола
Версия 9.0 — Retry-логика для polling, все критические ошибки исправлены
"""
import os
import sys
import logging
import asyncio
import re
import calendar
import tempfile
import feedparser
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ChatMemberHandler,
)
from telegram.constants import ParseMode
from telegram.error import Conflict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from supabase import create_client, Client
import pandas as pd
from flask import Flask, jsonify
from threading import Thread

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Часовой пояс Москвы
MSK = ZoneInfo("Europe/Moscow")

# Конфигурация из переменных окружения
BOT_TOKEN = os.environ.get('BOT_TOKEN')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ADMIN_USER_IDS = [int(x.strip()) for x in os.environ.get('ADMIN_USER_IDS', '').split(',') if x.strip()]

# Конвертация GROUP_CHAT_ID в int
try:
    GROUP_CHAT_ID = int(os.environ.get('GROUP_CHAT_ID')) if os.environ.get('GROUP_CHAT_ID') else None
except (ValueError, TypeError):
    GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
    logger.warning(f"GROUP_CHAT_ID не конвертирован в int: {GROUP_CHAT_ID}")

PORT = int(os.environ.get('PORT', 10000))
OWNER_ID = ADMIN_USER_IDS[0] if ADMIN_USER_IDS else None
BIRTHDAY_IMAGE_PATH = 'birthday.jpg'

# Профессиональные праздники (только указанные)
PROFESSIONAL_HOLIDAYS = {
    'miner': {'calc': lambda y: _last_sunday(y, 8), 'name': 'Днем Шахтера'},
    'metallurgist': {'calc': lambda y: _third_sunday(y, 7), 'name': 'Днем Металлурга'},
    'energetic': {'calc': lambda y: date(y, 12, 22), 'name': 'Днем Энергетика'},
    'transport': {'calc': lambda y: _first_sunday(y, 11), 'name': 'Днем работника транспорта'},
}

# Инициализация Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Flask приложение для keep-alive
app = Flask(__name__)

@app.route('/')
def home():
    return f"Bot is running! {datetime.now(MSK).strftime('%Y-%m-%d %H:%M:%S MSK')}"

@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(MSK).isoformat(),
        "timezone": "Europe/Moscow"
    })

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, use_reloader=False)


# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def _last_sunday(year: int, month: int) -> date:
    """Последнее воскресенье месяца"""
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day)
    while last_date.weekday() != 6:
        last_date -= timedelta(days=1)
    return last_date.date()

def _third_sunday(year: int, month: int) -> date:
    """Третье воскресенье месяца"""
    first_day = datetime(year, month, 1)
    first_sunday = first_day + timedelta(days=(6 - first_day.weekday()) % 7)
    third_sunday = first_sunday + timedelta(days=14)
    return third_sunday.date()

def _first_sunday(year: int, month: int) -> date:
    """Первое воскресенье месяца"""
    first_day = datetime(year, month, 1)
    first_sunday = first_day + timedelta(days=(6 - first_day.weekday()) % 7)
    return first_sunday.date()

async def safe_execute(job_func, application: Application, *args, **kwargs):
    """Обертка для безопасного выполнения задач"""
    try:
        await job_func(application, *args, **kwargs)
    except Exception as e:
        error_msg = f"⚠️ Ошибка в задаче {job_func.__name__}:\n{type(e).__name__}: {str(e)[:300]}"
        logger.error(error_msg, exc_info=True)
        if OWNER_ID:
            try:
                await application.bot.send_message(chat_id=OWNER_ID, text=error_msg)
            except Exception:
                pass

async def check_user_in_chat(bot: Bot, user_id: int, chat_id) -> bool:
    """Проверка, находится ли пользователь в чате"""
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception:
        return False

async def notify_owner(bot: Bot, text: str):
    """Отправка уведомления владельцу"""
    if OWNER_ID:
        try:
            await bot.send_message(chat_id=OWNER_ID, text=text)
        except Exception:
            pass

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    if OWNER_ID:
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"❌ Ошибка в боте: {type(context.error).__name__}: {str(context.error)[:300]}"
            )
        except Exception:
            pass


# ============ БАЗА ДАННЫХ ============

class DatabaseManager:
    """Управление базой данных Supabase"""
    
    @staticmethod
    async def init_tables():
        try:
            await asyncio.to_thread(
                lambda: supabase.table('polls').select('*').limit(1).execute()
            )
            logger.info("Таблицы инициализированы")
        except Exception as e:
            logger.error(f"Ошибка проверки таблиц: {e}")

    @staticmethod
    async def save_poll(poll_id: str, message_id: int, chat_id: int, training_date: str, created_at: datetime):
        try:
            data = {
                'poll_id': poll_id,
                'message_id': message_id,
                'chat_id': chat_id,
                'training_date': training_date,
                'created_at': created_at.isoformat()
            }
            await asyncio.to_thread(
                lambda: supabase.table('polls').insert(data).execute()
            )
            logger.info(f"Опрос {poll_id} сохранен")
        except Exception as e:
            logger.error(f"Ошибка сохранения опроса: {e}")

    @staticmethod
    async def save_response(poll_id: str, user_id: int, username: str, full_name: str, response: str):
        try:
            existing = await asyncio.to_thread(
                lambda: supabase.table('responses').select('*').eq('poll_id', poll_id).eq('user_id', user_id).execute()
            )
            data = {
                'poll_id': poll_id,
                'user_id': user_id,
                'username': username,
                'full_name': full_name,
                'response': response,
                'updated_at': datetime.now(MSK).isoformat()
            }
            if existing.data:
                await asyncio.to_thread(
                    lambda: supabase.table('responses').update(data).eq('poll_id', poll_id).eq('user_id', user_id).execute()
                )
            else:
                data['created_at'] = datetime.now(MSK).isoformat()
                await asyncio.to_thread(
                    lambda: supabase.table('responses').insert(data).execute()
                )
            await DatabaseManager.ensure_user_exists(user_id, username, full_name)
            logger.info(f"Ответ сохранен: {username} - {response}")
        except Exception as e:
            logger.error(f"Ошибка сохранения ответа: {e}")

    @staticmethod
    async def ensure_user_exists(user_id: int, username: str, full_name: str):
        try:
            existing = await asyncio.to_thread(
                lambda: supabase.table('birthdays').select('*').eq('user_id', user_id).execute()
            )
            if not existing.data:
                data = {
                    'user_id': user_id,
                    'username': username,
                    'full_name': full_name,
                    'birth_date': None,
                    'is_active': True,
                    'created_at': datetime.now(MSK).isoformat()
                }
                await asyncio.to_thread(
                    lambda: supabase.table('birthdays').insert(data).execute()
                )
                logger.info(f"Новый пользователь: {full_name}")
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")

    @staticmethod
    async def add_or_update_birthday(user_id: int, full_name: str, birth_date: str, username: str = ''):
        """Добавление/обновление ДР с корректной валидацией"""
        try:
            if birth_date:
                try:
                    datetime.strptime(birth_date, "%d-%m")
                except ValueError:
                    raise ValueError("Несуществующая дата")
            
            existing = await asyncio.to_thread(
                lambda: supabase.table('birthdays').select('*').eq('user_id', user_id).execute()
            )
            data = {
                'user_id': user_id,
                'full_name': full_name,
                'birth_date': birth_date if birth_date else None,
                'username': username,
                'is_active': True,
                'updated_at': datetime.now(MSK).isoformat()
            }
            if existing.data:
                await asyncio.to_thread(
                    lambda: supabase.table('birthdays').update(data).eq('user_id', user_id).execute()
                )
            else:
                data['created_at'] = datetime.now(MSK).isoformat()
                await asyncio.to_thread(
                    lambda: supabase.table('birthdays').insert(data).execute()
                )
            logger.info(f"ДР обновлен: {full_name} - {birth_date}")
        except Exception as e:
            logger.error(f"Ошибка добавления ДР: {e}")
            raise

    @staticmethod
    async def get_poll_responses(poll_id: str) -> List[Dict]:
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table('responses').select('*').eq('poll_id', poll_id).execute()
            )
            return result.data or []
        except Exception as e:
            logger.error(f"Ошибка получения ответов: {e}")
            return []

    @staticmethod
    async def get_monthly_stats(year: int, month: int) -> pd.DataFrame:
        try:
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"
            polls_result = await asyncio.to_thread(
                lambda: supabase.table('polls').select('*').gte('training_date', start_date).lt('training_date', end_date).execute()
            )
            polls = polls_result.data or []
            if not polls:
                return pd.DataFrame()
            all_responses = []
            for poll in polls:
                responses = await DatabaseManager.get_poll_responses(poll['poll_id'])
                for resp in responses:
                    resp['training_date'] = poll['training_date']
                all_responses.extend(responses)
            return pd.DataFrame(all_responses) if all_responses else pd.DataFrame()
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return pd.DataFrame()

    @staticmethod
    async def get_all_stats() -> pd.DataFrame:
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table('responses').select('*').execute()
            )
            data = result.data or []
            if not data:
                return pd.DataFrame()
            poll_ids = list(set([r['poll_id'] for r in data]))
            polls_result = await asyncio.to_thread(
                lambda: supabase.table('polls').select('poll_id, training_date').in_('poll_id', poll_ids).execute()
            )
            polls = {p['poll_id']: p['training_date'] for p in (polls_result.data or [])}
            for item in data:
                item['training_date'] = polls.get(item['poll_id'], 'Unknown')
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Ошибка получения полной статистики: {e}")
            return pd.DataFrame()

    @staticmethod
    async def get_today_birthdays() -> List[Dict]:
        try:
            today = datetime.now(MSK)
            today_str = f"{today.day:02d}-{today.month:02d}"
            result = await asyncio.to_thread(
                lambda: supabase.table('birthdays').select('*').eq('birth_date', today_str).eq('is_active', True).execute()
            )
            return result.data or []
        except Exception as e:
            logger.error(f"Ошибка получения именинников: {e}")
            return []

    @staticmethod
    async def mark_user_inactive(user_id: int):
        try:
            await asyncio.to_thread(
                lambda: supabase.table('birthdays').update({'is_active': False, 'updated_at': datetime.now(MSK).isoformat()}).eq('user_id', user_id).execute()
            )
        except Exception as e:
            logger.error(f"Ошибка деактивации пользователя {user_id}: {e}")

    @staticmethod
    async def check_news_sent(news_id: str) -> bool:
        """Проверка, отправлена ли уже новость"""
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table('sports_news').select('*').eq('news_id', news_id).execute()
            )
            return bool(result.data)
        except Exception:
            return False

    @staticmethod
    async def mark_news_sent(news_id: str, title: str):
        """Отметить новость как отправленную"""
        try:
            data = {
                'news_id': news_id,
                'title': title,
                'sent_at': datetime.now(MSK).isoformat()
            }
            await asyncio.to_thread(
                lambda: supabase.table('sports_news').insert(data).execute()
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения новости: {e}")


# ============ УПРАВЛЕНИЕ ОПРОСАМИ ============

class PollManager:
    RESPONSES = {'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'}

    @staticmethod
    def get_training_date() -> str:
        now = datetime.now(MSK)
        tomorrow = now + timedelta(days=1)
        months = ['', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
        return f"{tomorrow.day} {months[tomorrow.month]} {tomorrow.year}"

    @staticmethod
    def get_training_date_iso() -> str:
        now = datetime.now(MSK)
        tomorrow = now + timedelta(days=1)
        return tomorrow.strftime('%Y-%m-%d')

    @staticmethod
    def create_poll_text(training_date: str) -> str:
        return (f"🏃‍♂️ <b>Коллеги, добрый день!</b>\n"
                f"Кто <b>({training_date})</b> идет на тренировку?\n"
                f"Выберите вариант ответа:")

    @staticmethod
    def create_keyboard(poll_id: str) -> InlineKeyboardMarkup:
        keyboard = [
            [InlineKeyboardButton(f"✅ {PollManager.RESPONSES['yes']}", callback_data=f'poll:yes:{poll_id}')],
            [InlineKeyboardButton(f"❌ {PollManager.RESPONSES['no']}", callback_data=f'poll:no:{poll_id}')],
            [InlineKeyboardButton(f"⏰ {PollManager.RESPONSES['later']}", callback_data=f'poll:later:{poll_id}')],
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    async def update_poll_message(bot: Bot, chat_id: int, message_id: int, poll_id: str, training_date: str):
        try:
            responses = await DatabaseManager.get_poll_responses(poll_id)
            yes_count = len([r for r in responses if r['response'] == 'yes'])
            no_count = len([r for r in responses if r['response'] == 'no'])
            later_count = len([r for r in responses if r['response'] == 'later'])
            yes_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'yes']
            no_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'no']
            later_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'later']
            text = PollManager.create_poll_text(training_date)
            text += f"\n\n📊 <b>Текущие результаты:</b>"
            text += f"\n✅ Идут: <b>{yes_count}</b>"
            if yes_users:
                text += f"\n{', '.join(yes_users)}"
            text += f"\n❌ Не смогут: <b>{no_count}</b>"
            if no_users:
                text += f"\n{', '.join(no_users)}"
            text += f"\n⏰ Ответят позже: <b>{later_count}</b>"
            if later_users:
                text += f"\n{', '.join(later_users)}"
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=PollManager.create_keyboard(poll_id),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Ошибка обновления опроса: {e}")


# ============ ПРИВЕТСТВИЕ И ОТСЛЕЖИВАНИЕ УЧАСТНИКОВ ============

async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Единый обработчик входа/выхода участников"""
    try:
        if not update.chat_member:
            return
        
        member = update.chat_member
        user = member.new_chat_member.user
        old_status = member.old_chat_member.status if member.old_chat_member else None
        new_status = member.status
        
        if new_status in ['member', 'administrator'] and old_status not in ['member', 'administrator']:
            await welcome_new_member(update, context, user)
        elif new_status in ['left', 'kicked']:
            await DatabaseManager.mark_user_inactive(user.id)
            logger.info(f"Участник покинул чат: {user.id}")
    except Exception as e:
        logger.error(f"Ошибка в chat_member_handler: {e}")

async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """Приветствие новых участников"""
    try:
        full_name = user.full_name or user.first_name or "Коллега"
        
        welcome_text = (
            f"👋 <b>{full_name}</b>, добро пожаловать в наш чат!\n\n"
            f"📍 <b>Информация о тренировках:</b>\n"
            f"• Занятия проходят по <b>вторникам с 18:30 до 19:30</b>\n"
            f"• Для первой тренировки необходимо иметь с собой <b>паспорт РФ</b>\n\n"
            f"Рады видеть тебя и ожидаем на ближайшей тренировке! 🏀\n\n"
            f"Задавай вопросы — коллеги с радостью тебя проконсультируют и помогут адаптироваться.\n\n"
            f"⚠️ <b>Важное правило:</b> корректное поведение на площадке. "
            f"Грубости и жесткая травматичная игра отрицательно скажется на карме!"
        )
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=welcome_text,
            parse_mode=ParseMode.HTML
        )
        
        await DatabaseManager.ensure_user_exists(
            user_id=user.id,
            username=user.username or '',
            full_name=full_name
        )
        logger.info(f"Приветствовали: {full_name}")
    except Exception as e:
        logger.error(f"Ошибка в welcome_new_member: {e}")


# ============ ПОЗДРАВЛЕНИЯ ============

async def send_birthday_greetings(application: Application):
    if not GROUP_CHAT_ID:
        logger.error("GROUP_CHAT_ID не настроен для поздравлений")
        return
    
    birthdays = await DatabaseManager.get_today_birthdays()
    if not birthdays:
        logger.info("Сегодня именинников нет")
        return
    
    for person in birthdays:
        user_id = person.get('user_id')
        name = person.get('full_name') or person.get('username') or 'Коллега'
        birth_date = person.get('birth_date', '')
        
        try:
            day = int(birth_date.split('-')[0])
            is_even = day % 2 == 0
        except:
            is_even = True
        
        if is_even:
            text = (f"🎂 <b>{name}</b>, поздравляю Вас с днем рождения!\n\n"
                    f"Желаю Вам успехов в спорте и во всех ваших делах. "
                    f"Пусть этот год будет наполнен радостью, здоровьем и благополучием для Вас и ваших близких!")
        else:
            text = (f"🎂 <b>{name}</b>, с днем рождения!\n\n"
                    f"От всего сердца желаю Вам удачи в спорте и во всех ваших начинаниях. "
                    f"Пусть этот год принесет Вам много радости, здоровья и успехов во всех делах!")
        
        if user_id and not await check_user_in_chat(application.bot, user_id, GROUP_CHAT_ID):
            await DatabaseManager.mark_user_inactive(user_id)
            continue
        
        try:
            if os.path.exists(BIRTHDAY_IMAGE_PATH):
                try:
                    with open(BIRTHDAY_IMAGE_PATH, 'rb') as photo:
                        await application.bot.send_photo(
                            chat_id=GROUP_CHAT_ID,
                            photo=photo,
                            caption=text,
                            parse_mode=ParseMode.HTML
                        )
                except Exception as file_error:
                    logger.error(f"Ошибка отправки фото: {file_error}")
                    await application.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=text,
                        parse_mode=ParseMode.HTML
                    )
            else:
                await application.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=text,
                    parse_mode=ParseMode.HTML
                )
            logger.info(f"Поздравили: {name}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Ошибка поздравления {name}: {e}")
            if OWNER_ID:
                await notify_owner(application.bot, f"❌ Ошибка поздравления {name}: {e}")

async def send_professional_holiday(application: Application, holiday_name: str):
    if not GROUP_CHAT_ID:
        logger.error("GROUP_CHAT_ID не настроен для проф. праздников")
        return
    try:
        text = f"🎉 Коллеги, поздравляю с {holiday_name}!"
        await application.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Отправлено поздравление с {holiday_name}")
    except Exception as e:
        logger.error(f"Ошибка отправки поздравления: {e}")
        raise


# ============ НОВОСТИ О ЧЕМПИОНАХ ============

async def check_basketball_champions(application: Application):
    """Проверка новостей о чемпионах NBA, ВТБ, Евролиги"""
    if not GROUP_CHAT_ID:
        return
    
    try:
        rss_url = "https://www.sports.ru/rss/topic.xml"
        feed = await asyncio.to_thread(feedparser.parse, rss_url)
        
        if not feed.entries:
            logger.warning("RSS лента пуста или недоступна")
            return
        
        yesterday = (datetime.now(MSK) - timedelta(days=1)).date()
        
        leagues = {
            'nba': ['нба', 'nba', 'чемпион нба', 'победитель нба'],
            'vtb': ['втб', 'единая лига', 'чемпион единой лиги', 'победитель втб'],
            'euroleague': ['евролига', 'чемпион евролиги', 'победитель евролиги']
        }
        
        for entry in feed.entries:
            published = None
            if entry.get('published_parsed'):
                published = datetime(*entry.published_parsed[:6]).date()
            
            if published and published == yesterday:
                title_lower = entry.title.lower()
                summary_lower = entry.get('summary', '').lower()
                combined_text = title_lower + ' ' + summary_lower
                
                for league, keywords in leagues.items():
                    if any(kw in combined_text for kw in keywords):
                        news_id = entry.get('id', f"{league}_{published}_{entry.title[:50]}")
                        
                        if await DatabaseManager.check_news_sent(news_id):
                            logger.info(f"Новость уже отправлена: {news_id}")
                            continue
                        
                        text = f"🏆 <b>{entry.title}</b>\n\n{entry.get('summary', '')[:300]}...\n\nПодробнее: {entry.link}"
                        photo = None
                        
                        if 'media_content' in entry and entry.media_content:
                            photo = entry.media_content[0]['url']
                        elif 'links' in entry:
                            for link in entry.links:
                                if 'image' in link.get('type', ''):
                                    photo = link.href
                                    break
                        
                        try:
                            if photo:
                                await application.bot.send_photo(
                                    chat_id=GROUP_CHAT_ID,
                                    photo=photo,
                                    caption=text,
                                    parse_mode=ParseMode.HTML
                                )
                            else:
                                await application.bot.send_message(
                                    chat_id=GROUP_CHAT_ID,
                                    text=text,
                                    parse_mode=ParseMode.HTML
                                )
                            await DatabaseManager.mark_news_sent(news_id, entry.title)
                            logger.info(f"Отправлена новость: {entry.title[:50]}")
                            await asyncio.sleep(3)
                        except Exception as e:
                            logger.error(f"Ошибка отправки новости: {e}")
                            if OWNER_ID:
                                await notify_owner(application.bot, f"❌ Ошибка новости: {e}")
                        break
    except Exception as e:
        logger.error(f"Ошибка проверки новостей: {e}")
        if OWNER_ID:
            await notify_owner(application.bot, f"❌ Ошибка проверки новостей: {e}")
        raise


# ============ КОМАНДЫ ============

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        await update.message.reply_text(
            f"👋 Привет, {user.first_name}!\n"
            f"Я бот для учета посещаемости тренировок.\n"
            f"Каждый вторник создаю опрос о посещении.\n"
            f"/help - Помощь"
        )
    except Exception as e:
        logger.error(f"Ошибка в start_command: {e}")
        if OWNER_ID:
            await notify_owner(context.bot, f"❌ Ошибка в /start: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        is_admin = update.effective_user.id in ADMIN_USER_IDS
        text = "📋 <b>Доступные команды:</b>\n"
        text += "/start - Начать работу\n"
        text += "/help - Показать справку\n"
        text += "/setbirthday ДД-ММ - Указать свой день рождения\n"
        if is_admin:
            text += "\n🔐 <b>Команды администратора:</b>\n"
            text += "/poll - Создать опрос\n"
            text += "/stats - Полная статистика\n"
            text += "/monthlystats - Статистика за месяц\n"
            text += "/addbirthday user_id ДД-ММ Имя - Добавить ДР пользователю\n"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в help_command: {e}")

async def set_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /setbirthday для пользователей"""
    try:
        user = update.effective_user
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "📋 <b>Использование:</b>\n"
                "/setbirthday ДД-ММ\n\n"
                "📝 <b>Пример:</b>\n"
                "/setbirthday 15-03"
            )
            return
        
        birth_date = context.args[0]
        try:
            datetime.strptime(birth_date, "%d-%m")
        except ValueError:
            await update.message.reply_text("❌ Неверный формат даты. Используйте ДД-ММ")
            return
        
        await DatabaseManager.add_or_update_birthday(
            user_id=user.id,
            full_name=user.full_name or user.first_name,
            birth_date=birth_date,
            username=user.username or ''
        )
        await update.message.reply_text(f"✅ Ваш день рождения сохранен: {birth_date}")
        logger.info(f"Пользователь {user.username} установил ДР: {birth_date}")
    except Exception as e:
        logger.error(f"Ошибка в set_birthday_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
        if OWNER_ID:
            await notify_owner(context.bot, f"❌ Ошибка в /setbirthday: {e}")

async def poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("❌ Нет прав")
            return
        logger.info(f"Попытка отправить в GROUP_CHAT_ID: {GROUP_CHAT_ID!r} (тип {type(GROUP_CHAT_ID)})")
        if not GROUP_CHAT_ID:
            await update.message.reply_text("❌ GROUP_CHAT_ID не настроен")
            return
        training_date = PollManager.get_training_date()
        training_date_iso = PollManager.get_training_date_iso()
        poll_id = f"manual_{datetime.now(MSK).strftime('%Y%m%d_%H%M%S')}"
        text = PollManager.create_poll_text(training_date)
        message = await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=text,
            reply_markup=PollManager.create_keyboard(poll_id),
            parse_mode=ParseMode.HTML
        )
        await DatabaseManager.save_poll(
            poll_id=poll_id,
            message_id=message.message_id,
            chat_id=message.chat_id,
            training_date=training_date_iso,
            created_at=datetime.now(MSK)
        )
        await update.message.reply_text(f"✅ Опрос создан! ID: {poll_id}")
        logger.info(f"Админ {user.username} создал опрос: {poll_id}")
    except Exception as e:
        logger.error(f"Telegram API error: {type(e).__name__}: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
        raise

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("❌ Нет прав")
            return
        await update.message.reply_text("📊 Формирую статистику...")
        df = await DatabaseManager.get_all_stats()
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста")
            return
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df.sort_values('created_at')
        df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
        df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
        df_display['Ответ'] = df_display['Ответ'].map({'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'})
        
        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            temp_file.close()
            with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
                df_display.to_excel(writer, sheet_name='Все ответы', index=False)
                summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
                summary.to_excel(writer, sheet_name='Сводка', index=False)
            with open(temp_file.name, 'rb') as f:
                await context.bot.send_document(
                    chat_id=user.id,
                    document=f,
                    filename=f"stats_{datetime.now(MSK).strftime('%Y%m%d')}.xlsx",
                    caption="📊 Статистика"
                )
        finally:
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)
        logger.info(f"Админ {user.username} получил статистику")
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def monthly_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("❌ Нет прав")
            return
        now = datetime.now(MSK)
        await update.message.reply_text(f"📊 Статистика за {now.strftime('%B %Y')}...")
        df = await DatabaseManager.get_monthly_stats(now.year, now.month)
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста")
            return
        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            temp_file.close()
            with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
                df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
                df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
                df_display['Ответ'] = df_display['Ответ'].map({'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'})
                df_display.to_excel(writer, sheet_name='Все ответы', index=False)
                summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
                summary.to_excel(writer, sheet_name='Сводка', index=False)
                employee_stats = df[df['response'] == 'yes'].groupby(['full_name', 'username']).size().reset_index(name='Посещений')
                employee_stats.sort_values('Посещений', ascending=False).to_excel(writer, sheet_name='По сотрудникам', index=False)
            with open(temp_file.name, 'rb') as f:
                await context.bot.send_document(
                    chat_id=user.id,
                    document=f,
                    filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                    caption=f"📊 Статистика за {now.strftime('%B %Y')}"
                )
        finally:
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)
    except Exception as e:
        logger.error(f"Ошибка месячной статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def add_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("❌ Нет прав")
            return
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "📋 <b>Использование:</b>\n"
                "/addbirthday user_id ДД-ММ Фамилия Имя\n\n"
                "📝 <b>Пример:</b>\n"
                "/addbirthday 123456789 15-03 Иванов Иван"
            )
            return
        user_id = int(context.args[0])
        birth_date = context.args[1]
        full_name = ' '.join(context.args[2:])
        try:
            datetime.strptime(birth_date, "%d-%m")
        except ValueError:
            raise ValueError("Несуществующая дата")
        await DatabaseManager.add_or_update_birthday(user_id, full_name, birth_date)
        await update.message.reply_text(f"✅ Добавлено: {full_name}, ДР {birth_date}")
        logger.info(f"Админ {user.username} добавил ДР вручную: {full_name} - {birth_date}")
    except ValueError as ve:
        logger.error(f"Ошибка валидации в /addbirthday: {ve}")
        await update.message.reply_text(f"❌ Ошибка: {ve}")
    except Exception as e:
        logger.error(f"Ошибка в add_birthday_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
        if OWNER_ID:
            await notify_owner(context.bot, f"❌ Ошибка в /addbirthday: {e}")

async def poll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user = update.effective_user
        data = query.data
        if not data.startswith('poll:'):
            return
        parts = data.split(':')
        if len(parts) < 3:
            logger.error(f"Некорректный callback_data: {data}")
            return
        response = parts[1]
        poll_id = parts[2]
        response_text = PollManager.RESPONSES.get(response, response)
        message = query.message
        text = message.text or ""
        date_match = re.search(r'\(([^)]+)\)', text)
        training_date = date_match.group(1) if date_match else "Неизвестно"
        await DatabaseManager.save_response(
            poll_id=poll_id,
            user_id=user.id,
            username=user.username or '',
            full_name=user.full_name or '',
            response=response
        )
        await query.edit_message_text(
            text=f"✅ Вы выбрали: <b>{response_text}</b>\nСпасибо!",
            parse_mode=ParseMode.HTML
        )
        await PollManager.update_poll_message(
            bot=context.bot,
            chat_id=message.chat_id,
            message_id=message.message_id,
            poll_id=poll_id,
            training_date=training_date
        )
        logger.info(f"Пользователь {user.username} выбрал: {response_text}")
    except Exception as e:
        logger.error(f"Ошибка в poll_callback: {e}")


# ============ АВТОМАТИЧЕСКИЕ ЗАДАЧИ ============

async def scheduled_poll(application: Application):
    if not GROUP_CHAT_ID:
        logger.error("GROUP_CHAT_ID не настроен")
        return
    training_date = PollManager.get_training_date()
    training_date_iso = PollManager.get_training_date_iso()
    poll_id = f"auto_{datetime.now(MSK).strftime('%Y%m%d')}"
    text = PollManager.create_poll_text(training_date)
    message = await application.bot.send_message(
        chat_id=GROUP_CHAT_ID,
        text=text,
        reply_markup=PollManager.create_keyboard(poll_id),
        parse_mode=ParseMode.HTML
    )
    await DatabaseManager.save_poll(
        poll_id=poll_id,
        message_id=message.message_id,
        chat_id=message.chat_id,
        training_date=training_date_iso,
        created_at=datetime.now(MSK)
    )
    logger.info(f"Автоматический опрос создан: {poll_id}")

async def scheduled_monthly_stats(application: Application):
    now = datetime.now(MSK)
    for admin_id in ADMIN_USER_IDS:
        try:
            df = await DatabaseManager.get_monthly_stats(now.year, now.month)
            if df.empty:
                await application.bot.send_message(
                    chat_id=admin_id,
                    text=f"📭 Статистика за {now.strftime('%B %Y')} пуста"
                )
                continue
            temp_file = None
            try:
                temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
                temp_file.close()
                with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
                    df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
                    df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
                    df_display['Ответ'] = df_display['Ответ'].map({'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'})
                    df_display.to_excel(writer, sheet_name='Все ответы', index=False)
                    summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
                    summary.to_excel(writer, sheet_name='Сводка', index=False)
                with open(temp_file.name, 'rb') as f:
                    await application.bot.send_document(
                        chat_id=admin_id,
                        document=f,
                        filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                        caption=f"📊 Автоматическая статистика за {now.strftime('%B %Y')}"
                    )
            finally:
                if temp_file and os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
            logger.info(f"Статистика отправлена админу {admin_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки статистики: {e}")

def schedule_professional_holidays(scheduler, application: Application):
    now = datetime.now(MSK).date()
    year = now.year
    for key, holiday in PROFESSIONAL_HOLIDAYS.items():
        holiday_date = holiday['calc'](year)
        if holiday_date < now:
            holiday_date = holiday['calc'](year + 1)
        job_id = f"holiday_{key}_{year}"
        scheduler.add_job(
            send_professional_holiday,
            trigger=DateTrigger(run_date=datetime.combine(holiday_date, datetime.min.time().replace(hour=10)), timezone=MSK),
            id=job_id,
            args=[application, holiday['name']],
            replace_existing=True
        )
        logger.info(f"Запланирован праздник {holiday['name']} на {holiday_date}")

def setup_scheduler(application: Application):
    scheduler = AsyncIOScheduler(timezone=MSK)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day_of_week='tue', hour=10, minute=30, timezone=MSK),
        id='weekly_poll',
        args=[scheduled_poll, application],
        replace_existing=True
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day='last', hour=15, minute=0, timezone=MSK),
        id='monthly_stats',
        args=[scheduled_monthly_stats, application],
        replace_existing=True
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='birthday_greetings',
        args=[send_birthday_greetings, application],
        replace_existing=True
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='basketball_news',
        args=[check_basketball_champions, application],
        replace_existing=True
    )
    schedule_professional_holidays(scheduler, application)
    scheduler.start()
    logger.info("Планировщик запущен")
    return scheduler


# ============ ОСНОВНАЯ ФУНКЦИЯ ============

async def main():
    if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
        logger.error("Не все переменные окружения настроены!")
        return
    await DatabaseManager.init_tables()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("poll", poll_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("monthlystats", monthly_stats_command))
    application.add_handler(CommandHandler("addbirthday", add_birthday_command))
    application.add_handler(CommandHandler("setbirthday", set_birthday_command))
    application.add_handler(CallbackQueryHandler(poll_callback, pattern='^poll:'))
    application.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error_handler)
    scheduler = setup_scheduler(application)
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"Бот запущен! Время MSK: {datetime.now(MSK).strftime('%Y-%m-%d %H:%M:%S')}")
    await application.initialize()
    await application.start()
    
    # === RETRY-ЛОГИКА ДЛЯ POLLING (Render) ===
    max_retries = 3
    retry_delay = 30
    
    for attempt in range(max_retries):
        try:
            # Гарантированно удаляем webhook
            await application.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook удалён")
            
            # Даём время старому процессу завершиться (Render rolling deploy)
            await asyncio.sleep(5)
            
            # Запускаем polling с игнорированием старых сообщений
            await application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
            logger.info("Polling запущен успешно")
            break  # Успех — выходим из цикла
            
        except Conflict as e:
            if attempt < max_retries - 1:
                logger.warning(f"Conflict (попытка {attempt + 1}/{max_retries}). Ждем {retry_delay} сек...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Не удалось запустить polling после {max_retries} попыток")
                raise  # Перезапуск контейнера Render
                
        except Exception as e:
            logger.error(f"Ошибка запуска polling: {e}", exc_info=True)
            raise
    # ===========================================
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
        scheduler.shutdown()
        await application.stop()

if __name__ == '__main__':
    asyncio.run(main())
