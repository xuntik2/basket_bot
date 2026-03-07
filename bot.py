#!/usr/bin/env python3
"""
Telegram Bot для учета посещаемости тренировок
Автоматические опросы каждый вторник, статистика для админов
Версия 3.0 - Все критические ошибки исправлены
"""
import os
import logging
import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
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
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
PORT = int(os.environ.get('PORT', 10000))

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


class DatabaseManager:
    """Управление базой данных Supabase с асинхронными вызовами"""
    
    @staticmethod
    async def init_tables():
        """Инициализация таблиц (выполняется один раз при старте)"""
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table('polls').select('*').limit(1).execute()
            )
            logger.info("Таблицы инициализированы")
        except Exception as e:
            logger.error(f"Ошибка при проверке таблиц: {e}")
            logger.info("Убедитесь, что таблицы созданы в Supabase")

    @staticmethod
    async def save_poll(poll_id: str, message_id: int, chat_id: int, training_date: str, created_at: datetime):
        """Сохранение информации об опросе"""
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
            logger.info(f"Опрос {poll_id} сохранен в БД")
        except Exception as e:
            logger.error(f"Ошибка сохранения опроса: {e}")

    @staticmethod
    async def save_response(poll_id: str, user_id: int, username: str, full_name: str, response: str):
        """Сохранение ответа пользователя"""
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
            logger.info(f"Ответ сохранен: {username} - {response}")
        except Exception as e:
            logger.error(f"Ошибка сохранения ответа: {e}")

    @staticmethod
    async def get_poll_responses(poll_id: str) -> List[Dict]:
        """Получение всех ответов на опрос"""
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
        """Получение статистики за месяц"""
        try:
            start_date = f"{year}-{month:02d}-01"
            if month == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{month + 1:02d}-01"
            
            polls_result = await asyncio.to_thread(
                lambda: supabase.table('polls').select('*').gte('training_date', start_date).lt('training_date', end_date).execute()
            )
            polls = polls_result.data or []
            
            if not polls:
                return pd.DataFrame()
            
            poll_ids = [p['poll_id'] for p in polls]
            all_responses = []
            
            for poll_id in poll_ids:
                responses = await DatabaseManager.get_poll_responses(poll_id)
                poll_info = next((p for p in polls if p['poll_id'] == poll_id), None)
                if poll_info:
                    for resp in responses:
                        resp['training_date'] = poll_info['training_date']
                    all_responses.extend(responses)
            
            if not all_responses:
                return pd.DataFrame()
            
            return pd.DataFrame(all_responses)
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return pd.DataFrame()

    @staticmethod
    async def get_all_stats() -> pd.DataFrame:
        """Получение всей статистики"""
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


class PollManager:
    """Управление опросами с московским временем"""
    
    RESPONSES = {
        'yes': 'Да, иду',
        'no': 'Не смогу',
        'later': 'Отвечу завтра'
    }

    @staticmethod
    def get_training_date() -> str:
        """Получение даты завтрашней тренировки (по московскому времени)"""
        now = datetime.now(MSK)
        tomorrow = now + timedelta(days=1)
        months = ['', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
        return f"{tomorrow.day} {months[tomorrow.month]} {tomorrow.year}"

    @staticmethod
    def get_training_date_iso() -> str:
        """ISO формат даты для БД (по московскому времени)"""
        now = datetime.now(MSK)
        tomorrow = now + timedelta(days=1)
        return tomorrow.strftime('%Y-%m-%d')

    @staticmethod
    def create_poll_text(training_date: str) -> str:
        """Создание текста опроса"""
        return (f"🏃‍♂️ <b>Коллеги, добрый день!</b>\n"
                f"Кто <b>({training_date})</b> идет на тренировку?\n"
                f"Выберите вариант ответа:")

    @staticmethod
    def create_keyboard(poll_id: str) -> InlineKeyboardMarkup:
        """Создание клавиатуры для опроса с poll_id в callback_data"""
        keyboard = [
            [InlineKeyboardButton(f"✅ {PollManager.RESPONSES['yes']}", 
                                  callback_data=f'poll:yes:{poll_id}')],
            [InlineKeyboardButton(f"❌ {PollManager.RESPONSES['no']}", 
                                  callback_data=f'poll:no:{poll_id}')],
            [InlineKeyboardButton(f"⏰ {PollManager.RESPONSES['later']}", 
                                  callback_data=f'poll:later:{poll_id}')],
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    async def update_poll_message(bot: Bot, chat_id: int, message_id: int, poll_id: str, training_date: str):
        """Обновление сообщения опроса с текущими результатами"""
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


# ============ КОМАНДЫ БОТА ============

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user = update.effective_user
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n"
        f"Я бот для учета посещаемости тренировок.\n"
        f"Каждый вторник я создаю опрос о посещении тренировки.\n"
        f"Доступные команды:\n"
        f"/help - Помощь"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    is_admin = update.effective_user.id in ADMIN_USER_IDS
    text = "📋 <b>Доступные команды:</b>\n"
    text += "/start - Начать работу с ботом\n"
    text += "/help - Показать эту справку\n"
    if is_admin:
        text += "\n🔐 <b>Команды администратора:</b>\n"
        text += "/poll - Создать опрос вручную\n"
        text += "/stats - Получить полную статистику (Excel)\n"
        text += "/monthlystats - Статистика за текущий месяц\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /poll - создание опроса вручную (только для админов)"""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return
    if not GROUP_CHAT_ID:
        await update.message.reply_text("❌ GROUP_CHAT_ID не настроен в переменных окружения.")
        return
    
    try:
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
        
        await update.message.reply_text(f"✅ Опрос создан в группе! ID: {poll_id}")
        logger.info(f"Админ {user.username} создал опрос вручную: {poll_id}")
    except Exception as e:
        logger.error(f"Ошибка создания опроса: {e}")
        await update.message.reply_text(f"❌ Ошибка создания опроса: {e}")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /stats - получение полной статистики (только для админов)"""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        await update.message.reply_text("📊 Формирую полную статистику...")
        df = await DatabaseManager.get_all_stats()
        
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста. Еще нет данных.")
            return
        
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df.sort_values('created_at')
        
        df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
        df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
        
        response_map = {'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'}
        df_display['Ответ'] = df_display['Ответ'].map(response_map)
        
        filename = f"/tmp/stats_full_{datetime.now(MSK).strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_display.to_excel(writer, sheet_name='Все ответы', index=False)
            summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
            summary.to_excel(writer, sheet_name='Сводка по тренировкам', index=False)
        
        with open(filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=user.id,
                document=f,
                filename=f"stats_full_{datetime.now(MSK).strftime('%Y%m%d')}.xlsx",
                caption="📊 Полная статистика посещаемости тренировок"
            )
        
        os.remove(filename)
        logger.info(f"Админ {user.username} получил полную статистику")
    except Exception as e:
        logger.error(f"Ошибка формирования статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def monthly_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /monthlystats - статистика за текущий месяц"""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        now = datetime.now(MSK)
        await update.message.reply_text(f"📊 Формирую статистику за {now.strftime('%B %Y')}...")
        df = await DatabaseManager.get_monthly_stats(now.year, now.month)
        
        if df.empty:
            await update.message.reply_text("📭 Статистика за текущий месяц пуста.")
            return
        
        filename = f"/tmp/stats_monthly_{now.strftime('%Y%m')}.xlsx"
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
            df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
            response_map = {'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'}
            df_display['Ответ'] = df_display['Ответ'].map(response_map)
            df_display.to_excel(writer, sheet_name='Все ответы', index=False)
            
            summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
            summary.to_excel(writer, sheet_name='Сводка', index=False)
            
            employee_stats = df[df['response'] == 'yes'].groupby(['full_name', 'username']).size().reset_index(name='Посещений')
            employee_stats = employee_stats.sort_values('Посещений', ascending=False)
            employee_stats.to_excel(writer, sheet_name='По сотрудникам', index=False)
        
        with open(filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=user.id,
                document=f,
                filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                caption=f"📊 Статистика за {now.strftime('%B %Y')}"
            )
        
        os.remove(filename)
    except Exception as e:
        logger.error(f"Ошибка формирования месячной статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def poll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки опроса"""
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    data = query.data
    
    if not data.startswith('poll:'):
        return
    
    # ✅ ИСПРАВЛЕНО: Извлекаем poll_id из callback_data
    data_parts = data.split(':')
    if len(data_parts) < 3:
        logger.error(f"Некорректный формат callback_data: {data}")
        return
    
    response = data_parts[1]
    poll_id = data_parts[2]  # ✅ Берём из callback_data, не генерируем!
    response_text = PollManager.RESPONSES.get(response, response)
    
    message = query.message
    chat_id = message.chat_id
    message_id = message.message_id
    
    # Извлекаем дату тренировки из текста сообщения
    text = message.text or message.caption or ""
    date_match = re.search(r'\(([^)]+)\)', text)
    training_date = date_match.group(1) if date_match else "Неизвестно"
    
    # Сохраняем ответ
    await DatabaseManager.save_response(
        poll_id=poll_id,
        user_id=user.id,
        username=user.username or '',
        full_name=user.full_name or '',
        response=response
    )
    
    # Уведомляем пользователя
    await query.edit_message_text(
        text=f"✅ Вы выбрали: <b>{response_text}</b>\nСпасибо за ответ!",
        parse_mode=ParseMode.HTML
    )
    
    # Обновляем опрос в группе
    await PollManager.update_poll_message(
        bot=context.bot,
        chat_id=chat_id,
        message_id=message_id,
        poll_id=poll_id,
        training_date=training_date
    )
    
    logger.info(f"Пользователь {user.username} выбрал: {response_text}")


# ============ АВТОМАТИЧЕСКИЕ ЗАДАЧИ ============

async def scheduled_poll(application: Application):
    """Автоматическое создание опроса каждый вторник"""
    try:
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
    except Exception as e:
        logger.error(f"Ошибка автоматического создания опроса: {e}")


async def scheduled_monthly_stats(application: Application):
    """Автоматическая отправка статистики в последний день месяца"""
    try:
        now = datetime.now(MSK)
        for admin_id in ADMIN_USER_IDS:
            try:
                df = await DatabaseManager.get_monthly_stats(now.year, now.month)
                
                if df.empty:
                    await application.bot.send_message(
                        chat_id=admin_id,
                        text=f"📭 Статистика за {now.strftime('%B %Y')} пуста."
                    )
                    continue
                
                filename = f"/tmp/stats_auto_{now.strftime('%Y%m')}.xlsx"
                with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                    df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
                    df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
                    response_map = {'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'}
                    df_display['Ответ'] = df_display['Ответ'].map(response_map)
                    df_display.to_excel(writer, sheet_name='Все ответы', index=False)
                    
                    summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
                    summary.to_excel(writer, sheet_name='Сводка', index=False)
                
                with open(filename, 'rb') as f:
                    await application.bot.send_document(
                        chat_id=admin_id,
                        document=f,
                        filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                        caption=f"📊 Автоматическая статистика за {now.strftime('%B %Y')}"
                    )
                
                os.remove(filename)
                logger.info(f"Статистика отправлена админу {admin_id}")
            except Exception as e:
                logger.error(f"Ошибка отправки статистики админу {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка автоматической отправки статистики: {e}")


def setup_scheduler(application: Application):
    """Настройка планировщика задач с правильным временем"""
    scheduler = AsyncIOScheduler()
    
    # Опрос каждый вторник в 10:30 по Москве (07:30 UTC)
    scheduler.add_job(
        scheduled_poll,
        trigger=CronTrigger(day_of_week='tue', hour=7, minute=30, timezone='UTC'),
        args=[application],
        id='weekly_poll',
        replace_existing=True
    )
    
    # Статистика в последний день месяца в 15:00 по Москве (12:00 UTC)
    scheduler.add_job(
        scheduled_monthly_stats,
        trigger=CronTrigger(day='last', hour=12, minute=0, timezone='UTC'),
        args=[application],
        id='monthly_stats',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Планировщик запущен (время UTC, опросы в 07:30 UTC = 10:30 MSK)")
    return scheduler


# ============ ОСНОВНАЯ ФУНКЦИЯ ============

async def main():
    """Главная функция"""
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
    application.add_handler(CallbackQueryHandler(poll_callback, pattern='^poll:'))
    
    scheduler = setup_scheduler(application)
    
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    logger.info(f"Бот запущен! Текущее время MSK: {datetime.now(MSK).strftime('%Y-%m-%d %H:%M:%S')}")
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
        scheduler.shutdown()
        await application.stop()


if __name__ == '__main__':
    asyncio.run(main())
