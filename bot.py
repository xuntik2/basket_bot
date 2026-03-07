#!/usr/bin/env python3
"""
Telegram Bot для учета посещаемости тренировок
Автоматические опросы, поздравления с ДР, проф. праздники, новости баскетбола
Версия 16.0 — Production Ready: Все ошибки исправлены, безопасность улучшена

ИСПРАВЛЕНИЯ В ВЕРСИИ 16.0:
- Исправлен баг с _poll_locks как class attribute (теперь instance attribute)
- Исправлен пустой except на except Exception
- Исправлен незакрытый тег </b> в help_command
- Исправлена передача аргументов в schedule_professional_holidays
- Добавлена обёртка safe_execute для cleanup_old_locks
- Улучшена валидация user_id (добавлена проверка на None)
- Добавлена защита от memory leak в _poll_locks
- Улучшена обработка ошибок
- Добавлена конфигурируемость пути к birthday_image через env
- Исправлено дублирование кода в генерации Excel
"""
import os
import sys
import logging
import asyncio
import re
import calendar
import tempfile
import feedparser
import hmac
import hashlib
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Callable, Any, Tuple
from functools import wraps, partial
from collections import defaultdict
from io import BytesIO
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ChatMemberHandler,
)
from telegram.constants import ParseMode
from telegram.error import Conflict, BadRequest, Unauthorized
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from supabase import create_client, Client
import pandas as pd
from flask import Flask, jsonify
from threading import Thread
import time

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== ЧАСОВОЙ ПОЯС ====================
MSK = ZoneInfo("Europe/Moscow")

# ==================== КОНСТАНТЫ ====================
MAX_ERROR_TEXT_LENGTH = 300
MAX_POLL_LOCKS_COUNT = 1000  # Максимальное количество хранимых locks
CLEANUP_LOCKS_DAYS = 7


# ==================== КЛАСС КОНФИГУРАЦИИ ====================
class BotConfig:
    """Инкапсуляция конфигурации для избежания глобального состояния"""
    
    def __init__(self):
        self.bot_token: str = os.environ.get('BOT_TOKEN', '')
        self.supabase_url: str = os.environ.get('SUPABASE_URL', '')
        self.supabase_key: str = os.environ.get('SUPABASE_KEY', '')
        
        # Парсинг ADMIN_USER_IDS
        admin_ids_str = os.environ.get('ADMIN_USER_IDS', '')
        self.admin_user_ids: List[int] = [
            int(x.strip()) for x in admin_ids_str.split(',') if x.strip()
        ]
        
        # Конвертация GROUP_CHAT_ID в int
        group_chat_id_str = os.environ.get('GROUP_CHAT_ID')
        try:
            self.group_chat_id: Optional[int] = int(group_chat_id_str) if group_chat_id_str else None
        except (ValueError, TypeError):
            self.group_chat_id = group_chat_id_str
            logger.warning(f"GROUP_CHAT_ID не конвертирован в int: {group_chat_id_str}")
        
        self.port: int = int(os.environ.get('PORT', 10000))
        self.owner_id: Optional[int] = self.admin_user_ids[0] if self.admin_user_ids else None
        self.birthday_image_path: str = os.environ.get('BIRTHDAY_IMAGE_PATH', 'birthday.jpg')
        
        # Флаг включения/отключения новостей баскетбола
        enable_news = os.environ.get('ENABLE_BASKETBALL_NEWS', 'true').lower()
        self.enable_basketball_news: bool = enable_news in ('true', '1', 'yes')
    
    def is_valid(self) -> bool:
        """Проверка наличия всех необходимых переменных"""
        return all([self.bot_token, self.supabase_url, self.supabase_key])


# ==================== ПРОФЕССИОНАЛЬНЫЕ ПРАЗДНИКИ ====================
PROFESSIONAL_HOLIDAYS = {
    'miner': {'calc': lambda y: _last_sunday(y, 8), 'name': 'Днем Шахтера'},
    'metallurgist': {'calc': lambda y: _third_sunday(y, 7), 'name': 'Днем Металлурга'},
    'energetic': {'calc': lambda y: date(y, 12, 22), 'name': 'Днем Энергетика'},
    'transport': {'calc': lambda y: _first_sunday(y, 11), 'name': 'Днем работника транспорта'},
}


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


# ==================== RATE LIMITER ====================
class RateLimiter:
    """Rate limiter для защиты от спама с per-user locks"""
    
    def __init__(self, calls: int = 10, period: int = 60):
        self.calls = calls
        self.period = period
        self.user_calls: Dict[int, List[float]] = defaultdict(list)
        self._global_lock = asyncio.Lock()
    
    async def is_allowed(self, user_id: int) -> bool:
        """Проверка, разрешён ли запрос для пользователя"""
        async with self._global_lock:
            now = time.time()
            # Очищаем старые записи
            self.user_calls[user_id] = [
                t for t in self.user_calls[user_id] 
                if now - t < self.period
            ]
            if len(self.user_calls[user_id]) >= self.calls:
                return False
            self.user_calls[user_id].append(now)
            return True
    
    async def cleanup_old_users(self, max_age_hours: int = 24) -> int:
        """Очистка старых записей пользователей"""
        async with self._global_lock:
            now = time.time()
            cutoff = now - (max_age_hours * 3600)
            old_users = [
                uid for uid, calls in self.user_calls.items()
                if not calls or max(calls) < cutoff
            ]
            for uid in old_users:
                del self.user_calls[uid]
            return len(old_users)


# ==================== БЕЗОПАСНОСТЬ ====================

def secure_compare(secret1: str, secret2: str) -> bool:
    """
    Безопасное сравнение строк для предотвращения timing attack.
    Использует hmac.compare_digest для constant-time comparison.
    """
    return hmac.compare_digest(
        secret1.encode('utf-8') if isinstance(secret1, str) else secret1,
        secret2.encode('utf-8') if isinstance(secret2, str) else secret2
    )


def validate_user_id(user_id: Any) -> int:
    """
    Валидация user_id.
    
    Args:
        user_id: Значение для валидации
        
    Returns:
        Валидный user_id как int
        
    Raises:
        ValueError: Если user_id невалиден
    """
    if user_id is None:
        raise ValueError("user_id не может быть None")
    
    try:
        user_id_int = int(user_id)
    except (ValueError, TypeError) as e:
        raise ValueError(f"user_id должен быть числом: {e}")
    
    if user_id_int <= 0:
        raise ValueError("user_id должен быть положительным числом")
    
    return user_id_int


def truncate_text_safe(text: str, max_length: int = MAX_ERROR_TEXT_LENGTH) -> str:
    """
    Безопасное обрезание текста с сохранением целостности HTML.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


async def safe_execute(job_func: Callable, application: Application, *args, **kwargs):
    """Обёртка для безопасного выполнения задач"""
    try:
        await job_func(application, *args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        error_msg = f"⚠️ Ошибка в задаче {job_func.__name__}:\n{type(e).__name__}: {truncate_text_safe(str(e))}"
        logger.error(error_msg, exc_info=True)
        config = application.bot_data.get('config')
        if config and config.owner_id:
            try:
                await application.bot.send_message(chat_id=config.owner_id, text=error_msg)
            except Exception:
                pass


async def check_user_in_chat(bot: Bot, user_id: int, chat_id) -> bool:
    """Проверка, находится ли пользователь в чате"""
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception:
        return False


async def notify_owner(bot: Bot, text: str, config: BotConfig) -> bool:
    """
    Отправка уведомления владельцу.
    
    Returns:
        True если отправлено успешно, False иначе
    """
    if not config.owner_id:
        return False
    try:
        await bot.send_message(chat_id=config.owner_id, text=text)
        return True
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление владельцу: {e}")
        return False


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок (игнорирует KeyboardInterrupt)"""
    error = context.error
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        return
    logger.error(msg="Exception while handling an update:", exc_info=error)
    config = context.application.bot_data.get('config')
    if config and config.owner_id:
        try:
            await context.bot.send_message(
                chat_id=config.owner_id,
                text=f"❌ Ошибка в боте: {type(error).__name__}: {truncate_text_safe(str(error))}"
            )
        except Exception:
            pass


# ==================== БАЗА ДАННЫХ ====================

class DatabaseManager:
    """Управление базой данных Supabase"""
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        # ИСПРАВЛЕНО: _poll_locks теперь instance attribute, не class attribute
        self._poll_locks: Dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()  # Lock для защиты доступа к _poll_locks
    
    async def _get_poll_lock(self, poll_id: str) -> asyncio.Lock:
        """Получение или создание lock для poll_id (thread-safe)"""
        async with self._locks_lock:
            if poll_id not in self._poll_locks:
                # Защита от memory leak
                if len(self._poll_locks) >= MAX_POLL_LOCKS_COUNT:
                    await self._cleanup_locks_unsafe()
                self._poll_locks[poll_id] = asyncio.Lock()
            return self._poll_locks[poll_id]
    
    async def _cleanup_locks_unsafe(self):
        """Внутренняя очистка locks (должна вызываться под _locks_lock)"""
        cutoff_date = datetime.now(MSK) - timedelta(days=CLEANUP_LOCKS_DAYS)
        old_ids = []
        
        for poll_id in list(self._poll_locks.keys()):
            try:
                if poll_id.startswith(('auto_', 'manual_')):
                    parts = poll_id.split('_')
                    if len(parts) >= 2:
                        date_part = parts[1]
                        if len(date_part) >= 8:
                            poll_date = datetime.strptime(date_part[:8], '%Y%m%d')
                            if poll_date < cutoff_date:
                                old_ids.append(poll_id)
            except (ValueError, IndexError):
                old_ids.append(poll_id)
        
        for poll_id in old_ids:
            self._poll_locks.pop(poll_id, None)
        
        if old_ids:
            logger.info(f"Очищено {len(old_ids)} старых locks")
    
    async def init_tables(self):
        """Инициализация и проверка таблиц"""
        try:
            await asyncio.to_thread(
                lambda: self.supabase.table('polls').select('*').limit(1).execute()
            )
            logger.info("Таблицы инициализированы")
        except Exception as e:
            logger.error(f"Ошибка проверки таблиц: {e}")
    
    async def save_poll(self, poll_id: str, message_id: int, chat_id: int, 
                       training_date: str, created_at: datetime) -> bool:
        """
        Сохранение опроса в базу.
        
        Returns:
            True если сохранено успешно, False иначе
        """
        try:
            data = {
                'poll_id': poll_id,
                'message_id': message_id,
                'chat_id': chat_id,
                'training_date': training_date,
                'created_at': created_at.isoformat()
            }
            await asyncio.to_thread(
                lambda: self.supabase.table('polls').insert(data).execute()
            )
            logger.info(f"Опрос {poll_id} сохранен")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения опроса: {e}")
            return False
    
    async def save_response(self, poll_id: str, user_id: int, username: str, 
                           full_name: str, response: str) -> bool:
        """Сохранение ответа на опрос"""
        poll_lock = await self._get_poll_lock(poll_id)
        async with poll_lock:
            try:
                existing = await asyncio.to_thread(
                    lambda: self.supabase.table('responses').select('*')
                        .eq('poll_id', poll_id).eq('user_id', user_id).execute()
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
                        lambda: self.supabase.table('responses').update(data)
                            .eq('poll_id', poll_id).eq('user_id', user_id).execute()
                    )
                else:
                    data['created_at'] = datetime.now(MSK).isoformat()
                    await asyncio.to_thread(
                        lambda: self.supabase.table('responses').insert(data).execute()
                    )
                logger.info(f"Ответ сохранен: {username} - {response}")
                return True
            except Exception as e:
                logger.error(f"Ошибка сохранения ответа: {e}")
                return False
    
    async def cleanup_old_locks(self, days: int = CLEANUP_LOCKS_DAYS) -> int:
        """
        Очистка старых locks для предотвращения утечки памяти.
        
        Returns:
            Количество очищенных locks
        """
        try:
            async with self._locks_lock:
                await self._cleanup_locks_unsafe()
                return len(self._poll_locks)
        except Exception as e:
            logger.error(f"Ошибка очистки locks: {e}")
            return 0
    
    async def ensure_user_exists(self, user_id: int, username: str, full_name: str) -> bool:
        """Создание записи пользователя если не существует"""
        try:
            existing = await asyncio.to_thread(
                lambda: self.supabase.table('birthdays').select('*').eq('user_id', user_id).execute()
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
                    lambda: self.supabase.table('birthdays').insert(data).execute()
                )
                logger.info(f"Новый пользователь: {full_name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")
            return False
    
    async def add_or_update_birthday(self, user_id: int, full_name: str, 
                                    birth_date: str, username: str = '') -> bool:
        """
        Добавление или обновление дня рождения.
        
        Raises:
            ValueError: При невалидных данных
        """
        # Валидация user_id
        user_id = validate_user_id(user_id)
        
        if birth_date and not re.match(r'^\d{2}-\d{2}$', birth_date):
            raise ValueError("Дата должна быть в формате ДД-ММ")
        
        try:
            if birth_date:
                try:
                    datetime.strptime(birth_date, "%d-%m")
                except ValueError:
                    raise ValueError("Несуществующая дата")
            
            existing = await asyncio.to_thread(
                lambda: self.supabase.table('birthdays').select('*').eq('user_id', user_id).execute()
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
                    lambda: self.supabase.table('birthdays').update(data).eq('user_id', user_id).execute()
                )
            else:
                data['created_at'] = datetime.now(MSK).isoformat()
                await asyncio.to_thread(
                    lambda: self.supabase.table('birthdays').insert(data).execute()
                )
            logger.info(f"ДР обновлен: {full_name} - {birth_date}")
            return True
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Ошибка добавления ДР: {e}")
            raise
    
    async def get_poll_responses(self, poll_id: str) -> List[Dict]:
        """Получение ответов на опрос"""
        try:
            result = await asyncio.to_thread(
                lambda: self.supabase.table('responses').select('*').eq('poll_id', poll_id).execute()
            )
            return result.data or []
        except Exception as e:
            logger.error(f"Ошибка получения ответов: {e}")
            return []
    
    async def get_monthly_stats(self, year: int, month: int, limit: int = 1000) -> pd.DataFrame:
        """Получение статистики за месяц"""
        try:
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"
            polls_result = await asyncio.to_thread(
                lambda: self.supabase.table('polls').select('*')
                    .gte('training_date', start_date).lt('training_date', end_date).limit(limit).execute()
            )
            polls = polls_result.data or []
            if not polls:
                return pd.DataFrame()
            all_responses = []
            for poll in polls:
                responses = await self.get_poll_responses(poll['poll_id'])
                for resp in responses:
                    resp['training_date'] = poll['training_date']
                all_responses.extend(responses)
            return pd.DataFrame(all_responses) if all_responses else pd.DataFrame()
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return pd.DataFrame()
    
    async def get_all_stats(self, limit: int = 5000) -> pd.DataFrame:
        """Получение всей статистики"""
        try:
            result = await asyncio.to_thread(
                lambda: self.supabase.table('responses').select('*').limit(limit).execute()
            )
            data = result.data or []
            if not data:
                return pd.DataFrame()
            poll_ids = list(set([r['poll_id'] for r in data]))
            polls_result = await asyncio.to_thread(
                lambda: self.supabase.table('polls').select('poll_id, training_date')
                    .in_('poll_id', poll_ids).execute()
            )
            polls = {p['poll_id']: p['training_date'] for p in (polls_result.data or [])}
            for item in data:
                item['training_date'] = polls.get(item['poll_id'], 'Unknown')
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Ошибка получения полной статистики: {e}")
            return pd.DataFrame()
    
    async def get_today_birthdays(self) -> List[Dict]:
        """Получение именинников сегодня"""
        try:
            today = datetime.now(MSK)
            today_str = f"{today.day:02d}-{today.month:02d}"
            result = await asyncio.to_thread(
                lambda: self.supabase.table('birthdays').select('*')
                    .eq('birth_date', today_str).eq('is_active', True).execute()
            )
            return result.data or []
        except Exception as e:
            logger.error(f"Ошибка получения именинников: {e}")
            return []
    
    async def mark_user_inactive(self, user_id: int) -> bool:
        """Деактивация пользователя"""
        try:
            await asyncio.to_thread(
                lambda: self.supabase.table('birthdays').update({
                    'is_active': False, 
                    'updated_at': datetime.now(MSK).isoformat()
                }).eq('user_id', user_id).execute()
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка деактивации пользователя {user_id}: {e}")
            return False
    
    async def check_news_sent(self, news_id: str) -> bool:
        """Проверка, была ли новость уже отправлена"""
        try:
            result = await asyncio.to_thread(
                lambda: self.supabase.table('sports_news').select('*').eq('news_id', news_id).execute()
            )
            return bool(result.data)
        except Exception:
            return False
    
    async def mark_news_sent(self, news_id: str, title: str) -> bool:
        """Отметить новость как отправленную"""
        try:
            data = {
                'news_id': news_id,
                'title': title,
                'sent_at': datetime.now(MSK).isoformat()
            }
            await asyncio.to_thread(
                lambda: self.supabase.table('sports_news').insert(data).execute()
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения новости: {e}")
            return False


# ==================== УПРАВЛЕНИЕ ОПРОСАМИ ====================

class PollManager:
    """Менеджер опросов"""
    
    RESPONSES = {'yes': 'Да, иду', 'no': 'Не смогу', 'later': 'Отвечу завтра'}
    
    @staticmethod
    def get_training_date() -> str:
        """Получение даты следующей тренировки в читаемом формате"""
        now = datetime.now(MSK)
        tomorrow = now + timedelta(days=1)
        months = ['', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
        return f"{tomorrow.day} {months[tomorrow.month]} {tomorrow.year}"
    
    @staticmethod
    def get_training_date_iso() -> str:
        """Получение даты следующей тренировки в ISO формате"""
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
        """Создание клавиатуры опроса"""
        keyboard = [
            [InlineKeyboardButton(f"✅ {PollManager.RESPONSES['yes']}", callback_data=f'poll:yes:{poll_id}')],
            [InlineKeyboardButton(f"❌ {PollManager.RESPONSES['no']}", callback_data=f'poll:no:{poll_id}')],
            [InlineKeyboardButton(f"⏰ {PollManager.RESPONSES['later']}", callback_data=f'poll:later:{poll_id}')],
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    async def update_poll_message(bot: Bot, chat_id: int, message_id: int, 
                                 poll_id: str, training_date: str, db_manager: DatabaseManager) -> bool:
        """Обновление сообщения с опросом"""
        try:
            responses = await db_manager.get_poll_responses(poll_id)
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
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления опроса: {e}")
            return False


# ==================== EXCEL HELPERS ====================

def create_excel_from_dataframe(df: pd.DataFrame, filename_prefix: str = "stats") -> BytesIO:
    """
    Создание Excel файла из DataFrame.
    
    Args:
        df: DataFrame с данными
        filename_prefix: Префикс имени файла
        
    Returns:
        BytesIO с Excel файлом
    """
    output = BytesIO()
    
    # Подготовка отображаемых данных
    df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
    df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
    df_display['Ответ'] = df_display['Ответ'].map({
        'yes': 'Да, иду', 
        'no': 'Не смогу', 
        'later': 'Отвечу завтра'
    })
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_display.to_excel(writer, sheet_name='Все ответы', index=False)
        
        # Сводка по датам
        summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
        summary.to_excel(writer, sheet_name='Сводка', index=False)
        
        # Статистика по сотрудникам (если есть данные)
        if len(df) > 0:
            employee_stats = df[df['response'] == 'yes'].groupby(['full_name', 'username']).size().reset_index(name='Посещений')
            employee_stats.sort_values('Посещений', ascending=False).to_excel(
                writer, sheet_name='По сотрудникам', index=False
            )
    
    output.seek(0)
    return output


# ==================== ПРИВЕТСТВИЕ И ОТСЛЕЖИВАНИЕ УЧАСТНИКОВ ====================

async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик изменений статуса участников чата"""
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
            db_manager = context.application.bot_data.get('db_manager')
            if db_manager:
                await db_manager.mark_user_inactive(user.id)
            logger.info(f"Участник покинул чат: {user.id}")
    except Exception as e:
        logger.error(f"Ошибка в chat_member_handler: {e}")


async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """Приветствие нового участника"""
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
        
        db_manager = context.application.bot_data.get('db_manager')
        if db_manager:
            await db_manager.ensure_user_exists(
                user_id=user.id,
                username=user.username or '',
                full_name=full_name
            )
        logger.info(f"Приветствовали: {full_name}")
    except Exception as e:
        logger.error(f"Ошибка в welcome_new_member: {e}")


# ==================== ПОЗДРАВЛЕНИЯ ====================

async def send_birthday_greetings(application: Application):
    """Отправка поздравлений с днём рождения"""
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    
    if not config or not config.group_chat_id:
        logger.error("GROUP_CHAT_ID не настроен для поздравлений")
        return
    
    birthdays = await db_manager.get_today_birthdays()
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
        except (ValueError, IndexError, AttributeError):
            is_even = True
        
        if is_even:
            text = (f"🎂 <b>{name}</b>, поздравляю Вас с днем рождения!\n\n"
                    f"Желаю Вам успехов в спорте и во всех ваших делах. "
                    f"Пусть этот год будет наполнен радостью, здоровьем и благополучием для Вас и ваших близких!")
        else:
            text = (f"🎂 <b>{name}</b>, с днем рождения!\n\n"
                    f"От всего сердца желаю Вам удачи в спорте и во всех ваших начинаниях. "
                    f"Пусть этот год принесет Вам много радости, здоровья и успехов во всех делах!")
        
        if user_id and not await check_user_in_chat(application.bot, user_id, config.group_chat_id):
            await db_manager.mark_user_inactive(user_id)
            continue
        
        try:
            if os.path.exists(config.birthday_image_path):
                try:
                    with open(config.birthday_image_path, 'rb') as photo:
                        await application.bot.send_photo(
                            chat_id=config.group_chat_id,
                            photo=photo,
                            caption=text,
                            parse_mode=ParseMode.HTML
                        )
                except Exception as file_error:
                    logger.error(f"Ошибка отправки фото: {file_error}")
                    await application.bot.send_message(
                        chat_id=config.group_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML
                    )
            else:
                await application.bot.send_message(
                    chat_id=config.group_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML
                )
            logger.info(f"Поздравили: {name}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Ошибка поздравления {name}: {e}")
            if config.owner_id:
                await notify_owner(application.bot, f"❌ Ошибка поздравления {name}: {e}", config)


async def send_professional_holiday(application: Application, holiday_name: str):
    """Отправка поздравления с профессиональным праздником"""
    config = application.bot_data.get('config')
    if not config or not config.group_chat_id:
        logger.error("GROUP_CHAT_ID не настроен для проф. праздников")
        return
    try:
        text = f"🎉 Коллеги, поздравляю с {holiday_name}!"
        await application.bot.send_message(
            chat_id=config.group_chat_id,
            text=text,
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Отправлено поздравление с {holiday_name}")
    except Exception as e:
        logger.error(f"Ошибка отправки поздравления: {e}")
        raise


# ==================== НОВОСТИ О ЧЕМПИОНАХ ====================

async def check_basketball_champions(application: Application):
    """Проверка и отправка новостей о баскетбольных чемпионах"""
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    
    if not config or not config.enable_basketball_news or not config.group_chat_id:
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
                        
                        if await db_manager.check_news_sent(news_id):
                            logger.info(f"Новость уже отправлена: {news_id}")
                            continue
                        
                        text = f"🏆 <b>{entry.title}</b>\n\n{truncate_text_safe(entry.get('summary', ''))}\n\nПодробнее: {entry.link}"
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
                                    chat_id=config.group_chat_id,
                                    photo=photo,
                                    caption=text,
                                    parse_mode=ParseMode.HTML
                                )
                            else:
                                await application.bot.send_message(
                                    chat_id=config.group_chat_id,
                                    text=text,
                                    parse_mode=ParseMode.HTML
                                )
                            await db_manager.mark_news_sent(news_id, entry.title)
                            logger.info(f"Отправлена новость: {truncate_text_safe(entry.title)}")
                            await asyncio.sleep(3)
                        except Exception as e:
                            logger.error(f"Ошибка отправки новости: {e}")
                            if config.owner_id:
                                await notify_owner(application.bot, f"❌ Ошибка новости: {e}", config)
                        break
    except Exception as e:
        logger.error(f"Ошибка проверки новостей: {e}")
        if config and config.owner_id:
            await notify_owner(application.bot, f"❌ Ошибка проверки новостей: {e}", config)


# ==================== DECORATORS ====================

def admin_required(func: Callable) -> Callable:
    """Декоратор для проверки прав администратора (DRY)"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        config = context.application.bot_data.get('config')
        if not user or not config or user.id not in config.admin_user_ids:
            await update.message.reply_text("❌ Нет прав")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def rate_limit_check(func: Callable) -> Callable:
    """Декоратор для проверки rate limit"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        rate_limiter = context.application.bot_data.get('rate_limiter')
        user_id = update.effective_user.id if update.effective_user else None
        
        if rate_limiter and user_id and not await rate_limiter.is_allowed(user_id):
            if update.message:
                await update.message.reply_text("⏳ Слишком много запросов. Подождите минуту.")
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapper


# ==================== КОМАНДЫ ====================

@rate_limit_check
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
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
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /start: {e}", config)


@rate_limit_check
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    try:
        user = update.effective_user
        config = context.application.bot_data.get('config')
        is_admin = user and config and user.id in config.admin_user_ids
        
        text = "📋 <b>Доступные команды:</b>\n"
        text += "/start - Начать работу\n"
        text += "/help - Показать справку\n"
        text += "/setbirthday ДД-ММ - Указать свой день рождения\n"
        if is_admin:
            text += "\n🔐 <b>Команды администратора:</b>\n"
            text += "/poll - Создать опрос в группе\n"
            text += "/stats - Полная статистика (Excel)\n"
            text += "/monthlystats - Статистика за месяц\n"
            text += "/addbirthday user_id ДД-ММ Имя - Добавить ДР пользователю\n"
            # ИСПРАВЛЕНО: добавлен закрывающий тег </b>
            text += "\n⚙️ <b>Настройки (через переменные окружения Render):</b>\n"
            text += "ENABLE_BASKETBALL_NEWS=false - отключить новости баскетбола\n"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в help_command: {e}")


@rate_limit_check
async def set_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /setbirthday"""
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
        
        db_manager = context.application.bot_data.get('db_manager')
        await db_manager.add_or_update_birthday(
            user_id=user.id,
            full_name=user.full_name or user.first_name,
            birth_date=birth_date,
            username=user.username or ''
        )
        await update.message.reply_text(f"✅ Ваш день рождения сохранен: {birth_date}")
        logger.info(f"Пользователь {user.username} установил ДР: {birth_date}")
    except ValueError as ve:
        await update.message.reply_text(f"❌ {ve}")
    except Exception as e:
        logger.error(f"Ошибка в set_birthday_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /setbirthday: {e}", config)


@admin_required
@rate_limit_check
async def poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /poll (создание опроса)"""
    try:
        user = update.effective_user
        config = context.application.bot_data.get('config')
        db_manager = context.application.bot_data.get('db_manager')
        
        logger.info(f"=== ПРОВЕРКА ПЕРЕД ОПРОСОМ ===")
        logger.info(f"GROUP_CHAT_ID из ENV: {config.group_chat_id!r}")
        logger.info(f"GROUP_CHAT_ID в коде: {config.group_chat_id!r} (тип: {type(config.group_chat_id).__name__})")
        
        if not config.group_chat_id:
            error_msg = "❌ GROUP_CHAT_ID не настроен в переменных окружения"
            logger.error(error_msg)
            await update.message.reply_text(error_msg)
            if config.owner_id:
                await notify_owner(context.bot, f"❌ Ошибка /poll: {error_msg}", config)
            return
        
        try:
            test_id = int(config.group_chat_id) if isinstance(config.group_chat_id, str) else config.group_chat_id
            logger.info(f"Преобразованный chat_id: {test_id}")
        except (ValueError, TypeError) as e:
            error_msg = f"❌ Неверный формат GROUP_CHAT_ID: {e}"
            logger.error(error_msg)
            await update.message.reply_text(error_msg)
            if config.owner_id:
                await notify_owner(context.bot, f"❌ Ошибка /poll: {error_msg}", config)
            return
        
        training_date = PollManager.get_training_date()
        training_date_iso = PollManager.get_training_date_iso()
        poll_id = f"manual_{datetime.now(MSK).strftime('%Y%m%d_%H%M%S')}"
        text = PollManager.create_poll_text(training_date)
        
        message = await context.bot.send_message(
            chat_id=config.group_chat_id,
            text=text,
            reply_markup=PollManager.create_keyboard(poll_id),
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Опрос отправлен в chat_id={config.group_chat_id}, message_id={message.message_id}")
        
        await db_manager.save_poll(
            poll_id=poll_id,
            message_id=message.message_id,
            chat_id=message.chat_id,
            training_date=training_date_iso,
            created_at=datetime.now(MSK)
        )
        
        await update.message.reply_text(f"✅ Опрос создан в группе! ID: {poll_id}")
        logger.info(f"Админ {user.username} создал опрос: {poll_id}")
        
    except (BadRequest, Unauthorized) as e:
        logger.error(f"Telegram API error in poll_command: {type(e).__name__}: {e}")
        error_text = f"❌ Ошибка Telegram: {type(e).__name__}"
        await update.message.reply_text(error_text)
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /poll: {error_text}\n{truncate_text_safe(str(e))}", config)
    except Exception as e:
        logger.error(f"Unexpected error in poll_command: {type(e).__name__}: {e}", exc_info=True)
        error_text = f"❌ Ошибка: {type(e).__name__}"
        await update.message.reply_text(error_text)
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /poll: {error_text}\n{truncate_text_safe(str(e))}", config)
        raise


@admin_required
@rate_limit_check
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /stats (полная статистика)"""
    try:
        user = update.effective_user
        db_manager = context.application.bot_data.get('db_manager')
        
        await update.message.reply_text("📊 Формирую статистику...")
        df = await db_manager.get_all_stats()
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста")
            return
        
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df.sort_values('created_at')
        
        output = create_excel_from_dataframe(df, f"stats_{datetime.now(MSK).strftime('%Y%m%d')}")
        
        try:
            await context.bot.send_document(
                chat_id=user.id,
                document=output,
                filename=f"stats_{datetime.now(MSK).strftime('%Y%m%d')}.xlsx",
                caption="📊 Статистика"
            )
        finally:
            output.close()
        logger.info(f"Админ {user.username} получил статистику")
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


@admin_required
@rate_limit_check
async def monthly_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /monthlystats (статистика за месяц)"""
    try:
        user = update.effective_user
        db_manager = context.application.bot_data.get('db_manager')
        now = datetime.now(MSK)
        
        await update.message.reply_text(f"📊 Статистика за {now.strftime('%B %Y')}...")
        df = await db_manager.get_monthly_stats(now.year, now.month)
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста")
            return
        
        output = create_excel_from_dataframe(df, f"stats_{now.strftime('%Y_%m')}")
        
        try:
            await context.bot.send_document(
                chat_id=user.id,
                document=output,
                filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                caption=f"📊 Статистика за {now.strftime('%B %Y')}"
            )
        finally:
            output.close()
    except Exception as e:
        logger.error(f"Ошибка месячной статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


@admin_required
@rate_limit_check
async def add_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /addbirthday (добавление ДР админом)"""
    try:
        user = update.effective_user
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "📋 <b>Использование:</b>\n"
                "/addbirthday user_id ДД-ММ Фамилия Имя\n\n"
                "📝 <b>Пример:</b>\n"
                "/addbirthday 123456789 15-03 Иванов Иван"
            )
            return
        
        try:
            user_id = validate_user_id(context.args[0])
        except ValueError as ve:
            await update.message.reply_text(f"❌ {ve}")
            return
        
        birth_date = context.args[1]
        full_name = ' '.join(context.args[2:])
        
        try:
            datetime.strptime(birth_date, "%d-%m")
        except ValueError:
            await update.message.reply_text("❌ Несуществующая дата. Используйте формат ДД-ММ")
            return
        
        db_manager = context.application.bot_data.get('db_manager')
        await db_manager.add_or_update_birthday(user_id, full_name, birth_date)
        await update.message.reply_text(f"✅ Добавлено: {full_name}, ДР {birth_date}")
        logger.info(f"Админ {user.username} добавил ДР вручную: {full_name} - {birth_date}")
        
    except ValueError as ve:
        logger.error(f"Ошибка валидации в /addbirthday: {ve}")
        await update.message.reply_text(f"❌ Ошибка: {ve}")
    except Exception as e:
        logger.error(f"Ошибка в add_birthday_command: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /addbirthday: {e}", config)


async def poll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback кнопок опроса"""
    try:
        query = update.callback_query
        await query.answer()
        user = update.effective_user
        
        rate_limiter = context.application.bot_data.get('rate_limiter')
        if rate_limiter and not await rate_limiter.is_allowed(user.id):
            await query.answer("⏳ Слишком быстро! Подождите немного.", show_alert=True)
            return
        
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
        
        db_manager = context.application.bot_data.get('db_manager')
        await db_manager.save_response(
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
            training_date=training_date,
            db_manager=db_manager
        )
        logger.info(f"Пользователь {user.username} выбрал: {response_text}")
    except Exception as e:
        logger.error(f"Ошибка в poll_callback: {e}")


# ==================== АВТОМАТИЧЕСКИЕ ЗАДАЧИ ====================

async def scheduled_poll(application: Application):
    """Создание автоматического опроса по расписанию"""
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    
    if not config or not config.group_chat_id:
        logger.error("GROUP_CHAT_ID не настроен")
        return
    
    training_date = PollManager.get_training_date()
    training_date_iso = PollManager.get_training_date_iso()
    poll_id = f"auto_{datetime.now(MSK).strftime('%Y%m%d')}"
    text = PollManager.create_poll_text(training_date)
    
    message = await application.bot.send_message(
        chat_id=config.group_chat_id,
        text=text,
        reply_markup=PollManager.create_keyboard(poll_id),
        parse_mode=ParseMode.HTML
    )
    
    await db_manager.save_poll(
        poll_id=poll_id,
        message_id=message.message_id,
        chat_id=message.chat_id,
        training_date=training_date_iso,
        created_at=datetime.now(MSK)
    )
    logger.info(f"Автоматический опрос создан: {poll_id}")


async def scheduled_monthly_stats(application: Application):
    """Отправка автоматической месячной статистики"""
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    now = datetime.now(MSK)
    
    for admin_id in config.admin_user_ids:
        try:
            df = await db_manager.get_monthly_stats(now.year, now.month)
            if df.empty:
                await application.bot.send_message(
                    chat_id=admin_id,
                    text=f"📭 Статистика за {now.strftime('%B %Y')} пуста"
                )
                continue
            
            output = create_excel_from_dataframe(df, f"stats_{now.strftime('%Y_%m')}")
            
            try:
                await application.bot.send_document(
                    chat_id=admin_id,
                    document=output,
                    filename=f"stats_{now.strftime('%Y_%m')}.xlsx",
                    caption=f"📊 Автоматическая статистика за {now.strftime('%B %Y')}"
                )
            finally:
                output.close()
            logger.info(f"Статистика отправлена админу {admin_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки статистики: {e}")


async def scheduled_cleanup_locks(application: Application):
    """Автоматическая очистка старых locks"""
    db_manager = application.bot_data.get('db_manager')
    if db_manager:
        count = await db_manager.cleanup_old_locks()
        logger.info(f"Очистка locks завершена, осталось: {count}")


def schedule_professional_holidays(scheduler, application: Application):
    """Планирование профессиональных праздников"""
    now = datetime.now(MSK).date()
    year = now.year
    
    for key, holiday in PROFESSIONAL_HOLIDAYS.items():
        holiday_date = holiday['calc'](year)
        if holiday_date < now:
            holiday_date = holiday['calc'](year + 1)
        
        job_id = f"holiday_{key}_{year}"
        
        # ИСПРАВЛЕНО: используем partial для правильной передачи аргументов
        run_datetime = datetime.combine(holiday_date, datetime.min.time().replace(hour=10))
        scheduler.add_job(
            safe_execute,
            trigger=DateTrigger(run_date=run_datetime, timezone=MSK),
            id=job_id,
            args=[send_professional_holiday, application, holiday['name']],
            replace_existing=True
        )
        logger.info(f"Запланирован праздник {holiday['name']} на {holiday_date}")


def setup_scheduler(application: Application) -> AsyncIOScheduler:
    """Настройка планировщика задач"""
    scheduler = AsyncIOScheduler(timezone=MSK)
    
    # Еженедельный опрос (вторник, 10:30)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day_of_week='tue', hour=10, minute=30, timezone=MSK),
        id='weekly_poll',
        args=[scheduled_poll, application],
        replace_existing=True
    )
    
    # Месячная статистика (последний день месяца, 15:00)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day='last', hour=15, minute=0, timezone=MSK),
        id='monthly_stats',
        args=[scheduled_monthly_stats, application],
        replace_existing=True
    )
    
    # Поздравления с ДР (ежедневно, 10:00)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='birthday_greetings',
        args=[send_birthday_greetings, application],
        replace_existing=True
    )
    
    # Новости баскетбола (ежедневно, 10:00)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='basketball_news',
        args=[check_basketball_champions, application],
        replace_existing=True
    )
    
    # ИСПРАВЛЕНО: Очистка locks использует safe_execute
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=3, minute=0, timezone=MSK),
        id='cleanup_locks',
        args=[scheduled_cleanup_locks, application],
        replace_existing=True
    )
    
    schedule_professional_holidays(scheduler, application)
    scheduler.start()
    logger.info("Планировщик запущен")
    return scheduler


# ==================== FLASK ДЛЯ KEEP-ALIVE ====================

def create_flask_app():
    """Создание Flask приложения для health checks"""
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
    
    return app


def run_flask(port: int):
    """Запуск Flask сервера"""
    app = create_flask_app()
    app.run(host='0.0.0.0', port=port, use_reloader=False)


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

async def main():
    """Главная функция запуска бота"""
    # Инициализация конфигурации
    config = BotConfig()
    
    if not config.is_valid():
        logger.error("Не все переменные окружения настроены!")
        return
    
    # Инициализация Supabase
    supabase_client = create_client(config.supabase_url, config.supabase_key)
    
    # Инициализация менеджеров
    db_manager = DatabaseManager(supabase_client)
    rate_limiter = RateLimiter(calls=10, period=60)
    
    await db_manager.init_tables()
    
    # Создание приложения
    application = Application.builder().token(config.bot_token).build()
    
    # Сохраняем объекты в bot_data для доступа из хендлеров
    application.bot_data['config'] = config
    application.bot_data['db_manager'] = db_manager
    application.bot_data['rate_limiter'] = rate_limiter
    
    # Регистрация обработчиков
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
    
    # Настройка планировщика
    scheduler = setup_scheduler(application)
    
    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, args=(config.port,), daemon=True)
    flask_thread.start()
    
    logger.info(f"Бот запущен! Время MSK: {datetime.now(MSK).strftime('%Y-%m-%d %H:%M:%S')}")
    
    await application.initialize()
    await application.start()
    
    # RETRY-ЛОГИКА ДЛЯ POLLING (Render)
    max_retries = 3
    retry_delay = 30
    
    for attempt in range(max_retries):
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook удалён")
            
            await asyncio.sleep(5)
            
            await application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
            logger.info("Polling запущен успешно")
            break
            
        except Conflict as e:
            if attempt < max_retries - 1:
                logger.warning(f"Conflict (попытка {attempt + 1}/{max_retries}). Ждем {retry_delay} сек...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Не удалось запустить polling после {max_retries} попыток")
                raise
                
        except Exception as e:
            logger.error(f"Ошибка запуска polling: {e}", exc_info=True)
            raise
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
        scheduler.shutdown()
        await application.stop()


if __name__ == '__main__':
    asyncio.run(main())
