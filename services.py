#!/usr/bin/env python3
"""
Основные сервисы и бизнес-логика бота
Версия 22.2 — Production Ready с интегрированным shutdown flag
ИЗМЕНЕНИЯ В ВЕРСИИ 22.2:
- Интегрирован _shutdown_flag (request_shutdown / is_shutdown_requested)
- Добавлен retry для Supabase запросов (tenacity)
- Добавлены таймауты на БД операции (10 секунд)
- Periodic health refresh задача
- 3-state check_user_in_chat (True/False/None)
- validate_birth_date_dd_mm с поддержкой 29-02
- sanitize_html_summary для безопасного RSS
- poll_exists() и get_poll_training_date() helper методы
- upsert для save_poll с on_conflict='poll_id'
- run_daily_birthdays_with_guard с проверкой stats
"""
import os
import logging
import asyncio
import calendar
import hmac
import re
import feedparser
import html
import time
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Callable, Any, Tuple
from functools import wraps
from collections import defaultdict
from io import BytesIO
from zoneinfo import ZoneInfo
from threading import Lock, Event
from cachetools import TTLCache
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, ContextTypes
from telegram.constants import ParseMode
from telegram.error import Conflict, BadRequest, Forbidden
from supabase import create_client, Client
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
MAX_POLL_LOCKS_COUNT = 1000
CLEANUP_LOCKS_DAYS = 7
CACHE_TTL_SECONDS = 300
CACHE_MAX_SIZE = 500
SHUTDOWN_TIMEOUT = 30
JOB_RUNS_CLEANUP_DAYS = 30
DB_TIMEOUT_SECONDS = 10
DB_MAX_RETRIES = 3

# ==================== МЕТРИКИ ====================
class BotMetrics:
    """Потокобезопасные метрики"""
    def __init__(self):
        self._lock = Lock()
        self._counters: Dict[str, int] = {
            'polls_created': 0,
            'responses_saved': 0,
            'errors_count': 0,
            'birthdays_sent': 0,
            'news_sent': 0,
            'api_calls': 0,
        }
        self._start_time = datetime.now(MSK)

    async def increment(self, metric_name: str, value: int = 1) -> None:
        with self._lock:
            if metric_name in self._counters:
                self._counters[metric_name] += value

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def get_uptime(self) -> timedelta:
        return datetime.now(MSK) - self._start_time

    def get_uptime_str(self) -> str:
        uptime = self.get_uptime()
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"


class BotRuntimeState:
    """Синхронные снапшоты состояния для Flask + shutdown coordination"""
    def __init__(self):
        self._lock = Lock()
        self._db_status = {
            'status': 'unknown',
            'message': 'database check not run yet',
            'checked_at': None,
        }
        self._shutdown_flag = Event()

    def set_database_health(self, ok: bool, message: str) -> None:
        with self._lock:
            self._db_status = {
                'status': 'ok' if ok else 'error',
                'message': message,
                'checked_at': datetime.now(MSK).isoformat(),
            }

    def get_database_health(self) -> Dict[str, Optional[str]]:
        with self._lock:
            return dict(self._db_status)

    def request_shutdown(self) -> None:
        """Сигнал всем потокам на остановку"""
        self._shutdown_flag.set()
        logger.info("Shutdown signal sent to all threads")

    def is_shutdown_requested(self) -> bool:
        """Проверка флага shutdown"""
        return self._shutdown_flag.is_set()


async def refresh_database_health(state: BotRuntimeState, db_manager: 'DatabaseManager') -> None:
    ok, message = await db_manager.check_database_health()
    state.set_database_health(ok, message)


# ==================== КОНФИГУРАЦИЯ ====================
class BotConfig:
    """Инкапсуляция конфигурации"""
    def __init__(self):
        self.bot_token: str = os.environ.get('BOT_TOKEN', '')
        self.supabase_url: str = os.environ.get('SUPABASE_URL', '')
        self.supabase_key: str = os.environ.get('SUPABASE_KEY', '')
        admin_ids_str = os.environ.get('ADMIN_USER_IDS', '')
        self.admin_user_ids: List[int] = [
            int(x.strip()) for x in admin_ids_str.split(',') if x.strip()
        ]
        group_chat_id_str = os.environ.get('GROUP_CHAT_ID')
        try:
            self.group_chat_id: Optional[int] = int(group_chat_id_str) if group_chat_id_str else None
        except (ValueError, TypeError):
            self.group_chat_id = group_chat_id_str
            logger.warning(f"GROUP_CHAT_ID не конвертирован в int: {group_chat_id_str}")
        self.port: int = int(os.environ.get('PORT', 10000))
        self.owner_id: Optional[int] = self.admin_user_ids[0] if self.admin_user_ids else None
        self.birthday_image_path: str = os.environ.get('BIRTHDAY_IMAGE_PATH', 'birthday.jpg')
        enable_news = os.environ.get('ENABLE_BASKETBALL_NEWS', 'true').lower()
        self.enable_basketball_news: bool = enable_news in ('true', '1', 'yes')

    def is_valid(self) -> bool:
        return all([self.bot_token, self.supabase_url, self.supabase_key])


# ==================== ПРОФЕССИОНАЛЬНЫЕ ПРАЗДНИКИ ====================
PROFESSIONAL_HOLIDAYS = {
    'miner': {'calc': lambda y: _last_sunday(y, 8), 'name': 'Днем Шахтера'},
    'metallurgist': {'calc': lambda y: _third_sunday(y, 7), 'name': 'Днем Металлурга'},
    'energetic': {'calc': lambda y: date(y, 12, 22), 'name': 'Днем Энергетика'},
    'transport': {'calc': lambda y: _first_sunday(y, 11), 'name': 'Днем работника транспорта'},
}


def _last_sunday(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day)
    while last_date.weekday() != 6:
        last_date -= timedelta(days=1)
    return last_date.date()


def _third_sunday(year: int, month: int) -> date:
    first_day = datetime(year, month, 1)
    first_sunday = first_day + timedelta(days=(6 - first_day.weekday()) % 7)
    third_sunday = first_sunday + timedelta(days=14)
    return third_sunday.date()


def _first_sunday(year: int, month: int) -> date:
    first_day = datetime(year, month, 1)
    first_sunday = first_day + timedelta(days=(6 - first_day.weekday()) % 7)
    return first_sunday.date()


# ==================== RATE LIMITER ====================
class RateLimiter:
    """Rate limiter для защиты от спама"""
    def __init__(self, calls: int = 10, period: int = 60):
        self.calls = calls
        self.period = period
        self.user_calls: Dict[int, List[float]] = defaultdict(list)
        self._global_lock = asyncio.Lock()

    async def is_allowed(self, user_id: int) -> bool:
        async with self._global_lock:
            now = time.time()
            self.user_calls[user_id] = [
                t for t in self.user_calls[user_id] if now - t < self.period
            ]
            if len(self.user_calls[user_id]) >= self.calls:
                return False
            self.user_calls[user_id].append(now)
            return True

    async def cleanup_old_users(self, max_age_hours: int = 24) -> int:
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
    return hmac.compare_digest(
        secret1.encode('utf-8') if isinstance(secret1, str) else secret1,
        secret2.encode('utf-8') if isinstance(secret2, str) else secret2
    )


def validate_user_id(user_id: Any) -> int:
    if user_id is None:
        raise ValueError("user_id не может быть None")
    try:
        user_id_int = int(user_id)
    except (ValueError, TypeError) as e:
        raise ValueError(f"user_id должен быть числом: {e}")
    if user_id_int <= 0:
        raise ValueError("user_id должен быть положительным числом")
    return user_id_int


def validate_birth_date_dd_mm(birth_date: str) -> str:
    """Валидация даты рождения в формате ДД-ММ с поддержкой 29-02"""
    if not re.match(r'^\d{2}-\d{2}$', birth_date):
        raise ValueError("Дата должна быть в формате ДД-ММ")
    try:
        datetime.strptime(f"{birth_date}-2000", "%d-%m-%Y")
        return birth_date
    except ValueError as e:
        raise ValueError(f"Несуществующая дата: {birth_date}") from e


def truncate_text_safe(text: str, max_length: int = MAX_ERROR_TEXT_LENGTH) -> str:
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def sanitize_html_summary(text: str) -> str:
    """Очистка RSS summary для безопасной отправки с ParseMode.HTML"""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = html.escape(text)
    return text


def build_user_mention(user_id: Optional[int], full_name: str, username: str = "") -> str:
    safe_name = full_name or username or "Коллега"
    if username:
        username = username.lstrip("@")
        return f"@{username}"
    if user_id:
        return f'<a href="tg://user?id={user_id}">{safe_name}</a>'
    return safe_name


async def safe_execute(job_func: Callable, application: Application, *args, **kwargs):
    """Обёртка для безопасного выполнения задач"""
    try:
        await job_func(application, *args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        error_msg = f"⚠️ Ошибка в задаче {job_func.__name__}: {type(e).__name__}: {truncate_text_safe(str(e))}"
        logger.error(error_msg, exc_info=True)
        metrics = application.bot_data.get('metrics')
        if metrics:
            await metrics.increment('errors_count')
        config = application.bot_data.get('config')
        if config and config.owner_id:
            try:
                await application.bot.send_message(chat_id=config.owner_id, text=error_msg)
            except Exception as send_error:
                logger.debug(f"Не удалось отправить уведомление владельцу: {send_error}")


async def check_user_in_chat(bot: Bot, user_id: int, chat_id: Any) -> Optional[bool]:
    """
    Проверка, находится ли пользователь в чате.
    Returns:
        True — участник в чате
        False — точно left/kicked
        None — не удалось проверить (ошибка API/сети)
    """
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except (BadRequest, Forbidden) as e:
        logger.warning(f"Не удалось проверить участника {user_id}: {type(e).__name__}")
        return None
    except Exception as e:
        logger.warning(f"Ошибка проверки участника {user_id}: {e}")
        return None


async def notify_owner(bot: Bot, text: str, config: BotConfig) -> bool:
    if not config.owner_id:
        return False
    try:
        await bot.send_message(chat_id=config.owner_id, text=text)
        return True
    except Exception as e:
        logger.debug(f"Не удалось отправить уведомление владельцу: {e}")
        return False


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    error = context.error
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        return
    logger.error(msg="Exception while handling an update:", exc_info=error)
    metrics = context.application.bot_data.get('metrics')
    if metrics:
        await metrics.increment('errors_count')
    config = context.application.bot_data.get('config')
    if config and config.owner_id:
        try:
            await context.bot.send_message(
                chat_id=config.owner_id,
                text=f"❌ Ошибка в боте: {type(error).__name__}: {truncate_text_safe(str(error))}"
            )
        except Exception as send_error:
            logger.debug(f"Не удалось отправить уведомление об ошибке: {send_error}")


# ==================== DECORATORS ====================
def admin_required(func: Callable) -> Callable:
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


# ==================== БАЗА ДАННЫХ ====================
class DatabaseManager:
    """Управление базой данных Supabase с retry и таймаутами"""
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self._poll_locks: Dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()
        self._responses_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL_SECONDS)
        self._cache_lock = asyncio.Lock()

    async def _execute_with_retry(self, func: Callable, description: str = "DB operation") -> Any:
        """Выполнение запроса с retry и таймаутом"""
        @retry(
            stop=stop_after_attempt(DB_MAX_RETRIES),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(Exception),
            reraise=True
        )
        def _execute():
            return func()
        
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_execute),
                timeout=DB_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            logger.error(f"Timeout на {description} ({DB_TIMEOUT_SECONDS}s)")
            raise
        except Exception as e:
            logger.error(f"Ошибка после {DB_MAX_RETRIES} попыток ({description}): {e}")
            raise

    async def _get_poll_lock(self, poll_id: str) -> asyncio.Lock:
        async with self._locks_lock:
            if poll_id not in self._poll_locks:
                if len(self._poll_locks) >= MAX_POLL_LOCKS_COUNT:
                    await self._cleanup_locks_unsafe()
                self._poll_locks[poll_id] = asyncio.Lock()
            return self._poll_locks[poll_id]

    async def _cleanup_locks_unsafe(self) -> None:
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

    async def check_database_health(self) -> Tuple[bool, str]:
        try:
            await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('*').limit(1).execute(),
                "health check"
            )
            return True, "Database connection OK"
        except Exception as e:
            error_msg = truncate_text_safe(str(e))
            logger.error(f"Database health check failed: {error_msg}")
            return False, f"Database error: {error_msg}"

    async def init_tables(self) -> bool:
        try:
            await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('*').limit(1).execute(),
                "init tables"
            )
            logger.info("Таблицы инициализированы")
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки таблиц: {e}")
            return False

    async def poll_exists(self, poll_id: str) -> bool:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('poll_id')
                .eq('poll_id', poll_id).limit(1).execute(),
                "poll_exists"
            )
            return bool(result.data)
        except Exception as e:
            logger.error(f"Ошибка проверки poll_exists: {e}")
            return False

    async def get_poll_training_date(self, poll_id: str) -> Optional[str]:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('training_date')
                .eq('poll_id', poll_id).limit(1).execute(),
                "get_poll_training_date"
            )
            if result.data:
                return result.data[0].get('training_date')
            return None
        except Exception as e:
            logger.error(f"Ошибка получения даты опроса: {e}")
            return None

    async def save_poll(self, poll_id: str, message_id: int, chat_id: int,
                        training_date: str, created_at: datetime,
                        metrics: BotMetrics = None) -> bool:
        try:
            data = {
                'poll_id': poll_id,
                'message_id': message_id,
                'chat_id': chat_id,
                'training_date': training_date,
                'created_at': created_at.isoformat()
            }
            await self._execute_with_retry(
                lambda: self.supabase.table('polls').upsert(data, on_conflict='poll_id').execute(),
                "save_poll"
            )
            logger.info(f"Опрос {poll_id} сохранён (upsert)")
            if metrics:
                await metrics.increment('polls_created')
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения опроса: {e}")
            if metrics:
                await metrics.increment('errors_count')
            return False

    async def save_response(self, poll_id: str, user_id: int, username: str,
                            full_name: str, response: str,
                            metrics: BotMetrics = None) -> bool:
        poll_lock = await self._get_poll_lock(poll_id)
        async with poll_lock:
            try:
                existing = await self._execute_with_retry(
                    lambda: self.supabase.table('responses').select('*')
                    .eq('poll_id', poll_id).eq('user_id', user_id).execute(),
                    "check existing response"
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
                    await self._execute_with_retry(
                        lambda: self.supabase.table('responses').update(data)
                        .eq('poll_id', poll_id).eq('user_id', user_id).execute(),
                        "update response"
                    )
                else:
                    data['created_at'] = datetime.now(MSK).isoformat()
                    await self._execute_with_retry(
                        lambda: self.supabase.table('responses').insert(data).execute(),
                        "insert response"
                    )
                logger.info(f"Ответ сохранен: {username} - {response}")
                await self._invalidate_cache(poll_id)
                if metrics:
                    await metrics.increment('responses_saved')
                return True
            except Exception as e:
                logger.error(f"Ошибка сохранения ответа: {e}")
                if metrics:
                    await metrics.increment('errors_count')
                return False

    async def _invalidate_cache(self, poll_id: str) -> None:
        async with self._cache_lock:
            if poll_id in self._responses_cache:
                del self._responses_cache[poll_id]

    async def get_poll_responses(self, poll_id: str, use_cache: bool = True) -> List[Dict]:
        if use_cache:
            async with self._cache_lock:
                if poll_id in self._responses_cache:
                    return self._responses_cache[poll_id]
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('responses').select('*')
                .eq('poll_id', poll_id).execute(),
                "get_poll_responses"
            )
            data = result.data or []
            if use_cache:
                async with self._cache_lock:
                    self._responses_cache[poll_id] = data
            return data
        except Exception as e:
            logger.error(f"Ошибка получения ответов: {e}")
            return []

    async def cleanup_old_locks(self, days: int = CLEANUP_LOCKS_DAYS) -> int:
        try:
            async with self._locks_lock:
                await self._cleanup_locks_unsafe()
                return len(self._poll_locks)
        except Exception as e:
            logger.error(f"Ошибка очистки locks: {e}")
            return 0

    async def cleanup_cache(self) -> int:
        async with self._cache_lock:
            count = len(self._responses_cache)
            self._responses_cache.clear()
            return count

    async def cleanup_old_job_runs(self, days: int = JOB_RUNS_CLEANUP_DAYS) -> int:
        try:
            cutoff_date = datetime.now(MSK).date() - timedelta(days=days)
            result = await self._execute_with_retry(
                lambda: self.supabase.table('job_runs')
                .delete()
                .lt('run_date', cutoff_date.isoformat())
                .execute(),
                "cleanup_old_job_runs"
            )
            deleted_count = len(result.data) if result.data else 0
            logger.info(f"Очищено {deleted_count} старых записей job_runs")
            return deleted_count
        except Exception as e:
            logger.error(f"Ошибка очистки job_runs: {e}")
            return 0

    async def was_daily_job_done(self, job_name: str, run_date: str) -> bool:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('job_runs').select('*')
                .eq('job_name', job_name)
                .eq('run_date', run_date)
                .limit(1)
                .execute(),
                "was_daily_job_done"
            )
            return bool(result.data)
        except Exception as e:
            logger.error(f"Ошибка проверки job_runs: {e}")
            return False

    async def mark_daily_job_done(self, job_name: str, run_date: str) -> bool:
        try:
            await self._execute_with_retry(
                lambda: self.supabase.table('job_runs').insert({
                    'job_name': job_name,
                    'run_date': run_date,
                    'created_at': datetime.now(MSK).isoformat()
                }).execute(),
                "mark_daily_job_done"
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка записи job_runs: {e}")
            return False

    async def ensure_user_exists(self, user_id: int, username: str, full_name: str) -> bool:
        try:
            existing = await self._execute_with_retry(
                lambda: self.supabase.table('birthdays').select('*')
                .eq('user_id', user_id).execute(),
                "ensure_user_exists"
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
                await self._execute_with_retry(
                    lambda: self.supabase.table('birthdays').insert(data).execute(),
                    "insert user"
                )
                logger.info(f"Новый пользователь: {full_name}")
            else:
                logger.debug(f"Пользователь {user_id} уже существует")
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")
            return False

    async def add_or_update_birthday(self, user_id: int, full_name: str,
                                     birth_date: str, username: str = '') -> bool:
        user_id = validate_user_id(user_id)
        birth_date = validate_birth_date_dd_mm(birth_date)
        try:
            existing = await self._execute_with_retry(
                lambda: self.supabase.table('birthdays').select('*')
                .eq('user_id', user_id).execute(),
                "check birthday"
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
                await self._execute_with_retry(
                    lambda: self.supabase.table('birthdays').update(data)
                    .eq('user_id', user_id).execute(),
                    "update birthday"
                )
            else:
                data['created_at'] = datetime.now(MSK).isoformat()
                await self._execute_with_retry(
                    lambda: self.supabase.table('birthdays').insert(data).execute(),
                    "insert birthday"
                )
            logger.info(f"ДР обновлен: {full_name} - {birth_date}")
            return True
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Ошибка добавления ДР: {e}")
            raise

    async def get_monthly_stats(self, year: int, month: int, limit: int = 1000) -> pd.DataFrame:
        try:
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"
            polls_result = await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('*')
                .gte('training_date', start_date).lt('training_date', end_date)
                .limit(limit).execute(),
                "get_monthly_stats"
            )
            polls = polls_result.data or []
            if not polls:
                return pd.DataFrame()
            all_responses = []
            for poll in polls:
                responses = await self.get_poll_responses(poll['poll_id'], use_cache=False)
                for resp in responses:
                    resp['training_date'] = poll['training_date']
                    all_responses.append(resp)
            return pd.DataFrame(all_responses) if all_responses else pd.DataFrame()
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return pd.DataFrame()

    async def get_all_stats(self, limit: int = 5000) -> pd.DataFrame:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('responses').select('*').limit(limit).execute(),
                "get_all_stats"
            )
            data = result.data or []
            if not data:
                return pd.DataFrame()
            poll_ids = list(set([r['poll_id'] for r in data]))
            polls_result = await self._execute_with_retry(
                lambda: self.supabase.table('polls').select('poll_id, training_date')
                .in_('poll_id', poll_ids).execute(),
                "get polls for stats"
            )
            polls = {p['poll_id']: p['training_date'] for p in (polls_result.data or [])}
            for item in data:
                item['training_date'] = polls.get(item['poll_id'], 'Unknown')
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Ошибка получения полной статистики: {e}")
            return pd.DataFrame()

    async def get_today_birthdays(self) -> List[Dict]:
        try:
            today = datetime.now(MSK)
            today_str = f"{today.day:02d}-{today.month:02d}"
            result = await self._execute_with_retry(
                lambda: self.supabase.table('birthdays').select('*')
                .eq('birth_date', today_str).eq('is_active', True).execute(),
                "get_today_birthdays"
            )
            return result.data or []
        except Exception as e:
            logger.error(f"Ошибка получения именинников: {e}")
            return []

    async def get_user_birthday(self, user_id: int) -> Optional[str]:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('birthdays').select('*')
                .eq('user_id', user_id).limit(1).execute(),
                "get_user_birthday"
            )
            row = result.data[0] if result.data else None
            return row.get('birth_date') if row else None
        except Exception as e:
            logger.error(f"Ошибка получения ДР пользователя: {e}")
            return None

    async def mark_user_inactive(self, user_id: int) -> bool:
        try:
            await self._execute_with_retry(
                lambda: self.supabase.table('birthdays').update({
                    'is_active': False,
                    'updated_at': datetime.now(MSK).isoformat()
                }).eq('user_id', user_id).execute(),
                "mark_user_inactive"
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка деактивации пользователя {user_id}: {e}")
            return False

    async def check_news_sent(self, news_id: str) -> bool:
        try:
            result = await self._execute_with_retry(
                lambda: self.supabase.table('sports_news').select('*')
                .eq('news_id', news_id).execute(),
                "check_news_sent"
            )
            return bool(result.data)
        except Exception:
            return False

    async def mark_news_sent(self, news_id: str, title: str,
                             metrics: BotMetrics = None) -> bool:
        try:
            data = {
                'news_id': news_id,
                'title': title,
                'sent_at': datetime.now(MSK).isoformat()
            }
            await self._execute_with_retry(
                lambda: self.supabase.table('sports_news').insert(data).execute(),
                "mark_news_sent"
            )
            if metrics:
                await metrics.increment('news_sent')
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
            [InlineKeyboardButton(f"✅ {PollManager.RESPONSES['yes']}",
                                  callback_data=f'poll:yes:{poll_id}')],
            [InlineKeyboardButton(f"❌ {PollManager.RESPONSES['no']}",
                                  callback_data=f'poll:no:{poll_id}')],
            [InlineKeyboardButton(f"⏰ {PollManager.RESPONSES['later']}",
                                  callback_data=f'poll:later:{poll_id}')],
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    async def update_poll_message(bot: Bot, chat_id: int, message_id: int,
                                  poll_id: str, training_date: str,
                                  db_manager: DatabaseManager) -> bool:
        try:
            responses = await db_manager.get_poll_responses(poll_id)
            yes_count = len([r for r in responses if r['response'] == 'yes'])
            no_count = len([r for r in responses if r['response'] == 'no'])
            later_count = len([r for r in responses if r['response'] == 'later'])
            yes_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'yes']
            no_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'no']
            later_users = [r['full_name'] or r['username'] for r in responses if r['response'] == 'later']

            text = PollManager.create_poll_text(training_date)
            text += "\n\n📊 <b>Текущие результаты:</b>"
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
    output = BytesIO()
    df_display = df[['training_date', 'full_name', 'username', 'response', 'created_at']].copy()
    df_display.columns = ['Дата тренировки', 'Имя', 'Username', 'Ответ', 'Время ответа']
    df_display['Ответ'] = df_display['Ответ'].map({
        'yes': 'Да, иду',
        'no': 'Не смогу',
        'later': 'Отвечу завтра'
    })
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_display.to_excel(writer, sheet_name='Все ответы', index=False)
        summary = df[df['response'] == 'yes'].groupby('training_date').size().reset_index(name='Количество')
        summary.to_excel(writer, sheet_name='Сводка', index=False)
        if len(df) > 0:
            employee_stats = df[df['response'] == 'yes'].groupby(['full_name', 'username']).size().reset_index(name='Посещений')
            employee_stats.sort_values('Посещений', ascending=False).to_excel(
                writer, sheet_name='По сотрудникам', index=False
            )
    output.seek(0)
    return output


# ==================== ПРИВЕТСТВИЕ ====================
async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        full_name = user.full_name or user.first_name or "Коллега"
        welcome_text = (
            f"👋 <b>{full_name}</b>, добро пожаловать в наш чат!\n\n"
            f"📍 <b>Информация о тренировках:</b>\n"
            f"• Занятия проходят по <b>вторникам с 18:30 до 19:30</b>\n"
            f"• Для первой тренировки необходимо иметь с собой <b>паспорт РФ</b>\n\n"
            f"🏀 Рады видеть тебя в команде!\n"
            f"Задавай вопросы — коллеги с радостью тебя проконсультируют.\n\n"
            f"⚠️ <b>Важное правило:</b> корректное поведение на площадке."
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
        private_text = (
            f"Привет, {full_name}! 👋\n\n"
            f"Чтобы я мог автоматически поздравлять тебя с днём рождения, "
            f"отправь мне в личку команду:\n\n"
            f"/setbirthday ДД-ММ\n\n"
            f"Пример:\n"
            f"/setbirthday 15-03"
        )
        try:
            await context.bot.send_message(chat_id=user.id, text=private_text)
            logger.info(f"Отправлено личное приглашение указать ДР пользователю {user.id}")
        except Forbidden:
            logger.info(f"Нельзя написать пользователю {user.id} в личку")
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"ℹ️ {full_name}, чтобы я мог поздравлять тебя с днём рождения, "
                    f"открой личный чат со мной и отправь:\n"
                    f"/setbirthday ДД-ММ"
                )
            )
        logger.info(f"Приветствовали нового участника: {full_name}")
    except Exception as e:
        logger.error(f"Ошибка в welcome_new_member: {e}", exc_info=True)


# ==================== ПОЗДРАВЛЕНИЯ ====================
async def send_birthday_greetings(application: Application) -> Dict[str, int]:
    """
    Отправка поздравлений с днём рождения.
    Returns:
        Словарь со статистикой: {"attempted": n, "sent": m, "failed": k, "skipped": s}
    """
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    metrics = application.bot_data.get('metrics')

    stats = {"attempted": 0, "sent": 0, "failed": 0, "skipped": 0}

    if not config or not config.group_chat_id:
        logger.error("GROUP_CHAT_ID не настроен для поздравлений")
        return stats

    birthdays = await db_manager.get_today_birthdays()
    if not birthdays:
        logger.info("Сегодня именинников нет")
        return stats

    for person in birthdays:
        user_id = person.get('user_id')
        full_name = person.get('full_name') or 'Коллега'
        username = person.get('username') or ''
        mention = build_user_mention(user_id, full_name, username)

        stats["attempted"] += 1

        in_chat = await check_user_in_chat(application.bot, user_id, config.group_chat_id)
        
        if in_chat is False:
            await db_manager.mark_user_inactive(user_id)
            logger.info(f"Пользователь {user_id} покинул группу, деактивирован")
            stats["skipped"] += 1
            continue
        elif in_chat is None:
            logger.warning(f"Не удалось проверить участника {user_id}, пропускаем деактивацию")
            stats["skipped"] += 1
            continue

        text = (
            f"🎂 <b>Сегодня день рождения у {mention}!</b>\n\n"
            f"Поздравляем! Желаем крепкого здоровья, энергии, удачи, "
            f"отличного настроения и ярких побед — и в жизни, и на площадке! 🏀"
        )

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
            logger.info(f"Поздравили именинника: {full_name}")
            stats["sent"] += 1
            if metrics:
                await metrics.increment('birthdays_sent')
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Ошибка поздравления {full_name}: {e}", exc_info=True)
            stats["failed"] += 1
            if config.owner_id:
                await notify_owner(
                    application.bot,
                    f"❌ Ошибка поздравления {full_name}: {truncate_text_safe(str(e))}",
                    config
                )

    return stats


async def run_daily_birthdays_with_guard(application: Application) -> bool:
    """
    Безопасный ежедневный запуск поздравлений.
    job_runs ставится только при реальном успехе.
    Returns:
        True если задача выполнена успешно, False иначе
    """
    db_manager = application.bot_data.get('db_manager')
    today_str = datetime.now(MSK).strftime("%Y-%m-%d")
    job_name = "birthday_greetings"

    already_done = await db_manager.was_daily_job_done(job_name, today_str)
    if already_done:
        logger.info(f"Поздравления за {today_str} уже были отправлены")
        return True

    stats = await send_birthday_greetings(application)
    
    # Успех если: нет именинников ИЛИ были отправлены без ошибок
    success = (
        stats["attempted"] == 0 or 
        (stats["sent"] > 0 and stats["failed"] == 0)
    )
    
    # Особый случай: все skipped = проблема с API, НЕ помечаем как done
    if stats["attempted"] > 0 and stats["skipped"] == stats["attempted"]:
        logger.warning(f"Все именинники skipped — возможна проблема с API. job_runs не ставится")
        return False
    
    if success:
        await db_manager.mark_daily_job_done(job_name, today_str)
        logger.info(f"Поздравления за {today_str} отмечены как выполненные: {stats}")
        return True
    else:
        logger.warning(f"Поздравления выполнены с ошибками: {stats}, job_runs не ставится")
        return False


async def send_professional_holiday(application: Application, holiday_name: str):
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


# ==================== НОВОСТИ ====================
async def check_basketball_champions(application: Application):
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    metrics = application.bot_data.get('metrics')

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
                summary_raw = entry.get('summary', '')
                summary_clean = sanitize_html_summary(summary_raw)
                combined_text = title_lower + ' ' + summary_clean.lower()

                for league, keywords in leagues.items():
                    if any(kw in combined_text for kw in keywords):
                        news_id = entry.get('id', f"{league}_{published}_{entry.title[:50]}")
                        if await db_manager.check_news_sent(news_id):
                            logger.info(f"Новость уже отправлена: {news_id}")
                            continue

                        text = f"🏆 <b>{entry.title}</b>\n{truncate_text_safe(summary_clean)}\nПодробнее: {entry.link}"

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
                            await db_manager.mark_news_sent(news_id, entry.title, metrics)
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


# ==================== АВТОМАТИЧЕСКИЕ ЗАДАЧИ ====================
async def scheduled_cleanup_locks(application: Application):
    db_manager = application.bot_data.get('db_manager')
    rate_limiter = application.bot_data.get('rate_limiter')
    if db_manager:
        count = await db_manager.cleanup_old_locks()
        cache_count = await db_manager.cleanup_cache()
        job_runs_count = await db_manager.cleanup_old_job_runs()
        logger.info(f"Очистка завершена: locks={count}, cache={cache_count}, job_runs={job_runs_count}")
    if rate_limiter:
        cleaned = await rate_limiter.cleanup_old_users(max_age_hours=24)
        logger.info(f"Очищено {cleaned} старых записей rate limiter")


async def periodic_health_refresh(application: Application, runtime_state: BotRuntimeState, 
                                   db_manager: DatabaseManager) -> None:
    """
    Периодическое обновление статуса БД для Flask health endpoint.
    Работает пока не получен сигнал shutdown.
    """
    while not runtime_state.is_shutdown_requested():
        try:
            await refresh_database_health(runtime_state, db_manager)
        except Exception as e:
            logger.debug(f"Ошибка refresh health: {e}")
        await asyncio.sleep(30)
