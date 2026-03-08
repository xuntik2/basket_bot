#!/usr/bin/env python3
"""
Telegram Bot для учета посещаемости тренировок
Версия 22.2 — Production Ready с полной интеграцией shutdown
ИЗМЕНЕНИЯ В ВЕРСИИ 22.2:
- Интегрирован shutdown flag (request_shutdown вызывается при graceful shutdown)
- Убран daemon=True из Flask thread
- Добавлен periodic health refresh task
- Правильные импорты из services.py и handlers.py
- Все улучшения v22.1 интегрированы
"""
from __future__ import annotations
import asyncio
import logging
import signal
import sys
from datetime import datetime, time
from threading import Thread
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from flask import Flask, jsonify
from supabase import create_client
from telegram import Update
from telegram.error import Conflict
from telegram.ext import Application, CallbackQueryHandler, ChatMemberHandler, CommandHandler

from services import (
    MSK,
    SHUTDOWN_TIMEOUT,
    BotConfig,
    BotMetrics,
    BotRuntimeState,
    DatabaseManager,
    PROFESSIONAL_HOLIDAYS,
    RateLimiter,
    error_handler,
    refresh_database_health,
    safe_execute,
    send_professional_holiday,
    periodic_health_refresh,
)
from handlers import (
    add_birthday_command,
    chat_member_handler,
    check_basketball_champions,
    help_command,
    monthly_stats_command,
    my_birthday_command,
    poll_callback,
    poll_command,
    run_daily_birthdays_with_guard,
    scheduled_cleanup_locks,
    scheduled_monthly_stats,
    scheduled_poll,
    set_birthday_command,
    start_command,
    stats_command,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def schedule_professional_holidays(scheduler: AsyncIOScheduler, application: Application) -> None:
    now = datetime.now(MSK).date()
    for key, holiday in PROFESSIONAL_HOLIDAYS.items():
        holiday_date = holiday['calc'](now.year)
        if holiday_date < now:
            holiday_date = holiday['calc'](now.year + 1)
        run_datetime = datetime.combine(holiday_date, time(hour=10, minute=0), tzinfo=MSK)
        scheduler.add_job(
            safe_execute,
            trigger=DateTrigger(run_date=run_datetime, timezone=MSK),
            id=f'holiday_{key}_{holiday_date.isoformat()}',
            args=[send_professional_holiday, application, holiday['name']],
            replace_existing=True,
        )
        logger.info('Запланирован праздник %s на %s', holiday['name'], holiday_date)


def setup_scheduler(application: Application, runtime_state: BotRuntimeState, rate_limiter: RateLimiter) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=MSK)
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day_of_week='mon', hour=10, minute=30, timezone=MSK),
        id='weekly_poll',
        args=[scheduled_poll, application],
        replace_existing=True,
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(day=1, hour=10, minute=0, timezone=MSK),
        id='monthly_stats',
        args=[scheduled_monthly_stats, application],
        replace_existing=True,
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='birthday_greetings',
        args=[run_daily_birthdays_with_guard, application],
        replace_existing=True,
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=10, minute=0, timezone=MSK),
        id='basketball_news',
        args=[check_basketball_champions, application],
        replace_existing=True,
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(hour=3, minute=0, timezone=MSK),
        id='cleanup_locks',
        args=[scheduled_cleanup_locks, application],
        replace_existing=True,
    )
    scheduler.add_job(
        safe_execute,
        trigger=CronTrigger(minute='*/10', timezone=MSK),
        id='db_health_refresh',
        args=[periodic_health_refresh, application, runtime_state, application.bot_data['db_manager']],
        replace_existing=True,
    )
    scheduler.add_job(
        rate_limiter.cleanup_old_users,
        trigger=CronTrigger(hour=4, minute=0, timezone=MSK),
        id='rate_limiter_cleanup',
        kwargs={'max_age_hours': 24},
        replace_existing=True,
    )
    schedule_professional_holidays(scheduler, application)
    scheduler.start()
    logger.info('Планировщик запущен')
    return scheduler


class GracefulShutdown:
    def __init__(self) -> None:
        self._shutdown_event = asyncio.Event()
        self._application: Optional[Application] = None
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._is_shutting_down = False
        self._flask_thread: Optional[Thread] = None

    def setup(self, application: Application, scheduler: AsyncIOScheduler, flask_thread: Thread = None) -> None:
        self._application = application
        self._scheduler = scheduler
        self._flask_thread = flask_thread
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._signal_handler)
            except ValueError:
                pass

    def _signal_handler(self, signum: int, frame) -> None:
        if self._is_shutting_down:
            sys.exit(1)
        self._is_shutting_down = True
        logger.info('Получен сигнал %s, начинаем graceful shutdown', signal.Signals(signum).name)
        asyncio.create_task(self._shutdown())

    async def _shutdown(self) -> None:
        try:
            await asyncio.wait_for(self._do_shutdown(), timeout=SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning('Graceful shutdown превысил таймаут')
        finally:
            self._shutdown_event.set()

    async def _do_shutdown(self) -> None:
        # 1. Сигнал shutdown всем потокам
        runtime_state = self._application.bot_data.get('runtime_state')
        if runtime_state:
            runtime_state.request_shutdown()
            logger.info("Shutdown signal sent to all threads")
        
        # 2. Остановка планировщика
        if self._scheduler:
            logger.info("Остановка планировщика...")
            self._scheduler.shutdown(wait=False)
        
        # 3. Остановка Telegram бота
        if self._application:
            try:
                if self._application.updater and self._application.updater.running:
                    await self._application.updater.stop()
                await self._application.stop()
                await self._application.shutdown()
                logger.info("Telegram бот остановлен")
            except Exception:
                logger.exception('Ошибка при остановке приложения')
        
        # 4. Ожидание Flask thread (не daemon!)
        if self._flask_thread and self._flask_thread.is_alive():
            logger.info("Ожидание остановки Flask...")
            self._flask_thread.join(timeout=5)
            if self._flask_thread.is_alive():
                logger.warning("Flask thread не остановился за 5 сек")

    async def wait(self) -> None:
        await self._shutdown_event.wait()


def create_flask_app(runtime_state: BotRuntimeState, metrics: BotMetrics) -> Flask:
    app = Flask(__name__)

    @app.route('/')
    def home():
        return f"Bot is running! {datetime.now(MSK).strftime('%Y-%m-%d %H:%M:%S MSK')}"

    @app.route('/health')
    def health():
        # Проверка shutdown флага
        if runtime_state.is_shutdown_requested():
            return jsonify({'status': 'shutting_down'}), 503
        
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now(MSK).isoformat(),
            'timezone': 'Europe/Moscow',
            'uptime': metrics.get_uptime_str(),
            'database': runtime_state.get_database_health(),
            'metrics': metrics.snapshot(),
        })

    @app.route('/metrics')
    def metrics_endpoint():
        if runtime_state.is_shutdown_requested():
            return jsonify({'status': 'shutting_down'}), 503
        
        return jsonify({
            'counters': metrics.snapshot(),
            'uptime': metrics.get_uptime_str(),
            'uptime_seconds': metrics.get_uptime().total_seconds(),
            'database': runtime_state.get_database_health(),
        })

    return app


def run_flask(port: int, runtime_state: BotRuntimeState, metrics: BotMetrics) -> None:
    """Запуск Flask сервера без daemon=True"""
    app = create_flask_app(runtime_state, metrics)
    app.run(host='0.0.0.0', port=port, use_reloader=False, threaded=True)


async def main() -> None:
    config = BotConfig()
    if not config.is_valid():
        logger.error('Не все переменные окружения настроены')
        return

    supabase_client = create_client(config.supabase_url, config.supabase_key)
    db_manager = DatabaseManager(supabase_client)
    rate_limiter = RateLimiter(calls=10, period=60)
    metrics = BotMetrics()
    runtime_state = BotRuntimeState()

    db_ok, db_message = await db_manager.check_database_health()
    runtime_state.set_database_health(db_ok, db_message)
    if not db_ok:
        logger.warning('Database health check failed: %s', db_message)

    await db_manager.init_tables()

    application = Application.builder().token(config.bot_token).build()
    application.bot_data.update({
        'config': config,
        'db_manager': db_manager,
        'rate_limiter': rate_limiter,
        'metrics': metrics,
        'runtime_state': runtime_state,
    })

    application.add_handler(CommandHandler('start', start_command))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('poll', poll_command))
    application.add_handler(CommandHandler('stats', stats_command))
    application.add_handler(CommandHandler('monthlystats', monthly_stats_command))
    application.add_handler(CommandHandler('addbirthday', add_birthday_command))
    application.add_handler(CommandHandler('setbirthday', set_birthday_command))
    application.add_handler(CommandHandler('mybirthday', my_birthday_command))
    application.add_handler(CallbackQueryHandler(poll_callback, pattern='^poll:'))
    application.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error_handler)

    scheduler = setup_scheduler(application, runtime_state, rate_limiter)
    
    # Запуск Flask в отдельном потоке (НЕ daemon=True!)
    flask_thread = Thread(
        target=run_flask,
        args=(config.port, runtime_state, metrics),
        daemon=False  # FIX: теперь не daemon, shutdown контролируется через runtime_state
    )
    flask_thread.start()
    logger.info(f'Flask запущен на порту {config.port}')

    shutdown_manager = GracefulShutdown()
    shutdown_manager.setup(application, scheduler, flask_thread)

    await application.initialize()
    await application.start()

    max_retries = 3
    retry_delay = 30
    for attempt in range(max_retries):
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(2)
            await application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
            logger.info('Polling запущен успешно')
            break
        except Conflict:
            if attempt < max_retries - 1:
                logger.warning('Conflict (%s/%s), ждём %s сек...', attempt + 1, max_retries, retry_delay)
                await asyncio.sleep(retry_delay)
            else:
                raise

    await run_daily_birthdays_with_guard(application)
    await refresh_database_health(runtime_state, db_manager)

    try:
        await shutdown_manager.wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info('Получен KeyboardInterrupt, завершение...')
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        try:
            if application.updater and application.updater.running:
                await application.updater.stop()
        except Exception:
            logger.exception('Ошибка остановки updater')
        await application.stop()
        await application.shutdown()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Бот остановлен пользователем')
    except Exception:
        logger.exception('Критическая ошибка')
        sys.exit(1)
