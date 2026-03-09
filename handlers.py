#!/usr/bin/env python3
"""
Обработчики команд бота
Версия 25.2 — Production Ready
ИСПРАВЛЕНИЯ В ВЕРСИИ 25.2:
- ✅ Добавлен validate_birth_date_dd_mm в импорты
- ✅ Добавлен new_member_handler
- ✅ Добавлен rollback в poll_command
- ✅ Импортирован welcome_new_member из services
- ✅ Импортированы scheduled_* функции из services
- ✅ Импортированы MessageHandler, filters
- ✅ Импортирован should_skip_welcome
"""
from __future__ import annotations
import asyncio
import logging
import re
from datetime import datetime
from typing import Optional
import pandas as pd
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, ContextTypes, MessageHandler, filters
from services import (
    MSK,
    PollManager,
    create_excel_from_dataframe,
    notify_owner,
    truncate_text_safe,
    build_user_mention,
    check_user_in_chat,
    validate_user_id,
    validate_birth_date_dd_mm,
    admin_required,
    rate_limit_check,
    chat_member_handler,
    run_daily_birthdays_with_guard,
    check_basketball_champions,
    scheduled_cleanup_locks,
    scheduled_health_refresh,
    should_skip_welcome,
    welcome_new_member,
)
from services import DatabaseManager

logger = logging.getLogger(__name__)


# ==================== КОМАНДЫ ====================
@rate_limit_check
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        await update.message.reply_text(
            f"👋 Привет, {user.first_name}!\n"
            f"Я бот для учета посещаемости тренировок.\n"
            f"Каждый понедельник создаю опрос о посещении.\n"
            f"/help - Помощь"
        )
    except Exception as e:
        logger.error(f"Ошибка в start_command: {e}")
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(context.bot, f"❌ Ошибка в /start: {e}", config)


@rate_limit_check
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        config = context.application.bot_data.get('config')
        is_admin = user and config and user.id in config.admin_user_ids
        text = "📋 <b>Доступные команды:</b>\n"
        text += "/start - Начать работу\n"
        text += "/help - Показать справку\n"
        text += "/setbirthday ДД-ММ - Указать свой день рождения\n"
        text += "/mybirthday - Посмотреть свой день рождения\n"
        if is_admin:
            text += "\n🔐 <b>Команды администратора:</b>\n"
            text += "/poll - Создать опрос в группе\n"
            text += "/stats - Полная статистика (Excel)\n"
            text += "/monthlystats - Статистика за месяц\n"
            text += "/addbirthday user_id ДД-ММ Имя - Добавить ДР пользователю\n"
            text += "\n⚙️ <b>Настройки (через переменные окружения Render):\n</b>"
            text += "ENABLE_BASKETBALL_NEWS=false - отключить новости баскетбола\n"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в help_command: {e}")


@rate_limit_check
async def set_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "📋 <b>Использование:</b>\n"
                "/setbirthday ДД-ММ\n"
                "📝 <b>Пример:</b>\n"
                "/setbirthday 15-03"
            )
            return
        birth_date = context.args[0]
        try:
            validate_birth_date_dd_mm(birth_date)
        except ValueError as ve:
            await update.message.reply_text(f"❌ {ve}")
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


@rate_limit_check
async def my_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        db_manager = context.application.bot_data.get('db_manager')
        birth_date = await db_manager.get_user_birthday(user.id)
        if not birth_date:
            await update.message.reply_text(
                "📭 У вас пока не сохранена дата рождения.\n"
                "Укажите её так:\n"
                "/setbirthday ДД-ММ"
            )
            return
        await update.message.reply_text(f"🎂 Ваша дата рождения: {birth_date}")
    except Exception as e:
        logger.error(f"Ошибка в my_birthday_command: {e}", exc_info=True)
        await update.message.reply_text("❌ Не удалось получить дату рождения")


@admin_required
@rate_limit_check
async def poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ FIX v25.2: Добавлен rollback при ошибке БД"""
    try:
        user = update.effective_user
        chat = update.effective_chat
        config = context.application.bot_data.get('config')
        db_manager = context.application.bot_data.get('db_manager')
        metrics = context.application.bot_data.get('metrics')
        if not chat:
            await update.message.reply_text("❌ Не удалось определить чат")
            return
        if chat.type not in ("group", "supergroup"):
            await update.message.reply_text(
                "❌ Команду /poll нужно запускать прямо в группе.\n"
                "В личном чате опрос не создаётся."
            )
            return
        target_chat_id = chat.id
        training_date = PollManager.get_training_date()
        training_date_iso = PollManager.get_training_date_iso()
        poll_id = f"manual_{datetime.now(MSK).strftime('%Y%m%d_%H%M%S')}"
        text = PollManager.create_poll_text(training_date)
        message = await context.bot.send_message(
            chat_id=target_chat_id,
            text=text,
            reply_markup=PollManager.create_keyboard(poll_id),
            parse_mode=ParseMode.HTML
        )
        
        saved = await db_manager.save_poll(
            poll_id=poll_id,
            message_id=message.message_id,
            chat_id=message.chat_id,
            training_date=training_date_iso,
            created_at=datetime.now(MSK),
            metrics=metrics
        )

        if not saved:
            logger.error(f"Не удалось сохранить ручной опрос {poll_id} в БД, удаляем сообщение")
            try:
                await context.bot.delete_message(
                    chat_id=target_chat_id,
                    message_id=message.message_id
                )
            except Exception as delete_error:
                logger.warning(f"Не удалось удалить ручной опрос после ошибки БД: {delete_error}")
            await update.message.reply_text("❌ Не удалось сохранить опрос в БД")
            return

        logger.info(
            f"Ручной опрос создан админом {user.username or user.id}: "
            f"poll_id={poll_id}, chat_id={target_chat_id}"
        )
    except Exception as e:
        logger.error(f"Ошибка в poll_command: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка создания опроса: {type(e).__name__}")
        config = context.application.bot_data.get('config')
        if config and config.owner_id:
            await notify_owner(
                context.bot,
                f"❌ Ошибка в /poll: {type(e).__name__}: {truncate_text_safe(str(e))}",
                config
            )


@admin_required
@rate_limit_check
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        user = update.effective_user
        db_manager = context.application.bot_data.get('db_manager')
        now = datetime.now(MSK)
        first_of_month = now.day == 1 and now.hour < 12
        if first_of_month:
            if now.month == 1:
                year, month = now.year - 1, 12
            else:
                year, month = now.year, now.month - 1
        else:
            year, month = now.year, now.month
        month_name = datetime(year, month, 1).strftime('%B %Y')
        await update.message.reply_text(f"📊 Статистика за {month_name}...")
        df = await db_manager.get_monthly_stats(year, month)
        if df.empty:
            await update.message.reply_text("📭 Статистика пуста")
            return
        output = create_excel_from_dataframe(df, f"stats_{year}_{month:02d}")
        try:
            await context.bot.send_document(
                chat_id=user.id,
                document=output,
                filename=f"stats_{year}_{month:02d}.xlsx",
                caption=f"📊 Статистика за {month_name}"
            )
        finally:
            output.close()
    except Exception as e:
        logger.error(f"Ошибка месячной статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


@admin_required
@rate_limit_check
async def add_birthday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "📋 <b>Использование:</b>\n"
                "/addbirthday user_id ДД-ММ Фамилия Имя\n"
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
            validate_birth_date_dd_mm(birth_date)
        except ValueError as ve:
            await update.message.reply_text(f"❌ {ve}")
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
    try:
        query = update.callback_query
        if not query:
            return
        user = update.effective_user
        if not user:
            await query.answer("❌ Пользователь не определён", show_alert=True)
            return
        rate_limiter = context.application.bot_data.get('rate_limiter')
        if rate_limiter and not await rate_limiter.is_allowed(user.id):
            await query.answer("⏳ Слишком быстро. Подождите немного.", show_alert=True)
            return
        data = query.data or ""
        if not data.startswith("poll:"):
            await query.answer()
            return
        parts = data.split(":")
        if len(parts) != 3:
            logger.error(f"Некорректный callback_data: {data}")
            await query.answer("❌ Ошибка данных опроса", show_alert=True)
            return
        response = parts[1]
        poll_id = parts[2]
        if response not in PollManager.RESPONSES:
            await query.answer("❌ Неизвестный вариант ответа", show_alert=True)
            return
        message = query.message
        if not message:
            await query.answer("❌ Сообщение опроса не найдено", show_alert=True)
            return
        db_manager = context.application.bot_data.get('db_manager')
        metrics = context.application.bot_data.get('metrics')
        saved = await db_manager.save_response(
            poll_id=poll_id,
            user_id=user.id,
            username=user.username or '',
            full_name=user.full_name or user.first_name or '',
            response=response,
            metrics=metrics
        )
        if not saved:
            await query.answer("❌ Не удалось сохранить ответ", show_alert=True)
            return
        training_date = await db_manager.get_poll_training_date(poll_id)
        if not training_date:
            training_date = "Неизвестно"
        await query.answer(f"✅ Ваш ответ: {PollManager.RESPONSES[response]}")
        await PollManager.update_poll_message(
            bot=context.bot,
            chat_id=message.chat_id,
            message_id=message.message_id,
            poll_id=poll_id,
            training_date=training_date,
            db_manager=db_manager
        )
        logger.info(f"Пользователь {user.username or user.id} выбрал {response} в poll_id={poll_id}")
    except Exception as e:
        logger.error(f"Ошибка в poll_callback: {e}", exc_info=True)
        try:
            if update.callback_query:
                await update.callback_query.answer("❌ Ошибка при сохранении ответа", show_alert=True)
        except Exception:
            pass


# ==================== НОВЫЙ ОБРАБОТЧИК ====================
async def new_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Fallback-обработчик системного сообщения о новых участниках.
    Работает даже если бот не администратор.
    """
    try:
        message = update.effective_message
        chat = update.effective_chat
        
        if not message or not chat:
            return
        
        new_members = getattr(message, "new_chat_members", None)
        if not new_members:
            return
        
        for user in new_members:
            skip = await should_skip_welcome(chat.id, user.id)
            if skip:
                logger.info(f"Антидубль: пропускаем new_member_handler chat_id={chat.id}, user_id={user.id}")
                continue
            await welcome_new_member(update, context, user)
            
    except Exception as e:
        logger.error(f"Ошибка в new_member_handler: {e}", exc_info=True)


# ==================== АВТОМАТИЧЕСКИЕ ЗАДАЧИ ====================
async def scheduled_poll(application: Application):
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    metrics = application.bot_data.get('metrics')
    if not config or not config.group_chat_id:
        logger.error("GROUP_CHAT_ID не настроен")
        return
    training_date = PollManager.get_training_date()
    training_date_iso = PollManager.get_training_date_iso()
    poll_id = f"auto_{datetime.now(MSK).strftime('%Y%m%d')}"
    if await db_manager.poll_exists(poll_id):
        logger.info(f"Опрос {poll_id} уже существует, пропускаем")
        return
    text = PollManager.create_poll_text(training_date)
    message = await application.bot.send_message(
        chat_id=config.group_chat_id,
        text=text,
        reply_markup=PollManager.create_keyboard(poll_id),
        parse_mode=ParseMode.HTML
    )
    saved = await db_manager.save_poll(
        poll_id=poll_id,
        message_id=message.message_id,
        chat_id=message.chat_id,
        training_date=training_date_iso,
        created_at=datetime.now(MSK),
        metrics=metrics
    )
    if not saved:
        logger.error(f"Не удалось сохранить опрос {poll_id} в БД, удаляем сообщение")
        try:
            await application.bot.delete_message(
                chat_id=config.group_chat_id,
                message_id=message.message_id
            )
        except Exception as delete_error:
            logger.warning(f"Не удалось удалить сообщение опроса: {delete_error}")
    logger.info(f"Автоматический опрос создан: {poll_id}")


async def scheduled_monthly_stats(application: Application):
    config = application.bot_data.get('config')
    db_manager = application.bot_data.get('db_manager')
    now = datetime.now(MSK)
    first_of_month = now.day == 1 and now.hour < 12
    if first_of_month:
        if now.month == 1:
            year, month = now.year - 1, 12
        else:
            year, month = now.year, now.month - 1
    else:
        year, month = now.year, now.month
    month_name = datetime(year, month, 1).strftime('%B %Y')
    for admin_id in config.admin_user_ids:
        try:
            df = await db_manager.get_monthly_stats(year, month)
            if df.empty:
                await application.bot.send_message(
                    chat_id=admin_id,
                    text=f"📭 Статистика за {month_name} пуста"
                )
                continue
            output = create_excel_from_dataframe(df, f"stats_{year}_{month:02d}")
            try:
                await application.bot.send_document(
                    chat_id=admin_id,
                    document=output,
                    filename=f"stats_{year}_{month:02d}.xlsx",
                    caption=f"📊 Автоматическая статистика за {month_name}"
                )
            finally:
                output.close()
            logger.info(f"Статистика отправлена админу {admin_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки статистики: {e}")