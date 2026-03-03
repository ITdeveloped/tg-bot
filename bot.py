import asyncio, logging, aiosqlite, os
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (Message, BusinessConnection, BusinessMessagesDeleted,
                            BufferedInputFile, InlineKeyboardMarkup,
                            InlineKeyboardButton, BotCommand, CallbackQuery)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH   = "spy.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Кэш в памяти bcid -> owner_id (быстро, без запросов к БД)
BCID_CACHE: dict[str, int] = {}

# ── БД ──────────────────────────────────────────────────────────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bcid TEXT, owner_id INTEGER, chat_id INTEGER,
                message_id INTEGER, from_id INTEGER, from_name TEXT,
                chat_name TEXT, text TEXT, media_type TEXT, file_id TEXT, date TEXT,
                UNIQUE(bcid, chat_id, message_id))
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS connections (
                user_id INTEGER PRIMARY KEY, bcid TEXT,
                connected_at TEXT, is_active INTEGER DEFAULT 1)
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_owner ON messages(owner_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_lookup ON messages(bcid,chat_id,message_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bcid ON connections(bcid,is_active)")
        await db.commit()

        # Загружаем активные подключения в кэш при старте
        async with db.execute("SELECT user_id, bcid FROM connections WHERE is_active=1") as cur:
            rows = await cur.fetchall()
            for uid, bcid in rows:
                BCID_CACHE[bcid] = uid
            logger.info(f"Загружено подключений в кэш: {len(BCID_CACHE)}")

async def save_msg(bcid, owner_id, chat_id, mid, from_id, from_name,
                   chat_name=None, text=None, mt=None, fid=None):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO messages"
                "(bcid,owner_id,chat_id,message_id,from_id,from_name,chat_name,text,media_type,file_id,date)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (bcid,owner_id,chat_id,mid,from_id,from_name,chat_name,
                 text,mt,fid,datetime.now().isoformat()))
            await db.commit()
    except Exception as e:
        logger.error(f"save_msg error: {e}")

async def get_msg(bcid, chat_id, mid):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM messages WHERE bcid=? AND chat_id=? AND message_id=?",
                (bcid, chat_id, mid)) as cur:
                row = await cur.fetchone()
                if row: return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"get_msg error: {e}")
    return None

async def save_conn(uid, bcid):
    try:
        BCID_CACHE[bcid] = uid  # сразу в кэш
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO connections(user_id,bcid,connected_at,is_active) VALUES(?,?,?,1)",
                (uid, bcid, datetime.now().isoformat()))
            await db.commit()
    except Exception as e:
        logger.error(f"save_conn error: {e}")

async def del_conn(uid):
    try:
        # Убираем из кэша
        for k, v in list(BCID_CACHE.items()):
            if v == uid:
                del BCID_CACHE[k]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE connections SET is_active=0 WHERE user_id=?", (uid,))
            await db.commit()
    except Exception as e:
        logger.error(f"del_conn error: {e}")

def owner_by_bcid(bcid: str):
    """Мгновенно из кэша — никаких запросов к БД."""
    return BCID_CACHE.get(bcid)

async def get_history(owner_id, limit=20):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT from_name,chat_name,text,media_type,date FROM messages"
                " WHERE owner_id=? ORDER BY date DESC LIMIT ?",
                (owner_id, limit)) as cur:
                rows = await cur.fetchall()
                return [dict(zip([d[0] for d in cur.description], r)) for r in rows]
    except Exception as e:
        logger.error(f"get_history error: {e}")
    return []

async def get_stats(owner_id):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM messages WHERE owner_id=?", (owner_id,)) as cur:
                total = (await cur.fetchone())[0]
            active = 1 if any(v == owner_id for v in BCID_CACHE.values()) else 0
            return total, active
    except Exception as e:
        logger.error(f"get_stats error: {e}")
    return 0, 0

# ── ХЕЛПЕРЫ ─────────────────────────────────────────────────────────────────

def get_media(msg):
    if not msg: return None, None
    if msg.photo:      return "photo",      msg.photo[-1].file_id
    if msg.video:      return "video",      msg.video.file_id
    if msg.voice:      return "voice",      msg.voice.file_id
    if msg.video_note: return "video_note", msg.video_note.file_id
    if msg.document:   return "document",   msg.document.file_id
    if msg.sticker:    return "sticker",    msg.sticker.file_id
    if msg.audio:      return "audio",      msg.audio.file_id
    return None, None

def get_name(u):
    if not u: return "Неизвестно"
    n = (u.first_name or "").strip()
    if u.last_name: n += f" {u.last_name}"
    if u.username:  n += f" (@{u.username})"
    return n or "Неизвестно"

def get_chat_name(chat):
    if not chat: return None
    if chat.title: return chat.title
    n = (chat.first_name or "").strip()
    if chat.last_name: n += f" {chat.last_name}"
    if chat.username:  n += f" (@{chat.username})"
    return n.strip() or None

def fmt(iso): return iso[:16].replace("T", " ") if iso else "—"

def media_emoji(mt):
    return {"photo":"🖼","video":"🎥","voice":"🎙","video_note":"⭕️",
            "document":"📎","sticker":"🎭","audio":"🎵"}.get(mt or "", "📁")

def kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🗑 История удалённых", callback_data="history"),
         InlineKeyboardButton(text="❓ Помощь", callback_data="help")],
    ])

async def send_media_safe(bot, owner_id, mt, fid, caption):
    try:
        if mt == "photo":       await bot.send_photo(owner_id, fid, caption=caption)
        elif mt == "video":     await bot.send_video(owner_id, fid, caption=caption)
        elif mt == "voice":     await bot.send_voice(owner_id, fid, caption=caption)
        elif mt == "video_note":
            await bot.send_message(owner_id, caption)
            await bot.send_video_note(owner_id, fid)
        elif mt == "sticker":
            await bot.send_message(owner_id, caption)
            await bot.send_sticker(owner_id, fid)
        elif mt == "document":  await bot.send_document(owner_id, fid, caption=caption)
        elif mt == "audio":     await bot.send_audio(owner_id, fid, caption=caption)
        else: await bot.send_message(owner_id, caption)
    except Exception:
        try:
            file = await bot.get_file(fid)
            buf  = await bot.download_file(file.file_path)
            data = buf.read()
            ext  = {"photo":"jpg","video":"mp4","voice":"ogg","video_note":"mp4","audio":"mp3"}.get(mt,"bin")
            binf = BufferedInputFile(data, f"file.{ext}")
            if mt == "photo":       await bot.send_photo(owner_id, binf, caption=caption)
            elif mt == "video":     await bot.send_video(owner_id, binf, caption=caption)
            elif mt == "voice":     await bot.send_voice(owner_id, binf, caption=caption)
            elif mt == "video_note":
                await bot.send_message(owner_id, caption)
                await bot.send_video_note(owner_id, binf)
            elif mt == "document":  await bot.send_document(owner_id, binf, caption=caption)
            elif mt == "audio":     await bot.send_audio(owner_id, binf, caption=caption)
        except Exception as e2:
            logger.error(f"send_media_safe error: {e2}")
            try: await bot.send_message(owner_id, caption + "\n⚠️ Не удалось отправить медиа")
            except: pass

# ── РОУТЕР ───────────────────────────────────────────────────────────────────

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    me = await bot.get_me()
    await message.answer(
        "🤖 <b>Этот бот создан, чтобы помогать вам в переписке.</b>\n\n"
        "<i>Возможности бота:</i>\n"
        "• Моментально пришлёт уведомление, если собеседник изменит или удалит сообщение 🔔\n"
        "• Умеет скачивать файлы с таймером: фото/видео/голосовые/кружки ⏳\n\n"
        "📌 <b>Как подключить:</b>\n"
        "1. Настройки → <b>Telegram для бизнеса → Чат-боты</b>\n"
        f"2. Введи <code>@{me.username}</code> и нажми <b>«Добавить»</b>\n\n"
        "📌 <b>Как сохранить медиа с таймером:</b>\n"
        "Ответь на него точкой <code>.</code> прямо в диалоге\n\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "⚠️ Бот работает только с <b>НОВЫМИ</b> сообщениями после подключения\n"
        "⚠️ Требуется <b>Telegram Premium</b>",
        reply_markup=kb()
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🕵️ <b>Справка</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
        "🗑 Собеседник <b>удалил</b> → получаешь копию\n"
        "✏️ Собеседник <b>изменил</b> → видишь было/стало\n"
        "⏱ Фото/видео <b>с таймером</b> → ответь <code>.</code> в диалоге\n"
        "🎙 Голосовые, кружки, стикеры — тоже сохраняю\n\n"
        "⚠️ Только новые сообщения после подключения",
        reply_markup=kb()
    )

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    total, connected = await get_stats(message.from_user.id)
    await message.answer(
        "📊 <b>Статистика</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
        f"💾 Сохранено сообщений: <b>{total}</b>\n"
        f"🔗 Подключение: <b>{'Активно ✅' if connected else 'Отключено ❌'}</b>"
    )

@router.message(Command("history"))
async def cmd_history(message: Message):
    await send_history(message.from_user.id, message.bot, message.chat.id)

async def send_history(owner_id, bot, chat_id):
    data = await get_history(owner_id)
    if not data:
        await bot.send_message(chat_id, "📭 <b>История пуста</b>\n\nПока нет удалённых сообщений.")
        return
    text = "🗑 <b>Последние удалённые:</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
    for i, m in enumerate(data, 1):
        chat    = f"  💬 {m['chat_name']}" if m.get("chat_name") else ""
        content = m["text"] or f"{media_emoji(m['media_type'])} [{m['media_type'] or 'медиа'}]"
        if len(content) > 80: content = content[:80] + "…"
        text += f"{i}. 👤 <b>{m['from_name'] or '?'}</b>{chat}\n🕐 <i>{fmt(m['date'])}</i>\n{content}\n\n"
    await bot.send_message(chat_id, text)

@router.callback_query(F.data == "history")
async def cb_history(call: CallbackQuery):
    await call.answer()
    await send_history(call.from_user.id, call.bot, call.message.chat.id)

@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    await call.answer()
    await call.message.answer(
        "🕵️ <b>Справка</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
        "🗑 Собеседник <b>удалил</b> → получаешь копию\n"
        "✏️ Собеседник <b>изменил</b> → видишь было/стало\n"
        "⏱ Фото/видео <b>с таймером</b> → ответь <code>.</code> в диалоге\n"
        "🎙 Голосовые, кружки, стикеры — тоже сохраняю"
    )

@router.callback_query(F.data == "stats")
async def cb_stats(call: CallbackQuery):
    await call.answer()
    total, connected = await get_stats(call.from_user.id)
    await call.message.answer(
        "📊 <b>Статистика</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
        f"💾 Сохранено сообщений: <b>{total}</b>\n"
        f"🔗 Подключение: <b>{'Активно ✅' if connected else 'Отключено ❌'}</b>"
    )

@router.business_connection()
async def on_connect(event: BusinessConnection, bot: Bot):
    uid = event.user.id
    logger.info(f"business_connection: uid={uid} bcid={event.id} enabled={event.is_enabled}")
    if event.is_enabled:
        await save_conn(uid, event.id)
        await bot.send_message(uid,
            "✅ <b>Бот успешно привязан</b>\n\n"
            "<b>Как использовать?</b>\n"
            "— Если собеседник удалит сообщение — пришлю копию сразу же\n"
            "— Если изменит — покажу было/стало\n"
            "— Чтобы сохранить фото/видео с таймером — ответь на него <code>.</code> в диалоге\n\n"
            "❗ Работает только с <b>НОВЫМИ</b> сообщениями после подключения",
            reply_markup=kb()
        )
    else:
        await del_conn(uid)
        await bot.send_message(uid, "❌ <b>Бот отключён.</b>\n\nСлежка остановлена.")

@router.business_message()
async def cache_msg(message: Message):
    bcid = message.business_connection_id
    if not bcid: return
    owner_id = owner_by_bcid(bcid)
    logger.info(f"business_message: bcid={bcid} owner={owner_id} from={get_name(message.from_user)}")
    if not owner_id: return

    chat_name = get_chat_name(message.chat)

    # Ответ точкой — сохраняем медиа с таймером
    if message.text in (".", "·", "+", "save", "сохр") and message.reply_to_message:
        reply     = message.reply_to_message
        from_name = get_name(reply.from_user)
        caption   = (
            f"⏱ <b>Медиа с таймером сохранено</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 <b>{from_name}</b>\n"
            f"💬 {chat_name or 'Диалог'}\n"
            f"🕐 {fmt(datetime.now().isoformat())}"
        )
        try:
            bot  = message.bot
            sent = False
            for attr, mt, fname in [
                ("photo",      "photo",      "photo.jpg"),
                ("video",      "video",      "video.mp4"),
                ("voice",      "voice",      "voice.ogg"),
                ("video_note", "video_note", "video_note.mp4"),
            ]:
                obj = getattr(reply, attr, None)
                if not obj: continue
                fid  = obj[-1].file_id if isinstance(obj, list) else obj.file_id
                file = await bot.get_file(fid)
                buf  = await bot.download_file(file.file_path)
                data = buf.read()
                binf = BufferedInputFile(data, fname)
                if mt == "photo":       await bot.send_photo(owner_id, binf, caption=caption)
                elif mt == "video":     await bot.send_video(owner_id, binf, caption=caption)
                elif mt == "voice":     await bot.send_voice(owner_id, binf, caption=caption)
                elif mt == "video_note":
                    await bot.send_message(owner_id, caption)
                    await bot.send_video_note(owner_id, binf)
                sent = True
                break
            if not sent:
                await bot.send_message(owner_id, caption + "\n\n❌ Медиа не найдено или таймер истёк")
        except Exception as e:
            logger.error(f"Таймер-медиа ошибка: {e}")
            try: await message.bot.send_message(owner_id, f"⚠️ Не удалось сохранить медиа\n<code>{str(e)[:150]}</code>")
            except: pass
        return

    mt, fid = get_media(message)
    await save_msg(
        bcid, owner_id, message.chat.id, message.message_id,
        message.from_user.id if message.from_user else 0,
        get_name(message.from_user), chat_name,
        message.text or message.caption, mt, fid
    )

@router.deleted_business_messages()
async def on_deleted(event: BusinessMessagesDeleted, bot: Bot):
    owner_id = owner_by_bcid(event.business_connection_id)
    logger.info(f"deleted: bcid={event.business_connection_id} owner={owner_id} ids={event.message_ids}")
    if not owner_id: return

    for mid in event.message_ids:
        c = await get_msg(event.business_connection_id, event.chat.id, mid)
        if not c:
            logger.warning(f"Сообщение {mid} не найдено в кэше")
            continue
        name      = c["from_name"] or "?"
        chat_name = c.get("chat_name") or "Диалог"
        text      = c["text"]
        mt        = c["media_type"]
        fid       = c["file_id"]
        header    = f"🗑 <b>{name} удалил(а) сообщение:</b>\n\n"
        body      = f"<blockquote>{text}</blockquote>" if text else (f"{media_emoji(mt)} <i>[{mt}]</i>" if mt else "<i>[пустое]</i>")
        footer    = f"\n\n💬 {chat_name}  🕐 <i>{fmt(c['date'])}</i>"
        try:
            if fid:
                await send_media_safe(bot, owner_id, mt, fid, header + (text or "") + footer)
            else:
                await bot.send_message(owner_id, header + body + footer)
        except Exception as e:
            logger.error(f"on_deleted send error: {e}")

@router.edited_business_message()
async def on_edited(message: Message, bot: Bot):
    bcid = message.business_connection_id
    if not bcid: return
    owner_id = owner_by_bcid(bcid)
    if not owner_id: return

    c         = await get_msg(bcid, message.chat.id, message.message_id)
    old       = c["text"] if c else "—"
    new       = message.text or message.caption or "[медиа]"
    name      = get_name(message.from_user)
    chat_name = get_chat_name(message.chat) or "Диалог"

    try:
        await bot.send_message(owner_id,
            f"✏️ <b>{name} изменил(а) сообщение:</b>\n\n"
            f"Old:\n<blockquote>{old or '—'}</blockquote>\n\n"
            f"New:\n<blockquote>{new}</blockquote>\n\n"
            f"💬 {chat_name}  🕐 <i>{fmt(datetime.now().isoformat())}</i>"
        )
    except Exception as e:
        logger.error(f"on_edited error: {e}")

    mt, fid = get_media(message)
    await save_msg(bcid, owner_id, message.chat.id, message.message_id,
        message.from_user.id if message.from_user else 0,
        name, chat_name, new, mt, fid)

# ── ЗАПУСК ───────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        raise ValueError("Создай .env с BOT_TOKEN=твой_токен")

    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    await bot.set_my_commands([
        BotCommand(command="start",   description="🚀 Запустить бота"),
        BotCommand(command="stats",   description="📊 Статистика"),
        BotCommand(command="history", description="🗑 История удалённых"),
        BotCommand(command="help",    description="❓ Помощь"),
    ])

    dp = Dispatcher()
    dp.include_router(router)

    logger.info("🕵️ Dialog Spy Bot запущен!")
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True
    )

if __name__ == "__main__":
    asyncio.run(main())