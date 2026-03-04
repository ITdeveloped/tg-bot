import asyncio, logging, aiosqlite, os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (Message, BusinessConnection, BusinessMessagesDeleted,
                            BufferedInputFile, InlineKeyboardMarkup,
                            InlineKeyboardButton, BotCommand, CallbackQuery,
                            FSInputFile)

load_dotenv()
BOT_TOKEN  = os.getenv("BOT_TOKEN")
DB_PATH    = "spy.db"
ADMIN_ID   = 1122373839
PHOTO_PATH = "tutorial.png"
_cache     = {"photo_file_id": None}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

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
                connected_at TEXT, is_active INTEGER DEFAULT 1,
                deleted_count INTEGER DEFAULT 0,
                edited_count INTEGER DEFAULT 0)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_activity (
                owner_id INTEGER, date TEXT, count INTEGER DEFAULT 0,
                PRIMARY KEY (owner_id, date))
        """)
        try:
            await db.execute("ALTER TABLE connections ADD COLUMN deleted_count INTEGER DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE connections ADD COLUMN edited_count INTEGER DEFAULT 0")
        except: pass
        await db.execute("CREATE INDEX IF NOT EXISTS idx_owner    ON messages(owner_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_lookup   ON messages(bcid,chat_id,message_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bcid     ON connections(bcid,is_active)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_activity ON daily_activity(owner_id,date)")
        await db.commit()
        async with db.execute("SELECT user_id, bcid FROM connections WHERE is_active=1") as cur:
            for uid, bcid in await cur.fetchall():
                BCID_CACHE[bcid] = uid
        logger.info(f"Кэш загружен: {len(BCID_CACHE)} подключений")

async def save_msg(bcid, owner_id, chat_id, mid, from_id, from_name,
                   chat_name=None, text=None, mt=None, fid=None):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO messages"
                "(bcid,owner_id,chat_id,message_id,from_id,from_name,chat_name,text,media_type,file_id,date)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (bcid,owner_id,chat_id,mid,from_id,from_name,chat_name,
                 text,mt,fid,datetime.now().isoformat()))
            await db.execute(
                "INSERT INTO daily_activity(owner_id,date,count) VALUES(?,?,1) "
                "ON CONFLICT(owner_id,date) DO UPDATE SET count=count+1",
                (owner_id, today))
            await db.commit()
    except Exception as e:
        logger.error(f"save_msg: {e}")

async def get_msg(bcid, chat_id, mid):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM messages WHERE bcid=? AND chat_id=? AND message_id=?",
                (bcid, chat_id, mid)) as cur:
                row = await cur.fetchone()
                if row: return dict(zip([d[0] for d in cur.description], row))
    except Exception as e:
        logger.error(f"get_msg: {e}")
    return None

async def save_conn(uid, bcid):
    try:
        BCID_CACHE[bcid] = uid
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO connections(user_id,bcid,connected_at,is_active,deleted_count,edited_count) VALUES(?,?,?,1,0,0)",
                (uid, bcid, datetime.now().isoformat()))
            await db.commit()
    except Exception as e:
        logger.error(f"save_conn: {e}")

async def del_conn(uid):
    try:
        for k, v in list(BCID_CACHE.items()):
            if v == uid: del BCID_CACHE[k]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE connections SET is_active=0 WHERE user_id=?", (uid,))
            await db.commit()
    except Exception as e:
        logger.error(f"del_conn: {e}")

async def inc_deleted(owner_id):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE connections SET deleted_count=deleted_count+1 WHERE user_id=?", (owner_id,))
            await db.commit()
    except Exception as e:
        logger.error(f"inc_deleted: {e}")

async def inc_edited(owner_id):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE connections SET edited_count=edited_count+1 WHERE user_id=?", (owner_id,))
            await db.commit()
    except Exception as e:
        logger.error(f"inc_edited: {e}")

def owner_by_bcid(bcid): return BCID_CACHE.get(bcid)

async def get_stats(owner_id):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM messages WHERE owner_id=?", (owner_id,)) as cur:
                total = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT deleted_count, edited_count, connected_at FROM connections WHERE user_id=?", (owner_id,)) as cur:
                row = await cur.fetchone()
                deleted      = row[0] if row else 0
                edited       = row[1] if row else 0
                connected_at = row[2] if row else None
            async with db.execute(
                "SELECT COUNT(DISTINCT chat_id) FROM messages WHERE owner_id=?", (owner_id,)) as cur:
                chats = (await cur.fetchone())[0]
            week = []
            for i in range(6, -1, -1):
                day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                async with db.execute(
                    "SELECT COALESCE(count,0) FROM daily_activity WHERE owner_id=? AND date=?",
                    (owner_id, day)) as cur:
                    row = await cur.fetchone()
                    week.append((day, row[0] if row else 0))
            active = 1 if any(v == owner_id for v in BCID_CACHE.values()) else 0
            return total, deleted, edited, chats, connected_at, week, active
    except Exception as e:
        logger.error(f"get_stats: {e}")
    return 0, 0, 0, 0, None, [], 0

async def get_all_users():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT DISTINCT user_id FROM connections WHERE is_active=1") as cur:
                return [r[0] for r in await cur.fetchall()]
    except: return []

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

def fmt_date(iso):
    if not iso: return "—"
    try: return datetime.fromisoformat(iso).strftime("%d.%m.%Y")
    except: return "—"

def media_emoji(mt):
    return {"photo":"🖼","video":"🎥","voice":"🎙","video_note":"⭕️",
            "document":"📎","sticker":"🎭","audio":"🎵"}.get(mt or "", "📁")

DAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

def build_stats_text(user, total, deleted, edited, chats, connected_at, week, active):
    username = f"@{user.username}" if user.username else "—"
    uid      = user.id
    max_val  = max((c for _, c in week), default=1) or 1
    bar_line = ""
    for day_str, cnt in week:
        wd   = DAYS_RU[datetime.strptime(day_str, "%Y-%m-%d").weekday()]
        bars = "█" * round(cnt / max_val * 8) if cnt else "▁"
        bar_line += f"  {wd} {bars} {cnt}\n"
    status = "🟢 Активен" if active else "🔴 Отключён"
    return (
        "📊 <b>Статистика аккаунта</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 Пользователь: <b>{username}</b>\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📅 Дата подключения: <b>{fmt_date(connected_at)}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"💬 Всего сообщений: <b>{total:,}</b>\n"
        f"🗑 Удалено сообщений: <b>{deleted:,}</b>\n"
        f"✏️ Изменено сообщений: <b>{edited:,}</b>\n"
        f"👁 Отслеживаемых чатов: <b>{chats:,}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📈 <b>Активность за 7 дней:</b>\n"
        f"{bar_line}"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📡 Статус мониторинга: <b>{status}</b>\n"
        f"🔔 Уведомления: <b>Включены</b>"
    )

def build_stats_text_admin(user, total, deleted, edited, chats, connected_at, week, active, all_users):
    base = build_stats_text(user, total, deleted, edited, chats, connected_at, week, active)
    base += f"\n━━━━━━━━━━━━━━━━━━\n👥 Всего пользователей: <b>{len(all_users):,}</b>"
    return base

START_TEXT = (
    "👋 <b>Добро пожаловать!</b>\n\n"
    "Начни следить за изменениями в чатах через <b>Telegram Business!</b>\n\n"
    "🔔 Моментально пришлёт уведомление, если собеседник <b>изменит</b> или <b>удалит</b> сообщение\n\n"
    "⏳ Умеет скачивать файлы с таймером — <b>фото, видео, голосовые, кружки</b>\n\n"
    "<blockquote>❓ Как подключить бота — смотрите на картинке выше 👆</blockquote>"
)

def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика чата", callback_data="stats")],
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
         InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")],
    ])

async def send_start(bot: Bot, chat_id: int, is_admin: bool = False):
    kb = kb_admin() if is_admin else kb_main()
    try:
        fid = _cache["photo_file_id"]
        if fid:
            await bot.send_photo(chat_id, fid, caption=START_TEXT, reply_markup=kb)
        elif os.path.exists(PHOTO_PATH):
            msg = await bot.send_photo(chat_id, FSInputFile(PHOTO_PATH), caption=START_TEXT, reply_markup=kb)
            _cache["photo_file_id"] = msg.photo[-1].file_id
            logger.info(f"Картинка закэширована: {_cache['photo_file_id']}")
        else:
            await bot.send_message(chat_id, START_TEXT, reply_markup=kb)
    except Exception as e:
        logger.error(f"send_start: {e}")
        try: await bot.send_message(chat_id, START_TEXT, reply_markup=kb)
        except: pass

async def send_media_safe(bot, owner_id, mt, fid, caption):
    try:
        if mt == "photo":        await bot.send_photo(owner_id, fid, caption=caption)
        elif mt == "video":      await bot.send_video(owner_id, fid, caption=caption)
        elif mt == "voice":      await bot.send_voice(owner_id, fid, caption=caption)
        elif mt == "video_note":
            await bot.send_message(owner_id, caption)
            await bot.send_video_note(owner_id, fid)
        elif mt == "sticker":
            await bot.send_message(owner_id, caption)
            await bot.send_sticker(owner_id, fid)
        elif mt == "document":   await bot.send_document(owner_id, fid, caption=caption)
        elif mt == "audio":      await bot.send_audio(owner_id, fid, caption=caption)
        else: await bot.send_message(owner_id, caption)
    except Exception:
        try:
            file = await bot.get_file(fid)
            buf  = await bot.download_file(file.file_path)
            ext  = {"photo":"jpg","video":"mp4","voice":"ogg","video_note":"mp4","audio":"mp3"}.get(mt,"bin")
            binf = BufferedInputFile(buf.read(), f"file.{ext}")
            if mt == "photo":        await bot.send_photo(owner_id, binf, caption=caption)
            elif mt == "video":      await bot.send_video(owner_id, binf, caption=caption)
            elif mt == "voice":      await bot.send_voice(owner_id, binf, caption=caption)
            elif mt == "video_note":
                await bot.send_message(owner_id, caption)
                await bot.send_video_note(owner_id, binf)
            elif mt == "document":   await bot.send_document(owner_id, binf, caption=caption)
            elif mt == "audio":      await bot.send_audio(owner_id, binf, caption=caption)
        except Exception as e2:
            logger.error(f"send_media_safe: {e2}")
            try: await bot.send_message(owner_id, caption + "\n⚠️ Не удалось отправить медиа")
            except: pass

# ── РОУТЕР ───────────────────────────────────────────────────────────────────

router = Router()
broadcast_mode: set[int] = set()

@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    await send_start(bot, message.chat.id, message.from_user.id == ADMIN_ID)

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    user = message.from_user
    total, deleted, edited, chats, connected_at, week, active = await get_stats(user.id)
    if user.id == ADMIN_ID:
        users = await get_all_users()
        text = build_stats_text_admin(user, total, deleted, edited, chats, connected_at, week, active, users)
    else:
        text = build_stats_text(user, total, deleted, edited, chats, connected_at, week, active)
    await message.answer(text)

@router.message(Command("cancel"))
async def cmd_cancel(message: Message):
    broadcast_mode.discard(message.from_user.id)
    await message.answer("❌ Рассылка отменена.")

@router.callback_query(F.data == "stats")
async def cb_stats(call: CallbackQuery):
    await call.answer()
    user = call.from_user
    total, deleted, edited, chats, connected_at, week, active = await get_stats(user.id)
    if user.id == ADMIN_ID:
        users = await get_all_users()
        text = build_stats_text_admin(user, total, deleted, edited, chats, connected_at, week, active, users)
    else:
        text = build_stats_text(user, total, deleted, edited, chats, connected_at, week, active)
    await call.message.answer(text)

@router.callback_query(F.data == "broadcast")
async def cb_broadcast(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("⛔ Нет доступа", show_alert=True)
        return
    await call.answer()
    broadcast_mode.add(ADMIN_ID)
    await call.message.answer(
        "📢 <b>Режим рассылки</b>\n\n"
        "Напиши сообщение — отправлю всем пользователям.\n"
        "/cancel — отменить"
    )

@router.message()
async def handle_any(message: Message, bot: Bot):
    uid = message.from_user.id
    if uid == ADMIN_ID and uid in broadcast_mode:
        broadcast_mode.discard(uid)
        users = await get_all_users()
        ok, fail = 0, 0
        await message.answer(f"⏳ Рассылка для {len(users)} пользователей...")
        for user_id in users:
            try:
                await bot.copy_message(user_id, message.chat.id, message.message_id)
                ok += 1
                await asyncio.sleep(0.05)
            except Exception:
                fail += 1
        await message.answer(f"✅ Готово!\n📤 Отправлено: {ok}\n❌ Ошибок: {fail}")
        return
    await send_start(bot, message.chat.id, uid == ADMIN_ID)

# ── BUSINESS ─────────────────────────────────────────────────────────────────

@router.business_connection()
async def on_connect(event: BusinessConnection, bot: Bot):
    uid = event.user.id
    logger.info(f"business_connection uid={uid} bcid={event.id} enabled={event.is_enabled}")
    if event.is_enabled:
        await save_conn(uid, event.id)
        await bot.send_message(uid,
            "✅ <b>Бот успешно привязан</b>\n\n"
            "<b>Как использовать?</b>\n"
            "— Если собеседник удалит сообщение — пришлю копию сразу же\n"
            "— Если изменит — покажу было/стало\n"
            "— Чтобы сохранить фото/видео с таймером — ответь на него <code>.</code> в диалоге\n\n"
            "❗ Работает только с <b>НОВЫМИ</b> сообщениями после подключения",
            reply_markup=kb_main()
        )
    else:
        await del_conn(uid)
        await bot.send_message(uid, "❌ <b>Бот отключён.</b>\n\nСлежка остановлена.")

@router.business_message()
async def cache_msg(message: Message):
    bcid = message.business_connection_id
    if not bcid: return
    owner_id = owner_by_bcid(bcid)
    logger.info(f"business_message bcid={bcid} owner={owner_id} from={get_name(message.from_user)}")
    if not owner_id: return

    chat_name = get_chat_name(message.chat)

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
                binf = BufferedInputFile(buf.read(), fname)
                if mt == "photo":        await bot.send_photo(owner_id, binf, caption=caption)
                elif mt == "video":      await bot.send_video(owner_id, binf, caption=caption)
                elif mt == "voice":      await bot.send_voice(owner_id, binf, caption=caption)
                elif mt == "video_note":
                    await bot.send_message(owner_id, caption)
                    await bot.send_video_note(owner_id, binf)
                sent = True
                break
            if not sent:
                await bot.send_message(owner_id, caption + "\n\n❌ Медиа не найдено или таймер истёк")
        except Exception as e:
            logger.error(f"Таймер-медиа: {e}")
            try: await message.bot.send_message(owner_id, f"⚠️ Не удалось сохранить медиа\n<code>{str(e)[:150]}</code>")
            except: pass
        return

    mt, fid = get_media(message)
    await save_msg(bcid, owner_id, message.chat.id, message.message_id,
        message.from_user.id if message.from_user else 0,
        get_name(message.from_user), chat_name,
        message.text or message.caption, mt, fid)

@router.deleted_business_messages()
async def on_deleted(event: BusinessMessagesDeleted, bot: Bot):
    owner_id = owner_by_bcid(event.business_connection_id)
    logger.info(f"deleted bcid={event.business_connection_id} owner={owner_id} ids={event.message_ids}")
    if not owner_id: return
    for mid in event.message_ids:
        c = await get_msg(event.business_connection_id, event.chat.id, mid)
        if not c:
            logger.warning(f"Сообщение {mid} не в кэше")
            continue
        await inc_deleted(owner_id)
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
            logger.error(f"on_deleted: {e}")

@router.edited_business_message()
async def on_edited(message: Message, bot: Bot):
    bcid = message.business_connection_id
    if not bcid: return
    owner_id = owner_by_bcid(bcid)
    if not owner_id: return
    await inc_edited(owner_id)
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
        logger.error(f"on_edited: {e}")
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

    # Прогрев картинки при старте
    if os.path.exists(PHOTO_PATH) and not _cache["photo_file_id"]:
        try:
            msg = await bot.send_photo(ADMIN_ID, FSInputFile(PHOTO_PATH), caption="🔄 Прогрев...")
            _cache["photo_file_id"] = msg.photo[-1].file_id
            await bot.delete_message(ADMIN_ID, msg.message_id)
            logger.info(f"Картинка прогрета: {_cache['photo_file_id']}")
        except Exception as e:
            logger.warning(f"Прогрев не удался: {e}")

    await bot.set_my_commands([
        BotCommand(command="start",  description="🚀 Запустить бота"),
        BotCommand(command="stats",  description="📊 Статистика аккаунта"),
    ])

    dp = Dispatcher()
    dp.include_router(router)
    logger.info("🕵️ Dialog Spy Bot запущен!")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types(), drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
