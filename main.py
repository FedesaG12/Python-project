import logging
import asyncpg
import os
import asyncio
import re
from aiohttp import web 
from aiogram import Bot, Dispatcher, types, F, html
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup,
    KeyboardButton, ReplyKeyboardRemove, ForceReply
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application 
from datetime import datetime, timedelta, timezone
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from typing import Optional, Tuple, Dict, Any, List
from aiogram.dispatcher.middlewares.base import BaseMiddleware



# --- Constants ---
CATEGORIES = [
    "Relationship", "Family", "School", "Friendship",
    "Religion", "Mental", "Addiction", "Harassment", "Crush", "Health", "Trauma", "Sexual Assault",
    "Other"
]
POINTS_PER_CONFESSION = 1
POINTS_PER_LIKE_RECEIVED = 3
POINTS_PER_DISLIKE_RECEIVED = -3
MAX_CATEGORIES = 3
MAX_PHOTO_SIZE_MB = 5

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKENS")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "15"))
DATABASE_URL = os.getenv("DATABASE_URL")
HTTP_PORT_STR = os.getenv("PORT")

# Validate essential environment variables
if not BOT_TOKEN: raise ValueError("FATAL: BOT_TOKEN environment variable not set!")
if not ADMIN_ID_STR: raise ValueError("FATAL: ADMIN_ID environment variable not set!")
if not CHANNEL_ID: raise ValueError("FATAL: CHANNEL_ID environment variable not set!")
if not DATABASE_URL: raise ValueError("FATAL: DATABASE_URL environment variable not set!")

try:
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    raise ValueError("FATAL: ADMIN_ID environment variable must be a valid integer!")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Bot and Dispatcher
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# Bot info
bot_info = None

# --- FSM States ---
class ConfessionForm(StatesGroup):
    selecting_categories = State()
    waiting_for_text = State()

class CommentForm(StatesGroup):
    waiting_for_comment = State()
    waiting_for_reply = State()

class ContactAdminForm(StatesGroup):
    waiting_for_message = State()

class AdminActions(StatesGroup):
    waiting_for_rejection_reason = State()

# NEW: Feedback and Support States
class FeedbackStates(StatesGroup):
    waiting_for_feedback = State()
    waiting_for_admin_reply = State()

class SupportChatStates(StatesGroup):
    waiting_for_message = State()
    chatting = State()

# --- Database ---
db = None

async def create_db_pool():
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        logging.info("Database pool created successfully.")
        return pool
    except Exception as e:
        logging.error(f"Failed to create database pool: {e}")
        raise

async def setup():
    global db, bot_info
    db = await create_db_pool()
    bot_info = await bot.get_me()
    logging.info(f"Bot started: @{bot_info.username}")

    async with db.acquire() as conn:
        # --- Confessions Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS confessions (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                status VARCHAR(10) DEFAULT 'pending',
                message_id BIGINT,
                photo_file_id TEXT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                rejection_reason TEXT NULL,
                categories TEXT[] NULL
            );
        """)
        logging.info("Checked/Created 'confessions' table.")
        
        # Ensure photo_file_id column exists
        await conn.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                              WHERE table_name='confessions' AND column_name='photo_file_id') THEN
                    ALTER TABLE confessions ADD COLUMN photo_file_id TEXT NULL;
                END IF;
            END $$;
        """)
        
        # --- Comments Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                text TEXT,
                sticker_file_id TEXT,
                animation_file_id TEXT,
                parent_comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Checked/Created 'comments' table.")

        # Create indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_confession_id ON comments(confession_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);")
        
        # --- Reactions Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reactions ( 
                id SERIAL PRIMARY KEY, 
                comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL, 
                reaction_type VARCHAR(10) NOT NULL, 
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(comment_id, user_id) 
            );
        """)
        logging.info("Checked/Created 'reactions' table.")

        # --- Contact Requests Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS contact_requests (
                id SERIAL PRIMARY KEY,
                confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                comment_id INTEGER NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
                requester_user_id BIGINT NOT NULL,
                requested_user_id BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (comment_id, requester_user_id)
            );
        """)
        logging.info("Checked/Created 'contact_requests' table.")

        # --- User Points Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_points (
                user_id BIGINT PRIMARY KEY,
                points INTEGER NOT NULL DEFAULT 0
            );
        """)
        logging.info("Checked/Created 'user_points' table.")

        # --- Reports Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id SERIAL PRIMARY KEY,
                comment_id INTEGER NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
                reporter_user_id BIGINT NOT NULL,
                reported_user_id BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (comment_id, reporter_user_id)
            );
        """)
        logging.info("Checked/Created 'reports' table.")

        # --- Deletion Requests Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS deletion_requests (
                id SERIAL PRIMARY KEY,
                confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE,
                reviewed_at TIMESTAMP WITH TIME ZONE,
                UNIQUE (confession_id, user_id)
            );
        """)
        logging.info("Checked/Created 'deletion_requests' table.")

        # --- User Status Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_status (
                user_id BIGINT PRIMARY KEY,
                has_accepted_rules BOOLEAN NOT NULL DEFAULT FALSE,
                is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                blocked_until TIMESTAMP WITH TIME ZONE NULL,
                block_reason TEXT NULL
            );
        """)
        logging.info("Checked/Created 'user_status' table.")
        
        # --- NEW: Feedback Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                message TEXT NOT NULL,
                replied BOOLEAN DEFAULT FALSE,
                admin_reply TEXT,
                replied_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logging.info("Checked/Created 'feedback' table.")
        
        # --- NEW: Support Chat Sessions Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS support_sessions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                is_active BOOLEAN DEFAULT TRUE,
                last_message_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logging.info("Checked/Created 'support_sessions' table.")
        
        # --- NEW: Support Chat Messages Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS support_messages (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES support_sessions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                message TEXT,
                is_from_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logging.info("Checked/Created 'support_messages' table.")
        
        # --- NEW: Broadcast Stats Table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_stats (
                id SERIAL PRIMARY KEY,
                total_users BIGINT DEFAULT 0,
                last_broadcast TIMESTAMP WITH TIME ZONE,
                messages_sent BIGINT DEFAULT 0
            )
        """)
        logging.info("Checked/Created 'broadcast_stats' table.")

        logging.info("Database setup complete.")


# --- Dummy HTTP Server Functions ---
async def handle_health_check(request):
    logging.debug("Health check endpoint hit.")
    return web.Response(text="OK")

async def start_dummy_server():
    if not HTTP_PORT_STR:
        logging.info("PORT environment variable not set. Dummy HTTP server will not start.")
        return

    try: 
        port = int(HTTP_PORT_STR)
    except ValueError:
        logging.error(f"Invalid PORT environment variable: {HTTP_PORT_STR}")
        return

    app = web.Application()
    app.router.add_get('/', handle_health_check)
    app.router.add_get('/healthz', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    try:
        await site.start()
        logging.info(f"Dummy HTTP server started on port {port}.")
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logging.info("Dummy HTTP server task cancelled.")
    except Exception as e:
        logging.error(f"Dummy HTTP server failed on port {port}: {e}", exc_info=True)
    finally:
        await runner.cleanup()


# --- Helper Functions ---
def create_category_keyboard(selected_categories: List[str] = None):
    if selected_categories is None:
        selected_categories = []
    builder = InlineKeyboardBuilder()
    for category in CATEGORIES:
        prefix = "✅ " if category in selected_categories else ""
        builder.button(text=f"{prefix}{category}", callback_data=f"category_{category}")
    builder.adjust(2)
    if 1 <= len(selected_categories) <= MAX_CATEGORIES:
         builder.row(InlineKeyboardButton(text=f"➡️ Done Selecting ({len(selected_categories)}/{MAX_CATEGORIES})", callback_data="category_done"))
    elif len(selected_categories) > MAX_CATEGORIES:
         builder.row(InlineKeyboardButton(text=f"⚠️ Too Many ({len(selected_categories)}/{MAX_CATEGORIES}) - Click to Confirm", callback_data="category_done"))
    builder.row(InlineKeyboardButton(text="❌ Cancel Selection", callback_data="category_cancel"))
    return builder.as_markup()

async def get_comment_reactions(comment_id: int) -> Tuple[int, int]:
    async with db.acquire() as conn:
        counts = await conn.fetchrow(
            "SELECT COALESCE(SUM(CASE WHEN reaction_type = 'like' THEN 1 ELSE 0 END), 0) AS likes, COALESCE(SUM(CASE WHEN reaction_type = 'dislike' THEN 1 ELSE 0 END), 0) AS dislikes FROM reactions WHERE comment_id = $1", 
            comment_id 
        )
        if counts:
            return counts['likes'], counts['dislikes']
    return 0, 0

async def get_user_points(user_id: int) -> int:
    async with db.acquire() as conn:
        points = await conn.fetchval("SELECT points FROM user_points WHERE user_id = $1", user_id)
        return points or 0

async def update_user_points(conn: asyncpg.Connection, user_id: int, delta: int):
    if delta == 0: 
        return
    await conn.execute(
        "INSERT INTO user_points (user_id, points) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET points = user_points.points + $2", 
        user_id, delta
    )

async def build_comment_keyboard(comment_id: int, commenter_user_id: int, viewer_user_id: int, confession_owner_id: int):
    likes, dislikes = await get_comment_reactions(comment_id)
    builder = InlineKeyboardBuilder()
    builder.button(text=f"👍 {likes}", callback_data=f"react_like_{comment_id}")
    builder.button(text=f"👎 {dislikes}", callback_data=f"react_dislike_{comment_id}")
    builder.button(text="↪️ Reply", callback_data=f"reply_{comment_id}")
    builder.button(text="⚠️", callback_data=f"report_confirm_{comment_id}")

    if viewer_user_id == confession_owner_id and viewer_user_id != commenter_user_id:
        builder.button(text="🤝 Request Contact", callback_data=f"req_contact_{comment_id}")
        builder.adjust(4, 1)
    else:
        builder.adjust(4)
    return builder.as_markup()

async def safe_send_message(user_id: int, text: str, **kwargs) -> Optional[types.Message]:
    try:
        return await bot.send_message(user_id, text, **kwargs)
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        if "bot was blocked" in str(e) or "user is deactivated" in str(e) or "chat not found" in str(e):
            logging.warning(f"Could not send message to user {user_id}: Blocked/deactivated.")
        else:
            logging.warning(f"Telegram API error sending to {user_id}: {e}")
    except TelegramRetryAfter as e:
        logging.warning(f"Flood control for {user_id}. Retrying after {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return await safe_send_message(user_id, text, **kwargs)
    except Exception as e:
        logging.error(f"Unexpected error sending message to {user_id}: {e}", exc_info=True)
    return None

async def update_channel_post_button(confession_id: int):
    global bot_info
    await asyncio.sleep(0.1)
    if not bot_info: 
        logging.error(f"No bot info for {confession_id} button update.")
        return
    
    async with db.acquire() as conn:
        conf_data = await conn.fetchrow(
            "SELECT message_id FROM confessions WHERE id = $1 AND status = 'approved'", 
            confession_id
        )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM comments WHERE confession_id = $1", 
            confession_id
        ) or 0
    
    if not conf_data or not conf_data['message_id']: 
        logging.debug(f"No approved conf/msg_id for {confession_id} button update.")
        return
    
    ch_msg_id = conf_data['message_id']
    link = f"https://t.me/{bot_info.username}?start=view_{confession_id}"
    markup = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"💬 View / Add Comments ({count})", url=link)
    ]])
    
    try:
        await bot.edit_message_reply_markup(
            chat_id=CHANNEL_ID, 
            message_id=ch_msg_id, 
            reply_markup=markup
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logging.info(f"Button for {confession_id} already updated ({count}).")
        elif "message to edit not found" in str(e).lower():
            logging.warning(f"Msg {ch_msg_id} not found in {CHANNEL_ID} (conf {confession_id}).")
        else:
            logging.error(f"Failed edit channel post {ch_msg_id} for conf {confession_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected err updating btn for conf {confession_id}: {e}", exc_info=True)

async def get_comment_sequence_number(conn: asyncpg.Connection, comment_id: int, confession_id: int) -> Optional[int]:
    query = """
        WITH ranked_comments AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC) as rn
            FROM comments
            WHERE confession_id = $1
        )
        SELECT rn FROM ranked_comments WHERE id = $2;
    """
    try:
        return await conn.fetchval(query, confession_id, comment_id)
    except Exception as e:
        logging.error(f"Could not fetch sequence number for comment {comment_id}: {e}")
        return None

async def show_comments_for_confession(user_id: int, confession_id: int, message_to_edit: Optional[types.Message] = None, page: int = 1):
    async with db.acquire() as conn:
        conf_data = await conn.fetchrow("SELECT status, user_id FROM confessions WHERE id = $1", confession_id)
        if not conf_data or conf_data['status'] != 'approved':
            err_txt = f"Confession #{confession_id} not found or not approved."
            if message_to_edit: 
                await message_to_edit.edit_text(err_txt, reply_markup=None)
            else: 
                await safe_send_message(user_id, err_txt)
            return
        confession_owner_id = conf_data['user_id']
        total_count = await conn.fetchval("SELECT COUNT(*) FROM comments WHERE confession_id = $1", confession_id) or 0
        if total_count == 0:
            msg_text = "<i>No comments yet. Be the first!</i>"
            if message_to_edit: 
                await message_to_edit.edit_text(msg_text, reply_markup=None)
            else: 
                await safe_send_message(user_id, msg_text)
            nav = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Add Comment", callback_data=f"add_{confession_id}")]])
            await safe_send_message(user_id, "You can add your own comment below:", reply_markup=nav)
            return

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PAGE_SIZE
        comments_raw = await conn.fetch("""
            SELECT c.id, c.user_id, c.text, c.sticker_file_id, c.animation_file_id, 
                   c.parent_comment_id, c.created_at, COALESCE(up.points, 0) as user_points 
            FROM comments c 
            LEFT JOIN user_points up ON c.user_id = up.user_id 
            WHERE c.confession_id = $1 
            ORDER BY c.created_at ASC 
            LIMIT $2 OFFSET $3
        """, confession_id, PAGE_SIZE, offset)

    db_id_to_message_id: Dict[int, int] = {}

    if comments_raw:
        for i, c_data_row in enumerate(comments_raw):
            c_data = dict(c_data_row)
            seq_num = offset + i + 1
            db_id = c_data['id']
            commenter_uid = c_data['user_id']
            
            medal_str = f" 🏅{c_data.get('user_points', 0)} Aura"
            tag = "(Author)" if commenter_uid == confession_owner_id else "(You)" if commenter_uid == user_id else "Anonymous"
            admin_info = f" [UID: <code>{commenter_uid}</code>]" if user_id == ADMIN_ID else ""
            display_tag = f" {tag}{medal_str}"

            reply_to_msg_id = None
            text_reply_prefix = ""
            parent_db_id = c_data.get('parent_comment_id')
            if parent_db_id:
                if parent_db_id in db_id_to_message_id:
                    reply_to_msg_id = db_id_to_message_id[parent_db_id]
                else: 
                    async with db.acquire() as conn_for_seq:
                        parent_seq_num = await get_comment_sequence_number(conn_for_seq, parent_db_id, confession_id)
                    
                    if parent_seq_num:
                        text_reply_prefix = f"↪️ <i>Replying to comment #{parent_seq_num}...</i>\n"
                    else:
                        text_reply_prefix = "↪️ <i>Replying to another comment...</i>\n"

            metadata_text = f"<i>#{seq_num}{display_tag}{admin_info}</i>"
            keyboard = await build_comment_keyboard(db_id, commenter_uid, user_id, confession_owner_id)
            
            try:
                if c_data['sticker_file_id']:
                    sent_message = await bot.send_sticker(user_id, sticker=c_data['sticker_file_id'], reply_to_message_id=reply_to_msg_id)
                    await bot.send_message(user_id, f"{text_reply_prefix}{metadata_text}", reply_markup=keyboard)
                elif c_data['animation_file_id']:
                    sent_message = await bot.send_animation(user_id, animation=c_data['animation_file_id'], reply_to_message_id=reply_to_msg_id)
                    await bot.send_message(user_id, f"{text_reply_prefix}{metadata_text}", reply_markup=keyboard)
                elif c_data['text']:
                    full_text = f"{text_reply_prefix}💬 {html.quote(c_data['text'])}\n\n{metadata_text}"
                    sent_message = await bot.send_message(user_id, full_text, reply_markup=keyboard, disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
                
                if sent_message:
                    db_id_to_message_id[db_id] = sent_message.message_id
            except Exception as e:
                logging.warning(f"Could not send comment #{seq_num} to {user_id}: {e}")
                await safe_send_message(user_id, f"⚠️ Error displaying comment #{seq_num}.")
            await asyncio.sleep(0.1)

    nav_row = []
    if page > 1: 
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"comments_page_{confession_id}_{page-1}"))
    if total_pages > 1: 
        nav_row.append(InlineKeyboardButton(text=f"Page {page}/{total_pages}", callback_data="noop"))
    if page < total_pages: 
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"comments_page_{confession_id}_{page+1}"))
    
    nav_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            nav_row, 
            [InlineKeyboardButton(text="➕ Add Comment", callback_data=f"add_{confession_id}")]
        ]
    )
    end_txt = f"--- Showing comments {offset+1} to {min(offset+PAGE_SIZE, total_count)} of {total_count} for Confession #{confession_id} ---"
    await safe_send_message(user_id, end_txt, reply_markup=nav_keyboard)


# --- Middleware to check for blocked users ---
class BlockUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get('event_from_user')
        if not user:
            return await handler(event, data)

        user_id = user.id
        if user_id == ADMIN_ID:
            return await handler(event, data)

        async with db.acquire() as conn:
            status = await conn.fetchrow("SELECT is_blocked, blocked_until, block_reason FROM user_status WHERE user_id = $1", user_id)
        
        if status and status['is_blocked']:
            now = datetime.now(timezone.utc)
            if status['blocked_until'] and status['blocked_until'] < now:
                async with db.acquire() as conn:
                    await conn.execute("UPDATE user_status SET is_blocked = FALSE, blocked_until = NULL, block_reason = NULL WHERE user_id = $1", user_id)
                return await handler(event, data)
            else:
                expiry_info = f"until {status['blocked_until'].strftime('%Y-%m-%d %H:%M %Z')}" if status['blocked_until'] else "permanently"
                reason_info = f"\nReason: <i>{html.quote(status['block_reason'])}</i>" if status['block_reason'] else ""
                
                if isinstance(event, types.CallbackQuery):
                    await event.answer(f"You are blocked {expiry_info}.", show_alert=True)
                elif isinstance(event, types.Message):
                    await event.answer(f"❌ <b>You are blocked from using this bot {expiry_info}.</b>{reason_info}")
                return

        return await handler(event, data)


# --- Handlers ---

@dp.message(Command("rules"))
async def show_rules(message: types.Message):
    rules_text = (
        "<b>📜 Bot Rules & Regulations</b>\n\n"
        "1. <b>Stay Relevant:</b> This space is for sharing confessions and thoughts.\n"
        "2. <b>Respectful Communication:</b> Be respectful in all interactions.\n"
        "3. <b>No Harmful Content:</b> No hate speech, harassment, or bullying.\n"
        "4. <b>Privacy:</b> Don't share personal information about others.\n"
        "5. <b>No Spam:</b> Keep confessions genuine and meaningful.\n\n"
        "<i>Violations may result in being blocked from the bot.</i>"
    )
    await message.answer(rules_text)

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext, command: Optional[CommandObject] = None):
    await state.clear()
    user_id = message.from_user.id

    async with db.acquire() as conn:
        has_accepted = await conn.fetchval("SELECT has_accepted_rules FROM user_status WHERE user_id = $1", user_id)

    if not has_accepted:
        rules_text = (
            "<b>📜 Bot Rules & Regulations</b>\n\n"
            "1. <b>Stay Relevant:</b> This space is for sharing confessions and thoughts.\n"
            "2. <b>Respectful Communication:</b> Be respectful in all interactions.\n"
            "3. <b>No Harmful Content:</b> No hate speech, harassment, or bullying.\n"
            "4. <b>Privacy:</b> Don't share personal information about others.\n"
            "5. <b>No Spam:</b> Keep confessions genuine and meaningful.\n\n"
            "<i>Violations may result in being blocked from the bot.</i>"
        )
        accept_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ I Accept the Rules", callback_data="accept_rules")]
        ])
        await message.answer(rules_text, reply_markup=accept_keyboard)
        return

    deep_link_args = command.args if command else None
    if deep_link_args and deep_link_args.startswith("view_"):
        try:
            conf_id = int(deep_link_args.split("_", 1)[1])
            async with db.acquire() as conn:
                conf_data = await conn.fetchrow("""
                    SELECT c.text, c.categories, c.status, c.user_id, c.photo_file_id, 
                           COUNT(com.id) as comment_count 
                    FROM confessions c 
                    LEFT JOIN comments com ON c.id = com.confession_id 
                    WHERE c.id = $1 
                    GROUP BY c.id
                """, conf_id)
            
            if not conf_data or conf_data['status'] != 'approved':
                await message.answer(f"Confession #{conf_id} not found or not approved.")
                return
            
            comm_count = conf_data['comment_count']
            categories = conf_data['categories'] or []
            category_tags = " ".join([f"#{html.quote(cat)}" for cat in categories]) if categories else ""
            
            builder = InlineKeyboardBuilder()
            builder.button(text="➕ Add Comment", callback_data=f"add_{conf_id}")
            builder.button(text=f"💬 Browse Comments ({comm_count})", callback_data=f"browse_{conf_id}")
            builder.adjust(1)
            
            if conf_data['photo_file_id']:
                caption = f"<b>Confession #{conf_id}</b>\n\n{html.quote(conf_data['text'])}\n\n{category_tags}"
                await bot.send_photo(
                    chat_id=user_id,
                    photo=conf_data['photo_file_id'],
                    caption=caption,
                    reply_markup=builder.as_markup()
                )
            else:
                txt = f"<b>Confession #{conf_id}</b>\n\n{html.quote(conf_data['text'])}\n\n{category_tags}"
                await message.answer(txt, reply_markup=builder.as_markup())
        except Exception as e:
            logging.error(f"Error handling deep link: {e}")
            await message.answer("Error processing link.")
    else:
        welcome_text = (
            "👋 <b>Welcome to the Confession Bot!</b>\n\n"
            "Here's what you can do:\n\n"
            "📝 <b>/confess</b> - Share anonymous confession\n"
            "💬 <b>/feedback</b> - Send feedback to admin\n"
            "🤝 <b>/support</b> - Chat privately with admin\n"
            "👤 <b>/profile</b> - View your history\n"
            "❓ <b>/help</b> - Show all commands"
        )
        await message.answer(welcome_text)

@dp.callback_query(F.data == "accept_rules")
async def handle_accept_rules(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    async with db.acquire() as conn:
        await conn.execute(
            """INSERT INTO user_status (user_id, has_accepted_rules) VALUES ($1, TRUE)
               ON CONFLICT (user_id) DO UPDATE SET has_accepted_rules = TRUE""",
            user_id
        )
    await callback_query.message.edit_text(
        "Thank you for accepting the rules! You can now use the bot.\n\n"
        "Use /confess to share anonymously, /profile to see your history, or /help for more info."
    )
    await callback_query.answer("Rules accepted!")

@dp.message(Command("help"))
async def show_help(message: types.Message):
    help_text = (
        "<b>🤖 Bot Commands</b>\n\n"
        "🔹 /confess - Submit anonymous confession\n"
        "🔹 /profile - View your profile and history\n"
        "🔹 /feedback - Send feedback to admin\n"
        "🔹 /support - Start private chat with admin\n"
        "🔹 /feedback_status - Check your feedback status\n"
        "🔹 /endsupport - End support chat\n"
        "🔹 /rules - View bot rules\n"
        "🔹 /privacy - Privacy information\n"
        "🔹 /help - Show this help\n\n"
        "<b>Comment Features:</b>\n"
        "👍/👎 - Like/Dislike comments\n"
        "↪️ Reply - Reply to comments\n"
        "⚠️ Report - Report inappropriate comments"
    )
    
    if message.from_user and message.from_user.id == ADMIN_ID:
        help_text += (
            "\n\n<b>👑 Admin Commands:</b>\n"
            "🔹 /id <user_id> - Get user info\n"
            "🔹 /warn <user_id> <reason> - Warn user\n"
            "🔹 /block <user_id> <duration> - Temp block (7d, 2w)\n"
            "🔹 /pblock <user_id> - Permanent block\n"
            "🔹 /unblock <user_id> - Unblock user\n"
            "🔹 /feedback_stats - View feedback stats\n"
            "🔹 /broadcast <message> - Message all users\n"
            "🔹 /active_chats - View active support chats"
        )
    
    await message.answer(help_text)

@dp.message(Command("privacy"))
async def show_privacy(message: types.Message):
    privacy_text = (
        "<b>🔒 Privacy Information</b>\n\n"
        "▪️ Your Telegram User ID is stored but never shown publicly\n"
        "▪️ Comments are anonymous - only your medal points are visible\n"
        "▪️ Feedback messages are private between you and admin\n"
        "▪️ Support chats are private and deleted after 30 days\n"
        "▪️ Admins can access stored data for moderation\n\n"
        "We value your privacy and anonymity."
    )
    await message.answer(privacy_text)

@dp.message(Command("cancel"), StateFilter('*'))
async def cancel_any_state(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Action cancelled.", reply_markup=ReplyKeyboardRemove())

@dp.callback_query(F.data == "noop")
async def handle_noop(callback_query: types.CallbackQuery):
    await callback_query.answer()


# --- Profile Handlers ---
def create_profile_pagination_keyboard(base_callback: str, current_page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    row = []
    if current_page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"{base_callback}_{current_page - 1}"))
    if total_pages > 1:
        row.append(InlineKeyboardButton(text=f"Page {current_page}/{total_pages}", callback_data="noop"))
    if current_page < total_pages:
        row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"{base_callback}_{current_page + 1}"))
    if row:
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_menu_main_1"))
    return builder.as_markup()

@dp.message(Command("profile"))
async def user_profile(message: types.Message):
    user_id = message.from_user.id
    points = await get_user_points(user_id)

    profile_text = f"👤 <b>Your Profile</b>\n\n🏅 <b>Medal Points:</b> {points}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 My Confessions", callback_data="profile_menu_confessions_1")],
        [InlineKeyboardButton(text="💬 My Comments", callback_data="profile_menu_comments_1")]
    ])
    await message.answer(profile_text, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("profile_menu_"))
async def handle_profile_menu(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split("_")
    action = parts[2]
    page = int(parts[-1])

    try:
        if action == "main":
            points = await get_user_points(user_id)
            profile_text = f"👤 <b>Your Profile</b>\n\n🏅 <b>Medal Points:</b> {points}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 My Confessions", callback_data="profile_menu_confessions_1")],
                [InlineKeyboardButton(text="💬 My Comments", callback_data="profile_menu_comments_1")]
            ])
            await callback_query.message.edit_text(profile_text, reply_markup=keyboard)

        elif action == "confessions":
            async with db.acquire() as conn:
                total_count = await conn.fetchval("SELECT COUNT(*) FROM confessions WHERE user_id = $1", user_id) or 0
                if total_count == 0:
                    await callback_query.answer("No confessions yet.", show_alert=True)
                    return

                total_pages = (total_count + 5 - 1) // 5
                page = max(1, min(page, total_pages))
                offset = (page - 1) * 5
                confessions = await conn.fetch("""
                    SELECT id, text, status, created_at, photo_file_id 
                    FROM confessions 
                    WHERE user_id = $1 
                    ORDER BY created_at DESC 
                    LIMIT 5 OFFSET $2
                """, user_id, offset)

            response_text = f"<b>📜 Your Confessions (Page {page}/{total_pages})</b>\n\n"
            builder = InlineKeyboardBuilder()
            for conf in confessions:
                snippet = html.quote(conf['text'][:60]) + ('...' if len(conf['text']) > 60 else '')
                status_emoji = {"approved": "✅", "pending": "⏳", "rejected": "❌"}.get(conf['status'], "❓")
                photo_indicator = " 📷" if conf['photo_file_id'] else ""
                response_text += f"<b>#{conf['id']}</b> {status_emoji}{photo_indicator}\n<i>\"{snippet}\"</i>\n\n"
                if conf['status'] in ['approved', 'pending']:
                    builder.row(InlineKeyboardButton(text=f"Request Deletion #{conf['id']}", callback_data=f"req_del_conf_{conf['id']}"))

            nav_keyboard = create_profile_pagination_keyboard("profile_menu_confessions", page, total_pages)
            final_markup = builder.attach(InlineKeyboardBuilder.from_markup(nav_keyboard)).as_markup()
            await callback_query.message.edit_text(response_text, reply_markup=final_markup)

        elif action == "comments":
            async with db.acquire() as conn:
                total_count = await conn.fetchval("SELECT COUNT(*) FROM comments WHERE user_id = $1", user_id) or 0
                if total_count == 0:
                    await callback_query.answer("No comments yet.", show_alert=True)
                    return

                total_pages = (total_count + 5 - 1) // 5
                page = max(1, min(page, total_pages))
                offset = (page - 1) * 5
                comments = await conn.fetch("""
                    SELECT id, text, sticker_file_id, animation_file_id, confession_id, created_at 
                    FROM comments 
                    WHERE user_id = $1 
                    ORDER BY created_at DESC 
                    LIMIT 5 OFFSET $2
                """, user_id, offset)

            response_text = f"<b>💬 Your Comments (Page {page}/{total_pages})</b>\n\n"
            for comm in comments:
                if comm['text']: 
                    snippet = "💬 " + html.quote(comm['text'][:60]) + ('...' if len(comm['text']) > 60 else '')
                elif comm['sticker_file_id']: 
                    snippet = "[Sticker]"
                elif comm['animation_file_id']: 
                    snippet = "[GIF]"
                else: 
                    snippet = "[Unknown]"
                link = f"https://t.me/{bot_info.username}?start=view_{comm['confession_id']}"
                response_text += f"On <a href='{link}'>#{comm['confession_id']}</a>:\n<i>\"{snippet}\"</i>\n\n"

            nav_keyboard = create_profile_pagination_keyboard("profile_menu_comments", page, total_pages)
            await callback_query.message.edit_text(response_text, reply_markup=nav_keyboard, disable_web_page_preview=True)
    
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise
    finally:
        await callback_query.answer()

@dp.callback_query(F.data.startswith("req_del_conf_"))
async def request_deletion_prompt(callback_query: types.CallbackQuery):
    conf_id = int(callback_query.data.split("_")[-1])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Request Deletion", callback_data=f"confirm_del_conf_{conf_id}")],
        [InlineKeyboardButton(text="❌ No, Cancel", callback_data="profile_menu_confessions_1")]
    ])
    await callback_query.message.edit_text(
        f"Request deletion of Confession #{conf_id}? This action requires admin approval.",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_del_conf_"))
async def confirm_deletion_request(callback_query: types.CallbackQuery):
    conf_id = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id

    async with db.acquire() as conn:
        try:
            conf_data = await conn.fetchrow("SELECT user_id, text, status FROM confessions WHERE id = $1", conf_id)
            if not conf_data or conf_data['user_id'] != user_id:
                await callback_query.answer("Not your confession.", show_alert=True)
                return
            if conf_data['status'] not in ['approved', 'pending']:
                await callback_query.answer(f"Cannot delete (status: {conf_data['status']}).", show_alert=True)
                return

            await conn.execute(
                """INSERT INTO deletion_requests (confession_id, user_id, status) VALUES ($1, $2, 'pending')
                   ON CONFLICT (confession_id, user_id) DO NOTHING""", 
                conf_id, user_id
            )

            admin_text = (f"🗑️ <b>New Deletion Request</b>\n\n"
                          f"User ID: <code>{user_id}</code>\n"
                          f"Confession ID: <code>{conf_id}</code>")
            admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"admin_approve_delete_{conf_id}")],
                [InlineKeyboardButton(text="❌ Reject", callback_data=f"admin_reject_delete_{conf_id}")]
            ])
            await bot.send_message(ADMIN_ID, admin_text, reply_markup=admin_keyboard)
            await callback_query.answer("✅ Deletion request sent to admin.", show_alert=True)
            
            callback_query.data = "profile_menu_confessions_1"
            await handle_profile_menu(callback_query)

        except asyncpg.exceptions.UniqueViolationError:
            await callback_query.answer("Already requested deletion.", show_alert=True)
        except Exception as e:
            logging.error(f"Error processing deletion request: {e}")
            await callback_query.answer("An error occurred.", show_alert=True)


# --- Confession Submission Flow ---
@dp.message(Command("confess"), StateFilter(None))
async def start_confession(message: types.Message, state: FSMContext):
    await state.update_data(selected_categories=[])
    await message.answer(
        f"📝 <b>Confession Submission</b>\n\n"
        f"Choose 1 to {MAX_CATEGORIES} categories. Click 'Done' when finished.\n\n"
        f"<i>After selecting categories, send your confession text or photo with caption.</i>",
        reply_markup=create_category_keyboard([])
    )
    await state.set_state(ConfessionForm.selecting_categories)

@dp.callback_query(StateFilter(ConfessionForm.selecting_categories), F.data.startswith("category_"))
async def handle_category_selection(callback_query: types.CallbackQuery, state: FSMContext):
    action = callback_query.data.split("_", 1)[1]
    user_data = await state.get_data()
    selected_categories: List[str] = user_data.get("selected_categories", [])
    
    if action == "cancel":
        await state.clear()
        await callback_query.message.edit_text("Submission cancelled.")
        return
    
    if action == "done":
        if not selected_categories:
            await callback_query.answer("Select at least 1 category.", show_alert=True)
            return
        if len(selected_categories) > MAX_CATEGORIES:
            await callback_query.answer(f"Max {MAX_CATEGORIES} categories.", show_alert=True)
            return
        await state.set_state(ConfessionForm.waiting_for_text)
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in selected_categories])
        await callback_query.message.edit_text(
            f"✅ Categories: {category_tags}\n\n"
            f"Now send your confession text or photo with caption.\n"
            f"<i>Type /cancel to abort.</i>"
        )
        await callback_query.answer()
        return
    
    if action in CATEGORIES:
        if action in selected_categories:
            selected_categories.remove(action)
        elif len(selected_categories) < MAX_CATEGORIES:
            selected_categories.append(action)
        else:
            await callback_query.answer(f"Max {MAX_CATEGORIES} categories.", show_alert=True)
            return
        await state.update_data(selected_categories=selected_categories)
        await callback_query.message.edit_reply_markup(reply_markup=create_category_keyboard(selected_categories))
        await callback_query.answer(f"'{action}' {'selected' if action in selected_categories else 'deselected'}.")

@dp.message(ConfessionForm.waiting_for_text, F.text)
async def receive_text_confession(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    await process_confession(message, state, text=message.text, photo_file_id=None)

@dp.message(ConfessionForm.waiting_for_text, F.photo)
async def receive_photo_confession(message: types.Message, state: FSMContext):
    photo_file_id = message.photo[-1].file_id
    text = message.caption or ""
    
    if not text.strip():
        await message.answer("❌ Please add a caption to your photo.")
        return
    
    await process_confession(message, state, text=text, photo_file_id=photo_file_id)

async def process_confession(message: types.Message, state: FSMContext, text: str, photo_file_id: Optional[str] = None):
    user_id = message.from_user.id
    state_data = await state.get_data()
    selected_categories: List[str] = state_data.get("selected_categories", [])
    
    if not selected_categories:
        await message.answer("⚠️ Error: Please start again with /confess.")
        await state.clear()
        return
    
    if len(text) < 10:
        await message.answer("❌ Confession too short (min 10 chars).")
        return
    
    if len(text) > 3900:
        await message.answer("❌ Confession too long (max 3900 chars).")
        return
    
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                if photo_file_id:
                    conf_id = await conn.fetchval(
                        "INSERT INTO confessions (text, user_id, categories, status, photo_file_id) VALUES ($1, $2, $3::text[], 'pending', $4) RETURNING id", 
                        text, user_id, selected_categories, photo_file_id
                    )
                else:
                    conf_id = await conn.fetchval(
                        "INSERT INTO confessions (text, user_id, categories, status) VALUES ($1, $2, $3::text[], 'pending') RETURNING id", 
                        text, user_id, selected_categories
                    )
                
                await update_user_points(conn, user_id, POINTS_PER_CONFESSION)
        
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in selected_categories])
        
        if photo_file_id:
            admin_caption = (
                f"🖼️ <b>New Photo Confession</b>\n"
                f"ID: {conf_id}\n"
                f"User: <code>{user_id}</code>\n"
                f"Categories: {category_tags}\n\n"
                f"{html.quote(text)}"
            )
            kbd = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{conf_id}")],
                [InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{conf_id}")]
            ])
            await bot.send_photo(ADMIN_ID, photo=photo_file_id, caption=admin_caption, reply_markup=kbd)
            await message.answer(f"✅ Photo confession #{conf_id} submitted for review.")
        else:
            admin_msg = (
                f"<b>New Confession</b>\n"
                f"ID: {conf_id}\n"
                f"User: <code>{user_id}</code>\n"
                f"Categories: {category_tags}\n\n"
                f"{html.quote(text)}"
            )
            kbd = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{conf_id}")],
                [InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{conf_id}")]
            ])
            await bot.send_message(ADMIN_ID, admin_msg, reply_markup=kbd)
            await message.answer("✅ Confession submitted for review.")
        
        logging.info(f"Confession #{conf_id} submitted by user {user_id}")
        
    except Exception as e:
        logging.error(f"Error processing confession: {e}")
        await message.answer("❌ An error occurred.")
    finally: 
        await state.clear()


# --- Admin Action Handlers ---
@dp.callback_query(F.data.startswith("approve_"))
async def handle_approve_confession(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    try:
        conf_id = int(callback_query.data.split("_")[1])
    except (IndexError, ValueError):
        await callback_query.answer("Invalid ID.", show_alert=True)
        return
    
    async with db.acquire() as conn:
        conf = await conn.fetchrow(
            "SELECT id, text, user_id, categories, photo_file_id FROM confessions WHERE id = $1 AND status = 'pending'", 
            conf_id
        )
        
        if not conf:
            await callback_query.answer("Confession not found or already processed.", show_alert=True)
            return
    
    try:
        link = f"https://t.me/{bot_info.username}?start=view_{conf['id']}"
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in conf['categories'] or []])
        
        if conf['photo_file_id']:
            caption = f"<b>Confession #{conf['id']}</b>\n\n{html.quote(conf['text'])}\n\n{category_tags}"
            kbd = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💬 View Comments (0)", url=link)
            ]])
            msg = await bot.send_photo(CHANNEL_ID, photo=conf['photo_file_id'], caption=caption, reply_markup=kbd)
        else:
            text = f"<b>Confession #{conf['id']}</b>\n\n{html.quote(conf['text'])}\n\n{category_tags}"
            kbd = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💬 View Comments (0)", url=link)
            ]])
            msg = await bot.send_message(CHANNEL_ID, text, reply_markup=kbd)
        
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE confessions SET status = 'approved', message_id = $1 WHERE id = $2",
                msg.message_id, conf_id
            )
        
        await safe_send_message(conf['user_id'], f"✅ Your confession (#{conf_id}) has been approved!")
        await callback_query.message.edit_text(callback_query.message.html_text + "\n\n✅ Approved")
        await callback_query.answer(f"Confession #{conf_id} approved.")
        
    except Exception as e:
        logging.error(f"Error approving confession: {e}")
        await callback_query.answer(f"Error: {str(e)[:50]}", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def handle_reject_confession(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    try:
        conf_id = int(callback_query.data.split("_")[1])
    except (IndexError, ValueError):
        await callback_query.answer("Invalid ID.", show_alert=True)
        return
    
    await state.update_data(
        rejecting_conf_id=conf_id,
        original_text=callback_query.message.html_text,
        msg_id=callback_query.message.message_id
    )
    
    await state.set_state(AdminActions.waiting_for_rejection_reason)
    
    await callback_query.answer()
    await bot.send_message(
        callback_query.from_user.id,
        f"Reason for rejecting #{conf_id}? (or /skip, /cancel)"
    )

@dp.message(AdminActions.waiting_for_rejection_reason, F.text)
async def receive_rejection_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    conf_id = data.get("rejecting_conf_id")
    original_text = data.get("original_text")
    msg_id = data.get("msg_id")

    if not conf_id:
        await message.answer("Error: Context lost.")
        await state.clear()
        return
    
    reason = None
    reason_text = "Your confession was rejected."
    
    if message.text == "/skip":
        await message.answer("Skipping reason.")
    elif message.text == "/cancel":
        await message.answer("Rejection cancelled.")
        await state.clear()
        return
    else:
        reason = message.text.strip()
        reason_text = f"Your confession was rejected for:\n<i>{html.quote(reason)}</i>"
    
    async with db.acquire() as conn:
        conf_data = await conn.fetchrow(
            "SELECT user_id, categories FROM confessions WHERE id = $1 AND status = 'pending'", 
            conf_id
        )
        if conf_data:
            await conn.execute(
                "UPDATE confessions SET status = 'rejected', rejection_reason = $1 WHERE id = $2", 
                reason, conf_id
            )
            await safe_send_message(conf_data['user_id'], f"❌ {reason_text}")
    
    try:
        await bot.edit_message_text(
            original_text + f"\n\n❌ Rejected" + (f"\nReason: {html.quote(reason)}" if reason else ""),
            chat_id=ADMIN_ID,
            message_id=msg_id
        )
    except Exception as e:
        logging.error(f"Could not edit admin message: {e}")
    
    await message.answer(f"Confession #{conf_id} rejected.")
    await state.clear()


# --- Admin Deletion Request Handlers ---
@dp.callback_query(F.data.startswith(("admin_approve_delete_", "admin_reject_delete_")))
async def admin_handle_deletion_request(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("Unauthorized.", show_alert=True)
        return

    parts = callback_query.data.split("_")
    action = parts[1]
    conf_id = int(parts[-1])

    async with db.acquire() as conn:
        async with conn.transaction():
            req_data = await conn.fetchrow(
                "SELECT id, user_id FROM deletion_requests WHERE confession_id = $1 AND status = 'pending'", 
                conf_id
            )
            if not req_data:
                await callback_query.answer("Request not found.", show_alert=True)
                return

            if action == "approve":
                conf = await conn.fetchrow("SELECT message_id FROM confessions WHERE id = $1", conf_id)
                if conf:
                    await conn.execute("DELETE FROM confessions WHERE id = $1", conf_id)
                    try:
                        if conf['message_id']:
                            await bot.delete_message(CHANNEL_ID, conf['message_id'])
                    except Exception as e:
                        logging.warning(f"Could not delete channel message: {e}")
                await conn.execute(
                    "UPDATE deletion_requests SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP WHERE id = $1", 
                    req_data['id']
                )
                await safe_send_message(req_data['user_id'], f"✅ Deletion request for #{conf_id} approved.")
                await callback_query.message.edit_text(
                    callback_query.message.html_text + "\n\n✅ Deletion Approved"
                )
            else:
                await conn.execute(
                    "UPDATE deletion_requests SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP WHERE id = $1", 
                    req_data['id']
                )
                await safe_send_message(req_data['user_id'], f"❌ Deletion request for #{conf_id} rejected.")
                await callback_query.message.edit_text(
                    callback_query.message.html_text + "\n\n❌ Deletion Rejected"
                )
    
    await callback_query.answer()


# --- Commenting Flow Handlers ---
@dp.callback_query(F.data.startswith("browse_"))
async def browse_comments_action(callback_query: types.CallbackQuery):
    conf_id = int(callback_query.data.split("_", 1)[1])
    await callback_query.answer("Loading comments...")
    await show_comments_for_confession(callback_query.from_user.id, conf_id, callback_query.message)

@dp.callback_query(F.data.startswith("add_"))
async def add_comment_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    conf_id = int(callback_query.data.split("_", 1)[1])
    await state.update_data(confession_id=conf_id, parent_comment_id=None)
    await state.set_state(CommentForm.waiting_for_comment)
    await safe_send_message(
        callback_query.from_user.id, 
        f"📝 Add comment to #{conf_id}.\nSend text, sticker, or GIF. /cancel to abort."
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("comments_page_"))
async def comments_page_callback(callback_query: types.CallbackQuery):
    parts = callback_query.data.split("_")
    conf_id = int(parts[2])
    page = int(parts[3])
    await callback_query.answer("Loading page...")
    await show_comments_for_confession(callback_query.from_user.id, conf_id, callback_query.message, page=page)

@dp.message(CommentForm.waiting_for_comment, F.text | F.sticker | F.animation)
async def receive_comment(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    conf_id = data.get("confession_id")
    
    if not conf_id:
        await message.answer("⚠️ Error: Context lost.")
        await state.clear()
        return
    
    comm_text = None
    sticker_id = None
    animation_id = None
    content_type = "text"
    
    if message.text:
        comm_text = message.text.strip()
        content_type = "text"
    elif message.sticker:
        sticker_id = message.sticker.file_id
        content_type = "sticker"
    elif message.animation:
        animation_id = message.animation.file_id
        content_type = "gif"
    
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                conf_owner_id = await conn.fetchval(
                    "SELECT user_id FROM confessions WHERE id = $1 AND status = 'approved'", 
                    conf_id
                )
                if not conf_owner_id:
                    raise Exception("Confession not found or not approved")
                
                await conn.execute(
                    """INSERT INTO comments (confession_id, user_id, text, sticker_file_id, animation_file_id) 
                       VALUES ($1, $2, $3, $4, $5)""",
                    conf_id, user_id, comm_text, sticker_id, animation_id
                )
        
        await message.answer("💬 Comment added!")
        await update_channel_post_button(conf_id)
        
        if conf_owner_id and conf_owner_id != user_id:
            link = f"https://t.me/{bot_info.username}?start=view_{conf_id}"
            preview = comm_text[:100] + "..." if comm_text and len(comm_text) > 100 else comm_text or f"[{content_type}]"
            await safe_send_message(
                conf_owner_id,
                f"💬 New comment on your confession #{conf_id}.\n\n<i>{html.quote(preview)}</i>\n\n<a href='{link}'>View</a>",
                disable_web_page_preview=True
            )
        
        await show_comments_for_confession(user_id, conf_id)
    except Exception as e:
        logging.error(f"Error saving comment: {e}")
        await message.answer("❌ Error saving comment.")
    finally:
        await state.clear()

@dp.callback_query(F.data.startswith("reply_"))
async def reply_comment_prompt(callback_query: types.CallbackQuery, state: FSMContext):
    parent_id = int(callback_query.data.split("_", 1)[1])
    
    async with db.acquire() as conn:
        comm_data = await conn.fetchrow(
            "SELECT confession_id, user_id FROM comments WHERE id = $1", 
            parent_id
        )
    
    if not comm_data:
        await callback_query.answer("Comment not found.", show_alert=True)
        return
    
    if callback_query.from_user.id == comm_data['user_id']:
        await callback_query.answer("Cannot reply to yourself.", show_alert=True)
        return
    
    await state.update_data(
        confession_id=comm_data['confession_id'],
        parent_comment_id=parent_id
    )
    await state.set_state(CommentForm.waiting_for_reply)
    
    await safe_send_message(
        callback_query.from_user.id,
        "↪️ Send your reply (text, sticker, or GIF). /cancel to abort."
    )
    await callback_query.answer()

@dp.message(CommentForm.waiting_for_reply, F.text | F.sticker | F.animation)
async def receive_reply(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    conf_id = data.get("confession_id")
    parent_id = data.get("parent_comment_id")
    
    if not all([conf_id, parent_id]):
        await message.answer("⚠️ Error: Context lost.")
        await state.clear()
        return
    
    reply_text = None
    sticker_id = None
    animation_id = None
    
    if message.text:
        reply_text = message.text.strip()
    elif message.sticker:
        sticker_id = message.sticker.file_id
    elif message.animation:
        animation_id = message.animation.file_id
    else:
        await message.answer("Please send text, sticker, or GIF.")
        return
    
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                parent_data = await conn.fetchrow(
                    "SELECT user_id FROM comments WHERE id = $1", 
                    parent_id
                )
                if not parent_data:
                    await message.answer("⚠️ Original comment deleted.")
                    await state.clear()
                    return
                
                conf_data = await conn.fetchrow(
                    "SELECT user_id FROM confessions WHERE id = $1", 
                    conf_id
                )
                
                await conn.execute(
                    """INSERT INTO comments (confession_id, user_id, text, sticker_file_id, animation_file_id, parent_comment_id) 
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    conf_id, user_id, reply_text, sticker_id, animation_id, parent_id
                )
        
        await message.answer("↪️ Reply sent!")
        await update_channel_post_button(conf_id)
        
        if parent_data['user_id'] != user_id:
            link = f"https://t.me/{bot_info.username}?start=view_{conf_id}"
            tag = "(Author)" if user_id == conf_data['user_id'] else "Anonymous"
            await safe_send_message(
                parent_data['user_id'],
                f"↪️ {tag} replied to your comment.\n\n<a href='{link}'>View</a>",
                disable_web_page_preview=True
            )
        
        await show_comments_for_confession(user_id, conf_id)
    except Exception as e:
        logging.error(f"Error saving reply: {e}")
        await message.answer("❌ Error saving reply.")
    finally:
        await state.clear()


# --- Reaction Handling ---
@dp.callback_query(F.data.startswith("react_"))
async def handle_reaction(callback_query: types.CallbackQuery):
    parts = callback_query.data.split("_")
    r_type = parts[1]
    comm_id = int(parts[2])
    user_id = callback_query.from_user.id
    
    async with db.acquire() as conn:
        async with conn.transaction():
            info = await conn.fetchrow(
                """SELECT c.user_id as comment_owner, co.user_id as confession_owner 
                   FROM comments c 
                   JOIN confessions co ON c.confession_id = co.id 
                   WHERE c.id = $1""", 
                comm_id
            )
            
            if not info:
                await callback_query.answer("Comment not found.", show_alert=True)
                return
            
            if info['comment_owner'] == user_id:
                await callback_query.answer("Cannot react to own comment.", show_alert=True)
                return
            
            existing = await conn.fetchval(
                "SELECT reaction_type FROM reactions WHERE comment_id = $1 AND user_id = $2",
                comm_id, user_id
            )
            
            point_delta = 0
            alert = ""
            
            if existing:
                if existing == r_type:  # Remove reaction
                    await conn.execute(
                        "DELETE FROM reactions WHERE comment_id = $1 AND user_id = $2",
                        comm_id, user_id
                    )
                    point_delta = -POINTS_PER_LIKE_RECEIVED if r_type == 'like' else -POINTS_PER_DISLIKE_RECEIVED
                    alert = f"{r_type.capitalize()} removed"
                else:  # Change reaction
                    await conn.execute(
                        "UPDATE reactions SET reaction_type = $1 WHERE comment_id = $2 AND user_id = $3",
                        r_type, comm_id, user_id
                    )
                    point_delta = (POINTS_PER_LIKE_RECEIVED - POINTS_PER_DISLIKE_RECEIVED) if r_type == 'like' else (POINTS_PER_DISLIKE_RECEIVED - POINTS_PER_LIKE_RECEIVED)
                    alert = f"Changed to {r_type}"
            else:  # Add new reaction
                await conn.execute(
                    "INSERT INTO reactions (comment_id, user_id, reaction_type) VALUES ($1, $2, $3)",
                    comm_id, user_id, r_type
                )
                point_delta = POINTS_PER_LIKE_RECEIVED if r_type == 'like' else POINTS_PER_DISLIKE_RECEIVED
                alert = f"{r_type.capitalize()} added"
            
            if point_delta != 0:
                await update_user_points(conn, info['comment_owner'], point_delta)
    
    kbd = await build_comment_keyboard(comm_id, info['comment_owner'], user_id, info['confession_owner'])
    try:
        await callback_query.message.edit_reply_markup(reply_markup=kbd)
        await callback_query.answer(alert)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logging.warning(f"Could not edit markup: {e}")
        await callback_query.answer(alert)


# --- Report Handlers ---
@dp.callback_query(F.data.startswith("report_confirm_"))
async def report_confirm(callback_query: types.CallbackQuery):
    comment_id = int(callback_query.data.split("_")[-1])
    
    async with db.acquire() as conn:
        comment_data = await conn.fetchrow(
            "SELECT text, user_id FROM comments WHERE id = $1", 
            comment_id
        )
        
        if not comment_data:
            await callback_query.answer("Comment deleted.", show_alert=True)
            return
        
        if comment_data['user_id'] == callback_query.from_user.id:
            await callback_query.answer("Cannot report yourself.", show_alert=True)
            return
    
    confirm_kbd = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Yes, Report", callback_data=f"report_execute_{comment_id}"),
            InlineKeyboardButton(text="❌ No", callback_data="report_cancel")
        ]
    ])
    
    await safe_send_message(
        callback_query.from_user.id,
        "Are you sure you want to report this comment?",
        reply_markup=confirm_kbd
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("report_execute_"))
async def report_execute(callback_query: types.CallbackQuery):
    comment_id = int(callback_query.data.split("_")[-1])
    reporter_id = callback_query.from_user.id
    
    try:
        async with db.acquire() as conn:
            comment_data = await conn.fetchrow(
                "SELECT user_id, confession_id FROM comments WHERE id = $1", 
                comment_id
            )
            
            if not comment_data:
                await callback_query.message.edit_text("Comment no longer exists.")
                return
            
            await conn.execute(
                """INSERT INTO reports (comment_id, reporter_user_id, reported_user_id) 
                   VALUES ($1, $2, $3) 
                   ON CONFLICT (comment_id, reporter_user_id) DO NOTHING""",
                comment_id, reporter_id, comment_data['user_id']
            )
        
        await callback_query.message.edit_text("✅ Report submitted to admin.")
        await safe_send_message(
            ADMIN_ID,
            f"⚠️ New report for comment #{comment_id} in confession #{comment_data['confession_id']}"
        )
    except Exception as e:
        logging.error(f"Error reporting comment: {e}")
        await callback_query.message.edit_text("❌ Error submitting report.")
    
    await callback_query.answer()

@dp.callback_query(F.data == "report_cancel")
async def report_cancel(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text("Report cancelled.")
    await callback_query.answer()


# ==================== NEW FEEDBACK HANDLERS ====================

@dp.message(Command("feedback"), StateFilter(None))
async def cmd_feedback(message: types.Message, state: FSMContext):
    """Start feedback process"""
    await state.set_state(FeedbackStates.waiting_for_feedback)
    
    cancel_kbd = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="/cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await message.answer(
        "📝 <b>Send Feedback</b>\n\n"
        "Write your feedback, suggestion, or report below.\n"
        "Your message will be sent anonymously to the admin.\n\n"
        "<i>Type /cancel to abort.</i>",
        reply_markup=cancel_kbd
    )

@dp.message(FeedbackStates.waiting_for_feedback, F.text)
async def process_feedback(message: types.Message, state: FSMContext):
    """Process and save feedback"""
    user_id = message.from_user.id
    username = message.from_user.username or "No username"
    feedback_text = message.text.strip()
    
    if len(feedback_text) < 10:
        await message.answer("❌ Feedback too short (min 10 characters).")
        return
    
    if len(feedback_text) > 2000:
        await message.answer("❌ Feedback too long (max 2000 characters).")
        return
    
    try:
        async with db.acquire() as conn:
            feedback_id = await conn.fetchval("""
                INSERT INTO feedback (user_id, username, message)
                VALUES ($1, $2, $3)
                RETURNING id
            """, user_id, username, feedback_text)
        
        # Notify admin
        admin_msg = (
            f"📬 <b>New Feedback</b>\n\n"
            f"ID: <code>{feedback_id}</code>\n"
            f"User: <code>{user_id}</code>\n"
            f"Username: @{username}\n\n"
            f"Message:\n{html.quote(feedback_text)}"
        )
        
        admin_kbd = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Reply", callback_data=f"reply_feedback_{feedback_id}")]
        ])
        
        await safe_send_message(ADMIN_ID, admin_msg, reply_markup=admin_kbd)
        
        # Confirm to user
        await message.answer(
            "✅ <b>Feedback sent!</b>\n\n"
            "Use /feedback_status to check for replies.",
            reply_markup=ReplyKeyboardRemove()
        )
        
        logging.info(f"Feedback #{feedback_id} received from user {user_id}")
        
    except Exception as e:
        logging.error(f"Error saving feedback: {e}")
        await message.answer("❌ An error occurred. Please try again later.")
    
    await state.clear()

@dp.message(Command("feedback_status"))
async def check_feedback_status(message: types.Message):
    """Check status of user's feedback"""
    user_id = message.from_user.id
    
    async with db.acquire() as conn:
        feedbacks = await conn.fetch("""
            SELECT id, message, replied, admin_reply, created_at 
            FROM feedback 
            WHERE user_id = $1 
            ORDER BY created_at DESC 
            LIMIT 5
        """, user_id)
    
    if not feedbacks:
        await message.answer("You haven't sent any feedback yet.")
        return
    
    status_text = "<b>📊 Your Feedback</b>\n\n"
    
    for fb in feedbacks:
        status = "✅ Replied" if fb['replied'] else "⏳ Pending"
        date = fb['created_at'].strftime("%Y-%m-%d %H:%M")
        snippet = html.quote(fb['message'][:50]) + ('...' if len(fb['message']) > 50 else '')
        
        status_text += f"<b>#{fb['id']}</b> - {status} ({date})\n"
        status_text += f"<i>\"{snippet}\"</i>\n"
        
        if fb['replied'] and fb['admin_reply']:
            reply = html.quote(fb['admin_reply'][:100]) + ('...' if len(fb['admin_reply']) > 100 else '')
            status_text += f"<b>Reply:</b> {reply}\n"
        
        status_text += "\n"
    
    await message.answer(status_text)


# ==================== NEW SUPPORT CHAT HANDLERS ====================

@dp.message(Command("support"), StateFilter(None))
async def start_support_chat(message: types.Message, state: FSMContext):
    """Start support chat with admin"""
    user_id = message.from_user.id
    
    async with db.acquire() as conn:
        existing = await conn.fetchval("""
            SELECT id FROM support_sessions 
            WHERE user_id = $1 AND is_active = TRUE
        """, user_id)
        
        if existing:
            await state.set_state(SupportChatStates.chatting)
            await message.answer(
                "💬 <b>Chat Resumed</b>\n\n"
                "You have an active chat. Send your message or type /endsupport to end."
            )
            return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Start Chat", callback_data="confirm_support")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_support")]
    ])
    
    await message.answer(
        "💬 <b>Start Support Chat</b>\n\n"
        "Chat privately with the admin. Messages are anonymous.\n\n"
        "Continue?",
        reply_markup=keyboard
    )

@dp.callback_query(F.data == "confirm_support")
async def confirm_support_chat(callback: types.CallbackQuery, state: FSMContext):
    """Confirm support chat start"""
    user_id = callback.from_user.id
    
    async with db.acquire() as conn:
        session_id = await conn.fetchval("""
            INSERT INTO support_sessions (user_id)
            VALUES ($1)
            ON CONFLICT (user_id) DO UPDATE
            SET is_active = TRUE, last_message_at = CURRENT_TIMESTAMP
            RETURNING id
        """, user_id)
    
    await state.set_state(SupportChatStates.chatting)
    
    await callback.message.edit_text(
        "✅ <b>Chat Started</b>\n\n"
        "You can now message the admin.\n"
        "Type /endsupport to end the chat."
    )
    
    await safe_send_message(
        ADMIN_ID,
        f"💬 <b>New Support Chat</b>\n\n"
        f"User: <code>{user_id}</code>\n"
        f"Username: @{callback.from_user.username or 'None'}"
    )
    
    await callback.answer()

@dp.callback_query(F.data == "cancel_support")
async def cancel_support_chat(callback: types.CallbackQuery):
    """Cancel support chat start"""
    await callback.message.edit_text("Chat cancelled.")
    await callback.answer()

@dp.message(SupportChatStates.chatting, F.text)
async def handle_support_message(message: types.Message, state: FSMContext):
    """Handle messages during support chat"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    if text.startswith('/endsupport'):
        await end_support_chat(message, state)
        return
    
    if text.startswith('/'):
        return
    
    async with db.acquire() as conn:
        session = await conn.fetchrow("""
            SELECT id FROM support_sessions 
            WHERE user_id = $1 AND is_active = TRUE
        """, user_id)
        
        if not session:
            await state.clear()
            await message.answer("Chat expired. Use /support to start new.")
            return
        
        await conn.execute("""
            INSERT INTO support_messages (session_id, user_id, message)
            VALUES ($1, $2, $3)
        """, session['id'], user_id, text)
        
        await conn.execute("""
            UPDATE support_sessions 
            SET last_message_at = CURRENT_TIMESTAMP 
            WHERE id = $1
        """, session['id'])
    
    # Forward to admin
    admin_msg = f"💬 <b>Support Message</b>\n\nUser: <code>{user_id}</code>\n\n{text}"
    await safe_send_message(ADMIN_ID, admin_msg)
    await message.answer("✅ Message sent to admin.")

@dp.message(Command("endsupport"))
async def end_support_chat(message: types.Message, state: FSMContext):
    """End support chat session"""
    user_id = message.from_user.id
    
    async with db.acquire() as conn:
        await conn.execute("""
            UPDATE support_sessions 
            SET is_active = FALSE 
            WHERE user_id = $1
        """, user_id)
    
    await state.clear()
    await message.answer("Chat ended. Use /support to start new.")
    await safe_send_message(ADMIN_ID, f"User <code>{user_id}</code> ended support chat.")


# ==================== ADMIN FEEDBACK/SUPPORT HANDLERS ====================

@dp.callback_query(F.data.startswith("reply_feedback_"))
async def admin_reply_feedback_prompt(callback: types.CallbackQuery, state: FSMContext):
    """Admin starts replying to feedback"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Unauthorized", show_alert=True)
        return
    
    feedback_id = int(callback.data.split("_")[-1])
    await state.update_data(reply_feedback_id=feedback_id)
    await state.set_state(FeedbackStates.waiting_for_admin_reply)
    
    await callback.message.answer(f"✏️ Write your reply for feedback #{feedback_id}:")
    await callback.answer()

@dp.message(FeedbackStates.waiting_for_admin_reply, F.text, F.from_user.id == ADMIN_ID)
async def admin_send_feedback_reply(message: types.Message, state: FSMContext):
    """Admin sends reply to feedback"""
    data = await state.get_data()
    feedback_id = data.get('reply_feedback_id')
    reply_text = message.text.strip()
    
    if not feedback_id:
        await message.answer("Error: Context lost.")
        await state.clear()
        return
    
    async with db.acquire() as conn:
        user_id = await conn.fetchval("SELECT user_id FROM feedback WHERE id = $1", feedback_id)
        
        if user_id:
            await conn.execute("""
                UPDATE feedback 
                SET replied = TRUE, admin_reply = $1, replied_at = CURRENT_TIMESTAMP
                WHERE id = $2
            """, reply_text, feedback_id)
            
            user_msg = f"📬 <b>Reply to your feedback #{feedback_id}</b>\n\nAdmin: {reply_text}"
            if await safe_send_message(user_id, user_msg):
                await message.reply("✅ Reply sent to user.")
            else:
                await message.reply("⚠️ Could not send reply (user blocked bot?).")
        else:
            await message.reply("❌ User not found.")
    
    await state.clear()

@dp.message(Command("feedback_stats"), F.from_user.id == ADMIN_ID)
async def admin_feedback_stats(message: types.Message):
    """Show feedback statistics for admin"""
    async with db.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM feedback") or 0
        replied = await conn.fetchval("SELECT COUNT(*) FROM feedback WHERE replied = TRUE") or 0
        pending = total - replied
        
        recent = await conn.fetch("""
            SELECT id, user_id, message, replied, created_at 
            FROM feedback 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
    
    stats = (
        f"📊 <b>Feedback Statistics</b>\n\n"
        f"Total: {total}\n"
        f"Replied: {replied}\n"
        f"Pending: {pending}\n\n"
        f"<b>Recent Feedback:</b>\n"
    )
    
    for fb in recent:
        date = fb['created_at'].strftime("%Y-%m-%d")
        status = "✅" if fb['replied'] else "⏳"
        snippet = fb['message'][:30] + ('...' if len(fb['message']) > 30 else '')
        stats += f"{status} #{fb['id']} ({date}): {snippet}\n"
    
    await message.answer(stats)

@dp.message(Command("active_chats"), F.from_user.id == ADMIN_ID)
async def admin_active_chats(message: types.Message):
    """Show active support chats for admin"""
    async with db.acquire() as conn:
        active = await conn.fetch("""
            SELECT user_id, last_message_at 
            FROM support_sessions 
            WHERE is_active = TRUE 
            ORDER BY last_message_at DESC
        """)
    
    if not active:
        await message.answer("No active support chats.")
        return
    
    text = "💬 <b>Active Support Chats</b>\n\n"
    for chat in active:
        last = chat['last_message_at'].strftime("%H:%M %Y-%m-%d")
        text += f"User: <code>{chat['user_id']}</code> (Last: {last})\n"
    
    await message.answer(text)

@dp.message(Command("broadcast"), F.from_user.id == ADMIN_ID)
async def admin_broadcast(message: types.Message, command: CommandObject):
    """Broadcast message to all users"""
    if not command.args:
        await message.reply("Usage: /broadcast <message>")
        return
    
    broadcast_text = command.args
    
    async with db.acquire() as conn:
        users = await conn.fetch("SELECT DISTINCT user_id FROM feedback UNION SELECT DISTINCT user_id FROM support_sessions")
    
    sent = 0
    for user in users:
        if await safe_send_message(user['user_id'], f"📢 <b>Broadcast</b>\n\n{broadcast_text}"):
            sent += 1
        await asyncio.sleep(0.05)
    
    await message.reply(f"✅ Broadcast sent to {sent} users.")


# ==================== ADMIN USER MANAGEMENT ====================

@dp.message(Command("warn"), F.from_user.id == ADMIN_ID)
async def admin_warn(message: types.Message, command: CommandObject):
    """Warn a user"""
    if not command.args:
        await message.reply("Usage: /warn <user_id> <reason>")
        return
    
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Usage: /warn <user_id> <reason>")
        return
    
    try:
        target = int(parts[0])
        reason = parts[1]
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    warn_text = f"⚠️ <b>Warning</b>\n\nReason: {html.quote(reason)}"
    if await safe_send_message(target, warn_text):
        await message.reply(f"✅ Warning sent to {target}.")
    else:
        await message.reply(f"⚠️ Could not send warning to {target}.")

async def apply_block(message: types.Message, user_id: int, reason: Optional[str], permanent: bool, duration: Optional[str] = None):
    """Apply block to user"""
    blocked_until = None
    if not permanent:
        if not duration:
            await message.reply("Duration required for temporary block.")
            return False
        
        try:
            num = int(duration[:-1])
            unit = duration[-1].lower()
            if unit == 'd':
                blocked_until = datetime.now(timezone.utc) + timedelta(days=num)
            elif unit == 'w':
                blocked_until = datetime.now(timezone.utc) + timedelta(weeks=num)
            else:
                await message.reply("Use 'd' for days or 'w' for weeks (e.g., 7d, 2w).")
                return False
        except (ValueError, IndexError):
            await message.reply("Invalid duration format.")
            return False
    
    async with db.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_status (user_id, is_blocked, blocked_until, block_reason)
            VALUES ($1, TRUE, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET
            is_blocked = TRUE, blocked_until = $2, block_reason = $3
        """, user_id, blocked_until, reason)
    
    expiry = "permanently" if permanent else f"until {blocked_until.strftime('%Y-%m-%d')}"
    reason_text = f"\nReason: {reason}" if reason else ""
    await safe_send_message(user_id, f"❌ You have been blocked {expiry}.{reason_text}")
    await message.reply(f"✅ User {user_id} blocked {expiry}.")
    return True

@dp.message(Command("block"), F.from_user.id == ADMIN_ID)
async def admin_block(message: types.Message, command: CommandObject):
    """Temporarily block a user"""
    if not command.args:
        await message.reply("Usage: /block <user_id> <duration> [reason]")
        return
    
    parts = command.args.split(maxsplit=2)
    if len(parts) < 2:
        await message.reply("Usage: /block <user_id> <duration> [reason]")
        return
    
    try:
        target = int(parts[0])
        duration = parts[1]
        reason = parts[2] if len(parts) > 2 else None
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    await apply_block(message, target, reason, permanent=False, duration=duration)

@dp.message(Command("pblock"), F.from_user.id == ADMIN_ID)
async def admin_pblock(message: types.Message, command: CommandObject):
    """Permanently block a user"""
    if not command.args:
        await message.reply("Usage: /pblock <user_id> [reason]")
        return
    
    parts = command.args.split(maxsplit=1)
    try:
        target = int(parts[0])
        reason = parts[1] if len(parts) > 1 else None
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    await apply_block(message, target, reason, permanent=True)

@dp.message(Command("unblock"), F.from_user.id == ADMIN_ID)
async def admin_unblock(message: types.Message, command: CommandObject):
    """Unblock a user"""
    if not command.args:
        await message.reply("Usage: /unblock <user_id>")
        return
    
    try:
        target = int(command.args.strip())
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    async with db.acquire() as conn:
        result = await conn.execute("""
            UPDATE user_status 
            SET is_blocked = FALSE, blocked_until = NULL, block_reason = NULL 
            WHERE user_id = $1 AND is_blocked = TRUE
        """, target)
    
    if result == "UPDATE 1":
        await safe_send_message(target, "✅ You have been unblocked.")
        await message.reply(f"✅ User {target} unblocked.")
    else:
        await message.reply(f"User {target} was not blocked.")

@dp.message(Command("id"), F.from_user.id == ADMIN_ID)
async def admin_user_info(message: types.Message, command: CommandObject):
    """Get user info"""
    if not command.args:
        await message.reply("Usage: /id <user_id>")
        return
    
    try:
        target = int(command.args.strip())
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    info = [f"ℹ️ <b>User Info: <code>{target}</code></b>\n"]
    
    try:
        chat = await bot.get_chat(target)
        info.append(f"Username: @{chat.username or 'None'}")
        info.append(f"Name: {html.quote(chat.first_name or '')} {html.quote(chat.last_name or '')}")
    except Exception as e:
        info.append(f"Could not fetch Telegram info: {e}")
    
    async with db.acquire() as conn:
        points = await get_user_points(target)
        conf_count = await conn.fetchval("SELECT COUNT(*) FROM confessions WHERE user_id = $1", target) or 0
        comm_count = await conn.fetchval("SELECT COUNT(*) FROM comments WHERE user_id = $1", target) or 0
        status = await conn.fetchrow("SELECT * FROM user_status WHERE user_id = $1", target)
        
        info.append(f"\n<b>Bot Stats:</b>")
        info.append(f"Points: 🏅 {points}")
        info.append(f"Confessions: {conf_count}")
        info.append(f"Comments: {comm_count}")
        
        if status:
            info.append(f"Rules accepted: {'✅' if status['has_accepted_rules'] else '❌'}")
            if status['is_blocked']:
                until = f"until {status['blocked_until'].strftime('%Y-%m-%d')}" if status['blocked_until'] else "permanently"
                info.append(f"Blocked: ❌ {until}")
                if status['block_reason']:
                    info.append(f"Reason: {status['block_reason']}")
    
    await message.reply("\n".join(info))


# ==================== MAIN EXECUTION ====================

async def main():
    try:
        await setup()
        
        if not db or not bot_info:
            logging.critical("FATAL: Database or bot info missing after setup.")
            return

        # Register middleware
        dp.message.middleware(BlockUserMiddleware())
        dp.callback_query.middleware(BlockUserMiddleware())

        # Set bot commands
        commands = [
            types.BotCommand(command="start", description="Start"),
            types.BotCommand(command="confess", description="Confess anonymously"),
            types.BotCommand(command="profile", description="Your profile"),
            types.BotCommand(command="feedback", description="Send feedback"),
            types.BotCommand(command="support", description="Chat with admin"),
            types.BotCommand(command="help", description="Show help"),
        ]
        
        admin_commands = commands + [
            types.BotCommand(command="id", description="Get user info"),
            types.BotCommand(command="warn", description="Warn user"),
            types.BotCommand(command="block", description="Temp block user"),
            types.BotCommand(command="pblock", description="Perm block user"),
            types.BotCommand(command="unblock", description="Unblock user"),
            types.BotCommand(command="feedback_stats", description="Feedback stats"),
            types.BotCommand(command="active_chats", description="Active support chats"),
            types.BotCommand(command="broadcast", description="Broadcast to all"),
        ]
        
        await bot.set_my_commands(commands)
        await bot.set_my_commands(admin_commands, scope=types.BotCommandScopeChat(chat_id=ADMIN_ID))

        # Webhook setup for Render
        webhook_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "")
        if webhook_host:
            WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
            webhook_url = f"https://{webhook_host}{WEBHOOK_PATH}"
            
            await bot.set_webhook(
                webhook_url,
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types()
            )
            logging.info(f"Webhook set to: {webhook_url}")
            
            app = web.Application()
            webhook_requests_handler = SimpleRequestHandler(
                dispatcher=dp,
                bot=bot,
            )
            webhook_requests_handler.register(app, path=WEBHOOK_PATH)
            
            app.router.add_get('/', handle_health_check)
            app.router.add_get('/healthz', handle_health_check)
            
            setup_application(app, dp, bot=bot)
            
            port = int(HTTP_PORT_STR) if HTTP_PORT_STR else 10000
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            logging.info(f"Bot started with webhook on port {port}")
            
            while True:
                await asyncio.sleep(3600)
        else:
            # Polling for local development
            logging.info("Starting with polling...")
            await dp.start_polling(bot, skip_updates=True)

    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        logging.info("Shutting down...")
        if bot and bot.session:
            await bot.session.close()
        if db:
            await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
