import os
import re
import sys
import time
import uuid
import json
import random
import logging
import tempfile
import threading
import subprocess
import psutil
from io import BytesIO
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote, urljoin
import aiohttp
import aiofiles
import asyncio
import requests
import isodate
import psutil
import pymongo
from pymongo import MongoClient, ASCENDING
from bson import ObjectId
from bson.binary import Binary
from dotenv import load_dotenv
from flask import Flask, request
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from pyrogram import Client, filters, errors
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    ChatPermissions,
)
from pyrogram.errors import RPCError
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import MediaStream
from pytgcalls import filters as fl
from pytgcalls.types import (
    ChatUpdate,
    UpdatedGroupCallParticipant,
    Update as TgUpdate,
)
from pytgcalls.types.stream import StreamEnded
from typing import Union
import urllib
from FrozenMusic.infra.concurrency.ci import deterministic_privilege_validator
from FrozenMusic.telegram_client.vector_transport import vector_transport_resolver
from FrozenMusic.infra.vector.yt_vector_orchestrator import yt_vector_orchestrator
from FrozenMusic.infra.vector.yt_backup_engine import yt_backup_engine
from FrozenMusic.infra.chrono.chrono_formatter import quantum_temporal_humanizer
from FrozenMusic.vector_text_tools import vectorized_unicode_boldifier
from FrozenMusic.telegram_client.startup_hooks import precheck_channels

load_dotenv()


API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ASSISTANT_SESSION = os.environ.get("ASSISTANT_SESSION")
OWNER_ID = int(os.getenv("OWNER_ID", "5268762773"))

# ——— Monkey-patch resolve_peer ——————————————
logging.getLogger("pyrogram").setLevel(logging.ERROR)
_original_resolve_peer = Client.resolve_peer
async def _safe_resolve_peer(self, peer_id):
    try:
        return await _original_resolve_peer(self, peer_id)
    except (KeyError, ValueError) as e:
        if "ID not found" in str(e) or "Peer id invalid" in str(e):
            return None
        raise
Client.resolve_peer = _safe_resolve_peer

# ——— Suppress un‐retrieved task warnings —————————
def _custom_exception_handler(loop, context):
    exc = context.get("exception")
    if isinstance(exc, (KeyError, ValueError)) and (
        "ID not found" in str(exc) or "Peer id invalid" in str(exc)
    ):
        return  

    if isinstance(exc, AttributeError) and "has no attribute 'write'" in str(exc):
        return

    loop.default_exception_handler(context)

asyncio.get_event_loop().set_exception_handler(_custom_exception_handler)

session_name = os.environ.get("SESSION_NAME", "music_bot1")
bot = Client(session_name, bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)
assistant = Client("assistant_account", session_string=ASSISTANT_SESSION)
call_py = PyTgCalls(assistant)


ASSISTANT_USERNAME = None
ASSISTANT_CHAT_ID = None
API_ASSISTANT_USERNAME = os.getenv("API_ASSISTANT_USERNAME")


# ─── MongoDB Setup ─────────────────────────────────────────
mongo_uri = os.environ.get("MongoDB_url")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["music_bot"]


broadcast_collection  = db["broadcast"]


state_backup = db["state_backup"]


chat_containers = {}
playback_tasks = {}  
bot_start_time = time.time()
COOLDOWN = 10
chat_last_command = {}
chat_pending_commands = {}
QUEUE_LIMIT = 20
MAX_DURATION_SECONDS = 900  
LOCAL_VC_LIMIT = 10
playback_mode = {}



async def process_pending_command(chat_id, delay):
    await asyncio.sleep(delay)  
    if chat_id in chat_pending_commands:
        message, cooldown_reply = chat_pending_commands.pop(chat_id)
        await cooldown_reply.delete()  
        await play_handler(bot, message) 



async def skip_to_next_song(chat_id, message):
    """Skips to the next song in the queue and starts playback."""
    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await message.edit("❌ 𝖭𝗈 𝗆𝗈𝗋𝖾 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")
        await leave_voice_chat(chat_id)
        return

    await message.edit("⏭️ 𝖲𝗄𝗂𝗉𝗉𝗂𝗇𝗀 𝗍𝗈 𝗍𝗁𝖾 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀...")

    # Pick next song from queue
    next_song_info = chat_containers[chat_id][0]
    try:
        await fallback_local_playback(chat_id, message, next_song_info)
    except Exception as e:
        print(f"Error starting next local playback: {e}")
        await bot.send_message(chat_id, f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗌𝗍𝖺𝗋𝗍 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀: {e}")



def safe_handler(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Attempt to extract a chat ID (if available)
            chat_id = "Unknown"
            try:
                # If your function is a message handler, the second argument is typically the Message object.
                if len(args) >= 2:
                    chat_id = args[1].chat.id
                elif "message" in kwargs:
                    chat_id = kwargs["message"].chat.id
            except Exception:
                chat_id = "Unknown"
            error_text = (
                f"Error in handler `{func.__name__}` (chat id: {chat_id}):\n\n{str(e)}"
            )
            print(error_text)
            # Log the error to support
            await bot.send_message(5268762773, error_text)
    return wrapper


async def extract_invite_link(client, chat_id):
    try:
        chat_info = await client.get_chat(chat_id)
        if chat_info.invite_link:
            return chat_info.invite_link
        elif chat_info.username:
            return f"https://t.me/{chat_info.username}"
        return None
    except ValueError as e:
        if "Peer id invalid" in str(e):
            print(f"𝖨𝗇𝗏𝖺𝗅𝗂𝖽 𝗉𝖾𝖾𝗋 𝖨𝖣 𝖿𝗈𝗋 𝖼𝗁𝖺𝗍 {chat_id}. 𝖲𝗄𝗂𝗉𝗉𝗂𝗇𝗀 𝗂𝗇𝗏𝗂𝗍𝖾 𝗅𝗂𝗇𝗄 𝖾𝗑𝗍𝗋𝖺𝖼𝗍𝗂𝗈𝗇.")
            return None
        else:
            raise e  # re-raise if it's another ValueError
    except Exception as e:
        print(f"𝖤𝗋𝗋𝗈𝗋 𝖾𝗑𝗍𝗋𝖺𝖼𝗍𝗂𝗇𝗀 𝗂𝗇𝗏𝗂𝗍𝖾 𝗅𝗂𝗇𝗄 𝖿𝗈𝗋 𝖼𝗁𝖺𝗍 {chat_id}: {e}")
        return None

async def extract_target_user(message: Message):
    # If the moderator replied to someone:
    if message.reply_to_message:
        return message.reply_to_message.from_user.id

    # Otherwise expect an argument like "/ban @user" or "/ban 123456"
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ 𝖸𝗈𝗎 𝗆𝗎𝗌𝗍 𝗋𝖾𝗉𝗅𝗒 𝗍𝗈 𝖺 𝗎𝗌𝖾𝗋 𝗈𝗋 𝗌𝗉𝖾𝖼𝗂𝖿𝗒 𝗍𝗁𝖾𝗂𝗋 @username/user_id.")
        return None

    target = parts[1]
    # Strip @
    if target.startswith("@"):
        target = target[1:]
    try:
        user = await message._client.get_users(target)
        return user.id
    except:
        await message.reply("❌ 𝖢𝗈𝗎𝗅𝖽 𝗇𝗈𝗍 𝖿𝗂𝗇𝖽 𝗍𝗁𝖺𝗍 𝗎𝗌𝖾𝗋.")
        return None



async def is_assistant_in_chat(chat_id):
    try:
        member = await assistant.get_chat_member(chat_id, ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        error_message = str(e)
        if "USER_BANNED" in error_message or "Banned" in error_message:
            return "banned"
        elif "USER_NOT_PARTICIPANT" in error_message or "Chat not found" in error_message:
            return False
        print(f"Error checking assistant in chat: {e}")
        return False

async def is_api_assistant_in_chat(chat_id):
    try:
        member = await bot.get_chat_member(chat_id, API_ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        print(f"𝖤𝗋𝗋𝗈𝗋 𝖼𝗁𝖾𝖼𝗄𝗂𝗇𝗀 𝖠𝖯𝖨 𝖺𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝗂𝗇 𝖼𝗁𝖺𝗍: {e}")
        return False
    
def iso8601_to_seconds(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())
    except Exception as e:
        print(f"Error parsing duration: {e}")
        return 0


def iso8601_to_human_readable(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02}:{seconds:02}"
        return f"{minutes}:{seconds:02}"
    except Exception as e:
        return "Unknown duration"

async def fetch_youtube_link(query):
    try:
        url = f"https://teenage-liz-frozzennbotss-61567ab4.koyeb.app/search?title={query}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Check if the API response contains a playlist
                    if "playlist" in data:
                        return data
                    else:
                        return (
                            data.get("link"),
                            data.get("title"),
                            data.get("duration"),
                            data.get("thumbnail")
                        )
                else:
                    raise Exception(f"API returned status code {response.status}")
    except Exception as e:
        raise Exception(f"Failed to fetch YouTube link: {str(e)}")


    
async def fetch_youtube_link_backup(query):
    if not BACKUP_SEARCH_API_URL:
        raise Exception("Backup Search API URL not configured")
    # Build the correct URL:
    backup_url = (
        f"{BACKUP_SEARCH_API_URL.rstrip('/')}"
        f"/search?title={urllib.parse.quote(query)}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(backup_url, timeout=30) as resp:
                if resp.status != 200:
                    raise Exception(f"Backup API returned status {resp.status}")
                data = await resp.json()
                # Mirror primary API’s return:
                if "playlist" in data:
                    return data
                return (
                    data.get("link"),
                    data.get("title"),
                    data.get("duration"),
                    data.get("thumbnail")
                )
    except Exception as e:
        raise Exception(f"Backup Search API error: {e}")
    
BOT_NAME = os.environ.get("BOT_NAME", "Dreams Music")
BOT_LINK = os.environ.get("BOT_LINK", "https://t.me/DreamSongRobot")

from pyrogram.errors import UserAlreadyParticipant, RPCError

async def invite_assistant(chat_id, invite_link, processing_message):
    """
    Internally invite the assistant to the chat by using the assistant client to join the chat.
    If the assistant is already in the chat, treat as success.
    On other errors, display and return False.
    """
    try:
        # Attempt to join via invite link
        await assistant.join_chat(invite_link)
        return True

    except UserAlreadyParticipant:
        # Assistant is already in the chat, no further action needed
        return True

    except RPCError as e:
        # Handle other Pyrogram RPC errors
        error_message = f"❌ 𝖤𝗋𝗋𝗈𝗋 𝗐𝗁𝗂𝗅𝖾 𝗂𝗇𝗏𝗂𝗍𝗂𝗇𝗀 𝖺𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍: 𝖳𝖾𝗅𝖾𝗀𝗋𝖺𝗆 𝗌𝖺𝗒𝗌: {e.code} {e.error_message}"
        await processing_message.edit(error_message)
        return False

    except Exception as e:
        # Catch-all for any unexpected exceptions
        error_message = f"❌ 𝖴𝗇𝖾𝗑𝗉𝖾𝖼𝗍𝖾𝖽 𝖾𝗋𝗋𝗈𝗋 𝗐𝗁𝗂𝗅𝖾 𝗂𝗇𝗏𝗂𝗍𝗂𝗇𝗀 𝖺𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍: {str(e)}"
        await processing_message.edit(error_message)
        return False


# Helper to convert ASCII letters to Unicode bold
def to_bold_unicode(text: str) -> str:
    bold_text = ""
    for char in text:
        if 'A' <= char <= 'Z':
            bold_text += chr(ord('𝗔') + (ord(char) - ord('A')))
        elif 'a' <= char <= 'z':
            bold_text += chr(ord('𝗮') + (ord(char) - ord('a')))
        else:
            bold_text += char
    return bold_text

@bot.on_message(filters.command("start"))
async def start_handler(_, message):
    user_id = message.from_user.id
    raw_name = message.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("𝖠𝖽𝖽 𝖬𝖾")
    updates_text = to_bold_unicode("𝖴𝗉𝖽𝖺𝗍𝖾𝗌")
    support_text = to_bold_unicode("𝖲𝗎𝗉𝗉𝗈𝗋𝗍")
    help_text = to_bold_unicode("𝖧𝖾𝗅𝗉")

    caption = (
        f"👋 𝖧𝖾𝗒 {user_link} \n\n"
        f"I am {BOT_NAME.upper()} ❄️\n"
        f"๏ 𝖸𝗈𝗎𝗋 ♾️ 𝗉𝖾𝗋𝗌𝗈𝗇𝖺𝗅 𝖣𝖩 𝗂𝗌 𝗇𝗈𝗐 𝗈𝗇𝗅𝗂𝗇𝖾 — 𝗋𝖾𝖺𝖽𝗒 𝗍𝗈 𝖽𝗋𝗈𝗉 𝗍𝗁𝖾 𝖻𝖾𝖺𝗍𝗌 𝖺𝗇𝗒𝗍𝗂𝗆𝖾, 𝖺𝗇𝗒𝗐𝗁𝖾𝗋𝖾!."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"©️ {updates_text}", url="https://t.me/CFCBots")
        ],
        [
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await message.reply_animation(
        animation="https://graph.org/file/2255334da15aee384dcb5-c7b55af2c8cb40c4cb.jpg",
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

    # Register chat ID for broadcasting silently
    chat_id = message.chat.id
    chat_type = message.chat.type
    if chat_type == ChatType.PRIVATE:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "private"})
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "group"})



@bot.on_callback_query(filters.regex("^go_back$"))
async def go_back_callback(_, callback_query):
    user_id = callback_query.from_user.id
    raw_name = callback_query.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("𝖠𝖽𝖽 𝖬𝖾")
    updates_text = to_bold_unicode("𝖴𝗉𝖽𝖺𝗍𝖾𝗌")
    support_text = to_bold_unicode("𝖲𝗎𝗉𝗉𝗈𝗋𝗍")
    help_text = to_bold_unicode("𝖧𝖾𝗅𝗉")

    caption = (
        f"👋 𝖧𝖾𝗒 {user_link} \n\n"
        f"I am {BOT_NAME.upper()} ❄️\n"
        f"๏ 𝖸𝗈𝗎𝗋 ♾️ 𝗉𝖾𝗋𝗌𝗈𝗇𝖺𝗅 𝖣𝖩 𝗂𝗌 𝗇𝗈𝗐 𝗈𝗇𝗅𝗂𝗇𝖾 — 𝗋𝖾𝖺𝖽𝗒 𝗍𝗈 𝖽𝗋𝗈𝗉 𝗍𝗁𝖾 𝖻𝖾𝖺𝗍𝗌 𝖺𝗇𝗒𝗍𝗂𝗆𝖾, 𝖺𝗇𝗒𝗐𝗁𝖾𝗋𝖾!."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"©️ {updates_text}", url="https://t.me/CFCBots")
        ],
        [
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await callback_query.message.edit_caption(
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )



@bot.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(_, callback_query):
    help_text = ">📜 *Choose a category to explore commands:*"
    buttons = [
        [
            InlineKeyboardButton("𝖬𝗎𝗌𝗂𝖼 𝖢𝗈𝗇𝗍𝗋𝗈𝗅𝗌", callback_data="help_music"),
        ],
         [
            InlineKeyboardButton("𝖠𝖽𝗆𝗂𝗇 𝖢𝗈𝗇𝗍𝗋𝗈𝗅𝗌", callback_data="help_admin")
        ],
        [
            InlineKeyboardButton("𝖤𝗑𝗍𝗋𝖺 𝖢𝗈𝗇𝗍𝗋𝗈𝗅𝗌", callback_data="help_couple"),
        ],
         [
            InlineKeyboardButton("𝖲𝖾𝗍𝗍𝗂𝗇𝗀𝗌", callback_data="help_util")
        ],
        [
            InlineKeyboardButton("𝖡𝖺𝖼𝗄", callback_data="go_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(help_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)


@bot.on_callback_query(filters.regex("^help_music$"))
async def help_music_callback(_, callback_query):
    text = (
        ">🎵 *𝗠𝘂𝘀𝗶𝗰 & 𝗣𝗹𝗮𝘆𝗯𝗮𝗰𝗸 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀*\n\n"
        ">➜ `/play <song name or URL>`\n"
        "   • Play a song (YouTube/Spotify/Resso/Apple Music/SoundCloud).\n"
        "   • If replied to an audio/video, plays it directly.\n\n"
        ">➜ `/playlist`\n"
        "   • View or manage your saved playlist.\n\n"
        ">➜ `/skip`\n"
        "   • Skip the currently playing song. (Admins only)\n\n"
        ">➜ `/pause`\n"
        "   • Pause the current stream. (Admins only)\n\n"
        ">➜ `/resume`\n"
        "   • Resume a paused stream. (Admins only)\n\n"
        ">➜ `/stop` or `/end`\n"
        "   • Stop playback and clear the queue. (Admins only)"
    )
    buttons = [[InlineKeyboardButton("ʙᴀᴄᴋ", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_admin$"))
async def help_admin_callback(_, callback_query):
    text = (
        "🛡️ *𝗔𝗱𝗺𝗶𝗻 & 𝗠𝗼𝗱𝗲𝗿𝗮𝘁𝗶𝗼𝗻 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀*\n\n"
        ">➜ `/𝗆𝗎𝗍𝖾 @𝗎𝗌𝖾𝗋`\n"
        "   • 𝖬𝗎𝗍𝖾 𝖺 𝗎𝗌𝖾𝗋 𝗂𝗇𝖽𝖾𝖿𝗂𝗇𝗂𝗍𝖾𝗅𝗒. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ `/𝗎𝗇𝗆𝗎𝗍𝖾 @𝗎𝗌𝖾𝗋`\n"
        "   • 𝖴𝗇𝗆𝗎𝗍𝖾 𝖺 𝗉𝗋𝖾𝗏𝗂𝗈𝗎𝗌𝗅𝗒 𝗆𝗎𝗍𝖾𝖽 𝗎𝗌𝖾𝗋. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ `/𝗍𝗆𝗎𝗍𝖾 @𝗎𝗌𝖾𝗋 <𝗆𝗂𝗇𝗎𝗍𝖾𝗌>`\n"
        "   • 𝖳𝖾𝗆𝗉𝗈𝗋𝖺𝗋𝗂𝗅𝗒 𝗆𝗎𝗍𝖾 𝖿𝗈𝗋 𝖺 𝗌𝖾𝗍 𝖽𝗎𝗋𝖺𝗍𝗂𝗈𝗇. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ `/𝗄𝗂𝖼𝗄 @𝗎𝗌𝖾𝗋`\n"
        "   • 𝖪𝗂𝖼𝗄 (𝖻𝖺𝗇 + 𝗎𝗇𝖻𝖺𝗇) 𝖺 𝗎𝗌𝖾𝗋 𝗂𝗆𝗆𝖾𝖽𝗂𝖺𝗍𝖾𝗅𝗒. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ `/𝖻𝖺𝗇 @𝗎𝗌𝖾𝗋`\n"
        "   • 𝖡𝖺𝗇 𝖺 𝗎𝗌𝖾𝗋. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ `/𝗎𝗇𝖻𝖺𝗇 @𝗎𝗌𝖾𝗋`\n"
        "   • 𝖴𝗇𝖻𝖺𝗇 𝖺 𝗉𝗋𝖾𝗏𝗂𝗈𝗎𝗌𝗅𝗒 𝖻𝖺𝗇𝗇𝖾𝖽 𝗎𝗌𝖾𝗋. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)"
    )
    buttons = [[InlineKeyboardButton("𝖡𝖺𝖼𝗄", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_couple$"))
async def help_couple_callback(_, callback_query):
    text = (
        " ❓*𝗘𝘅𝘁𝗿𝗮 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀*\n\n"
        ">➜ `/𝖼𝗈𝗎𝗉𝗅𝖾`\n"
        "   • 𝖯𝗂𝖼𝗄𝗌 𝗍𝗐𝗈 𝗋𝖺𝗇𝖽𝗈𝗆 𝗇𝗈𝗇-𝖻𝗈𝗍 𝗆𝖾𝗆𝖻𝖾𝗋𝗌 𝖺𝗇𝖽 𝗉𝗈𝗌𝗍𝗌 𝖺 “𝖼𝗈𝗎𝗉𝗅𝖾” 𝗂𝗆𝖺𝗀𝖾 𝗐𝗂𝗍𝗁 𝗍𝗁𝖾𝗂𝗋 𝗇𝖺𝗆𝖾𝗌.\n"
        "   • 𝖢𝖺𝖼𝗁𝖾𝗌 𝖽𝖺𝗂𝗅𝗒 𝗌𝗈 𝗍𝗁𝖾 𝗌𝖺𝗆𝖾 𝗉𝖺𝗂𝗋 𝖺𝗉𝗉𝖾𝖺𝗋𝗌 𝗎𝗇𝗍𝗂𝗅 𝗆𝗂𝖽𝗇𝗂𝗀𝗁𝗍 𝖴𝖳𝖢.\n"
        "   • 𝖴𝗌𝖾𝗌 𝗉𝖾𝗋-𝗀𝗋𝗈𝗎𝗉 𝗆𝖾𝗆𝖻𝖾𝗋 𝖼𝖺𝖼𝗁𝖾 𝖿𝗈𝗋 𝗌𝗉𝖾𝖾𝖽."
    )
    buttons = [[InlineKeyboardButton("𝖡𝖺𝖼𝗄", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_util$"))
async def help_util_callback(_, callback_query):
    text = (
        "🔍 *𝗨𝘁𝗹𝗶𝘁𝘆 & 𝗘𝘅𝘁𝗿𝗮 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀*\n\n"
        ">➜ `/𝗉𝗂𝗇𝗀`\n"
        "   • 𝖢𝗁𝖾𝖼𝗄 𝖻𝗈𝗍’𝗌 𝗋𝖾𝗌𝗉𝗈𝗇𝗌𝖾 𝗍𝗂𝗆𝖾 𝖺𝗇𝖽 𝗎𝗉𝗍𝗂𝗆𝖾.\n\n"
        ">➜ `/𝖼𝗅𝖾𝖺𝗋`\n"
        "   • 𝖢𝗅𝖾𝖺𝗋 𝗍𝗁𝖾 𝖾𝗇𝗍𝗂𝗋𝖾 𝗊𝗎𝖾𝗎𝖾. (𝖠𝖽𝗆𝗂𝗇𝗌 𝗈𝗇𝗅𝗒)\n\n"
        ">➜ 𝖠𝗎𝗍𝗈-𝖲𝗎𝗀𝗀𝖾𝗌𝗍𝗂𝗈𝗇𝗌:\n"
        "   • 𝖶𝗁𝖾𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾 𝖾𝗇𝖽𝗌, 𝗍𝗁𝖾 𝖻𝗈𝗍 𝖺𝗎𝗍𝗈𝗆𝖺𝗍𝗂𝖼𝖺𝗅𝗅𝗒 𝗌𝗎𝗀𝗀𝖾𝗌𝗍𝗌 𝗇𝖾𝗐 𝗌𝗈𝗇𝗀𝗌 𝗏𝗂𝖺 𝗂𝗇𝗅𝗂𝗇𝖾 𝖻𝗎𝗍𝗍𝗈𝗇𝗌.\n\n"
        ">➜ *𝖠𝗎𝖽𝗂𝗈 𝖰𝗎𝖺𝗅𝗂𝗍𝗒 & 𝖫𝗂𝗆𝗂𝗍𝗌*\n"
        "   • 𝖲𝗍𝗋𝖾𝖺𝗆𝗌 𝗎𝗉 𝗍𝗈 2 𝗁𝗈𝗎𝗋𝗌 10 𝗆𝗂𝗇𝗎𝗍𝖾𝗌, 𝖻𝗎𝗍 𝖺𝗎𝗍𝗈-𝖿𝖺𝗅𝗅𝖻𝖺𝖼𝗄 𝖿𝗈𝗋 𝗅𝗈𝗇𝗀𝖾𝗋. (See `MAX_DURATION_SECONDS`)\n"
    )
    buttons = [[InlineKeyboardButton("Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_message(filters.group & filters.regex(r'^/play(?:@\w+)?(?:\s+(?P<query>.+))?$'))
async def play_handler(_, message: Message):
    chat_id = message.chat.id

    # If replying to an audio/video message, handle local playback
    if message.reply_to_message and (message.reply_to_message.audio or message.reply_to_message.video):
        processing_message = await message.reply("❄️")

        # Fetch fresh media reference and download
        orig = message.reply_to_message
        fresh = await bot.get_messages(orig.chat.id, orig.id)
        media = fresh.video or fresh.audio
        if fresh.audio and getattr(fresh.audio, 'file_size', 0) > 100 * 1024 * 1024:
            await processing_message.edit("❌ 𝖠𝗎𝖽𝗂𝗈 𝖿𝗂𝗅𝖾 𝗍𝗈𝗈 𝗅𝖺𝗋𝗀𝖾. 𝖬𝖺𝗑𝗂𝗆𝗎𝗆 𝖺𝗅𝗅𝗈𝗐𝖾𝖽 𝗌𝗂𝗓𝖾 𝗂𝗌 100𝖬𝖡.")
            return

        await processing_message.edit("⏳ 𝖯𝗅𝖾𝖺𝗌𝖾 𝗐𝖺𝗂𝗍, 𝖽𝗈𝗐𝗇𝗅𝗈𝖺𝖽𝗂𝗇𝗀 𝖺𝗎𝖽𝗂𝗈…")
        try:
            file_path = await bot.download_media(media)
        except Exception as e:
            await processing_message.edit(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝖽𝗈𝗐𝗇𝗅𝗈𝖺𝖽 𝗆𝖾𝖽𝗂𝖺 : {e}")
            return

        # Download thumbnail if available
        thumb_path = None
        try:
            thumbs = fresh.video.thumbs if fresh.video else fresh.audio.thumbs
            thumb_path = await bot.download_media(thumbs[0])
        except Exception:
            pass

        # Prepare song_info and fallback to local playback
        duration = media.duration or 0
        title = getattr(media, 'file_name', 'Untitled')
        song_info = {
            'url': file_path,
            'title': title,
            'duration': format_time(duration),
            'duration_seconds': duration,
            'requester': message.from_user.first_name,
            'thumbnail': thumb_path
        }
        await fallback_local_playback(chat_id, processing_message, song_info)
        return

    # Otherwise, process query-based search
    match = message.matches[0]
    query = (match.group('query') or "").strip()

    try:
        await message.delete()
    except Exception:
        pass

    # Enforce cooldown
    now_ts = time.time()
    if chat_id in chat_last_command and (now_ts - chat_last_command[chat_id]) < COOLDOWN:
        remaining = int(COOLDOWN - (now_ts - chat_last_command[chat_id]))
        if chat_id in chat_pending_commands:
            await bot.send_message(chat_id, f"⏳ 𝖠 𝖼𝗈𝗆𝗆𝖺𝗇𝖽 𝗂𝗌 𝖺𝗅𝗋𝖾𝖺𝖽𝗒 𝗊𝗎𝖾𝗎𝖾𝖽 𝖿𝗈𝗋 𝗍𝗁𝗂𝗌 𝖼𝗁𝖺𝗍. 𝖯𝗅𝖾𝖺𝗌𝖾 𝗐𝖺𝗂𝗍 {remaining}s.")
        else:
            cooldown_reply = await bot.send_message(chat_id, f"⏳ 𝖮𝗇 𝖼𝗈𝗈𝗅𝖽𝗈𝗐𝗇. 𝖯𝗋𝗈𝖼𝖾𝗌𝗌𝗂𝗇𝗀 𝗂𝗇 {remaining}s.")
            chat_pending_commands[chat_id] = (message, cooldown_reply)
            asyncio.create_task(process_pending_command(chat_id, remaining))
        return
    chat_last_command[chat_id] = now_ts

    if not query:
        await bot.send_message(
            chat_id,
            "❌ 𝖸𝗈𝗎 𝖽𝗂𝖽 𝗇𝗈𝗍 𝗌𝗉𝖾𝖼𝗂𝖿𝗒 𝖺 𝗌𝗈𝗇𝗀.\n\n"
            "𝖢𝗈𝗋𝗋𝖾𝖼𝗍 𝗎𝗌𝖺𝗀𝖾: /𝗉𝗅𝖺𝗒 <𝗌𝗈𝗇𝗀 𝗇𝖺𝗆𝖾>\𝗇𝖤𝗑𝖺𝗆𝗉𝗅𝖾: /𝗉𝗅𝖺𝗒 𝗌𝗁𝖺𝗉𝖾 𝗈𝖿 𝗒𝗈𝗎"
        )
        return

    # Delegate to query processor
    await process_play_command(message, query)



async def process_play_command(message: Message, query: str):
    chat_id = message.chat.id
    processing_message = await message.reply("❄️")

    # --- ensure assistant is in the chat before we queue/play anything ----
    status = await is_assistant_in_chat(chat_id)
    if status == "banned":
        await processing_message.edit("❌ 𝖠𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝗂𝗌 𝖻𝖺𝗇𝗇𝖾𝖽 𝖿𝗋𝗈𝗆 𝗍𝗁𝗂𝗌 𝖼𝗁𝖺𝗍.")
        return
    if status is False:
        # try to fetch an invite link to add the assistant
        invite_link = await extract_invite_link(bot, chat_id)
        if not invite_link:
            await processing_message.edit("❌ 𝖢𝗈𝗎𝗅𝖽 𝗇𝗈𝗍 𝗈𝖻𝗍𝖺𝗂𝗇 𝖺𝗇 𝗂𝗇𝗏𝗂𝗍𝖾 𝗅𝗂𝗇𝗄 𝗍𝗈 𝖺𝖽𝖽 𝗍𝗁𝖾 𝖺𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍.")
            return
        invited = await invite_assistant(chat_id, invite_link, processing_message)
        if not invited:
            # invite_assistant handles error editing
            return

    # Convert short URLs to full YouTube URLs
    if "youtu.be" in query:
        m = re.search(r"youtu\.be/([^?&]+)", query)
        if m:
            query = f"https://www.youtube.com/watch?v={m.group(1)}"

    # Perform YouTube search and handle results
    try:
        result = await fetch_youtube_link(query)
    except Exception as primary_err:
        await processing_message.edit(
            "⚠️ 𝖯𝗋𝗂𝗆𝖺𝗋𝗒 𝗌𝖾𝖺𝗋𝖼𝗁 𝖿𝖺𝗂𝗅𝖾𝖽. 𝖴𝗌𝗂𝗇𝗀 𝖻𝖺𝖼𝗄𝗎𝗉 𝖠𝖯𝖨, 𝗍𝗁𝗂𝗌 𝗆𝖺𝗒 𝗍𝖺𝗄𝖾 𝖺 𝖿𝖾𝗐 𝗌𝖾𝖼𝗈𝗇𝖽𝗌…"
        )
        try:
            result = await fetch_youtube_link_backup(query)
        except Exception as backup_err:
            await processing_message.edit(
                f"❌ 𝖡𝗈𝗍𝗁 𝗌𝖾𝖺𝗋𝖼𝗁 𝖠𝖯𝖨𝗌 𝖿𝖺𝗂𝗅𝖾𝖽:\n"
                f"𝖯𝗋𝗂𝗆𝖺𝗋𝗒: {primary_err}\n"
                f"𝖡𝖺𝖼𝗄𝗎𝗉:  {backup_err}"
            )
            return

    # Handle playlist vs single video
    if isinstance(result, dict) and "playlist" in result:
        playlist_items = result["playlist"]
        if not playlist_items:
            await processing_message.edit("❌ 𝖭𝗈 𝗏𝗂𝖽𝖾𝗈𝗌 𝖿𝗈𝗎𝗇𝖽 𝗂𝗇 𝗍𝗁𝖾 𝗉𝗅𝖺𝗒𝗅𝗂𝗌𝗍.")
            return

        chat_containers.setdefault(chat_id, [])
        for item in playlist_items:
            secs = isodate.parse_duration(item["duration"]).total_seconds()
            chat_containers[chat_id].append({
                "url": item["link"],
                "title": item["title"],
                "duration": iso8601_to_human_readable(item["duration"]),
                "duration_seconds": secs,
                "requester": message.from_user.first_name if message.from_user else "Unknown",
                "thumbnail": item["thumbnail"]
            })

        total = len(playlist_items)
        reply_text = (
            f"✨ 𝖠𝖽𝖽𝖾𝖽 𝗍𝗈 𝗉𝗅𝖺𝗒𝗅𝗂𝗌𝗍\n"
            f"𝖳𝗈𝗍𝖺𝗅 𝗌𝗈𝗇𝗀𝗌 𝖺𝖽𝖽𝖾𝖽 𝗍𝗈 𝗊𝗎𝖾𝗎𝖾: {total}\n"
            f"#1 - {playlist_items[0]['title']}"
        )
        if total > 1:
            reply_text += f"\n#2 - {playlist_items[1]['title']}"
        await message.reply(reply_text)

        # If first playlist song, start playback
        if len(chat_containers[chat_id]) == total:
            first_song_info = chat_containers[chat_id][0]
            await fallback_local_playback(chat_id, processing_message, first_song_info)
        else:
            await processing_message.delete()

    else:
        video_url, title, duration_iso, thumb = result
        if not video_url:
            await processing_message.edit(
                "❌ 𝖢𝗈𝗎𝗅𝖽 𝗇𝗈𝗍 𝖿𝗂𝗇𝖽 𝗍𝗁𝖾 𝗌𝗈𝗇𝗀. 𝖳𝗋𝗒 𝖺𝗇𝗈𝗍𝗁𝖾𝗋 𝗊𝗎𝖾𝗋𝗒.\𝗇𝖲𝗎𝗉𝗉𝗈𝗋𝗍: @CloseFriendsCommunity"
            )
            return

        secs = isodate.parse_duration(duration_iso).total_seconds()
        if secs > MAX_DURATION_SECONDS:
            await processing_message.edit(
                "❌ 𝖲𝗍𝗋𝖾𝖺𝗆𝗌 𝗅𝗈𝗇𝗀𝖾𝗋 𝗍𝗁𝖺𝗇 15 𝗆𝗂𝗇 𝖺𝗋𝖾 𝗇𝗈𝗍 𝖺𝗅𝗅𝗈𝗐𝖾𝖽. 𝖨𝖿 𝗎 𝖺𝗋𝖾 𝗍𝗁𝖾 𝗈𝗐𝗇𝖾𝗋 𝗈𝖿 𝗍𝗁𝗂𝗌 𝖻𝗈𝗍 𝖼𝗈𝗇𝗍𝖺𝖼𝗍 @𝗑𝗒𝗓09723 𝗍𝗈 𝗎𝗉𝗀𝗋𝖺𝖽𝖾 𝗒𝗈𝗎𝗋 𝗉𝗅𝖺𝗇"
            )
            return

        readable = iso8601_to_human_readable(duration_iso)
        chat_containers.setdefault(chat_id, [])
        chat_containers[chat_id].append({
            "url": video_url,
            "title": title,
            "duration": readable,
            "duration_seconds": secs,
            "requester": message.from_user.first_name if message.from_user else "Unknown",
            "thumbnail": thumb
        })

        # If it's the first song, start playback immediately using fallback
        if len(chat_containers[chat_id]) == 1:
            await fallback_local_playback(chat_id, processing_message, chat_containers[chat_id][0])
        else:
            queue_buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ 𝖲𝗄𝗂𝗉", callback_data="skip"),
                 InlineKeyboardButton("🗑 𝖢𝗅𝖾𝖺𝗋", callback_data="clear")]
            ])
            await message.reply(
                f"✨ 𝖠𝖽𝖽𝖾𝖽 𝗍𝗈 𝗊𝗎𝖾𝗎𝖾 :\n\n"
                f"**❍ 𝖳𝗂𝗍𝗅𝖾 ➥** {title}\n"
                f"**❍ 𝖳𝗂𝗆𝖾 ➥** {readable}\n"
                f"**❍ 𝖡𝗒 ➥ ** {message.from_user.first_name if message.from_user else 'Unknown'}\n"
                f"**𝖰𝗎𝖾𝗎𝖾 𝗇𝗎𝗆𝖻𝖾𝗋:** {len(chat_containers[chat_id]) - 1}",
                reply_markup=queue_buttons
            )
            await processing_message.delete()


# ─── Utility functions ──────────────────────────────────────────────────────────────

MAX_TITLE_LEN = 20

def _one_line_title(full_title: str) -> str:
    """
    Truncate `full_title` to at most MAX_TITLE_LEN chars.
    If truncated, append “…” so it still reads cleanly in one line.
    """
    if len(full_title) <= MAX_TITLE_LEN:
        return full_title
    else:
        return full_title[: (MAX_TITLE_LEN - 1) ] + "…"  # one char saved for the ellipsis

def parse_duration_str(duration_str: str) -> int:
    """
    Convert a duration string to total seconds.
    First, try ISO 8601 parsing (e.g. "PT3M9S"). If that fails,
    fall back to colon-separated formats like "3:09" or "1:02:30".
    """
    try:
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception as e:
        if ':' in duration_str:
            try:
                parts = [int(x) for x in duration_str.split(':')]
                if len(parts) == 2:
                    minutes, seconds = parts
                    return minutes * 60 + seconds
                elif len(parts) == 3:
                    hours, minutes, seconds = parts
                    return hours * 3600 + minutes * 60 + seconds
            except Exception as e2:
                print(f"Error parsing colon-separated duration '{duration_str}': {e2}")
                return 0
        else:
            print(f"Error parsing duration '{duration_str}': {e}")
            return 0

def format_time(seconds: float) -> str:
    """
    Given total seconds, return "H:MM:SS" or "M:SS" if hours=0.
    """
    secs = int(seconds)
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    else:
        return f"{m}:{s:02d}"

def get_progress_bar_styled(elapsed: float, total: float, bar_length: int = 14) -> str:
    """
    Build a progress bar string in the style:
      elapsed_time  <dashes>❄️<dashes>  total_time
    For example: 0:30 —❄️———— 3:09
    """
    if total <= 0:
        return "Progress: N/A"
    fraction = min(elapsed / total, 1)
    marker_index = int(fraction * bar_length)
    if marker_index >= bar_length:
        marker_index = bar_length - 1
    left = "━" * marker_index
    right = "─" * (bar_length - marker_index - 1)
    bar = left + "❄️" + right
    return f"{format_time(elapsed)} {bar} {format_time(total)}"


async def update_progress_caption(
    chat_id: int,
    progress_message: Message,
    start_time: float,
    total_duration: float,
    base_caption: str
):
    """
    Periodically update the inline keyboard so that the second row's button text
    shows the current progress bar. The caption remains `base_caption`.
    """
    while True:
        elapsed = time.time() - start_time
        if elapsed > total_duration:
            elapsed = total_duration
        progress_bar = get_progress_bar_styled(elapsed, total_duration)

        # Rebuild the keyboard with updated progress bar in the second row
        control_row = [
            InlineKeyboardButton(text="▷", callback_data="pause"),
            InlineKeyboardButton(text="II", callback_data="resume"),
            InlineKeyboardButton(text="‣‣I", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="stop")
        ]
        progress_button = InlineKeyboardButton(text=progress_bar, callback_data="progress")
        playlist_button = InlineKeyboardButton(text="𝖠𝖽𝖽 𝗍𝗈 𝗉𝗅𝖺𝗒𝗅𝗂𝗌𝗍", callback_data="add_to_playlist")

        new_keyboard = InlineKeyboardMarkup([
            control_row,
            [progress_button],
            [playlist_button]
        ])

        try:
            await bot.edit_message_caption(
                chat_id,
                progress_message.id,
                caption=base_caption,
                reply_markup=new_keyboard
            )
        except Exception as e:
            # Ignore MESSAGE_NOT_MODIFIED, otherwise break
            if "MESSAGE_NOT_MODIFIED" in str(e):
                pass
            else:
                print(f"Error updating progress caption for chat {chat_id}: {e}")
                break

        if elapsed >= total_duration:
            break

        await asyncio.sleep(18)



LOG_CHAT_ID = "@frozenmusiclogs"

async def fallback_local_playback(chat_id: int, message: Message, song_info: dict):
    playback_mode[chat_id] = "local"
    try:
        # Cancel any existing playback task
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()

        # Validate URL
        video_url = song_info.get("url")
        if not video_url:
            print(f"Invalid video URL for song: {song_info}")
            chat_containers[chat_id].pop(0)
            return

        # Notify
        try:
            await message.edit(f"Starting local playback for ⚡ {song_info['title']}...")
        except Exception:
            message = await bot.send_message(
                chat_id,
                f"Starting local playback for ⚡ {song_info['title']}..."
            )

        # Download & play locally
        media_path = await vector_transport_resolver(video_url)
        await call_py.play(
            chat_id,
            MediaStream(media_path, video_flags=MediaStream.Flags.IGNORE)
        )
        playback_tasks[chat_id] = asyncio.current_task()

        # Prepare caption & keyboard
        total_duration = parse_duration_str(song_info.get("duration", "0:00"))
        one_line = _one_line_title(song_info["title"])
        base_caption = (
            "<blockquote>"
            "<b>🎧 Dream ✘ Music Streaming</b> (Local Playback)\n\n"
            f"❍ <b>Title:</b> {one_line}\n"
            f"❍ <b>Requested by:</b> {song_info['requester']}"
            "</blockquote>"
        )
        initial_progress = get_progress_bar_styled(0, total_duration)

        control_row = [
            InlineKeyboardButton(text="▷", callback_data="pause"),
            InlineKeyboardButton(text="II", callback_data="resume"),
            InlineKeyboardButton(text="‣‣I", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="stop"),
        ]
        progress_button = InlineKeyboardButton(text=initial_progress, callback_data="progress")
        base_keyboard = InlineKeyboardMarkup([control_row, [progress_button]])

        # Use raw thumbnail if available
        thumb_url = song_info.get("thumbnail")
        progress_message = await message.reply_photo(
            photo=thumb_url,
            caption=base_caption,
            reply_markup=base_keyboard,
            parse_mode=ParseMode.HTML
        )

        # Remove "processing" message
        await message.delete()

        # Kick off progress updates
        asyncio.create_task(
            update_progress_caption(
                chat_id,
                progress_message,
                time.time(),
                total_duration,
                base_caption
            )
        )

        # Log start
        asyncio.create_task(
            bot.send_message(
                LOG_CHAT_ID,
                "#started_streaming\n"
                f"• Title: {song_info.get('title','Unknown')}\n"
                f"• Duration: {song_info.get('duration','Unknown')}\n"
                f"• Requested by: {song_info.get('requester','Unknown')}\n"
                f"• Mode: local"
            )
        )

    except Exception as e:
        print(f"Error during fallback local playback in chat {chat_id}: {e}")
        await bot.send_message(
            chat_id,
            f"❌ Failed to play “{song_info.get('title','Unknown')}” locally: {e}"
        )

        if chat_id in chat_containers and chat_containers[chat_id]:
            chat_containers[chat_id].pop(0)




@bot.on_callback_query()
async def callback_query_handler(client, callback_query):
    chat_id = callback_query.message.chat.id
    user_id = callback_query.from_user.id
    data = callback_query.data
    user = callback_query.from_user

    # Check admin
    if not await deterministic_privilege_validator(callback_query):
        await callback_query.answer("❌ You need to be an admin to use this button.", show_alert=True)
        return

    # ----------------- PAUSE -----------------
    if data == "pause":
        try:
            await call_py.pause(chat_id)
            await callback_query.answer("⏸ Playback paused.")
            await client.send_message(chat_id, f"⏸️ 𝖯𝗅𝖺𝗒𝖻𝖺𝖼𝗄 𝗉𝖺𝗎𝗌𝖾𝖽 𝖻𝗒 {user.first_name}.")
        except Exception as e:
            await callback_query.answer("❌ 𝖤𝗋𝗋𝗈𝗋 𝗉𝖺𝗎𝗌𝗂𝗇𝗀 𝗉𝗅𝖺𝗒𝖻𝖺𝖼𝗄.", show_alert=True)

    # ----------------- RESUME -----------------
    elif data == "resume":
        try:
            await call_py.resume(chat_id)
            await callback_query.answer("▶️ Playback resumed.")
            await client.send_message(chat_id, f"▶️ 𝖯𝗅𝖺𝗒𝖻𝖺𝖼𝗄 𝗋𝖾𝗌𝗎𝗆𝖾𝖽 𝖻𝗒 {user.first_name}.")
        except Exception as e:
            await callback_query.answer("❌ 𝖤𝗋𝗋𝗈𝗋 𝗋𝖾𝗌𝗎𝗆𝗂𝗇𝗀 𝗉𝗅𝖺𝗒𝖻𝖺𝖼𝗄.", show_alert=True)

    # ----------------- SKIP -----------------
    elif data == "skip":
        if chat_id in chat_containers and chat_containers[chat_id]:
            skipped_song = chat_containers[chat_id].pop(0)

            try:
                await call_py.leave_call(chat_id)
            except Exception as e:
                print("Local leave_call error:", e)
            await asyncio.sleep(3)

            try:
                os.remove(skipped_song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")

            await client.send_message(chat_id, f"⏩ {user.first_name} skipped **{skipped_song['title']}**.")

            if chat_id in chat_containers and chat_containers[chat_id]:
                await callback_query.answer("⏩ 𝖲𝗄𝗂𝗉𝗉𝖾𝖽! 𝖯𝗅𝖺𝗒𝗂𝗇𝗀 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀...")

                # Play next song directly using fallback_local_playback
                next_song_info = chat_containers[chat_id][0]
                try:
                    dummy_msg = await bot.send_message(chat_id, f"🎧 𝖯𝗋𝖾𝗉𝖺𝗋𝗂𝗇𝗀 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀: **{next_song_info['title']}** ...")
                    await fallback_local_playback(chat_id, dummy_msg, next_song_info)
                except Exception as e:
                    print(f"Error starting next local playback: {e}")
                    await bot.send_message(chat_id, f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗌𝗍𝖺𝗋𝗍 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀: {e}")

            else:
                await callback_query.answer("⏩ 𝖲𝗄𝗂𝗉𝗉𝖾𝖽! 𝖭𝗈 𝗆𝗈𝗋𝖾 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")
        else:
            await callback_query.answer("❌ 𝖭𝗈 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾 𝗍𝗈 𝗌𝗄𝗂𝗉.", show_alert=True)

    # ----------------- CLEAR -----------------
    elif data == "clear":
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            chat_containers.pop(chat_id)
            await callback_query.message.edit("🗑️ Cleared the queue.")
            await callback_query.answer("🗑️ Cleared the queue.")
        else:
            await callback_query.answer("❌ 𝖭𝗈 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾 𝗍𝗈 𝖼𝗅𝖾𝖺𝗋.", show_alert=True)

    # ----------------- STOP -----------------
    elif data == "stop":
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            chat_containers.pop(chat_id)

        try:
            await call_py.leave_call(chat_id)
            await callback_query.answer("🛑 𝖯𝗅𝖺𝗒𝖻𝖺𝖼𝗄 𝗌𝗍𝗈𝗉𝗉𝖾𝖽 𝖺𝗇𝖽 𝗊𝗎𝖾𝗎𝖾 𝖼𝗅𝖾𝖺𝗋𝖾𝖽.")
            await client.send_message(chat_id, f"🛑 𝖯𝗅𝖺𝗒𝖻𝖺𝖼𝗄 𝗌𝗍𝗈𝗉𝗉𝖾𝖽 𝖺𝗇𝖽 𝗊𝗎𝖾𝗎𝖾 𝖼𝗅𝖾𝖺𝗋𝖾𝖽 𝖻𝗒 {user.first_name}.")
        except Exception as e:
            print("Stop error:", e)
            await callback_query.answer("❌ 𝖤𝗋𝗋𝗈𝗋 𝗌𝗍𝗈𝗉𝗉𝗂𝗇𝗀 𝗉𝗅𝖺𝗒𝖻𝖺𝖼𝗄.", show_alert=True)




@call_py.on_update(fl.stream_end())
async def stream_end_handler(_: PyTgCalls, update: StreamEnded):
    chat_id = update.chat_id

    if chat_id in chat_containers and chat_containers[chat_id]:
        # Remove the finished song from the queue
        skipped_song = chat_containers[chat_id].pop(0)
        await asyncio.sleep(3)  # Delay to ensure the stream has fully ended

        try:
            os.remove(skipped_song.get('file_path', ''))
        except Exception as e:
            print(f"Error deleting file: {e}")

        if chat_id in chat_containers and chat_containers[chat_id]:
            # If there are more songs, play next song directly using fallback_local_playback
            next_song_info = chat_containers[chat_id][0]
            try:
                # Create a fake message object to pass
                dummy_msg = await bot.send_message(chat_id, f"🎧 𝖯𝗋𝖾𝗉𝖺𝗋𝗂𝗇𝗀 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀: **{next_song_info['title']}** ...")
                await fallback_local_playback(chat_id, dummy_msg, next_song_info)
            except Exception as e:
                print(f"Error starting next local playback: {e}")
                await bot.send_message(chat_id, f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗌𝗍𝖺𝗋𝗍 𝗇𝖾𝗑𝗍 𝗌𝗈𝗇𝗀: {e}")
        else:
            # Queue empty; leave VC
            await leave_voice_chat(chat_id)
            await bot.send_message(chat_id, "❌ 𝖭𝗈 𝗆𝗈𝗋𝖾 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")
    else:
        # No songs in the queue
        await leave_voice_chat(chat_id)
        await bot.send_message(chat_id, "❌ 𝖭𝗈 𝗆𝗈𝗋𝖾 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")



async def leave_voice_chat(chat_id):
    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        print(f"Error leaving the voice chat: {e}")

    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]



@bot.on_message(filters.group & filters.command(["stop", "end"]))
async def stop_handler(client, message):
    chat_id = message.chat.id

    # Check admin rights
    if not await deterministic_privilege_validator(message):
        await message.reply("❌ 𝖸𝗈𝗎 𝗇𝖾𝖾𝖽 𝗍𝗈 𝖻𝖾 𝖺𝗇 𝖺𝖽𝗆𝗂𝗇 𝗍𝗈 𝗎𝗌𝖾 𝗍𝗁𝗂𝗌 𝖼𝗈𝗆𝗆𝖺𝗇𝖽.")
        return

    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        if "not in a call" in str(e).lower():
            await message.reply("❌ 𝖳𝗁𝖾 𝖻𝗈𝗍 𝗂𝗌 𝗇𝗈𝗍 𝖼𝗎𝗋𝗋𝖾𝗇𝗍𝗅𝗒 𝗂𝗇 𝖺 𝗏𝗈𝗂𝖼𝖾 𝖼𝗁𝖺𝗍.")
        else:
            await message.reply(f"❌ 𝖠𝗇 𝖾𝗋𝗋𝗈𝗋 𝗈𝖼𝖼𝗎𝗋𝗋𝖾𝖽 𝗐𝗁𝗂𝗅𝖾 𝗅𝖾𝖺𝗏𝗂𝗇𝗀 𝗍𝗁𝖾 𝗏𝗈𝗂𝖼𝖾 𝖼𝗁𝖺𝗍: {str(e)}\n\nSupport: @CFCBots")
        return

    # Clear the song queue
    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    # Cancel any playback tasks if present
    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]

    await message.reply("❇️ 𝖲𝗍𝗈𝗉𝗉𝖾𝖽 𝗍𝗁𝖾 𝗆𝗎𝗌𝗂𝖼 𝖺𝗇𝖽 𝖼𝗅𝖾𝖺𝗋𝖾𝖽 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")


@bot.on_message(filters.command("song"))
async def song_command_handler(_, message):
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎶 Download Now", url="https://t.me/songdownloader1bot?start=true")]]
    )
    text = (
        "ᴄʟɪᴄᴋ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ᴜsᴇ ᴛʜᴇ sᴏɴɢ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ. 🎵\n\n"
        "ʏᴏᴜ ᴄᴀɴ sᴇɴᴅ ᴛʜᴇ sᴏɴɢ ɴᴀᴍᴇ ᴏʀ ᴀɴʏ ǫᴜᴇʀʏ ᴅɪʀᴇᴄᴛʟʏ ᴛᴏ ᴛʜᴇ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ, ⬇️\n\n"
        "ᴀɴᴅ ɪᴛ ᴡɪʟʟ ғᴇᴛᴄʜ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅ ᴛʜᴇ sᴏɴɢ ғᴏʀ ʏᴏᴜ. 🚀"
    )
    await message.reply(text, reply_markup=keyboard)



@bot.on_message(filters.group & filters.command("pause"))
async def pause_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ 𝖸𝗈𝗎 𝗇𝖾𝖾𝖽 𝗍𝗈 𝖻𝖾 𝖺𝗇 𝖺𝖽𝗆𝗂𝗇 𝗍𝗈 𝗎𝗌𝖾 𝗍𝗁𝗂𝗌 𝖼𝗈𝗆𝗆𝖺𝗇𝖽.")
        return

    try:
        await call_py.pause(chat_id)
        await message.reply("⏸ 𝖯𝖺𝗎𝗌𝖾𝖽 𝗍𝗁𝖾 𝗌𝗍𝗋𝖾𝖺𝗆.")
    except Exception as e:
        await message.reply(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗉𝖺𝗎𝗌𝖾 𝗍𝗁𝖾 𝗌𝗍𝗋𝖾𝖺𝗆.\nError: {str(e)}")


@bot.on_message(filters.group & filters.command("resume"))
async def resume_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ 𝖸𝗈𝗎 𝗇𝖾𝖾𝖽 𝗍𝗈 𝖻𝖾 𝖺𝗇 𝖺𝖽𝗆𝗂𝗇 𝗍𝗈 𝗎𝗌𝖾 𝗍𝗁𝗂𝗌 𝖼𝗈𝗆𝗆𝖺𝗇𝖽.")
        return

    try:
        await call_py.resume(chat_id)
        await message.reply("▶️ 𝖱𝖾𝗌𝗎𝗆𝖾𝖽 𝗍𝗁𝖾 𝗌𝗍𝗋𝖾𝖺𝗆.")
    except Exception as e:
        await message.reply(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗋𝖾𝗌𝗎𝗆𝖾 𝗍𝗁𝖾 𝗌𝗍𝗋𝖾𝖺𝗆.\nError: {str(e)}")



@bot.on_message(filters.group & filters.command("skip"))
async def skip_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ 𝖸𝗈𝗎 𝗇𝖾𝖾𝖽 𝗍𝗈 𝖻𝖾 𝖺𝗇 𝖺𝖽𝗆𝗂𝗇 𝗍𝗈 𝗎𝗌𝖾 𝗍𝗁𝗂𝗌 𝖼𝗈𝗆𝗆𝖺𝗇𝖽.")
        return

    status_message = await message.reply("⏩ 𝖲𝗄𝗂𝗉𝗉𝗂𝗇𝗀 𝗍𝗁𝖾 𝖼𝗎𝗋𝗋𝖾𝗇𝗍 𝗌𝗈𝗇𝗀...")

    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await status_message.edit("❌ 𝖭𝗈 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾 𝗍𝗈 𝗌𝗄𝗂𝗉.")
        return

    # Remove the current song from the queue
    skipped_song = chat_containers[chat_id].pop(0)

    # Always local mode only
    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        print("Local leave_call error:", e)

    await asyncio.sleep(3)

    # Delete the local file if exists
    try:
        if skipped_song.get('file_path'):
            os.remove(skipped_song['file_path'])
    except Exception as e:
        print(f"Error deleting file: {e}")

    # Check for next song
    if not chat_containers.get(chat_id):
        await status_message.edit(
            f"⏩ 𝖲𝗄𝗂𝗉𝗉𝖾𝖽 **{skipped_song['title']}**.\n\n❄️ No more songs in the queue."
        )
    else:
        await status_message.edit(
            f"⏩ 𝖲𝗄𝗂𝗉𝗉𝖾𝖽 **{skipped_song['title']}**.\n\n❄️ Playing the next song..."
        )
        await skip_to_next_song(chat_id, status_message)




@bot.on_message(filters.command("reboot"))
async def reboot_handler(_, message):
    chat_id = message.chat.id

    try:
        # Remove audio files for songs in the queue for this chat.
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file for chat {chat_id}: {e}")
            # Clear the queue for this chat.
            chat_containers.pop(chat_id, None)
        
        # Cancel any playback tasks for this chat.
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()
            del playback_tasks[chat_id]

        # Remove chat-specific cooldown and pending command entries.
        chat_last_command.pop(chat_id, None)
        chat_pending_commands.pop(chat_id, None)

        # Remove playback mode for this chat.
        playback_mode.pop(chat_id, None)

        # Clear any API playback records for this chat.
        global api_playback_records
        api_playback_records = [record for record in api_playback_records if record.get("chat_id") != chat_id]

        # Leave the voice chat for this chat.
        try:
            await call_py.leave_call(chat_id)
        except Exception as e:
            print(f"Error leaving call for chat {chat_id}: {e}")

        await message.reply("♻️ 𝖱𝖾𝖻𝗈𝗈𝗍𝖾𝖽 𝖿𝗈𝗋 𝗍𝗁𝗂𝗌 𝖼𝗁𝖺𝗍. 𝖠𝗅𝗅 𝖽𝖺𝗍𝖺 𝖿𝗈𝗋 𝗍𝗁𝗂𝗌 𝖼𝗁𝖺𝗍 𝗁𝖺𝗌 𝖻𝖾𝖾𝗇 𝖼𝗅𝖾𝖺𝗋𝖾𝖽.")
    except Exception as e:
        await message.reply(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝗋𝖾𝖻𝗈𝗈𝗍 𝖿𝗈𝗋 𝗍𝗁𝗂𝗌 𝖼𝗁𝖺𝗍. 𝖤𝗋𝗋𝗈𝗋: {str(e)}\n\n 𝗌𝗎𝗉𝗉𝗈𝗋𝗍 - @CloseFriendsCommunity")



@bot.on_message(filters.command("ping"))
async def ping_handler(_, message):
    try:
        # Calculate uptime
        current_time = time.time()
        uptime_seconds = int(current_time - bot_start_time)
        uptime_str = str(timedelta(seconds=uptime_seconds))

        # Local system stats
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        ram_usage = f"{memory.used // (1024 ** 2)}MB / {memory.total // (1024 ** 2)}MB ({memory.percent}%)"
        disk = psutil.disk_usage('/')
        disk_usage = f"{disk.used // (1024 ** 3)}GB / {disk.total // (1024 ** 3)}GB ({disk.percent}%)"

        # Build the final message
        response = (
            f"🏓 **Pong!**\n\n"
            f"**Local Server Stats:**\n"
            f"• **Uptime:** `{uptime_str}`\n"
            f"• **CPU Usage:** `{cpu_usage}%`\n"
            f"• **RAM Usage:** `{ram_usage}`\n"
            f"• **Disk Usage:** `{disk_usage}`"
        )

        await message.reply(response)
    except Exception as e:
        await message.reply(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝖾𝗑𝖾𝖼𝗎𝗍𝖾 𝗍𝗁𝖾 𝖼𝗈𝗆𝗆𝖺𝗇𝖽.\nError: {str(e)}\n\nSupport: @CFCBots")




@bot.on_message(filters.group & filters.command("clear"))
async def clear_handler(_, message):
    chat_id = message.chat.id

    if chat_id in chat_containers:
        # Clear the chat-specific queue
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        
        chat_containers.pop(chat_id)
        await message.reply("🗑️ 𝖢𝗅𝖾𝖺𝗋𝖾𝖽 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾.")
    else:
        await message.reply("❌ 𝖭𝗈 𝗌𝗈𝗇𝗀𝗌 𝗂𝗇 𝗍𝗁𝖾 𝗊𝗎𝖾𝗎𝖾 𝗍𝗈 𝖼𝗅𝖾𝖺𝗋.")


@bot.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_handler(_, message):
    # Ensure the command is used in reply to a message
    if not message.reply_to_message:
        await message.reply("❌ 𝖯𝗅𝖾𝖺𝗌𝖾 𝗋𝖾𝗉𝗅𝗒 𝗍𝗈 𝗍𝗁𝖾 𝗆𝖾𝗌𝗌𝖺𝗀𝖾 𝗒𝗈𝗎 𝗐𝖺𝗇𝗍 𝗍𝗈 𝖻𝗋𝗈𝖺𝖽𝖼𝖺𝗌𝗍.")
        return

    broadcast_message = message.reply_to_message

    # Retrieve all broadcast chat IDs from the collection
    all_chats = list(broadcast_collection.find({}))
    success = 0
    failed = 0

    # Loop through each chat ID and forward the message
    for chat in all_chats:
        try:
            # Ensure the chat ID is an integer (this will handle group IDs properly)
            target_chat_id = int(chat.get("chat_id"))
        except Exception as e:
            print(f"Error casting chat_id: {chat.get('chat_id')} - {e}")
            failed += 1
            continue

        try:
            await bot.forward_messages(
                chat_id=target_chat_id,
                from_chat_id=broadcast_message.chat.id,
                message_ids=broadcast_message.id
            )
            success += 1
        except Exception as e:
            print(f"Failed to broadcast to {target_chat_id}: {e}")
            failed += 1

        # Wait for 1 second to avoid flooding the server and Telegram
        await asyncio.sleep(1)

    await message.reply(f"Broadcast complete!\n✅ Success: {success}\n❌ Failed: {failed}")



@bot.on_message(filters.command("frozen_check"))
async def frozen_check_command(client: Client, message):
    await message.reply_text("frozen check successful ✨")



def save_state_to_db():
    """
    Persist only chat_containers (queues) into MongoDB before restart.
    """
    data = {
        "chat_containers": { str(cid): queue for cid, queue in chat_containers.items() }
    }

    state_backup.replace_one(
        {"_id": "singleton"},
        {"_id": "singleton", "state": data},
        upsert=True
    )

    chat_containers.clear()


def load_state_from_db():
    """
    Load persisted chat_containers (queues) from MongoDB on startup.
    """
    doc = state_backup.find_one_and_delete({"_id": "singleton"})
    if not doc or "state" not in doc:
        return

    data = doc["state"]

    for cid_str, queue in data.get("chat_containers", {}).items():
        try:
            chat_containers[int(cid_str)] = queue
        except ValueError:
            continue



class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot is running!")
        elif self.path == "/status":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot status: Running")
        elif self.path == "/restart":
            save_state_to_db()
            os.execl(sys.executable, sys.executable, *sys.argv)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/webhook":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                update = json.loads(body)
                bot._process_update(update)
            except Exception as e:
                print("Error processing update:", e)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


def run_http_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("", port), WebhookHandler)
    print(f"HTTP server running on port {port}")
    server.serve_forever()


threading.Thread(target=run_http_server, daemon=True).start()


logger = logging.getLogger(__name__)

frozen_check_event = asyncio.Event()

async def restart_bot():
    port = int(os.environ.get("PORT", 8080))
    url = f"http://localhost:{port}/restart"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    logger.info("Local restart endpoint triggered successfully.")
                else:
                    logger.error(f"Local restart endpoint failed: {resp.status}")
    except Exception as e:
        logger.error(f"Error calling local restart endpoint: {e}")

async def frozen_check_loop(bot_username: str):
    while True:
        try:
            # 1) send the check command
            await assistant.send_message(bot_username, "/frozen_check")
            logger.info(f"Sent /frozen_check to @{bot_username}")

            # 2) poll for a reply for up to 30 seconds
            deadline = time.time() + 30
            got_ok = False

            while time.time() < deadline:
                async for msg in assistant.get_chat_history(bot_username, limit=1):
                    text = msg.text or ""
                    if "frozen check successful ✨" in text.lower():
                        got_ok = True
                        logger.info("Received frozen check confirmation.")
                        break
                if got_ok:
                    break
                await asyncio.sleep(3)

            # 3) if no confirmation, restart
            if not got_ok:
                logger.warning("No frozen check reply—restarting bot.")
                await restart_bot()

        except Exception as e:
            logger.error(f"Error in frozen_check_loop: {e}")

        await asyncio.sleep(60)




logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Loading persisted state from MongoDB...")
    load_state_from_db()
    logger.info("State loaded successfully.")

    logger.info("→ Starting PyTgCalls client...")
    call_py.start()
    logger.info("PyTgCalls client started.")

    logger.info("→ Starting Telegram bot client (bot.start)...")
    try:
        bot.start()
    except Exception as e:
        logger.error(f"❌ Failed to start Pyrogram client: {e}")
        sys.exit(1)

    me = bot.get_me()
    BOT_NAME = me.first_name or "Frozen Music"
    BOT_USERNAME = me.username or os.getenv("BOT_USERNAME", "vcmusiclubot")
    BOT_LINK = f"https://t.me/{BOT_USERNAME}"

    logger.info(f"✅ Bot Name: {BOT_NAME!r}")
    logger.info(f"✅ Bot Username: {BOT_USERNAME}")
    logger.info(f"✅ Bot Link: {BOT_LINK}")

    # start the frozen‑check loop (no handler registration needed)
    asyncio.get_event_loop().create_task(frozen_check_loop(BOT_USERNAME))

    if not assistant.is_connected:
        logger.info("Assistant not connected; starting assistant client...")
        assistant.run()
        logger.info("Assistant client connected.")

    try:
        assistant_user = assistant.get_me()
        ASSISTANT_USERNAME = assistant_user.username
        ASSISTANT_CHAT_ID = assistant_user.id
        logger.info(f"❄️ 𝖠𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝖴𝗌𝖾𝗋𝗇𝖺𝗆𝖾: {ASSISTANT_USERNAME}")
        logger.info(f"❄️ 𝖠𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝖢𝗁𝖺𝗍 𝖨𝖣: {ASSISTANT_CHAT_ID}")

        asyncio.get_event_loop().run_until_complete(precheck_channels(assistant))
        logger.info("✅ 𝖠𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝗉𝗋𝖾𝖼𝗁𝖾𝖼𝗄 𝖼𝗈𝗆𝗉𝗅𝖾𝗍𝖾𝖽.")

    except Exception as e:
        logger.error(f"❌ 𝖥𝖺𝗂𝗅𝖾𝖽 𝗍𝗈 𝖿𝖾𝗍𝖼𝗁 𝖺𝗌𝗌𝗂𝗌𝗍𝖺𝗇𝗍 𝗂𝗇𝖿𝗈: {e}")

    logger.info("→ Entering idle() (long-polling)")
    idle()

    bot.stop()
    logger.info("Bot stopped.")
    logger.info("✅  𝖠𝗅𝗅 𝗌𝖾𝗋𝗏𝗂𝖼𝖾𝗌 𝖺𝗋𝖾 𝗎𝗉 𝖺𝗇𝖽 𝗋𝗎𝗇𝗇𝗂𝗇𝗀. 𝖡𝗈𝗍 𝗌𝗍𝖺𝗋𝗍𝖾𝖽 𝗌𝗎𝖼𝖼𝖾𝗌𝗌𝖿𝗎𝗅𝗅𝗒.")



