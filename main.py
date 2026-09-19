import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from html import escape
from zoneinfo import ZoneInfo

import aiohttp
import asyncpg
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    ErrorEvent,
    ChatMemberUpdated,
    MessageEntity,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================================================
#                       КОНФИГ И НАСТРОЙКИ
# =========================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN", "adminrpl")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "rpl1488")

REMIND_BEFORE_1 = 45
REMIND_BEFORE_2 = 15

ALLOWED_HASHTAGS = {"#rplpuck", "#matchday", "#result"}
PUCK_BOT_USERNAME = "@rplpuck_bot"

SCHEDULER_INTERVAL = 20
WARNING_AUTODELETE_SECONDS = 10
ADMIN_RIGHTS_CHECK_INTERVAL = 300
ROSTER_CHECK_INTERVAL = 300
STEAM_REMINDER_INTERVAL = 10800

# ---------- GitHub Gist (whitelist transport) ----------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GIST_ID = os.getenv("GIST_ID", "")
GIST_FILENAME = "whitelist.json"

MSK = ZoneInfo("Europe/Moscow")

NICKNAME_PATTERN = re.compile(r"^.+\s+#\d+$")


def is_valid_steam_id(raw_input: str) -> bool:
    s = raw_input.strip()
    if any(bad in s.lower() for bad in ["http://", "https://", "steamcommunity.com", "/", "\\", "?", "#"]):
        return False
    pattern = re.compile(r"^(7656\d{13}|STEAM_[0-5]:[0-1]:\d+|\[U:[0-1]:\d+\]|[a-zA-Z0-9_\-]{3,32})$")
    return bool(pattern.match(s))


def esc(value) -> str:
    return escape(str(value if value is not None else ""))


# =========================================================
#                        БАЗА ДАННЫХ
# =========================================================

_pool: asyncpg.Pool | None = None


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=20, command_timeout=10)
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                league TEXT NOT NULL DEFAULT 'RPL',
                logo_emoji_id TEXT,
                logo_emoji TEXT
            );

            CREATE TABLE IF NOT EXISTS captain_chats (
                league TEXT PRIMARY KEY,
                chat_id BIGINT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT UNIQUE NOT NULL,
                title TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channel_chats (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT NOT NULL REFERENCES channels(channel_id) ON DELETE CASCADE,
                chat_id BIGINT NOT NULL,
                UNIQUE(channel_id, chat_id)
            );

            CREATE TABLE IF NOT EXISTS servers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                ip TEXT NOT NULL,
                port TEXT NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                team_home_chat_id BIGINT NOT NULL,
                team_home_name TEXT NOT NULL,
                team_away_chat_id BIGINT NOT NULL,
                team_away_name TEXT NOT NULL,
                match_time TIMESTAMPTZ NOT NULL,
                server_name TEXT NOT NULL DEFAULT '',
                server_ip TEXT NOT NULL DEFAULT '',
                server_port TEXT NOT NULL DEFAULT '',
                server_password TEXT NOT NULL DEFAULT '',
                notified_45 BOOLEAN NOT NULL DEFAULT FALSE,
                notified_15 BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS players (
                tg_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                steam_id TEXT UNIQUE,
                nickname TEXT,
                team_chat_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS player_chats (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                UNIQUE(tg_id, chat_id)
            );

            CREATE TABLE IF NOT EXISTS player_history (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                team_name TEXT NOT NULL,
                league TEXT NOT NULL,
                joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                left_at TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS bans (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT NOT NULL,
                league TEXT NOT NULL DEFAULT 'ALL',
                reason TEXT NOT NULL,
                until TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS exceptions (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_players_team ON players(team_chat_id);
            CREATE INDEX IF NOT EXISTS idx_bans_tg_id ON bans(tg_id);
            CREATE INDEX IF NOT EXISTS idx_player_chats_tg ON player_chats(tg_id);
            CREATE INDEX IF NOT EXISTS idx_player_chats_chat ON player_chats(chat_id);
            """
        )

        alter_commands = [
            "ALTER TABLE chats ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'RPL'",
            "ALTER TABLE chats ADD COLUMN IF NOT EXISTS logo_emoji_id TEXT",
            "ALTER TABLE chats ADD COLUMN IF NOT EXISTS logo_emoji TEXT",
            "ALTER TABLE bans ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'ALL'",
        ]
        for cmd in alter_commands:
            try:
                await conn.execute(cmd)
            except Exception as e:
                logging.debug(f"Миграция (ALTER): {e}")


# =========================================================
#          GITHUB GIST (WHITELIST TRANSPORT ДЛЯ VPS)
# =========================================================

_whitelist_lock = asyncio.Lock()


async def _get_whitelist_ids() -> list[str]:
    """Собирает SteamID игроков, которые в команде и не забанены."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT p.steam_id
            FROM players p
            WHERE p.team_chat_id IS NOT NULL
              AND p.steam_id IS NOT NULL
              AND p.steam_id <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM bans b
                  WHERE b.tg_id = p.tg_id
                    AND (b.until IS NULL OR b.until > NOW())
              )
            """
        )
    return sorted({r["steam_id"] for r in rows})


async def _push_to_gist(content: str) -> tuple[bool, str]:
    """Отправляет whitelist в GitHub Gist через PATCH."""
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN не задан"
    if not GIST_ID:
        return False, "GIST_ID не задан"

    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {
        "files": {
            GIST_FILENAME: {
                "content": content
            }
        }
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                text = await resp.text()
                if resp.status in (200, 201):
                    return True, f"GitHub {resp.status}, {len(content)} байт"
                return False, f"GitHub {resp.status}: {text[:300]}"
    except asyncio.TimeoutError:
        return False, "GitHub timeout (30 сек)"
    except Exception as e:
        return False, f"GitHub ошибка: {e}"


async def rebuild_whitelist_file():
    """Собирает SteamID из БД и пушит их в Gist."""
    if not GITHUB_TOKEN or not GIST_ID:
        logging.debug("GITHUB_TOKEN или GIST_ID не задан — обновление whitelist пропущено.")
        return

    if _whitelist_lock.locked():
        logging.info("⏭ Whitelist уже обновляется — пропускаю повторный запуск.")
        return

    async with _whitelist_lock:
        try:
            steam_ids = await _get_whitelist_ids()
            content = json.dumps(steam_ids, indent=2, ensure_ascii=False) + "\n"

            ok, msg = await _push_to_gist(content)
            if ok:
                logging.info(f"✅ Whitelist обновлён: {len(steam_ids)} SteamID. {msg}")
            else:
                logging.error(f"❌ Не удалось обновить whitelist: {msg}")
        except Exception as e:
            logging.error(f"❌ Ошибка rebuild_whitelist_file: {e}")


# =========================================================
#                   ФУНКЦИИ БАЗЫ ДАННЫХ
# =========================================================

async def add_chat(chat_id: int, name: str, league: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO chats (chat_id, name, league) VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE SET name = EXCLUDED.name, league = EXCLUDED.league
            """,
            chat_id, name, league,
        )


async def get_chats(league: str | None = None):
    async with _pool.acquire() as conn:
        if league:
            return await conn.fetch("SELECT * FROM chats WHERE league = $1 ORDER BY name", league)
        return await conn.fetch("SELECT * FROM chats ORDER BY league, name")


async def get_chat(chat_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM chats WHERE chat_id = $1", chat_id)


async def update_chat_name(chat_id: int, name: str):
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE chats SET name = $1 WHERE chat_id = $2", name, chat_id)


async def update_chat_emoji(chat_id: int, emoji_id: str, emoji_char: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE chats SET logo_emoji_id = $1, logo_emoji = $2 WHERE chat_id = $3",
            emoji_id, emoji_char, chat_id,
        )


async def set_captain_chat(league: str, chat_id: int):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO captain_chats (league, chat_id) VALUES ($1, $2)
            ON CONFLICT (league) DO UPDATE SET chat_id = EXCLUDED.chat_id
            """,
            league, chat_id,
        )


async def get_captain_chat(league: str):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM captain_chats WHERE league = $1", league)


async def get_all_captain_chats():
    async with _pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM captain_chats")


async def is_player_captain(bot: Bot, tg_id: int, league: str) -> bool:
    cap_row = await get_captain_chat(league)
    if not cap_row:
        return False
    try:
        member = await bot.get_chat_member(cap_row["chat_id"], tg_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False


async def add_channel(channel_id: int, title: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO channels (channel_id, title) VALUES ($1, $2)
            ON CONFLICT (channel_id) DO UPDATE SET title = EXCLUDED.title
            """,
            channel_id, title,
        )


async def get_channels():
    async with _pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM channels ORDER BY title")


async def get_channel(channel_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM channels WHERE channel_id = $1", channel_id)


async def link_channel_chat(channel_id: int, chat_id: int):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO channel_chats (channel_id, chat_id) VALUES ($1, $2)
            ON CONFLICT (channel_id, chat_id) DO NOTHING
            """,
            channel_id, chat_id,
        )


async def get_linked_chats(channel_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT c.* FROM chats c
            JOIN channel_chats cc ON cc.chat_id = c.chat_id
            WHERE cc.channel_id = $1
            ORDER BY c.name
            """,
            channel_id,
        )


async def get_linked_chat_ids(channel_id: int):
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM channel_chats WHERE channel_id = $1", channel_id)
        return {r["chat_id"] for r in rows}


async def add_server(name: str, ip: str, port: str, password: str):
    async with _pool.acquire() as conn:
        return await conn.fetchrow(
            "INSERT INTO servers (name, ip, port, password) VALUES ($1,$2,$3,$4) RETURNING *",
            name, ip, port, password,
        )


async def get_servers():
    async with _pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM servers ORDER BY id DESC")


async def get_server(server_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM servers WHERE id = $1", server_id)


async def delete_server(server_id: int):
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM servers WHERE id = $1", server_id)


async def add_match(team_home_chat_id, team_home_name, team_away_chat_id, team_away_name,
                     match_time, server_name, server_ip, server_port, server_password):
    if match_time.tzinfo is None:
        match_time = match_time.replace(tzinfo=MSK)
    match_time = match_time.astimezone(timezone.utc)

    async with _pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO matches (
                team_home_chat_id, team_home_name,
                team_away_chat_id, team_away_name,
                match_time, server_name, server_ip, server_port, server_password
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING *
            """,
            team_home_chat_id, team_home_name, team_away_chat_id, team_away_name,
            match_time, server_name, server_ip, server_port, server_password,
        )


async def get_upcoming_matches():
    async with _pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM matches WHERE match_time > NOW() - INTERVAL '2 hours' ORDER BY match_time"
        )


async def get_match(match_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM matches WHERE id = $1", match_id)


async def delete_match(match_id: int):
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM matches WHERE id = $1", match_id)


async def get_matches_due_for(minutes_before: int, column: str):
    query = f"""
        SELECT * FROM matches
        WHERE {column} = FALSE
          AND match_time - (INTERVAL '1 minute' * $1) <= NOW()
          AND match_time > NOW() - INTERVAL '30 minutes'
    """
    async with _pool.acquire() as conn:
        return await conn.fetch(query, minutes_before)


async def mark_notified(match_id: int, column: str):
    query = f"UPDATE matches SET {column} = TRUE WHERE id = $1"
    async with _pool.acquire() as conn:
        await conn.execute(query, match_id)


async def upsert_player_basic(tg_id: int, username: str | None, first_name: str | None):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (tg_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (tg_id) DO UPDATE SET username = COALESCE(EXCLUDED.username, players.username),
                                              first_name = COALESCE(EXCLUDED.first_name, players.first_name)
            """,
            tg_id, username, first_name,
        )


async def get_player(tg_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM players WHERE tg_id = $1", tg_id)


async def get_all_players_db(limit: int = 100, offset: int = 0):
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.*, c.name as team_name, c.league as team_league
            FROM players p
            LEFT JOIN chats c ON c.chat_id = p.team_chat_id
            ORDER BY p.created_at DESC
            LIMIT $1 OFFSET $2
            """,
            limit, offset,
        )
        total = await conn.fetchval("SELECT COUNT(*) FROM players")
        return rows, total


async def get_player_by_username(username: str):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM players WHERE lower(username) = lower($1)", username)


async def get_player_by_steam_id(steam_id: str):
    async with _pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM players WHERE steam_id = $1", steam_id)


async def set_player_steam(tg_id: int, steam_id: str):
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE players SET steam_id = $1 WHERE tg_id = $2", steam_id, tg_id)
    asyncio.create_task(rebuild_whitelist_file())


async def reset_all_players_steam_ids():
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE players SET steam_id = NULL")
    asyncio.create_task(rebuild_whitelist_file())


async def set_player_nickname(tg_id: int, nickname: str):
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE players SET nickname = $1 WHERE tg_id = $2", nickname, tg_id)


async def set_player_team(tg_id: int, chat_id: int | None):
    changed = False
    async with _pool.acquire() as conn:
        old_player = await conn.fetchrow("SELECT team_chat_id FROM players WHERE tg_id = $1", tg_id)
        old_chat_id = old_player["team_chat_id"] if old_player else None

        if old_chat_id == chat_id:
            return

        changed = True

        if old_chat_id:
            await conn.execute(
                "UPDATE player_history SET left_at = NOW() WHERE tg_id = $1 AND chat_id = $2 AND left_at IS NULL",
                tg_id, old_chat_id,
            )

        if chat_id:
            chat_info = await conn.fetchrow("SELECT name, league FROM chats WHERE chat_id = $1", chat_id)
            if chat_info:
                await conn.execute(
                    """
                    INSERT INTO player_history (tg_id, chat_id, team_name, league)
                    VALUES ($1, $2, $3, $4)
                    """,
                    tg_id, chat_id, chat_info["name"], chat_info["league"],
                )

        await conn.execute("UPDATE players SET team_chat_id = $1 WHERE tg_id = $2", chat_id, tg_id)

    if changed:
        asyncio.create_task(rebuild_whitelist_file())


async def add_player_chat(tg_id: int, chat_id: int):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO player_chats (tg_id, chat_id) VALUES ($1, $2) ON CONFLICT (tg_id, chat_id) DO NOTHING",
            tg_id, chat_id,
        )


async def remove_player_chat(tg_id: int, chat_id: int):
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM player_chats WHERE tg_id = $1 AND chat_id = $2", tg_id, chat_id)


async def get_player_chat_ids(tg_id: int) -> list[int]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM player_chats WHERE tg_id = $1", tg_id)
        return [r["chat_id"] for r in rows]


async def get_linked_player_ids(chat_id: int) -> set[int]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT tg_id FROM player_chats WHERE chat_id = $1", chat_id)
        return {r["tg_id"] for r in rows}


async def get_all_known_player_ids() -> list[int]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT tg_id FROM players")
        return [r["tg_id"] for r in rows]


async def get_unlinked_steam_in_teams() -> list[dict]:
    async with _pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT DISTINCT p.tg_id, p.first_name, p.username, c.name as team_name
            FROM players p
            JOIN chats c ON c.chat_id = p.team_chat_id
            WHERE (p.steam_id IS NULL OR p.steam_id = '')
            """
        )


async def get_team_roster(chat_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT p.* FROM players p
            WHERE p.team_chat_id = $1
            ORDER BY p.nickname NULLS LAST, p.first_name
            """,
            chat_id,
        )


async def get_player_history(tg_id: int):
    async with _pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM player_history WHERE tg_id = $1 ORDER BY id DESC",
            tg_id,
        )


async def ban_player_db(tg_id: int, league: str, reason: str, until: datetime | None):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bans (tg_id, league, reason, until) VALUES ($1, $2, $3, $4)",
            tg_id, league, reason, until,
        )
    asyncio.create_task(rebuild_whitelist_file())


async def unban_player_db(ban_id: int):
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM bans WHERE id = $1", ban_id)
    asyncio.create_task(rebuild_whitelist_file())


async def get_active_ban(tg_id: int, league: str | None = None):
    async with _pool.acquire() as conn:
        if league:
            return await conn.fetchrow(
                """
                SELECT * FROM bans 
                WHERE tg_id = $1 AND (league = $2 OR league = 'ALL') AND (until IS NULL OR until > NOW()) 
                ORDER BY id DESC LIMIT 1
                """,
                tg_id, league,
            )
        return await conn.fetchrow(
            "SELECT * FROM bans WHERE tg_id = $1 AND (until IS NULL OR until > NOW()) ORDER BY id DESC LIMIT 1",
            tg_id,
        )


async def get_active_bans():
    async with _pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM bans
            WHERE (until IS NULL OR until > NOW())
            ORDER BY id DESC
            """
        )


async def add_exception_db(tg_id: int):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO exceptions (tg_id) VALUES ($1) ON CONFLICT (tg_id) DO NOTHING",
            tg_id,
        )


async def remove_exception_db(tg_id: int):
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM exceptions WHERE tg_id = $1", tg_id)


async def is_exception(tg_id: int) -> bool:
    async with _pool.acquire() as conn:
        val = await conn.fetchval("SELECT 1 FROM exceptions WHERE tg_id = $1", tg_id)
        return bool(val)


async def get_exceptions():
    async with _pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT e.tg_id, e.created_at, p.username, p.first_name, p.nickname
            FROM exceptions e
            LEFT JOIN players p ON p.tg_id = e.tg_id
            ORDER BY e.id DESC
            """
        )


# =========================================================
#                    ТЕКСТЫ СООБЩЕНИЙ
# =========================================================

def reminder_45_text(team_home: str, team_away: str) -> str:
    return (
        "⏰ <b>Внимание, до матча осталось 45 минут!</b>\n"
        "Не забудьте прийти! 🙌\n\n"
        f"🆚 <b>{esc(team_home)}</b> — <b>{esc(team_away)}</b>"
    )


def raskatka_text(server_name, server_ip, server_port, server_password, team_home, team_away, color) -> str:
    return (
        "🎮 <b>Калл! Раскатка!</b>\n\n"
        f"🖥 <b>{esc(server_name)}</b>\n"
        f"🌐 IP сервера: <code>{esc(server_ip)}</code>\n"
        f"🔌 Port сервера: <code>{esc(server_port)}</code>\n"
        f"🔑 Password сервера: <code>{esc(server_password)}</code>\n\n"
        f"👉 Вы <b>{color}</b>.\n\n"
        "ℹ️ Составы по цветам:\n"
        f"🔴 Хозяева (ред) — {esc(team_home)}\n"
        f"🔵 Гости (блу) — {esc(team_away)}"
    )


async def build_profile_text(player) -> str:
    nickname = f"<code>{esc(player['nickname'])}</code>" if player["nickname"] else "❌ <i>не указан (формат: Nick #00)</i>"
    steam_id = f"<code>{esc(player['steam_id'])}</code>" if player["steam_id"] else "❌ <i>не привязан</i>"

    if player["team_chat_id"]:
        chat = await get_chat(player["team_chat_id"])
        logo = f"{chat['logo_emoji']} " if chat and chat["logo_emoji"] else ""
        league_badge = f" [{chat['league']}]" if chat else ""
        team_name = f"{logo}<b>{esc(chat['name'])}</b>{league_badge}" if chat else "🆓 Free Agent"
    else:
        team_name = "🆓 Free Agent"

    username = f"@{esc(player['username'])}" if player["username"] else "—"

    return (
        "🏒 <b>Профиль игрока RPL / FPHL</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷 <b>Никнейм и Номер:</b> {nickname}\n"
        f"🎮 <b>SteamID:</b> {steam_id}\n"
        f"🛡 <b>Команда:</b> {team_name}\n"
        f"💬 <b>Telegram:</b> {username}\n\n"
        "<i>Для игр в лигах необходимо заполнить никнейм с номером и привязать SteamID!</i>"
    )


# =========================================================
#                  FSM-СОСТОЯНИЯ АДМИНКИ
# =========================================================

class AdminAuth(StatesGroup):
    waiting_login = State()
    waiting_password = State()


class AddChat(StatesGroup):
    waiting_league = State()
    waiting_id = State()
    waiting_name = State()


class AddCaptainsChat(StatesGroup):
    waiting_league = State()
    waiting_id = State()


class AddServer(StatesGroup):
    waiting_name = State()
    waiting_ip = State()
    waiting_port = State()
    waiting_password = State()


class AddMatch(StatesGroup):
    waiting_team1 = State()
    waiting_team2 = State()
    waiting_datetime = State()
    waiting_server = State()


class AddChannel(StatesGroup):
    waiting_id = State()
    waiting_title = State()
    waiting_chats = State()


class EditChat(StatesGroup):
    waiting_name = State()
    waiting_emoji = State()


class BanPlayer(StatesGroup):
    waiting_target = State()
    waiting_league = State()
    waiting_reason = State()
    waiting_duration = State()


class AdminExceptions(StatesGroup):
    waiting_target = State()


class AdminEditProfile(StatesGroup):
    waiting_target = State()
    waiting_nickname = State()
    waiting_steamid = State()


class SetProfile(StatesGroup):
    waiting_steamid = State()
    waiting_nickname = State()


# =========================================================
#                       КЛАВИАТУРЫ
# =========================================================

def admin_main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить команду", callback_data="adm:add_chat")
    kb.button(text="👑 Чаты капитанов", callback_data="adm:captains_menu")
    kb.button(text="📋 Список команд", callback_data="adm:list_chats")
    kb.button(text="👥 Все игроки", callback_data="adm:all_players:0")
    kb.button(text="✏️ Ред. команду", callback_data="adm:edit_chat")
    kb.button(text="🆚 Добавить матч", callback_data="adm:add_match")
    kb.button(text="📅 Список матчей", callback_data="adm:list_matches")
    kb.button(text="🖥 Добавить сервер", callback_data="adm:add_server")
    kb.button(text="🗄 Список серверов", callback_data="adm:list_servers")
    kb.button(text="📡 Привязать канал", callback_data="adm:add_channel")
    kb.button(text="🔗 Список каналов", callback_data="adm:list_channels")
    kb.button(text="🛡 Исключения", callback_data="adm:exceptions_menu")
    kb.button(text="👤 Ред. профиля", callback_data="adm:edit_profile")
    kb.button(text="🚫 Забанить игрока", callback_data="adm:ban_player")
    kb.button(text="📋 Список банов", callback_data="adm:list_bans")
    kb.button(text="🔄 Сбросить всем SteamID", callback_data="adm:reset_steam_confirm")
    kb.button(text="🔁 Обновить whitelist", callback_data="adm:rebuild_wl")
    kb.adjust(2)
    return kb.as_markup()


def league_choice_kb(prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏆 RPL", callback_data=f"{prefix}:RPL")
    kb.button(text="🏒 FPHL", callback_data=f"{prefix}:FPHL")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def ban_league_choice_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏆 Только RPL", callback_data="banleague:RPL")
    kb.button(text="🏒 Только FPHL", callback_data="banleague:FPHL")
    kb.button(text="🚫 В обеих лигах (ALL)", callback_data="banleague:ALL")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def back_to_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    return kb.as_markup()


def exceptions_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить исключение", callback_data="adm:add_exception")
    kb.button(text="📋 Список исключений", callback_data="adm:list_exceptions")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def exceptions_list_kb(exceptions) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ex in exceptions:
        label = f"❌ Удалить {ex['tg_id']}"
        kb.button(text=label, callback_data=f"adm:del_exception:{ex['tg_id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_player_card_kb(tg_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить Никнейм", callback_data=f"adm_pedit:nick:{tg_id}")
    kb.button(text="🎮 Изменить SteamID", callback_data=f"adm_pedit:steam:{tg_id}")
    kb.button(text="🏒 Изменить Команду", callback_data=f"adm_pedit:team:{tg_id}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_teams_choice_kb(chats, tg_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🆓 Free Agent (Без команды)", callback_data=f"adm_pedit_setteam:{tg_id}:0")
    for c in chats:
        kb.button(text=f"[{c['league']}] {c['name']}", callback_data=f"adm_pedit_setteam:{tg_id}:{c['chat_id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def chats_choice_kb(chats, prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for c in chats:
        kb.button(text=f"[{c['league']}] {c['name']}", callback_data=f"{prefix}:{c['chat_id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def chats_multiselect_kb(chats, selected: set) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for c in chats:
        mark = "✅" if c["chat_id"] in selected else "⬜️"
        kb.button(text=f"{mark} [{c['league']}] {c['name']}", callback_data=f"cf_toggle:{c['chat_id']}")
    kb.button(text="✔️ Готово", callback_data="cf_done")
    kb.adjust(1)
    return kb.as_markup()


def matches_list_kb(matches) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for m in matches:
        local_time = m["match_time"].astimezone(MSK)
        label = f"{m['team_home_name']} 🆚 {m['team_away_name']} — {local_time.strftime('%d.%m %H:%M')} МСК"
        kb.button(text=label, callback_data=f"adm:match:{m['id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def match_card_kb(match_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Удалить матч", callback_data=f"adm:del_match:{match_id}")
    kb.button(text="⬅️ К списку матчей", callback_data="adm:list_matches")
    kb.adjust(1)
    return kb.as_markup()


def servers_choice_kb(servers) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for s in servers:
        kb.button(text=f"🖥 {s['name']} ({s['ip']}:{s['port']})", callback_data=f"srv:{s['id']}")
    kb.button(text="➕ Новый сервер", callback_data="adm:add_server_inline")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def servers_list_kb(servers) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for s in servers:
        kb.button(text=f"🗑 {s['name']} ({s['ip']}:{s['port']})", callback_data=f"adm:del_server:{s['id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def edit_chat_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить название", callback_data="editchat:name")
    kb.button(text="🖼 Изменить эмодзи-лого", callback_data="editchat:emoji")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def ban_duration_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for label, days in [("1 день", 1), ("3 дня", 3), ("7 дней", 7), ("30 дней", 30)]:
        kb.button(text=label, callback_data=f"bandur:{days}")
    kb.button(text="♾ Навсегда", callback_data="bandur:0")
    kb.adjust(2)
    return kb.as_markup()


def bans_list_kb(bans) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for b in bans:
        kb.button(text=f"✅ Разбанить #{b['id']} ({b['tg_id']})", callback_data=f"adm:unban:{b['id']}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)
    return kb.as_markup()


def profile_menu_kb(player=None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if not (player and player["steam_id"]):
        kb.button(text="🔗 Привязать SteamID", callback_data="profile:steamid")
    kb.button(text="✏️ Указать Ник и Номер", callback_data="profile:nickname")
    kb.button(text="📋 Составы команд", callback_data="profile:teams")
    kb.button(text="📜 Моя история команд", callback_data="profile:history")
    kb.adjust(1)
    return kb.as_markup()


def teams_list_kb(chats) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for c in chats:
        logo = f"{c['logo_emoji']} " if c["logo_emoji"] else ""
        kb.button(text=f"[{c['league']}] {logo}{c['name']}", callback_data=f"team_view:{c['chat_id']}")
    kb.button(text="⬅️ Назад", callback_data="profile:menu")
    kb.adjust(1)
    return kb.as_markup()


def team_roster_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ К списку команд", callback_data="profile:teams")
    kb.button(text="🏠 В профиль", callback_data="profile:menu")
    kb.adjust(1)
    return kb.as_markup()


# =========================================================
#                    ХЕНДЛЕРЫ И РОУТЕРЫ
# =========================================================

start_router = Router()
channel_router = Router()
auth_router = Router()
panel_router = Router()
dm_router = Router()
team_chat_router = Router()

dm_router.message.filter(F.chat.type == "private")
team_chat_router.message.filter(F.chat.type.in_({"group", "supergroup"}))
team_chat_router.chat_member.filter(F.chat.type.in_({"group", "supergroup"}))

AUTHED_ADMINS: set[int] = set()


def is_authed(user_id: int) -> bool:
    return user_id in AUTHED_ADMINS


@start_router.message(CommandStart(), F.chat.type != "private")
async def cmd_start_chat(message: Message):
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Данный бот предназначен для автоматизации раскаток, ведения составов и уведомлений RPL / FPHL.\n\n"
        f"🃏 Играйте в наш коллекционный бот карточек игроков Puck — {PUCK_BOT_USERNAME}"
    )
    await message.answer(text)


def _has_allowed_hashtag(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(tag in lowered for tag in ALLOWED_HASHTAGS)


@channel_router.channel_post()
async def on_channel_post(message: Message, bot: Bot):
    channel = await get_channel(message.chat.id)
    if not channel:
        return
    text = message.text or message.caption or ""
    if not _has_allowed_hashtag(text):
        return
    chat_ids = await get_linked_chat_ids(message.chat.id)
    for chat_id in chat_ids:
        try:
            await bot.copy_message(chat_id=chat_id, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logging.warning(f"⚠️ Не удалось переслать пост в чат {chat_id}: {e}")


@start_router.message(Command("lineup"))
async def cmd_lineup(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏆 RPL", callback_data="lineup_league:RPL")
    kb.button(text="🏒 FPHL", callback_data="lineup_league:FPHL")
    kb.adjust(2)
    await message.answer("🏒 <b>Выберите лигу для просмотра составов:</b>", reply_markup=kb.as_markup())


@start_router.callback_query(F.data.startswith("lineup_league:"))
async def cb_lineup_league(call: CallbackQuery):
    league = call.data.split(":")[1]
    chats = await get_chats(league)
    if not chats:
        await call.answer(f"В лиге {league} пока нет команд.", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    for c in chats:
        logo = f"{c['logo_emoji']} " if c["logo_emoji"] else ""
        kb.button(text=f"{logo}{c['name']}", callback_data=f"team_view:{c['chat_id']}")
    kb.button(text="⬅️ Выбор лиги", callback_data="lineup_back")
    kb.adjust(1)
    await call.message.edit_text(f"🏒 <b>Команды лиги {league}:</b>", reply_markup=kb.as_markup())
    await call.answer()


@start_router.callback_query(F.data == "lineup_back")
async def cb_lineup_back(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏆 RPL", callback_data="lineup_league:RPL")
    kb.button(text="🏒 FPHL", callback_data="lineup_league:FPHL")
    kb.adjust(2)
    await call.message.edit_text("🏒 <b>Выберите лигу для просмотра составов:</b>", reply_markup=kb.as_markup())
    await call.answer()


@auth_router.message(Command("adminkarpl"))
async def cmd_admin(message: Message, state: FSMContext):
    if is_authed(message.from_user.id):
        await message.answer("🔐 <b>Админ-панель RPL / FPHL</b>", reply_markup=admin_main_menu())
        return
    await state.set_state(AdminAuth.waiting_login)
    await message.answer("🔐 Введите <b>логин</b>:")


@auth_router.message(AdminAuth.waiting_login)
async def process_login(message: Message, state: FSMContext):
    if message.text != ADMIN_LOGIN:
        await message.answer("❌ Неверный логин. Попробуйте ещё раз /adminkarpl")
        await state.clear()
        return
    await state.set_state(AdminAuth.waiting_password)
    await message.answer("🔑 Введите <b>пароль</b>:")


@auth_router.message(AdminAuth.waiting_password)
async def process_password(message: Message, state: FSMContext):
    if message.text != ADMIN_PASSWORD:
        await message.answer("❌ Неверный пароль. Попробуйте ещё раз /adminkarpl")
        await state.clear()
        return
    AUTHED_ADMINS.add(message.from_user.id)
    await state.clear()
    await message.answer("✅ Доступ разрешён!\n\n🔐 <b>Админ-панель RPL / FPHL</b>", reply_markup=admin_main_menu())


@panel_router.callback_query.middleware()
async def check_authed_cb(handler, event: CallbackQuery, data):
    if not is_authed(event.from_user.id):
        await event.answer("⛔️ Сначала войдите: /adminkarpl", show_alert=True)
        return
    return await handler(event, data)


@panel_router.message.middleware()
async def check_authed_msg(handler, event: Message, data):
    if not is_authed(event.from_user.id):
        return
    return await handler(event, data)


@panel_router.callback_query(F.data == "adm:menu")
async def cb_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("🔐 <b>Админ-панель RPL / FPHL</b>", reply_markup=admin_main_menu())
    await call.answer()


@panel_router.callback_query(F.data == "adm:rebuild_wl")
async def cb_rebuild_whitelist(call: CallbackQuery):
    await call.answer("⏳ Обновляю whitelist в фоне...")
    asyncio.create_task(rebuild_whitelist_file())
    await call.message.edit_text(
        "✅ <b>Обновление whitelist запущено в фоне.</b>\n"
        "Проверь логи Railway через 5–10 секунд, чтобы увидеть результат.",
        reply_markup=back_to_menu_kb(),
    )


@panel_router.callback_query(F.data.startswith("adm:all_players:"))
async def cb_admin_all_players(call: CallbackQuery):
    page = int(call.data.split(":")[2])
    limit = 15
    offset = page * limit
    players, total = await get_all_players_db(limit=limit, offset=offset)

    if not players:
        await call.message.edit_text("📋 Игроки в базе отсутствуют.", reply_markup=back_to_menu_kb())
        await call.answer()
        return

    lines = []
    for p in players:
        nick = esc(p["nickname"]) if p["nickname"] else esc(p["first_name"] or "Игрок")
        team = f"[{p['team_league']}] {esc(p['team_name'])}" if p["team_name"] else "Free Agent"
        steam = f"<code>{esc(p['steam_id'])}</code>" if p["steam_id"] else "❌"
        user = f"@{esc(p['username'])}" if p["username"] else f"ID:{p['tg_id']}"
        lines.append(f"👤 <b>{nick}</b> ({user})\n   ↳ Команда: {team} | SteamID: {steam}")

    total_pages = (total + limit - 1) // limit
    text = f"👥 <b>Все зарегистрированные игроки ({total}):</b> (Стр. {page + 1}/{total_pages})\n━━━━━━━━━━━━━━━━━━━\n\n" + "\n\n".join(lines)

    kb = InlineKeyboardBuilder()
    if page > 0:
        kb.button(text="◀️ Назад", callback_data=f"adm:all_players:{page - 1}")
    if (page + 1) < total_pages:
        kb.button(text="Вперёд ▶️", callback_data=f"adm:all_players:{page + 1}")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(2, 1)

    await call.message.edit_text(text, reply_markup=kb.as_markup())
    await call.answer()


@panel_router.callback_query(F.data == "adm:reset_steam_confirm")
async def cb_admin_reset_steam_confirm(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 ДА, СБРОСИТЬ ВСЕМ", callback_data="adm:reset_steam_execute")
    kb.button(text="❌ Отмена", callback_data="adm:menu")
    kb.adjust(1)

    await call.message.edit_text(
        "⚠️ <b>ВНИМАНИЕ! Вы действительно хотите сбросить SteamID ВСЕМ игрокам?</b>\n\n"
        "После этого бот автоматически уведомит абсолютно всех игроков в личные сообщения, "
        "а также отправит объявления во все командные чаты!",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@panel_router.callback_query(F.data == "adm:reset_steam_execute")
async def cb_admin_reset_steam_execute(call: CallbackQuery, bot: Bot):
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE players SET steam_id = NULL")

    asyncio.create_task(rebuild_whitelist_file())

    await call.message.edit_text(
        "✅ Все SteamID сброшены! Whitelist обновится в фоне. Запущена рассылка...",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()
    asyncio.create_task(broadcast_steam_reset(bot))


async def broadcast_steam_reset(bot: Bot):
    reset_text = (
        "📢 <b>ВНИМАНИЕ ВСЕМ ИГРОКАМ!</b>\n\n"
        "Администрация лиги выполнила <b>сброс SteamID</b> для всех игроков.\n"
        "Пожалуйста, зайдите в личные сообщения бота и привяжите ваш <b>SteamID</b> заново!"
    )

    chats = await get_chats()
    for c in chats:
        try:
            await bot.send_message(c["chat_id"], reset_text)
            await asyncio.sleep(0.05)
        except Exception:
            pass

    cap_chats = await get_all_captain_chats()
    for cc in cap_chats:
        try:
            await bot.send_message(cc["chat_id"], reset_text)
            await asyncio.sleep(0.05)
        except Exception:
            pass

    all_player_ids = await get_all_known_player_ids()
    for tg_id in all_player_ids:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔗 Привязать SteamID", callback_data="profile:steamid")
        try:
            await bot.send_message(tg_id, reset_text, reply_markup=kb.as_markup())
            await asyncio.sleep(0.05)
        except Exception:
            pass


@panel_router.callback_query(F.data == "adm:captains_menu")
async def cb_captains_menu(call: CallbackQuery):
    caps = await get_all_captain_chats()
    lines = []
    for c in caps:
        lines.append(f"👑 <b>Капитанская {c['league']}:</b> <code>{c['chat_id']}</code>")
    text = "👑 <b>Настройка капитанских чатов</b>\n\n" + ("\n".join(lines) if lines else "<i>Чаты еще не настроены</i>")

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Задать капитанскую RPL", callback_data="adm:set_cap:RPL")
    kb.button(text="➕ Задать капитанскую FPHL", callback_data="adm:set_cap:FPHL")
    kb.button(text="⬅️ В меню", callback_data="adm:menu")
    kb.adjust(1)

    await call.message.edit_text(text, reply_markup=kb.as_markup())
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:set_cap:"))
async def cb_set_captain_prompt(call: CallbackQuery, state: FSMContext):
    league = call.data.split(":")[2]
    await state.update_data(cap_league=league)
    await state.set_state(AddCaptainsChat.waiting_id)
    await call.message.edit_text(
        f"👑 <b>Привязка капитанского чата {league}</b>\n\nПришлите ID чата Капитанской (например: -1001234567890):",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AddCaptainsChat.waiting_id)
async def process_captain_chat_id(message: Message, state: FSMContext):
    try:
        chat_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом. Попробуйте ещё раз:")
        return

    data = await state.get_data()
    league = data.get("cap_league", "RPL")
    await set_captain_chat(league, chat_id)
    await state.clear()
    await message.answer(f"✅ Капитанский чат для лиги <b>{league}</b> установлен (<code>{chat_id}</code>)!", reply_markup=admin_main_menu())


@panel_router.callback_query(F.data == "adm:exceptions_menu")
async def cb_exceptions_menu(call: CallbackQuery):
    await call.message.edit_text("🛡 <b>Управление исключениями (мультичат)</b>", reply_markup=exceptions_menu_kb())
    await call.answer()


@panel_router.callback_query(F.data == "adm:add_exception")
async def cb_add_exception_prompt(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminExceptions.waiting_target)
    await call.message.edit_text(
        "➕ <b>Добавление исключения</b>\n\n"
        "Пришлите TG ID или @username игрока, которому разрешено находиться сразу в нескольких чатах команд:",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AdminExceptions.waiting_target)
async def process_add_exception(message: Message, state: FSMContext):
    raw = message.text.strip()
    target_id = None
    if raw.startswith("@"):
        player = await get_player_by_username(raw[1:])
        if player:
            target_id = player["tg_id"]
    else:
        try:
            target_id = int(raw)
        except ValueError:
            target_id = None

    if not target_id:
        await message.answer("❌ Игрок не найден. Отправьте числовой TG ID или корректный @username игрока:")
        return

    await add_exception_db(target_id)
    await state.clear()
    await message.answer(
        f"✅ Игрок <code>{target_id}</code> успешно добавлен в исключения! Теперь ему можно состоять в нескольких чатах.",
        reply_markup=admin_main_menu(),
    )


@panel_router.callback_query(F.data == "adm:list_exceptions")
async def cb_list_exceptions(call: CallbackQuery):
    ex_list = await get_exceptions()
    if not ex_list:
        await call.message.edit_text("📋 Список исключений пуст.", reply_markup=exceptions_menu_kb())
        await call.answer()
        return

    lines = []
    for ex in ex_list:
        username = f"@{esc(ex['username'])}" if ex["username"] else "—"
        nickname = esc(ex["nickname"]) if ex["nickname"] else esc(ex["first_name"] or "Игрок")
        lines.append(f"🛡 <b>{nickname}</b> (<code>{ex['tg_id']}</code> | {username})")

    await call.message.edit_text(
        "📋 <b>Игроки в исключениях (мультичат):</b>\n\n" + "\n".join(lines),
        reply_markup=exceptions_list_kb(ex_list),
    )
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:del_exception:"))
async def cb_delete_exception(call: CallbackQuery):
    tg_id = int(call.data.split(":")[2])
    await remove_exception_db(tg_id)
    await call.answer("🗑 Исключение удалено", show_alert=True)
    ex_list = await get_exceptions()
    if not ex_list:
        await call.message.edit_text("📋 Список исключений пуст.", reply_markup=exceptions_menu_kb())
        return
    await call.message.edit_text("📋 <b>Игроки в исключениях:</b>", reply_markup=exceptions_list_kb(ex_list))


async def show_admin_player_card(message_or_call, tg_id: int):
    player = await get_player(tg_id)
    if not player:
        await upsert_player_basic(tg_id, None, "Игрок")
        player = await get_player(tg_id)

    profile_text = await build_profile_text(player)
    text = f"👤 <b>Редактирование профиля игрока</b>\n\n{profile_text}"

    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=admin_player_card_kb(tg_id))
    else:
        await message_or_call.message.edit_text(text, reply_markup=admin_player_card_kb(tg_id))


@panel_router.callback_query(F.data == "adm:edit_profile")
async def cb_edit_profile_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminEditProfile.waiting_target)
    await call.message.edit_text(
        "👤 <b>Редактор профиля</b>\n\nПришлите TG ID или @username игрока, чей профиль вы хотите изменить:",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AdminEditProfile.waiting_target)
async def process_edit_profile_target(message: Message, state: FSMContext):
    raw = message.text.strip()
    target_id = None
    if raw.startswith("@"):
        player = await get_player_by_username(raw[1:])
        if player:
            target_id = player["tg_id"]
    else:
        try:
            target_id = int(raw)
        except ValueError:
            target_id = None

    if not target_id:
        await message.answer("❌ Игрок не найден. Пришлите числовой TG ID или корректный @username:")
        return

    await state.clear()
    await show_admin_player_card(message, target_id)


@panel_router.callback_query(F.data.startswith("adm_pedit:nick:"))
async def cb_edit_p_nick_prompt(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split(":")[2])
    await state.update_data(pedit_target=tg_id)
    await state.set_state(AdminEditProfile.waiting_nickname)
    await call.message.edit_text(
        f"✏️ <b>Изменение Никнейма (TG ID: <code>{tg_id}</code>)</b>\n\n"
        "Пришлите новый никнейм и номер (формат: <code>Nick #Number</code>):",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AdminEditProfile.waiting_nickname)
async def process_admin_set_nickname(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    tg_id = data.get("pedit_target")
    if not tg_id:
        await state.clear()
        await message.answer("❌ Ошибка определения игрока.", reply_markup=admin_main_menu())
        return

    new_nick = message.text.strip()
    await set_player_nickname(tg_id, new_nick)
    await state.clear()
    await sync_player_chats_and_team(bot, tg_id)
    await message.answer("✅ Никнейм игрока обновлён!")
    await show_admin_player_card(message, tg_id)


@panel_router.callback_query(F.data.startswith("adm_pedit:steam:"))
async def cb_edit_p_steam_prompt(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split(":")[2])
    await state.update_data(pedit_target=tg_id)
    await state.set_state(AdminEditProfile.waiting_steamid)
    await call.message.edit_text(
        f"🎮 <b>Изменение SteamID (TG ID: <code>{tg_id}</code>)</b>\n\n"
        "Пришлите новый SteamID для этого игрока:",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AdminEditProfile.waiting_steamid)
async def process_admin_set_steamid(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    tg_id = data.get("pedit_target")
    if not tg_id:
        await state.clear()
        await message.answer("❌ Ошибка определения игрока.", reply_markup=admin_main_menu())
        return

    new_steam = message.text.strip()
    if not is_valid_steam_id(new_steam):
        await message.answer("❌ Неверный формат SteamID! Не присылайте ссылки на steam. Попробуйте еще раз:")
        return

    await set_player_steam(tg_id, new_steam)
    await state.clear()
    await sync_player_chats_and_team(bot, tg_id)
    await message.answer("✅ SteamID игрока обновлён! VPS подхватит обновление в течение ~30 секунд.")
    await show_admin_player_card(message, tg_id)


@panel_router.callback_query(F.data.startswith("adm_pedit:team:"))
async def cb_edit_p_team_prompt(call: CallbackQuery):
    tg_id = int(call.data.split(":")[2])
    chats = await get_chats()
    await call.message.edit_text(
        f"🏒 <b>Изменение команды игрока (TG ID: <code>{tg_id}</code>)</b>\n\n"
        "Выберите новую команду для игрока:",
        reply_markup=admin_teams_choice_kb(chats, tg_id),
    )
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm_pedit_setteam:"))
async def cb_edit_p_set_team(call: CallbackQuery):
    parts = call.data.split(":")
    tg_id = int(parts[1])
    chat_id = int(parts[2])

    if chat_id == 0:
        await set_player_team(tg_id, None)
        await call.answer("Игрок переведён в статус Free Agent", show_alert=True)
    else:
        await set_player_team(tg_id, chat_id)
        await add_player_chat(tg_id, chat_id)
        chat = await get_chat(chat_id)
        tname = chat['name'] if chat else "команда"
        await call.answer(f"Игрок переведён в команду {tname}", show_alert=True)

    await show_admin_player_card(call, tg_id)


@panel_router.callback_query(F.data == "adm:add_chat")
async def cb_add_chat(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddChat.waiting_league)
    await call.message.edit_text(
        "➕ <b>Создание команды</b>\n\n1️⃣ Выберите лигу:",
        reply_markup=league_choice_kb("pick_league"),
    )
    await call.answer()


@panel_router.callback_query(AddChat.waiting_league, F.data.startswith("pick_league:"))
async def cb_pick_league(call: CallbackQuery, state: FSMContext):
    league = call.data.split(":")[1]
    await state.update_data(league=league)
    await state.set_state(AddChat.waiting_id)
    await call.message.edit_text(
        f"Выбрана лига: <b>{league}</b>\n\n2️⃣ Пришлите <b>ID чата</b> (например, -1001234567890):",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AddChat.waiting_id)
async def process_chat_id(message: Message, state: FSMContext):
    try:
        chat_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом. Попробуйте ещё раз:")
        return
    await state.update_data(chat_id=chat_id)
    await state.set_state(AddChat.waiting_name)
    await message.answer("✏️ Пришлите <b>название</b> команды (например, «Динамо Москва»):")


@panel_router.message(AddChat.waiting_name)
async def process_chat_name(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["chat_id"]
    league = data.get("league", "RPL")
    name = message.text.strip()
    await add_chat(chat_id, name, league)
    await state.clear()
    await message.answer(f"✅ Команда <b>{esc(name)}</b> [{league}] (<code>{chat_id}</code>) успешно добавлена!", reply_markup=admin_main_menu())


@panel_router.callback_query(F.data == "adm:list_chats")
async def cb_list_chats(call: CallbackQuery):
    chats = await get_chats()
    if not chats:
        await call.message.edit_text("📋 Пока нет ни одной добавленной команды.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    lines = [f"🏒 [{c['league']}] <b>{esc(c['name'])}</b> — <code>{c['chat_id']}</code>" for c in chats]
    await call.message.edit_text("📋 <b>Список команд:</b>\n\n" + "\n".join(lines), reply_markup=back_to_menu_kb())
    await call.answer()


@panel_router.callback_query(F.data == "adm:edit_chat")
async def cb_edit_chat_list(call: CallbackQuery):
    chats = await get_chats()
    if not chats:
        await call.message.edit_text("Нет ни одного чата.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    await call.message.edit_text("Выберите команду для редактирования:", reply_markup=chats_choice_kb(chats, "editchatpick"))
    await call.answer()


@panel_router.callback_query(F.data.startswith("editchatpick:"))
async def cb_edit_chat_pick(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    await state.update_data(edit_chat_id=chat_id)
    await call.message.edit_text("Что редактируем?", reply_markup=edit_chat_menu_kb())
    await call.answer()


@panel_router.callback_query(F.data == "editchat:name")
async def cb_edit_chat_name_prompt(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "edit_chat_id" not in data:
        await call.answer("Сначала выберите команду", show_alert=True)
        return
    await state.set_state(EditChat.waiting_name)
    await call.message.edit_text("Пришлите новое название команды:")
    await call.answer()


@panel_router.message(EditChat.waiting_name)
async def process_edit_chat_name(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("edit_chat_id")
    if not chat_id:
        await state.clear()
        await message.answer("❌ Команда не определена.", reply_markup=admin_main_menu())
        return
    new_name = message.text.strip()
    await update_chat_name(chat_id, new_name)
    await state.clear()
    await message.answer(f"✅ Название команды обновлено на «<b>{esc(new_name)}</b>»", reply_markup=admin_main_menu())


@panel_router.callback_query(F.data == "editchat:emoji")
async def cb_edit_chat_emoji_prompt(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "edit_chat_id" not in data:
        await call.answer("Сначала выберите команду", show_alert=True)
        return
    await state.set_state(EditChat.waiting_emoji)
    await call.message.edit_text(
        "Пришлите сообщение, содержащее один premium-эмодзи (логотип команды).\n"
        "Просто отправьте этот эмодзи обычным сообщением."
    )
    await call.answer()


@panel_router.message(EditChat.waiting_emoji)
async def process_edit_chat_emoji(message: Message, state: FSMContext, bot: Bot):
    entity = next((e for e in (message.entities or []) if e.type == "custom_emoji"), None)
    if not entity or not message.text:
        await message.answer("❌ Не найден premium-эмодзи. Отправьте сообщение, содержащее custom emoji.")
        return

    emoji_char = message.text[entity.offset: entity.offset + entity.length]
    data = await state.get_data()
    chat_id = data.get("edit_chat_id")
    if not chat_id:
        await state.clear()
        await message.answer("❌ Ошибка определения команды.", reply_markup=admin_main_menu())
        return

    await update_chat_emoji(chat_id, entity.custom_emoji_id, emoji_char)
    await state.clear()
    await message.answer("✅ Эмодзи-лого команды обновлён!", reply_markup=admin_main_menu())

    chat_info = await get_chat(chat_id)
    team_name = chat_info["name"] if chat_info else ""
    try:
        entities = [MessageEntity(type="custom_emoji", offset=0, length=len(emoji_char), custom_emoji_id=entity.custom_emoji_id)]
        await bot.send_message(chat_id, f"{emoji_char} Логотип команды «<b>{esc(team_name)}</b>» обновлён!", entities=entities)
    except Exception as e:
        logging.warning(f"Не удалось отправить логотип в чат {chat_id}: {e}")


@panel_router.callback_query(F.data == "adm:add_server")
async def cb_add_server(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddServer.waiting_name)
    await call.message.edit_text(
        "🖥 <b>Новый сервер</b>\n\n1️⃣ Пришлите <b>название</b> сервера (например, «Сервер #1»):",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.callback_query(AddMatch.waiting_server, F.data == "adm:add_server_inline")
async def cb_add_server_inline(call: CallbackQuery, state: FSMContext):
    await state.update_data(resume_match=True)
    await state.set_state(AddServer.waiting_name)
    await call.message.edit_text("🖥 <b>Новый сервер</b>\n\n1️⃣ Пришлите <b>название</b> сервера:")
    await call.answer()


@panel_router.message(AddServer.waiting_name)
async def process_server_name(message: Message, state: FSMContext):
    await state.update_data(srv_name=message.text.strip())
    await state.set_state(AddServer.waiting_ip)
    await message.answer("2️⃣ Пришлите <b>IP</b> сервера:")


@panel_router.message(AddServer.waiting_ip)
async def process_server_ip(message: Message, state: FSMContext):
    await state.update_data(srv_ip=message.text.strip())
    await state.set_state(AddServer.waiting_port)
    await message.answer("3️⃣ Пришлите <b>Port</b> сервера:")


@panel_router.message(AddServer.waiting_port)
async def process_server_port(message: Message, state: FSMContext):
    await state.update_data(srv_port=message.text.strip())
    await state.set_state(AddServer.waiting_password)
    await message.answer("4️⃣ Пришлите <b>Password</b> сервера:")


@panel_router.message(AddServer.waiting_password)
async def process_server_password(message: Message, state: FSMContext):
    data = await state.get_data()
    password = message.text.strip()
    server = await add_server(data["srv_name"], data["srv_ip"], data["srv_port"], password)

    if data.get("resume_match"):
        required = ("team1_id", "team1_name", "team2_id", "team2_name", "match_time")
        if not all(key in data for key in required):
            await state.clear()
            await message.answer("❌ Данные матча утеряны.", reply_markup=admin_main_menu())
            return

        try:
            match_time = datetime.fromisoformat(data["match_time"])
        except ValueError:
            await state.clear()
            await message.answer("❌ Неверный формат даты.", reply_markup=admin_main_menu())
            return

        match = await add_match(
            team_home_chat_id=data["team1_id"],
            team_home_name=data["team1_name"],
            team_away_chat_id=data["team2_id"],
            team_away_name=data["team2_name"],
            match_time=match_time,
            server_name=server["name"],
            server_ip=server["ip"],
            server_port=server["port"],
            server_password=server["password"],
        )

        await state.clear()
        local_time = match["match_time"].astimezone(MSK)
        text = (
            "✅ <b>Сервер сохранён и матч создан!</b>\n\n"
            f"🆚 <b>{esc(match['team_home_name'])}</b> — <b>{esc(match['team_away_name'])}</b>\n"
            f"🕒 {local_time.strftime('%d.%m.%Y %H:%M')} МСК\n"
            f"🖥 {esc(match['server_name'])}\n\n"
            "Уведомления за 45 и 15 минут будут отправлены автоматически! 🔔"
        )
        await message.answer(text, reply_markup=admin_main_menu())
        return

    await state.clear()
    await message.answer(f"✅ Сервер <b>{esc(server['name'])}</b> сохранён!", reply_markup=admin_main_menu())


@panel_router.callback_query(F.data == "adm:list_servers")
async def cb_list_servers(call: CallbackQuery):
    servers = await get_servers()
    if not servers:
        await call.message.edit_text("🗄 Пока нет сохранённых серверов.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    await call.message.edit_text(
        "🗄 <b>Серверы</b> (нажмите, чтобы удалить):",
        reply_markup=servers_list_kb(servers),
    )
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:del_server:"))
async def cb_delete_server(call: CallbackQuery):
    server_id = int(call.data.split(":")[2])
    await delete_server(server_id)
    await call.answer("🗑 Сервер удалён", show_alert=True)
    servers = await get_servers()
    if not servers:
        await call.message.edit_text("🗄 Пока нет сохранённых серверов.", reply_markup=back_to_menu_kb())
        return
    await call.message.edit_text("🗄 <b>Серверы:</b>", reply_markup=servers_list_kb(servers))


@panel_router.callback_query(F.data == "adm:add_match")
async def cb_add_match(call: CallbackQuery, state: FSMContext):
    chats = await get_chats()
    if len(chats) < 2:
        await call.message.edit_text(
            "❌ Нужно как минимум 2 добавленных чата для создания матча.",
            reply_markup=back_to_menu_kb(),
        )
        await call.answer()
        return
    await state.set_state(AddMatch.waiting_team1)
    await call.message.edit_text(
        "🆚 <b>Новый матч</b>\n\n1️⃣ Выберите <b>хозяев (ред)</b>:",
        reply_markup=chats_choice_kb(chats, "team1"),
    )
    await call.answer()


@panel_router.callback_query(AddMatch.waiting_team1, F.data.startswith("team1:"))
async def cb_pick_team1(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    chat = await get_chat(chat_id)
    await state.update_data(team1_id=chat_id, team1_name=chat["name"])
    all_chats = await get_chats()
    remaining = [c for c in all_chats if c["chat_id"] != chat_id]
    await state.set_state(AddMatch.waiting_team2)
    await call.message.edit_text(
        f"1️⃣ Хозяева: <b>{esc(chat['name'])}</b> ✅\n\n2️⃣ Выберите <b>гостей (блу)</b>:",
        reply_markup=chats_choice_kb(remaining, "team2"),
    )
    await call.answer()


@panel_router.callback_query(AddMatch.waiting_team2, F.data.startswith("team2:"))
async def cb_pick_team2(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    chat = await get_chat(chat_id)
    await state.update_data(team2_id=chat_id, team2_name=chat["name"])
    await state.set_state(AddMatch.waiting_datetime)
    data = await state.get_data()
    await call.message.edit_text(
        f"1️⃣ Хозяева: <b>{esc(data['team1_name'])}</b> ✅\n"
        f"2️⃣ Гости: <b>{esc(chat['name'])}</b> ✅\n\n"
        "3️⃣ Пришлите <b>дату и время матча по МСК</b> в формате:\n"
        "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\n"
        "Например: <code>25.08.2026 20:30</code>",
    )
    await call.answer()


@panel_router.message(AddMatch.waiting_datetime)
async def process_match_datetime(message: Message, state: FSMContext):
    try:
        naive_dt = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer(
            "❌ Неверный формат. Пришлите так: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\nПример: <code>25.08.2026 20:30</code>"
        )
        return
    match_time_msk = naive_dt.replace(tzinfo=MSK)
    await state.update_data(match_time=match_time_msk.isoformat())
    servers = await get_servers()
    if not servers:
        await state.update_data(resume_match=True)
        await state.set_state(AddServer.waiting_name)
        await message.answer(
            "🖥 Сохранённых серверов пока нет — добавьте сервер:\n\n1️⃣ Пришлите <b>название</b> сервера:"
        )
        return
    await state.set_state(AddMatch.waiting_server)
    await message.answer("4️⃣ Выберите <b>сервер</b> для матча:", reply_markup=servers_choice_kb(servers))


@panel_router.callback_query(AddMatch.waiting_server, F.data.startswith("srv:"))
async def cb_pick_server(call: CallbackQuery, state: FSMContext):
    await call.answer()
    server_id = int(call.data.split(":")[1])
    server = await get_server(server_id)
    if not server:
        await call.message.answer("❌ Сервер не найден.")
        return

    data = await state.get_data()
    required = ("team1_id", "team1_name", "team2_id", "team2_name", "match_time")
    if not all(key in data for key in required):
        await state.clear()
        await call.message.edit_text("❌ Данные матча утеряны.", reply_markup=back_to_menu_kb())
        return

    match_time = datetime.fromisoformat(data["match_time"])
    match = await add_match(
        team_home_chat_id=data["team1_id"],
        team_home_name=data["team1_name"],
        team_away_chat_id=data["team2_id"],
        team_away_name=data["team2_name"],
        match_time=match_time,
        server_name=server["name"],
        server_ip=server["ip"],
        server_port=server["port"],
        server_password=server["password"],
    )

    await state.clear()
    local_time = match["match_time"].astimezone(MSK)
    text = (
        "✅ <b>Матч успешно создан!</b>\n\n"
        f"🆚 <b>{esc(match['team_home_name'])}</b> — <b>{esc(match['team_away_name'])}</b>\n"
        f"🕒 {local_time.strftime('%d.%m.%Y %H:%M')} МСК\n"
        f"🖥 {esc(match['server_name'])}\n\n"
        "Напоминание и раскатка отправятся автоматически 🔔"
    )
    await call.message.edit_text(text, reply_markup=admin_main_menu())


@panel_router.callback_query(F.data == "adm:list_matches")
async def cb_list_matches(call: CallbackQuery):
    matches = await get_upcoming_matches()
    if not matches:
        await call.message.edit_text("📅 Ближайших матчей нет.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    await call.message.edit_text(
        "📅 <b>Ближайшие матчи</b> (МСК):",
        reply_markup=matches_list_kb(matches),
    )
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:match:"))
async def cb_match_card(call: CallbackQuery):
    match_id = int(call.data.split(":")[2])
    m = await get_match(match_id)
    if not m:
        await call.answer("Матч не найден", show_alert=True)
        return
    local_time = m["match_time"].astimezone(MSK)
    text = (
        f"🆚 <b>{esc(m['team_home_name'])} — {esc(m['team_away_name'])}</b>\n"
        f"🕒 {local_time.strftime('%d.%m.%Y %H:%M')} МСК\n\n"
        f"🖥 {esc(m['server_name'])}\n"
        f"🌐 IP: <code>{esc(m['server_ip'])}</code>\n"
        f"🔌 Port: <code>{esc(m['server_port'])}</code>\n"
        f"🔑 Password: <code>{esc(m['server_password'])}</code>\n\n"
        f"🔔 45 мин: {'✅' if m['notified_45'] else '⏳'}   "
        f"🔔 15 мин: {'✅' if m['notified_15'] else '⏳'}"
    )
    await call.message.edit_text(text, reply_markup=match_card_kb(match_id))
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:del_match:"))
async def cb_delete_match(call: CallbackQuery):
    match_id = int(call.data.split(":")[2])
    await delete_match(match_id)
    await call.answer("🗑 Матч удалён", show_alert=True)
    matches = await get_upcoming_matches()
    if not matches:
        await call.message.edit_text("📅 Ближайших матчей нет.", reply_markup=back_to_menu_kb())
        return
    await call.message.edit_text("📅 <b>Ближайшие матчи:</b>", reply_markup=matches_list_kb(matches))


@panel_router.callback_query(F.data == "adm:add_channel")
async def cb_add_channel(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddChannel.waiting_id)
    await call.message.edit_text(
        "📡 <b>Привязка канала</b>\n\n1️⃣ Пришлите <b>ID канала</b> (например, -1001234567890):",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(AddChannel.waiting_id)
async def process_channel_id(message: Message, state: FSMContext):
    try:
        channel_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом:")
        return
    await state.update_data(channel_id=channel_id)
    await state.set_state(AddChannel.waiting_title)
    await message.answer("✏️ Пришлите <b>название</b> канала:")


@panel_router.message(AddChannel.waiting_title)
async def process_channel_title(message: Message, state: FSMContext):
    data = await state.get_data()
    title = message.text.strip()
    await add_channel(data["channel_id"], title)
    await state.update_data(selected=set())
    chats = await get_chats()
    if not chats:
        await state.clear()
        await message.answer("✅ Канал добавлен! Добавьте чаты для пересылки.", reply_markup=admin_main_menu())
        return
    await state.set_state(AddChannel.waiting_chats)
    await message.answer(
        "2️⃣ Выберите чаты для пересылки постов:",
        reply_markup=chats_multiselect_kb(chats, set()),
    )


@panel_router.callback_query(AddChannel.waiting_chats, F.data.startswith("cf_toggle:"))
async def cb_toggle_chat(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    data = await state.get_data()
    selected: set = data.get("selected", set())
    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.add(chat_id)
    await state.update_data(selected=selected)
    chats = await get_chats()
    await call.message.edit_reply_markup(reply_markup=chats_multiselect_kb(chats, selected))
    await call.answer()


@panel_router.callback_query(AddChannel.waiting_chats, F.data == "cf_done")
async def cb_channel_done(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    channel_id = data["channel_id"]
    selected: set = data.get("selected", set())
    for chat_id in selected:
        await link_channel_chat(channel_id, chat_id)
    await state.clear()
    await call.message.edit_text(f"✅ Канал привязан к {len(selected)} чат(ам)!", reply_markup=admin_main_menu())
    await call.answer()


@panel_router.callback_query(F.data == "adm:list_channels")
async def cb_list_channels(call: CallbackQuery):
    channels = await get_channels()
    if not channels:
        await call.message.edit_text("🔗 Пока нет привязанных каналов.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    lines = []
    for ch in channels:
        linked = await get_linked_chats(ch["channel_id"])
        names = ", ".join(esc(c["name"]) for c in linked) or "—"
        lines.append(f"📡 <b>{esc(ch['title'])}</b> (<code>{ch['channel_id']}</code>)\n   ↳ чаты: {names}")
    await call.message.edit_text("🔗 <b>Каналы:</b>\n\n" + "\n\n".join(lines), reply_markup=back_to_menu_kb())
    await call.answer()


@panel_router.callback_query(F.data == "adm:ban_player")
async def cb_ban_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(BanPlayer.waiting_target)
    await call.message.edit_text(
        "🚫 <b>Бан игрока</b>\n\n1️⃣ Пришлите TG ID или @username игрока:",
        reply_markup=back_to_menu_kb(),
    )
    await call.answer()


@panel_router.message(BanPlayer.waiting_target)
async def process_ban_target(message: Message, state: FSMContext):
    raw = message.text.strip()
    target_id = None
    if raw.startswith("@"):
        player = await get_player_by_username(raw[1:])
        if player:
            target_id = player["tg_id"]
    else:
        try:
            target_id = int(raw)
        except ValueError:
            target_id = None

    if not target_id:
        await message.answer("❌ Игрок не найден в базе. Отправьте корректный TG ID или @username:")
        return

    await state.update_data(ban_target=target_id)
    await state.set_state(BanPlayer.waiting_league)
    await message.answer("2️⃣ Выберите лигу для блокировки:", reply_markup=ban_league_choice_kb())


@panel_router.callback_query(BanPlayer.waiting_league, F.data.startswith("banleague:"))
async def process_ban_league(call: CallbackQuery, state: FSMContext):
    league = call.data.split(":")[1]
    await state.update_data(ban_league=league)
    await state.set_state(BanPlayer.waiting_reason)
    await call.message.edit_text(f"Выбрана лига: <b>{league}</b>\n\n3️⃣ Укажите причину бана:")
    await call.answer()


@panel_router.message(BanPlayer.waiting_reason)
async def process_ban_reason(message: Message, state: FSMContext):
    await state.update_data(ban_reason=message.text.strip())
    await state.set_state(BanPlayer.waiting_duration)
    await message.answer("4️⃣ Выберите срок бана:", reply_markup=ban_duration_kb())


@panel_router.callback_query(BanPlayer.waiting_duration, F.data.startswith("bandur:"))
async def process_ban_duration(call: CallbackQuery, state: FSMContext, bot: Bot):
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    target_id = data.get("ban_target")
    league = data.get("ban_league", "ALL")
    reason = data.get("ban_reason", "не указана")
    if not target_id:
        await state.clear()
        await call.message.edit_text("❌ Данные утеряны.", reply_markup=admin_main_menu())
        await call.answer()
        return

    until = None if days == 0 else datetime.now(timezone.utc) + timedelta(days=days)
    await ban_player_db(target_id, league, reason, until)
    await state.clear()

    dur_text = "навсегда" if until is None else until.astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")

    ban_notice = (
        f"🚫 <b>Вы были забанены!</b>\n━━━━━━━━━━━━━━━━━━━\n\n"
        f"🛡 <b>Лига:</b> {league}\n"
        f"✏️ <b>Причина:</b> {esc(reason)}\n"
        f"⏱ <b>Срок бана:</b> {dur_text}\n\n"
        f"<i>При возникновении вопросов обратитесь к администрации.</i>"
    )
    try:
        await bot.send_message(target_id, ban_notice)
    except Exception as e:
        logging.warning(f"Не удалось отправить уведомление о бане пользователю {target_id}: {e}")

    await call.message.edit_text(
        f"✅ Игрок <code>{target_id}</code> забанен.\n"
        f"Лига: <b>{league}</b>\n"
        f"Причина: {esc(reason)}\n"
        f"Срок: {dur_text}",
        reply_markup=admin_main_menu(),
    )
    await call.answer()


@panel_router.callback_query(F.data == "adm:list_bans")
async def cb_list_bans(call: CallbackQuery):
    bans = await get_active_bans()
    if not bans:
        await call.message.edit_text("📋 Активных банов нет.", reply_markup=back_to_menu_kb())
        await call.answer()
        return
    lines = []
    for b in bans:
        until = "навсегда" if not b["until"] else b["until"].astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")
        lines.append(f"🚫 #{b['id']} | ID: <code>{b['tg_id']}</code> [{b['league']}] — {esc(b['reason'])} (до {until})")
    await call.message.edit_text("📋 <b>Активные баны:</b>\n\n" + "\n".join(lines), reply_markup=bans_list_kb(bans))
    await call.answer()


@panel_router.callback_query(F.data.startswith("adm:unban:"))
async def cb_unban(call: CallbackQuery):
    ban_id = int(call.data.split(":")[2])
    await unban_player_db(ban_id)
    await call.answer("✅ Игрок разбанен (обновится в течение ~30 секунд)", show_alert=True)
    bans = await get_active_bans()
    if not bans:
        await call.message.edit_text("📋 Активных банов нет.", reply_markup=back_to_menu_kb())
        return
    await call.message.edit_text("📋 <b>Активные баны:</b>", reply_markup=bans_list_kb(bans))


# =========================================================
#           СИНХРОНИЗАЦИЯ СОСТАВОВ И ЛС БОТА
# =========================================================

async def sync_player_chats_and_team(bot: Bot, tg_id: int):
    chats = await get_chats()
    for chat in chats:
        try:
            member = await bot.get_chat_member(chat["chat_id"], tg_id)
            if member.status in ("member", "administrator", "creator"):
                await add_player_chat(tg_id, chat["chat_id"])
            else:
                await remove_player_chat(tg_id, chat["chat_id"])
        except Exception:
            pass
    await recompute_player_team(bot, tg_id)


@dm_router.message(CommandStart())
async def dm_start(message: Message, bot: Bot):
    tg_id = message.from_user.id
    await upsert_player_basic(tg_id, message.from_user.username, message.from_user.full_name)

    active_ban = await get_active_ban(tg_id)
    if active_ban:
        until = "навсегда" if not active_ban["until"] else active_ban["until"].astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")
        ban_text = (
            f"🚫 <b>У вас активный бан в лиге!</b>\n━━━━━━━━━━━━━━━━━━━\n\n"
            f"🛡 <b>Лига:</b> {active_ban['league']}\n"
            f"✏️ <b>Причина:</b> {esc(active_ban['reason'])}\n"
            f"⏱ <b>Срок бана:</b> {until}\n\n"
            f"<i>Вы не можете пользоваться возможностями бота, пока действует бан.</i>"
        )
        await message.answer(ban_text)
        return

    await sync_player_chats_and_team(bot, tg_id)
    player = await get_player(tg_id)
    text = await build_profile_text(player)
    await message.answer(text, reply_markup=profile_menu_kb(player))


@dm_router.message(Command("myhistory"))
async def cmd_myhistory(message: Message):
    tg_id = message.from_user.id
    await show_user_history(message, tg_id)


async def show_user_history(target_message_or_call, tg_id: int):
    history = await get_player_history(tg_id)
    if not history:
        text = "📜 <b>Ваша история команд пуста.</b>\n Вы ещё не состояли ни в одной команде."
    else:
        lines = []
        for h in history:
            j_date = h["joined_at"].astimezone(MSK).strftime("%d.%m.%Y")
            if h["left_at"]:
                l_date = h["left_at"].astimezone(MSK).strftime("%d.%m.%Y")
                period = f"{j_date} — {l_date}"
            else:
                period = f"{j_date} — по н.в."

            lines.append(f"🛡 <b>{esc(h['team_name'])}</b> [{h['league']}] ({period})")

        text = "📜 <b>Ваша история выступлений в командах:</b>\n━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines)

    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 В профиль", callback_data="profile:menu")

    if isinstance(target_message_or_call, Message):
        await target_message_or_call.answer(text, reply_markup=kb.as_markup())
    else:
        await target_message_or_call.message.edit_text(text, reply_markup=kb.as_markup())


@dm_router.callback_query(F.data == "profile:history")
async def cb_profile_history(call: CallbackQuery):
    await show_user_history(call, call.from_user.id)
    await call.answer()


@dm_router.callback_query(F.data == "profile:menu")
async def cb_profile_menu(call: CallbackQuery):
    tg_id = call.from_user.id
    player = await get_player(tg_id)
    if not player:
        await upsert_player_basic(tg_id, call.from_user.username, call.from_user.full_name)
        player = await get_player(tg_id)
    text = await build_profile_text(player)
    await call.message.edit_text(text, reply_markup=profile_menu_kb(player))
    await call.answer()


@dm_router.callback_query(F.data == "profile:steamid")
async def cb_prompt_steamid(call: CallbackQuery, state: FSMContext):
    player = await get_player(call.from_user.id)
    if player and player["steam_id"]:
        await call.answer("❌ Вы уже привязали SteamID! Повторная привязка запрещена.", show_alert=True)
        return
    await state.set_state(SetProfile.waiting_steamid)
    await call.message.answer(
        "🎮 <b>Привязка SteamID</b>\n\n"
        "Отправьте ваш <b>SteamID</b> (например: <code>76561198000000000</code> или кастомный никнейм).\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b>\n"
        "1. Отправлять нужно <b>ТОЛЬКО SteamID</b>, а не ссылку на профиль steam!\n"
        "2. Привязать SteamID можно <b>ТОЛЬКО 1 РАЗ</b>! Будьте внимательны."
    )
    await call.answer()


@dm_router.message(SetProfile.waiting_steamid)
async def process_set_steamid(message: Message, state: FSMContext, bot: Bot):
    tg_id = message.from_user.id
    player = await get_player(tg_id)

    if player and player["steam_id"]:
        await state.clear()
        await message.answer("❌ SteamID уже привязан к вашему аккаунту и не подлежит изменению.", reply_markup=profile_menu_kb(player))
        return

    steam_id = message.text.strip()

    if not is_valid_steam_id(steam_id):
        await message.answer(
            "❌ <b>Некорректный формат SteamID!</b>\n\n"
            "Не отправляйте ссылку на профиль Steam (например, <code>https://steamcommunity.com/...</code>).\n"
            "Пришлите только сам <b>SteamID</b> (например: <code>76561198000000000</code>) или кастомный идентификатор:\n\n"
            "Попробуйте ещё раз:"
        )
        return

    existing = await get_player_by_steam_id(steam_id)
    if existing and existing["tg_id"] != tg_id:
        await message.answer("❌ Этот SteamID уже занят другим игроком! Укажите ваш личный SteamID:")
        return

    await set_player_steam(tg_id, steam_id)
    await state.clear()

    await sync_player_chats_and_team(bot, tg_id)

    player = await get_player(tg_id)
    text = await build_profile_text(player)
    await message.answer("✅ <b>SteamID успешно привязан!</b> VPS подхватит обновление в течение ~30 секунд.\n\n" + text, reply_markup=profile_menu_kb(player))


@dm_router.callback_query(F.data == "profile:nickname")
async def cb_prompt_nickname(call: CallbackQuery, state: FSMContext):
    await state.set_state(SetProfile.waiting_nickname)
    await call.message.answer(
        "✏️ <b>Укажите ваш Ник и Игровой номер</b>\n\n"
        "Формат строго: <code>Nick #Number</code>\n"
        "Пример: <code>Ovechkin #8</code> или <code>McDavid #97</code>"
    )
    await call.answer()


@dm_router.message(SetProfile.waiting_nickname)
async def process_set_nickname(message: Message, state: FSMContext, bot: Bot):
    nickname = message.text.strip()

    if not NICKNAME_PATTERN.match(nickname):
        await message.answer(
            "❌ <b>Неверный формат никнейма!</b>\n\n"
            "Необходимо ввести ник и номер через решётку (#).\n"
            "Пример: <code>Ovechkin #8</code> или <code>Crosby #87</code>\n\n"
            "Попробуйте ещё раз:"
        )
        return

    tg_id = message.from_user.id
    await set_player_nickname(tg_id, nickname)
    await state.clear()

    await sync_player_chats_and_team(bot, tg_id)

    player = await get_player(message.from_user.id)
    text = await build_profile_text(player)
    await message.answer("✅ <b>Никнейм и номер сохранены!</b>\n\n" + text, reply_markup=profile_menu_kb(player))


async def build_roster_text(bot: Bot, chat) -> str:
    roster = await get_team_roster(chat["chat_id"])
    logo = f"{chat['logo_emoji']} " if chat["logo_emoji"] else ""
    league = chat["league"]

    if roster:
        lines = []
        for p in roster:
            name = esc(p["nickname"]) if p["nickname"] else esc(p["first_name"] or "Без имени")
            steam = esc(p["steam_id"]) if p["steam_id"] else "❌ Не привязан"
            user_tag = f"@{esc(p['username'])}" if p["username"] else "—"

            is_cap = await is_player_captain(bot, p["tg_id"], league)
            cap_tag = " 👑 <b>(Капитан)</b>" if is_cap else ""

            lines.append(f"• <b>{name}</b>{cap_tag} | Telegram: {user_tag} (SteamID: <code>{steam}</code>)")
        body = "\n".join(lines)
    else:
        body = "<i>В составе пока нет зарегистрированных игроков</i>"
    return f"{logo}<b>Состав команды {esc(chat['name'])} [{league}]:</b>\n━━━━━━━━━━━━━━━━━━━\n\n{body}"


@dm_router.callback_query(F.data == "profile:teams")
async def cb_show_teams(call: CallbackQuery):
    chats = await get_chats()
    if not chats:
        await call.answer("Команд пока нет.", show_alert=True)
        return
    await call.message.edit_text("🏒 <b>Выберите команду:</b>", reply_markup=teams_list_kb(chats))
    await call.answer()


@dm_router.callback_query(F.data.startswith("team_view:"))
async def cb_view_team(call: CallbackQuery, bot: Bot):
    chat_id = int(call.data.split(":")[1])
    chat = await get_chat(chat_id)
    if not chat:
        await call.answer("Команда не найдена.", show_alert=True)
        return
    text = await build_roster_text(bot, chat)
    await call.message.edit_text(text, reply_markup=team_roster_kb())
    await call.answer()


@dm_router.message(Command("teams"))
async def cmd_teams(message: Message):
    chats = await get_chats()
    if not chats:
        await message.answer("Команд пока нет.")
        return
    await message.answer("🏒 <b>Выберите команду:</b>", reply_markup=teams_list_kb(chats))


@dm_router.callback_query(F.data.startswith("chooseteam:"))
async def cb_choose_team(call: CallbackQuery, bot: Bot):
    chosen_chat_id = int(call.data.split(":")[1])
    tg_id = call.from_user.id

    if await is_exception(tg_id):
        await set_player_team(tg_id, chosen_chat_id)
        chat_info = await get_chat(chosen_chat_id)
        name = chat_info["name"] if chat_info else "команда"
        await call.message.edit_text(f"✅ Вы выбрали основную команду: <b>{esc(name)}</b>")
        await call.answer()
        return

    chat_ids = await get_player_chat_ids(tg_id)

    if chosen_chat_id not in chat_ids:
        await call.answer("Вы больше не состоите в этом чате.", show_alert=True)
        return

    for chat_id in chat_ids:
        if chat_id == chosen_chat_id:
            continue
        await remove_player_chat(tg_id, chat_id)
        try:
            await bot.ban_chat_member(chat_id, tg_id)
            await bot.unban_chat_member(chat_id, tg_id)
        except Exception as e:
            logging.warning(f"Не удалось исключить игрока {tg_id} излишнего чата {chat_id}: {e}")

    await set_player_team(tg_id, chosen_chat_id)
    chat_info = await get_chat(chosen_chat_id)
    name = chat_info["name"] if chat_info else "команда"
    await call.message.edit_text(f"✅ Вы выбрали основную команду: <b>{esc(name)}</b>")
    await call.answer()


# =========================================================
#      КОМАНДНЫЕ ЧАТЫ: ПРОВЕРКИ И ГЕЙТ СООБЩЕНИЙ
# =========================================================

async def send_and_autodelete(bot: Bot, chat_id: int, text: str, delay: int = WARNING_AUTODELETE_SECONDS):
    try:
        msg = await bot.send_message(chat_id, text)
    except Exception as e:
        logging.warning(f"⚠️ Ошибка отправки автоудаляемого сообщения в {chat_id}: {e}")
        return

    async def _delete_later():
        await asyncio.sleep(delay)
        try:
            await bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass

    asyncio.create_task(_delete_later())


async def is_chat_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


@team_chat_router.message(Command("checkplayers"))
async def cmd_checkplayers(message: Message, bot: Bot):
    chat = await get_chat(message.chat.id)
    if not chat:
        return

    if not await is_chat_admin(bot, message.chat.id, message.from_user.id):
        try:
            await message.delete()
        except Exception:
            pass
        await send_and_autodelete(
            bot, message.chat.id,
            "⛔️ Команда /checkplayers доступна только администраторам чата.",
        )
        return

    status_msg = await message.answer("🔄 Запущена ручная проверка составов...")
    await sync_all_rosters(bot)
    await status_msg.edit_text("✅ <b>Проверка составов всех команд успешно завершена!</b>")


@team_chat_router.message()
async def team_chat_gate(message: Message, bot: Bot):
    chat = await get_chat(message.chat.id)
    if not chat:
        return
    if message.from_user is None or message.from_user.is_bot:
        return
    if message.new_chat_members or message.left_chat_member or message.pinned_message:
        return

    tg_id = message.from_user.id
    full_name = esc(message.from_user.full_name)

    if await is_chat_admin(bot, message.chat.id, tg_id):
        return

    ban = await get_active_ban(tg_id, chat["league"])
    if ban:
        try:
            await message.delete()
        except Exception:
            pass
        until = "навсегда" if not ban["until"] else ban["until"].astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")
        await send_and_autodelete(
            bot, message.chat.id,
            f"🚫 {full_name}, вы забанены в лиге {ban['league']}.\nПричина: {esc(ban['reason'])}\nСрок: {until}",
        )
        return

    player = await get_player(tg_id)
    if not player or not player["steam_id"] or not player["nickname"]:
        try:
            await message.delete()
        except Exception:
            pass
        await send_and_autodelete(
            bot, message.chat.id,
            f"⚠️ {full_name}, вы не привязали SteamID или Никнейм (#номер).\nПривяжите их в личных сообщениях бота!",
        )
        return

    if not await is_exception(tg_id):
        chat_ids = await get_player_chat_ids(tg_id)
        if len(chat_ids) >= 2:
            try:
                await message.delete()
            except Exception:
                pass
            await send_and_autodelete(
                bot, message.chat.id,
                f"⚠️ {full_name}, вы состоите в чатах двух команд сразу! Выберите основную команду в ЛС бота.",
            )
            return


async def recompute_player_team(bot: Bot, tg_id: int):
    if await is_exception(tg_id):
        return

    chat_ids = await get_player_chat_ids(tg_id)

    if len(chat_ids) <= 1:
        await set_player_team(tg_id, chat_ids[0] if chat_ids else None)
        return

    await set_player_team(tg_id, None)
    kb = InlineKeyboardBuilder()
    for cid in chat_ids:
        chat_info = await get_chat(cid)
        name = chat_info["name"] if chat_info else str(cid)
        league = chat_info["league"] if chat_info else "RPL"
        kb.button(text=f"🏒 [{league}] {name}", callback_data=f"chooseteam:{cid}")
    kb.adjust(1)
    try:
        await bot.send_message(
            tg_id,
            "⚠️ Вы состоите сразу в нескольких командных чатах RPL/FPHL.\nПожалуйста, выберите вашу команду:",
            reply_markup=kb.as_markup(),
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить выбор команды игроку {tg_id}: {e}")


@team_chat_router.chat_member()
async def on_team_chat_member_update(update: ChatMemberUpdated, bot: Bot):
    chat = await get_chat(update.chat.id)
    if not chat:
        return

    user = update.new_chat_member.user
    if user.is_bot:
        return

    active_statuses = {"member", "administrator", "creator"}
    was_member = update.old_chat_member.status in active_statuses
    is_member = update.new_chat_member.status in active_statuses

    if is_member and not was_member:
        await upsert_player_basic(user.id, user.username, user.full_name)
        await add_player_chat(user.id, update.chat.id)
        await recompute_player_team(bot, user.id)
    elif was_member and not is_member:
        await remove_player_chat(user.id, update.chat.id)
        await recompute_player_team(bot, user.id)


# =========================================================
#            АВТОМАТИЧЕСКАЯ ПРОВЕРКА СОСТАВОВ (5 МИН)
# =========================================================

async def sync_all_rosters(bot: Bot):
    known_players = await get_all_known_player_ids()
    chats = await get_chats()
    if not chats or not known_players:
        return

    for chat in chats:
        chat_id = chat["chat_id"]
        linked_ids = await get_linked_player_ids(chat_id)

        for tg_id in known_players:
            try:
                member = await bot.get_chat_member(chat_id, tg_id)
                is_in_chat = member.status in ("member", "administrator", "creator")
            except Exception:
                is_in_chat = False

            is_linked = tg_id in linked_ids

            if is_in_chat and not is_linked:
                await add_player_chat(tg_id, chat_id)
                await recompute_player_team(bot, tg_id)
            elif not is_in_chat and is_linked:
                await remove_player_chat(tg_id, chat_id)
                await recompute_player_team(bot, tg_id)

            await asyncio.sleep(0.01)


async def auto_roster_check_loop(bot: Bot):
    while True:
        try:
            await sync_all_rosters(bot)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка в автоматической проверке составов: {e}")
        await asyncio.sleep(ROSTER_CHECK_INTERVAL)


# =========================================================
#            НАПОМИНАНИЕ STEAMID В ЛС КАЖДЫЕ 3 ЧАСА
# =========================================================

async def steam_reminder_loop(bot: Bot):
    while True:
        try:
            unlinked_players = await get_unlinked_steam_in_teams()
            for p in unlinked_players:
                tg_id = p["tg_id"]
                team_name = p["team_name"]
                kb = InlineKeyboardBuilder()
                kb.button(text="🔗 Привязать SteamID", callback_data="profile:steamid")

                text = (
                    f"⚠️ <b>Напоминание о привязке SteamID!</b>\n\n"
                    f"Вы состоите в команде <b>{esc(team_name)}</b>, но до сих пор не привязали ваш <b>SteamID</b>.\n"
                    "Пожалуйста, нажмите кнопку ниже и укажите свой SteamID, чтобы играть в лигах без ограничений!"
                )
                try:
                    await bot.send_message(tg_id, text, reply_markup=kb.as_markup())
                except Exception as e:
                    logging.debug(f"Не удалось отправить напоминание SteamID игроку {tg_id}: {e}")
                await asyncio.sleep(0.05)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка в рассылке напоминаний SteamID: {e}")

        await asyncio.sleep(STEAM_REMINDER_INTERVAL)


# =========================================================
#            ЕЖЕДНЕВНЫЙ ОТЧЁТ (24 ЧАСА В 14:30 МСК)
# =========================================================

async def send_daily_steam_report(bot: Bot):
    chats = await get_chats()
    for chat in chats:
        chat_id = chat["chat_id"]
        roster = await get_team_roster(chat_id)

        linked_players = []
        unlinked_players = []

        for p in roster:
            name = esc(p["nickname"]) if p["nickname"] else esc(p["first_name"] or "Без имени")
            user_tag = f" (@{esc(p['username'])})" if p["username"] else ""
            if p["steam_id"]:
                linked_players.append(f"• {name}{user_tag} (<code>{esc(p['steam_id'])}</code>)")
            else:
                unlinked_players.append(f"• {name}{user_tag}")

        text = f"📊 <b>Ежедневный отчёт по привязке SteamID [{chat['league']}]</b>\n━━━━━━━━━━━━━━━━━━━\n\n"

        text += "✅ <b>Игроки кто привязал steamid:</b>\n"
        text += "\n".join(linked_players) if linked_players else "<i>Никто не привязал</i>"

        text += "\n\n❌ <b>Игроки кто не привязал:</b>\n"
        text += "\n".join(unlinked_players) if unlinked_players else "<i>Все игроки привязали! 🎉</i>"

        try:
            await bot.send_message(chat_id, text)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка отправки отчёта в чат {chat_id}: {e}")


async def daily_report_loop(bot: Bot):
    while True:
        now = datetime.now(MSK)
        target = now.replace(hour=14, minute=30, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)

        seconds_until_target = (target - now).total_seconds()
        logging.info(f"⏳ Следующий ежедневный отчёт по SteamID запланирован на {target.strftime('%d.%m.%Y %H:%M МСК')} (через {int(seconds_until_target)} сек)")

        await asyncio.sleep(seconds_until_target)

        try:
            await send_daily_steam_report(bot)
        except Exception as e:
            logging.error(f"⚠️ Ошибка в таймере ежедневного отчёта: {e}")

        await asyncio.sleep(60)


# =========================================================
#              ПЛАНИРОВЩИК НАПОМИНАНИЙ
# =========================================================

async def _send_safe(bot: Bot, chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception as e:
        logging.warning(f"⚠️ Ошибка отправки в чат {chat_id}: {e}")


async def check_reminders(bot: Bot):
    due_45 = await get_matches_due_for(REMIND_BEFORE_1, "notified_45")
    for m in due_45:
        text = reminder_45_text(m["team_home_name"], m["team_away_name"])
        await _send_safe(bot, m["team_home_chat_id"], text)
        await _send_safe(bot, m["team_away_chat_id"], text)
        await mark_notified(m["id"], "notified_45")

    due_15 = await get_matches_due_for(REMIND_BEFORE_2, "notified_15")
    for m in due_15:
        home_text = raskatka_text(
            m["server_name"], m["server_ip"], m["server_port"], m["server_password"],
            m["team_home_name"], m["team_away_name"], color="ред",
        )
        away_text = raskatka_text(
            m["server_name"], m["server_ip"], m["server_port"], m["server_password"],
            m["team_home_name"], m["team_away_name"], color="блу",
        )
        await _send_safe(bot, m["team_home_chat_id"], home_text)
        await _send_safe(bot, m["team_away_chat_id"], away_text)
        await mark_notified(m["id"], "notified_15")


async def scheduler_loop(bot: Bot):
    while True:
        try:
            await check_reminders(bot)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка планировщика матчей: {e}")
        await asyncio.sleep(SCHEDULER_INTERVAL)


# =========================================================
#         ПРОВЕРКА ПРАВ АДМИНИСТРАТОРА БОТА В ЧАТАХ
# =========================================================

async def check_bot_admin_rights(bot: Bot):
    chats = await get_chats()
    for chat in chats:
        try:
            member = await bot.get_chat_member(chat["chat_id"], bot.id)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка проверки прав бота в чате {chat['chat_id']}: {e}")
            continue
        if member.status not in ("administrator", "creator"):
            await _send_safe(
                bot, chat["chat_id"],
                "⚠️ У бота нет прав администратора в этом чате. Выдайте их для корректной работы!",
            )


async def admin_rights_check_loop(bot: Bot):
    while True:
        try:
            await check_bot_admin_rights(bot)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка проверки прав админа: {e}")
        await asyncio.sleep(ADMIN_RIGHTS_CHECK_INTERVAL)


# =========================================================
#                          MAIN
# =========================================================

async def main():
    logging.basicConfig(level=logging.INFO)
    if not BOT_TOKEN:
        raise RuntimeError("❌ Не задан BOT_TOKEN!")
    if not DATABASE_URL:
        raise RuntimeError("❌ Не задан DATABASE_URL!")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(start_router)
    dp.include_router(auth_router)
    dp.include_router(panel_router)
    dp.include_router(dm_router)
    dp.include_router(team_chat_router)
    dp.include_router(channel_router)

    @dp.errors()
    async def global_error_handler(event: ErrorEvent):
        logging.exception("Необработанное исключение при обработке апдейта", exc_info=event.exception)
        try:
            update = event.update
            if update.message:
                await update.message.answer("⚠️ Произошла внутренняя ошибка. Попробуйте снова или используйте /adminkarpl")
            elif update.callback_query:
                await update.callback_query.message.answer("⚠️ Произошла ошибка. Попробуйте снова.")
        except Exception:
            pass
        return True

    await init_pool()
    logging.info("✅ База данных подключена и готова к работе")

    # Однократная синхронизация whitelist при старте
    if GITHUB_TOKEN and GIST_ID:
        asyncio.create_task(rebuild_whitelist_file())
        logging.info("✅ Запущена фоновая синхронизация whitelist при старте бота")

    asyncio.create_task(scheduler_loop(bot))
    logging.info("✅ Запущен планировщик напоминаний о матчах")

    asyncio.create_task(admin_rights_check_loop(bot))
    logging.info("✅ Запущена регулярная проверка прав бота в чатах")

    asyncio.create_task(auto_roster_check_loop(bot))
    logging.info("✅ Запущена автопроверка составов каждые 5 минут")

    asyncio.create_task(daily_report_loop(bot))
    logging.info("✅ Запущен таймер ежедневного отчёта по SteamID (14:30 МСК)")

    asyncio.create_task(steam_reminder_loop(bot))
    logging.info("✅ Запущен цикл персональных напоминаний по SteamID (раз в 3 часа)")

    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("🚀 Бот запущен, начинаю polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
