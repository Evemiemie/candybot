import os
import re
import io
import csv
import time
import asyncio
import logging
import datetime
from typing import Optional, List, Tuple, Set, Dict

import discord
from discord import app_commands
from discord.ext import commands
import aiosqlite
from dotenv import load_dotenv

log = logging.getLogger("candybot")

# =========================
# CONFIG
# =========================
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "cs_helper.db")
_GUILD_RAW = os.getenv("GUILD_ID", "").strip()
GUILD_ID = int(_GUILD_RAW) if _GUILD_RAW.isdigit() else 0

STATUS_OPEN = "open"
STATUS_CLOSED = "closed"

TYPE_MANDATORY = "mandatory"
TYPE_OPTIONAL = "optional"

LEADERBOARD_PAGE_SIZE = 20

CURRENCY = "💰"  # символ валюты — поменять тут

# Роли, дающие доступ к управляющим командам (помимо admin/manage_guild/manage_roles).
# .env: STAFF_ROLE_IDS=123,456
STAFF_ROLE_IDS: Set[int] = {
    int(x) for x in os.getenv("STAFF_ROLE_IDS", "").replace(" ", "").split(",") if x.isdigit()
}

# RL роли — обязательный /content_create. .env: RL_ROLE_IDS=111,222
RL_ROLE_IDS: Set[int] = {
    int(x) for x in os.getenv("RL_ROLE_IDS", "").replace(" ", "").split(",") if x.isdigit()
}

# Рекрут — только мемберские команды. .env: RECRUIT_ROLE_IDS=333,444
RECRUIT_ROLE_IDS: Set[int] = {
    int(x) for x in os.getenv("RECRUIT_ROLE_IDS", "").replace(" ", "").split(",") if x.isdigit()
}

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

COLOR_BLUE   = 0x5865F2
COLOR_GREEN  = 0x57F287
COLOR_RED    = 0xED4245
COLOR_YELLOW = 0xFEE75C
COLOR_GOLD   = 0xF1C40F

# Глобальный коннект к БД (инициализируется в db_init).
# aiosqlite сериализует операции через свой воркер-поток => безопасно для конкурентных await,
# а WAL + busy_timeout убирают "database is locked".
DB: Optional[aiosqlite.Connection] = None
DB_WLOCK = asyncio.Lock()  # сериализует write-транзакции на общем коннекте


# =========================
# DB INIT
# =========================
async def db_init() -> None:
    global DB
    if DB is not None:
        return
    DB = await aiosqlite.connect(DB_PATH)
    DB.row_factory = aiosqlite.Row
    await DB.execute("PRAGMA journal_mode=WAL")
    await DB.execute("PRAGMA busy_timeout=5000")
    await DB.execute("PRAGMA foreign_keys=ON")

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS content_payouts (
        guild_id INTEGER NOT NULL,
        content_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        paid_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, content_id, user_id)
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS contents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        message_id INTEGER,
        thread_id INTEGER,
        title TEXT NOT NULL,
        roles_text TEXT NOT NULL,
        after_text TEXT,
        ends_at INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'open',
        created_by INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        payout_role_id INTEGER,
        payout_role_name TEXT,
        hosted_by TEXT,
        start_ts INTEGER,
        content_type TEXT NOT NULL DEFAULT 'mandatory',
        builds_link TEXT,
        photo_url TEXT
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS content_assignments (
        content_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        role_index INTEGER NOT NULL,
        assigned_at INTEGER NOT NULL,
        PRIMARY KEY (content_id, user_id),
        UNIQUE (content_id, role_index)
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS content_attendance (
        content_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        added_by INTEGER NOT NULL,
        added_at INTEGER NOT NULL,
        label TEXT,
        PRIMARY KEY (content_id, user_id)
    );
    """)

    # Источник истины для статистики: одно начисление = одна строка с привязкой к контенту.
    # % и лидерборд считаются JOIN-ом с contents по start_ts/content_type.
    await DB.execute("""
    CREATE TABLE IF NOT EXISTS attendance_awards (
        guild_id INTEGER NOT NULL,
        content_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        awarded_by INTEGER NOT NULL,
        awarded_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, content_id, user_id)
    );
    """)

    # Дата "в сборах с" — неизменна, ставится при первом начислении.
    await DB.execute("""
    CREATE TABLE IF NOT EXISTS attendance_join (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        joined_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS attendance_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        content_id INTEGER,
        delta INTEGER NOT NULL,
        kind TEXT NOT NULL,
        actor_id INTEGER NOT NULL,
        created_at INTEGER NOT NULL
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS balances (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );
    """)

    await DB.execute("""
    CREATE TABLE IF NOT EXISTS balance_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        delta INTEGER NOT NULL,
        balance_after INTEGER NOT NULL,
        kind TEXT NOT NULL,
        reason TEXT,
        actor_id INTEGER NOT NULL,
        created_at INTEGER NOT NULL
    );
    """)

    # Siphoned Energy: уникальность строки лога (дата+ник+reason+сумма),
    # пересекающиеся выгрузки не дублируют проводки.
    await DB.execute("""
    CREATE TABLE IF NOT EXISTS siphon_events (
        guild_id INTEGER NOT NULL,
        occurred_at INTEGER NOT NULL,
        occurred_at_raw TEXT NOT NULL,
        player_key TEXT NOT NULL,
        player TEXT NOT NULL,
        reason TEXT NOT NULL,
        amount INTEGER NOT NULL,
        imported_at INTEGER NOT NULL,
        imported_by INTEGER NOT NULL,
        PRIMARY KEY (guild_id, occurred_at_raw, player_key, reason, amount)
    );
    """)
    await DB.execute("""
    CREATE TABLE IF NOT EXISTS siphon_balances (
        guild_id INTEGER NOT NULL,
        player_key TEXT NOT NULL,
        player TEXT NOT NULL,
        deposited INTEGER NOT NULL DEFAULT 0,
        withdrawn INTEGER NOT NULL DEFAULT 0,
        net INTEGER NOT NULL DEFAULT 0,
        ops INTEGER NOT NULL DEFAULT 0,
        last_at INTEGER,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (guild_id, player_key)
    );
    """)

    # Миграции для старой БД (новые колонки contents).
    migrations = [
        "ALTER TABLE contents ADD COLUMN content_type TEXT NOT NULL DEFAULT 'mandatory'",
        "ALTER TABLE contents ADD COLUMN builds_link TEXT",
        "ALTER TABLE contents ADD COLUMN photo_url TEXT",
        "ALTER TABLE contents ADD COLUMN hosted_by TEXT",
        "ALTER TABLE contents ADD COLUMN start_ts INTEGER",
        "ALTER TABLE content_attendance ADD COLUMN label TEXT",
    ]
    for sql in migrations:
        try:
            await DB.execute(sql)
        except aiosqlite.OperationalError as e:
            msg = str(e).lower()
            if "duplicate column" in msg or "already exists" in msg:
                continue
            raise

    await DB.execute("CREATE INDEX IF NOT EXISTS idx_contents_guild_type_start ON contents(guild_id, content_type, start_ts)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_contents_guild_status ON contents(guild_id, status)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_contents_thread ON contents(thread_id)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_awards_guild_user ON attendance_awards(guild_id, user_id)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_balevents_guild_user ON balance_events(guild_id, user_id)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_awards_guild_awarded ON attendance_awards(guild_id, awarded_at)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_siphon_guild_player ON siphon_events(guild_id, player_key, occurred_at)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_siphon_bal_net ON siphon_balances(guild_id, net)")

    # Самолечение: joined_at должен = времени первого начисления.
    # Чинит данные, записанные старым кодом (где joined_at ошибочно = start_ts в будущем).
    await DB.execute("""
        UPDATE attendance_join
        SET joined_at = (
            SELECT MIN(a.awarded_at) FROM attendance_awards a
            WHERE a.guild_id = attendance_join.guild_id AND a.user_id = attendance_join.user_id
        )
        WHERE EXISTS (
            SELECT 1 FROM attendance_awards a
            WHERE a.guild_id = attendance_join.guild_id AND a.user_id = attendance_join.user_id
        )
        AND joined_at <> (
            SELECT MIN(a.awarded_at) FROM attendance_awards a
            WHERE a.guild_id = attendance_join.guild_id AND a.user_id = attendance_join.user_id
        )
    """)
    await DB.commit()


# =========================
# CONTENTS DB
# =========================
async def db_create_content(
    guild_id: int,
    channel_id: int,
    title: str,
    roles_text: str,
    after_text: Optional[str],
    created_by: int,
    content_type: str,
    hosted_by: Optional[str] = None,
    start_ts: Optional[int] = None,
    builds_link: Optional[str] = None,
) -> int:
    async with DB_WLOCK:
        cur = await DB.execute(
            """
            INSERT INTO contents
                (guild_id, channel_id, title, roles_text, after_text, ends_at, status,
                 created_by, created_at, hosted_by, start_ts, content_type, builds_link)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (guild_id, channel_id, title, roles_text, after_text, STATUS_OPEN,
             created_by, int(time.time()), hosted_by, start_ts, content_type, builds_link)
        )
        rid = cur.lastrowid
        await DB.commit()
        if rid is None:
            raise RuntimeError("не удалось получить id контента")
        return int(rid)

async def db_payout_content_once(
    guild_id: int, content_id: int, user_ids: List[int],
    amount: int, actor_id: int, force: bool = False,
    reason: Optional[str] = None,
) -> Tuple[int, int]:
    """Выплата за контент. force=False → пропускает уже оплаченных (анти-дабл). Возвращает (paid, skipped)."""
    now = int(time.time())
    paid = skipped = 0
    payout_reason = reason or f"content:{content_id}"
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            for uid in user_ids:
                if not force:
                    cur = await DB.execute(
                        "SELECT 1 FROM content_payouts WHERE guild_id=? AND content_id=? AND user_id=?",
                        (guild_id, content_id, uid))
                    if await cur.fetchone():
                        skipped += 1; continue
                await _balance_apply_tx(guild_id, uid, amount, "payout_content",
                                        payout_reason, actor_id, now)
                await DB.execute(
                    """INSERT INTO content_payouts (guild_id, content_id, user_id, amount, paid_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(guild_id, content_id, user_id)
                       DO UPDATE SET amount = amount + excluded.amount, paid_at = excluded.paid_at""",
                    (guild_id, content_id, uid, amount, now))
                paid += 1
            await DB.commit()
        except Exception:
            await DB.rollback(); raise
    return paid, skipped

async def db_set_message_thread(
    content_id: int, guild_id: int, message_id: int,
    thread_id: Optional[int], photo_url: Optional[str],
) -> None:
    async with DB_WLOCK:
        await DB.execute(
            """UPDATE contents SET message_id = ?, thread_id = ?, photo_url = ?
               WHERE id = ? AND guild_id = ?""",
            (message_id, thread_id, photo_url, content_id, guild_id)
        )
        await DB.commit()


async def db_set_photo_url(content_id: int, guild_id: int, photo_url: Optional[str]) -> None:
    async with DB_WLOCK:
        await DB.execute(
            "UPDATE contents SET photo_url = ? WHERE id = ? AND guild_id = ?",
            (photo_url, content_id, guild_id),
        )
        await DB.commit()


async def db_get_content_by_id(content_id: int, guild_id: int):
    cur = await DB.execute(
        "SELECT * FROM contents WHERE id = ? AND guild_id = ?",
        (content_id, guild_id),
    )
    return await cur.fetchone()


async def db_get_content_by_thread(thread_id: int, guild_id: int):
    cur = await DB.execute(
        "SELECT * FROM contents WHERE thread_id = ? AND guild_id = ?",
        (thread_id, guild_id),
    )
    return await cur.fetchone()


async def db_close_content(content_id: int, guild_id: int) -> None:
    async with DB_WLOCK:
        await DB.execute(
            "UPDATE contents SET status = ? WHERE id = ? AND guild_id = ?",
            (STATUS_CLOSED, content_id, guild_id),
        )
        await DB.commit()


async def db_set_payout_role(content_id: int, guild_id: int, role_id: int, role_name: str) -> None:
    async with DB_WLOCK:
        await DB.execute(
            """UPDATE contents SET payout_role_id = ?, payout_role_name = ?
               WHERE id = ? AND guild_id = ?""",
            (role_id, role_name, content_id, guild_id)
        )
        await DB.commit()


async def db_clear_payout_role(content_id: int, guild_id: int) -> None:
    async with DB_WLOCK:
        await DB.execute(
            "UPDATE contents SET payout_role_id = NULL, payout_role_name = NULL WHERE id = ? AND guild_id = ?",
            (content_id, guild_id)
        )
        await DB.commit()


async def db_delete_content(content_id: int, guild_id: int) -> None:
    """Удаляет контент и связанные слоты/присутствие. Awards/payouts не трогает."""
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            cur = await DB.execute(
                "SELECT id FROM contents WHERE id = ? AND guild_id = ?",
                (content_id, guild_id),
            )
            if await cur.fetchone() is None:
                await DB.rollback()
                return
            await DB.execute("DELETE FROM content_assignments WHERE content_id = ?", (content_id,))
            await DB.execute("DELETE FROM content_attendance WHERE content_id = ?", (content_id,))
            await DB.execute("DELETE FROM contents WHERE id = ? AND guild_id = ?", (content_id, guild_id))
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise


async def db_list_contents(guild_id: int, status: Optional[str], limit: int = 25):
    if status in (STATUS_OPEN, STATUS_CLOSED):
        cur = await DB.execute(
            """SELECT id, title, status, start_ts, content_type, created_by
               FROM contents WHERE guild_id = ? AND status = ?
               ORDER BY id DESC LIMIT ?""",
            (guild_id, status, limit),
        )
    else:
        cur = await DB.execute(
            """SELECT id, title, status, start_ts, content_type, created_by
               FROM contents WHERE guild_id = ?
               ORDER BY id DESC LIMIT ?""",
            (guild_id, limit),
        )
    return await cur.fetchall()


# ---------- slots ----------
async def db_assign_user(content_id: int, user_id: int, role_index: int) -> Tuple[bool, str]:
    # asyncio-лок + BEGIN IMMEDIATE => нет interleaving чужого BEGIN на общем коннекте.
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            cur = await DB.execute(
                "SELECT user_id FROM content_assignments WHERE content_id = ? AND role_index = ?",
                (content_id, role_index)
            )
            row = await cur.fetchone()
            if row is not None and int(row[0]) != int(user_id):
                await DB.rollback()
                return False, "Роль занята."

            await DB.execute(
                "DELETE FROM content_assignments WHERE content_id = ? AND user_id = ?",
                (content_id, user_id)
            )
            await DB.execute(
                "INSERT INTO content_assignments (content_id, user_id, role_index, assigned_at) VALUES (?, ?, ?, ?)",
                (content_id, user_id, role_index, int(time.time()))
            )
            await DB.commit()
        except aiosqlite.IntegrityError:
            await DB.rollback()
            return False, "Роль занята."
        except Exception:
            await DB.rollback()
            raise
    return True, f"Записан на роль {role_index}."


async def db_unassign_user(content_id: int, user_id: int) -> bool:
    async with DB_WLOCK:
        cur = await DB.execute(
            "DELETE FROM content_assignments WHERE content_id = ? AND user_id = ?",
            (content_id, user_id)
        )
        await DB.commit()
        return cur.rowcount > 0


async def db_unassign_by_role_index(content_id: int, role_index: int) -> Optional[int]:
    async with DB_WLOCK:
        cur = await DB.execute(
            "SELECT user_id FROM content_assignments WHERE content_id = ? AND role_index = ?",
            (content_id, role_index)
        )
        row = await cur.fetchone()
        if row is None:
            return None
        user_id = int(row[0])
        await DB.execute(
            "DELETE FROM content_assignments WHERE content_id = ? AND role_index = ?",
            (content_id, role_index)
        )
        await DB.commit()
        return user_id


async def db_get_roster(content_id: int) -> List[Tuple[int, int]]:
    cur = await DB.execute(
        "SELECT role_index, user_id FROM content_assignments WHERE content_id = ? ORDER BY role_index ASC",
        (content_id,)
    )
    rows = await cur.fetchall()
    return [(int(r[0]), int(r[1])) for r in rows]


# ---------- attendance (per-content presence) ----------
async def db_attend_add_many(content_id: int, user_ids: List[int], added_by: int,
                             label: Optional[str] = None) -> Tuple[int, int]:
    added = already = 0
    now = int(time.time())
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            for uid in user_ids:
                try:
                    await DB.execute(
                        "INSERT INTO content_attendance (content_id, user_id, added_by, added_at, label) VALUES (?, ?, ?, ?, ?)",
                        (content_id, uid, added_by, now, label))
                    added += 1
                except aiosqlite.IntegrityError:  # дубль не рвёт транзакцию в sqlite
                    already += 1
                    if label is not None:
                        await DB.execute(
                            "UPDATE content_attendance SET label = ? WHERE content_id = ? AND user_id = ?",
                            (label, content_id, uid))
            await DB.commit()
        except Exception:
            await DB.rollback(); raise
    return added, already


async def db_attend_remove(content_id: int, user_id: int) -> bool:
    async with DB_WLOCK:
        cur = await DB.execute(
            "DELETE FROM content_attendance WHERE content_id = ? AND user_id = ?",
            (content_id, user_id)
        )
        await DB.commit()
        return cur.rowcount > 0


async def db_attend_list(content_id: int) -> List[int]:
    cur = await DB.execute(
        "SELECT user_id FROM content_attendance WHERE content_id = ? ORDER BY added_at ASC",
        (content_id,)
    )
    rows = await cur.fetchall()
    return [int(r[0]) for r in rows]


async def db_attend_list_labeled(content_id: int) -> List[Tuple[int, Optional[str]]]:
    cur = await DB.execute(
        "SELECT user_id, label FROM content_attendance WHERE content_id = ? ORDER BY added_at ASC",
        (content_id,)
    )
    rows = await cur.fetchall()
    return [(int(r[0]), (str(r[1]) if r[1] else None)) for r in rows]


async def db_get_all_participants(content_id: int) -> List[int]:
    roster = await db_get_roster(content_id)
    attend = await db_attend_list(content_id)
    out: List[int] = []
    seen: Set[int] = set()
    for _, uid in roster:
        if uid not in seen:
            seen.add(uid); out.append(uid)
    for uid in attend:
        if uid not in seen:
            seen.add(uid); out.append(uid)
    return out


# =========================
# ATTENDANCE STATS (computed)
# =========================
async def db_att_award_for_content(
    guild_id: int, content_id: int, user_ids: List[int],
    awarded_by: int,
) -> Tuple[int, int]:
    now = int(time.time())
    awarded = already = 0
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            for uid in user_ids:
                # joined_at = время первого НАЧИСЛЕНИЯ ("начал аттендить"), неизменно
                await DB.execute(
                    "INSERT OR IGNORE INTO attendance_join (guild_id, user_id, joined_at) VALUES (?, ?, ?)",
                    (guild_id, uid, now)
                )
                try:
                    await DB.execute(
                        "INSERT INTO attendance_awards (guild_id, content_id, user_id, awarded_by, awarded_at) VALUES (?, ?, ?, ?, ?)",
                        (guild_id, content_id, uid, awarded_by, now)
                    )
                except aiosqlite.IntegrityError:
                    already += 1
                    continue
                await DB.execute(
                    "INSERT INTO attendance_events (guild_id, user_id, content_id, delta, kind, actor_id, created_at) VALUES (?, ?, ?, 1, 'content_award', ?, ?)",
                    (guild_id, uid, content_id, awarded_by, now)
                )
                awarded += 1
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise
    return awarded, already


async def db_att_remove_for_content(guild_id: int, content_id: int, user_id: int, removed_by: int) -> Tuple[bool, str]:
    now = int(time.time())
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            cur = await DB.execute(
                "DELETE FROM attendance_awards WHERE guild_id = ? AND content_id = ? AND user_id = ?",
                (guild_id, content_id, user_id))
            if cur.rowcount == 0:
                await DB.rollback()
                return False, "У пользователя нет аттенданса за этот контент."
            await DB.execute(
                """INSERT INTO attendance_events
                   (guild_id, user_id, content_id, delta, kind, actor_id, created_at)
                   VALUES (?, ?, ?, -1, 'content_remove', ?, ?)""",
                (guild_id, user_id, content_id, removed_by, now))
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise
    return True, "Аттенданс удалён."


async def db_get_joined_at(guild_id: int, user_id: int) -> Optional[int]:
    cur = await DB.execute(
        "SELECT joined_at FROM attendance_join WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id)
    )
    row = await cur.fetchone()
    return int(row[0]) if row else None


def window_bounds(scope: str) -> Tuple[int, int]:
    """(lo, hi) unix-границы окна. Полуинтервал [lo, hi). UTC."""
    now = datetime.datetime.now(datetime.timezone.utc)
    hi = int(now.timestamp()) + 1  # +1 чтобы включить только что начисленное
    if scope == "all":
        return 0, hi
    if scope == "week":
        lo = int((now - datetime.timedelta(days=7)).timestamp())
    elif scope == "month":  # текущий календарный месяц
        lo = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
    elif scope == "prev_month":  # прошлый календарный месяц
        first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_prev = first_this - datetime.timedelta(seconds=1)
        first_prev = last_prev.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return int(first_prev.timestamp()), int(first_this.timestamp())
    elif scope == "3months":  # последние ~3 месяца (rolling 90д)
        lo = int((now - datetime.timedelta(days=90)).timestamp())
    else:
        lo = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
    return lo, hi


async def db_att_profile(guild_id: int, user_id: int, scope: str) -> Optional[dict]:
    """
    % посещаемости по обяз/необяз в окне scope.
    Окно считается по awarded_at (когда начислен аттенданс), а не по запланированному start_ts.
    Знаменатель = контенты этого типа, по которым в окне был начислен аттенданс кому-либо
    (т.е. реально проведённые); числитель — те из них, что посетил юзер.
    None, если юзер ни разу не аттендил.
    """
    joined = await db_get_joined_at(guild_id, user_id)
    if joined is None:
        return None
    lo, hi = window_bounds(scope)
    floor = max(joined, lo)  # контент до того, как юзер начал аттендить, не учитываем

    res = {"joined_at": joined, "scope": scope}
    for ctype in (TYPE_MANDATORY, TYPE_OPTIONAL):
        cur = await DB.execute(
            """SELECT COUNT(DISTINCT a.content_id)
               FROM attendance_awards a JOIN contents c ON c.id = a.content_id
               WHERE a.guild_id = ? AND c.content_type = ?
                 AND a.awarded_at >= ? AND a.awarded_at < ?""",
            (guild_id, ctype, floor, hi)
        )
        total = int((await cur.fetchone())[0])
        cur = await DB.execute(
            """SELECT COUNT(DISTINCT a.content_id)
               FROM attendance_awards a JOIN contents c ON c.id = a.content_id
               WHERE a.guild_id = ? AND a.user_id = ? AND c.content_type = ?
                 AND a.awarded_at >= ? AND a.awarded_at < ?""",
            (guild_id, user_id, ctype, floor, hi)
        )
        attended = int((await cur.fetchone())[0])
        pct = round(attended / total * 100) if total > 0 else 0
        res[ctype] = {"total": total, "attended": attended, "pct": pct}
    return res


async def db_att_leaderboard(guild_id: int, scope: str = "month") -> List[Tuple[int, int]]:
    """Лидерборд посещаемости за окно scope (по awarded_at)."""
    lo, hi = window_bounds(scope)
    cur = await DB.execute(
        """SELECT user_id, COUNT(*) AS cnt
           FROM attendance_awards
           WHERE guild_id = ? AND awarded_at >= ? AND awarded_at < ?
           GROUP BY user_id
           HAVING cnt > 0
           ORDER BY cnt DESC, MIN(awarded_at) ASC""",
        (guild_id, lo, hi)
    )
    rows = await cur.fetchall()
    return [(int(r[0]), int(r[1])) for r in rows]


async def db_att_leaderboard_month(guild_id: int) -> List[Tuple[int, int]]:
    """Лидерборд за текущий календарный месяц (по awarded_at)."""
    return await db_att_leaderboard(guild_id, "month")


# =========================
# BALANCE DB
# =========================
async def db_balance_get(guild_id: int, user_id: int) -> int:
    cur = await DB.execute(
        "SELECT amount FROM balances WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id)
    )
    row = await cur.fetchone()
    return int(row[0]) if row else 0


async def _balance_apply_tx(
    guild_id: int, user_id: int, delta: int,
    kind: str, reason: Optional[str], actor_id: int, now: int,
) -> Tuple[int, int]:
    """Ядро начисления. Вызывать ТОЛЬКО внутри DB_WLOCK + открытой транзакции."""
    cur = await DB.execute(
        "SELECT amount FROM balances WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id)
    )
    row = await cur.fetchone()
    current = int(row[0]) if row else 0

    new = current + delta
    if new < 0:
        new = 0
    applied = new - current

    await DB.execute(
        """INSERT INTO balances (guild_id, user_id, amount, updated_at) VALUES (?, ?, ?, ?)
           ON CONFLICT(guild_id, user_id) DO UPDATE SET amount = excluded.amount, updated_at = excluded.updated_at""",
        (guild_id, user_id, new, now)
    )
    await DB.execute(
        """INSERT INTO balance_events (guild_id, user_id, delta, balance_after, kind, reason, actor_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (guild_id, user_id, applied, new, kind, reason, actor_id, now)
    )
    return new, applied


async def db_balance_apply(
    guild_id: int, user_id: int, delta: int,
    kind: str, reason: Optional[str], actor_id: int,
) -> Tuple[int, int]:
    """
    Применяет дельту к балансу. Клампит снизу нулём (минимум 0).
    Возвращает (new_balance, applied_delta) — applied_delta может отличаться от delta при клампе.
    """
    now = int(time.time())
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            res = await _balance_apply_tx(guild_id, user_id, delta, kind, reason, actor_id, now)
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise
    return res


async def db_balance_payout_many(
    guild_id: int, user_ids: List[int], amount: int,
    kind: str, reason: Optional[str], actor_id: int,
) -> None:
    """Атомарная массовая выплата: всё в одной транзакции (all-or-nothing => нет двойных выплат при падении)."""
    now = int(time.time())
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            for uid in user_ids:
                await _balance_apply_tx(guild_id, uid, amount, kind, reason, actor_id, now)
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise

async def db_balance_leaderboard(guild_id: int, limit: int = 10000) -> List[Tuple[int, int]]:
    """Топ по текущему балансу (только > 0)."""
    cur = await DB.execute(
        """SELECT user_id, amount FROM balances
           WHERE guild_id = ? AND amount > 0
           ORDER BY amount DESC, user_id ASC LIMIT ?""",
        (guild_id, limit))
    return [(int(r[0]), int(r[1])) for r in await cur.fetchall()]


async def db_balance_bank(guild_id: int) -> Tuple[int, int]:
    """(сумма всех балансов, число держателей с amount > 0)."""
    cur = await DB.execute(
        """SELECT COALESCE(SUM(amount), 0),
                  COALESCE(SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END), 0)
           FROM balances WHERE guild_id = ?""",
        (guild_id,))
    row = await cur.fetchone()
    return int(row[0]), int(row[1])


def _event_row(r) -> dict:
    return {
        "created_at": int(r[0]), "user_id": int(r[1]), "delta": int(r[2]),
        "balance_after": int(r[3]), "kind": str(r[4]),
        "reason": (str(r[5]) if r[5] else ""), "actor_id": int(r[6]),
    }


async def db_balance_events_range(
    guild_id: int, lo: int, hi: int, user_id: Optional[int] = None,
) -> List[dict]:
    """Транзакции в полуинтервале [lo, hi). Опционально фильтр по user_id."""
    if user_id is None:
        cur = await DB.execute(
            """SELECT created_at, user_id, delta, balance_after, kind, reason, actor_id
               FROM balance_events
               WHERE guild_id = ? AND created_at >= ? AND created_at < ?
               ORDER BY created_at ASC, id ASC""",
            (guild_id, lo, hi))
    else:
        cur = await DB.execute(
            """SELECT created_at, user_id, delta, balance_after, kind, reason, actor_id
               FROM balance_events
               WHERE guild_id = ? AND created_at >= ? AND created_at < ? AND user_id = ?
               ORDER BY created_at ASC, id ASC""",
            (guild_id, lo, hi, user_id))
    return [_event_row(r) for r in await cur.fetchall()]


async def db_balance_period_stats(
    guild_id: int, lo: int, hi: int, user_id: Optional[int] = None,
) -> dict:
    """Суммы +/- и число транзакций за окно. user_id=None → вся гильдия."""
    extra = " AND user_id = ?" if user_id is not None else ""
    params: tuple = (guild_id, lo, hi) if user_id is None else (guild_id, lo, hi, user_id)
    cur = await DB.execute(
        f"""SELECT
              COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END), 0),
              COALESCE(SUM(delta), 0),
              COUNT(*)
            FROM balance_events
            WHERE guild_id = ? AND created_at >= ? AND created_at < ?{extra}""",
        params)
    row = await cur.fetchone()
    return {"awarded": int(row[0]), "removed": int(row[1]), "net": int(row[2]), "count": int(row[3])}


# =========================
# SIPHONED ENERGY
# =========================
_SIPHON_TS = "%Y-%m-%d %H:%M:%S"


def parse_siphon_txt(text: str) -> Tuple[List[dict], List[str]]:
    """Парсит TSV из игры: Date, Player, Reason, Amount. (rows, errors)."""
    rows: List[dict] = []
    errors: List[str] = []
    raw = text.lstrip("\ufeff")
    reader = csv.reader(io.StringIO(raw), delimiter="\t", quotechar='"')
    header_skipped = False
    for lineno, parts in enumerate(reader, start=1):
        if not parts or all(not (p or "").strip() for p in parts):
            continue
        if not header_skipped:
            joined = " ".join(p.strip().strip('"') for p in parts).lower()
            header_skipped = True
            if "date" in joined and "player" in joined:
                continue
        if len(parts) < 4:
            errors.append(f"стр. {lineno}: мало колонок")
            continue
        date_s = parts[0].strip().strip('"')
        player = parts[1].strip().strip('"')
        reason = parts[2].strip().strip('"')
        amount_s = parts[3].strip().strip('"').replace(" ", "").replace(",", "")
        if not player:
            errors.append(f"стр. {lineno}: пустой ник")
            continue
        reason_norm = reason.capitalize()
        if reason_norm not in ("Deposit", "Withdrawal"):
            errors.append(f"стр. {lineno}: неизвестный Reason `{reason}`")
            continue
        try:
            amount = int(amount_s)
        except ValueError:
            errors.append(f"стр. {lineno}: сумма `{amount_s}`")
            continue
        try:
            dt = datetime.datetime.strptime(date_s, _SIPHON_TS)
        except ValueError:
            errors.append(f"стр. {lineno}: дата `{date_s}`")
            continue
        if reason_norm == "Deposit" and amount < 0:
            amount = abs(amount)
        if reason_norm == "Withdrawal" and amount > 0:
            amount = -amount
        rows.append({
            "occurred_at": int(dt.replace(tzinfo=datetime.timezone.utc).timestamp()),
            "occurred_at_raw": date_s,
            "player": player,
            "player_key": player.casefold(),
            "reason": reason_norm,
            "amount": amount,
        })
    return rows, errors


async def db_siphon_import(guild_id: int, rows: List[dict], actor_id: int) -> Tuple[int, int]:
    """Вставляет только новые строки. Возвращает (inserted, skipped)."""
    inserted = skipped = 0
    now = int(time.time())
    async with DB_WLOCK:
        await DB.execute("BEGIN IMMEDIATE")
        try:
            for r in rows:
                cur = await DB.execute(
                    """INSERT OR IGNORE INTO siphon_events
                       (guild_id, occurred_at, occurred_at_raw, player_key, player,
                        reason, amount, imported_at, imported_by)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (guild_id, r["occurred_at"], r["occurred_at_raw"], r["player_key"],
                     r["player"], r["reason"], r["amount"], now, actor_id),
                )
                if cur.rowcount and cur.rowcount > 0:
                    inserted += 1
                    dep = r["amount"] if r["amount"] > 0 else 0
                    wth = -r["amount"] if r["amount"] < 0 else 0
                    await DB.execute(
                        """INSERT INTO siphon_balances
                           (guild_id, player_key, player, deposited, withdrawn, net, ops, last_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                           ON CONFLICT(guild_id, player_key) DO UPDATE SET
                             player = excluded.player,
                             deposited = deposited + excluded.deposited,
                             withdrawn = withdrawn + excluded.withdrawn,
                             net = net + excluded.net,
                             ops = ops + 1,
                             last_at = MAX(COALESCE(last_at, 0), excluded.last_at),
                             updated_at = excluded.updated_at""",
                        (guild_id, r["player_key"], r["player"], dep, wth, r["amount"],
                         r["occurred_at"], now),
                    )
                else:
                    skipped += 1
            await DB.commit()
        except Exception:
            await DB.rollback()
            raise
    return inserted, skipped


async def db_siphon_player(guild_id: int, nick: str) -> Optional[dict]:
    key = nick.strip().casefold()
    cur = await DB.execute(
        """SELECT player, deposited, withdrawn, net, ops, last_at
           FROM siphon_balances WHERE guild_id = ? AND player_key = ?""",
        (guild_id, key),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    cur = await DB.execute(
        """SELECT occurred_at_raw, reason, amount FROM siphon_events
           WHERE guild_id = ? AND player_key = ?
           ORDER BY occurred_at DESC, occurred_at_raw DESC LIMIT 20""",
        (guild_id, key),
    )
    events = [{"at": str(e[0]), "reason": str(e[1]), "amount": int(e[2])}
              for e in await cur.fetchall()]
    return {
        "player": str(row[0]),
        "deposited": int(row[1]),
        "withdrawn": int(row[2]),
        "net": int(row[3]),
        "ops": int(row[4]),
        "last_at": int(row[5]) if row[5] else None,
        "events": events,
    }


async def db_siphon_suggest(guild_id: int, nick: str, limit: int = 8) -> List[str]:
    key = nick.strip().casefold()
    cur = await DB.execute(
        """SELECT player FROM siphon_balances
           WHERE guild_id = ? AND player_key LIKE ?
           ORDER BY ops DESC LIMIT ?""",
        (guild_id, f"%{key}%", limit),
    )
    return [str(r[0]) for r in await cur.fetchall()]


async def db_siphon_leaderboard(guild_id: int) -> List[dict]:
    cur = await DB.execute(
        """SELECT player, deposited, withdrawn, net, ops
           FROM siphon_balances WHERE guild_id = ?
           ORDER BY net DESC, player ASC""",
        (guild_id,),
    )
    return [{"player": str(r[0]), "deposited": int(r[1]), "withdrawn": int(r[2]),
             "net": int(r[3]), "ops": int(r[4])} for r in await cur.fetchall()]


async def db_siphon_summary(guild_id: int) -> Optional[dict]:
    cur = await DB.execute(
        """SELECT COUNT(*),
                  COALESCE(SUM(deposited), 0),
                  COALESCE(SUM(withdrawn), 0),
                  COALESCE(SUM(net), 0),
                  COALESCE(SUM(ops), 0),
                  COALESCE(SUM(CASE WHEN net < 0 THEN 1 ELSE 0 END), 0),
                  COALESCE(SUM(CASE WHEN net > 0 THEN 1 ELSE 0 END), 0)
           FROM siphon_balances WHERE guild_id = ?""",
        (guild_id,),
    )
    b = await cur.fetchone()
    if b is None or int(b[0]) == 0:
        return None
    cur = await DB.execute(
        """SELECT MIN(imported_at), MAX(imported_at),
                  MIN(occurred_at_raw), MAX(occurred_at_raw)
           FROM siphon_events WHERE guild_id = ?""",
        (guild_id,),
    )
    e = await cur.fetchone()
    return {
        "players": int(b[0]),
        "deposited": int(b[1]),
        "withdrawn": int(b[2]),
        "net": int(b[3]),
        "ops": int(b[4]),
        "debtors": int(b[5]),
        "creditors": int(b[6]),
        "import_first": int(e[0]) if e and e[0] else None,
        "import_last": int(e[1]) if e and e[1] else None,
        "log_first": str(e[2]) if e and e[2] else None,
        "log_last": str(e[3]) if e and e[3] else None,
    }


# =========================
# HELPERS
# =========================
def ts_discord(unix_ts: int, fmt: str = "F") -> str:
    return f"<t:{unix_ts}:{fmt}>"


def fmt_money(n: int) -> str:
    return f"{n:,}".replace(",", " ") + f" {CURRENCY}"


def make_csv_file(filename: str, header: List[str], rows: List[list]) -> discord.File:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    writer.writerows(rows)
    return discord.File(fp=io.BytesIO(output.getvalue().encode("utf-8-sig")), filename=filename)


def thread_name_for(title: str, content_id: int) -> str:
    suffix = f" (CS#{content_id})"
    room = 100 - len(suffix)
    if room < 1:
        return f"CS#{content_id}"[:100]
    base = (title or "Content").strip() or "Content"
    if len(base) > room:
        base = base[: max(1, room - 1)] + "…"
    return (base + suffix)[:100]


def add_chunked_field(embed: discord.Embed, name: str, lines: List[str], limit: int = 950) -> None:
    """Режет длинные списки на несколько field (лимит Discord 1024 / 25 fields)."""
    if not lines:
        return
    leftover_budget = max(1, 20 - len(embed.fields))
    chunk: List[str] = []
    chunk_size = 0
    field_num = 0
    skipped = 0
    for line in lines:
        if field_num >= leftover_budget:
            skipped += 1
            continue
        if chunk_size + len(line) + 1 > limit and chunk:
            embed.add_field(
                name=name if field_num == 0 else "\u200b",
                value="\n".join(chunk),
                inline=False,
            )
            field_num += 1
            chunk, chunk_size = [], 0
            if field_num >= leftover_budget:
                skipped += 1
                continue
        chunk.append(line)
        chunk_size += len(line) + 1
    if chunk and field_num < leftover_budget:
        embed.add_field(
            name=name if field_num == 0 else "\u200b",
            value="\n".join(chunk),
            inline=False,
        )
        field_num += 1
    if skipped:
        embed.add_field(name="\u200b", value=f"… и ещё {skipped}", inline=False)


def parse_utc_time(time_str: str) -> Optional[int]:
    """'ЧЧ:ММ' как UTC сегодня -> unix. Если прошло — следующий день."""
    try:
        parts = time_str.strip().split(":")
        if len(parts) != 2:
            return None
        hh, mm = int(parts[0]), int(parts[1])
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        target = now_utc.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now_utc:
            target += datetime.timedelta(days=1)
        return int(target.timestamp())
    except Exception:
        return None


def normalize_roles_lines(raw: str) -> List[str]:
    lines = [ln.strip() for ln in raw.splitlines()]
    return [ln for ln in lines if ln]


def member_is_staff(member: discord.Member) -> bool:
    p = member.guild_permissions
    if p.administrator or p.manage_guild or p.manage_roles:
        return True
    return bool(STAFF_ROLE_IDS & {r.id for r in member.roles})


def member_is_rl(member: discord.Member) -> bool:
    """RL — может создавать обязательный контент."""
    return bool(RL_ROLE_IDS & {r.id for r in member.roles})


def member_is_recruit(member: discord.Member) -> bool:
    """Рекрут — только мемберские команды."""
    return bool(RECRUIT_ROLE_IDS & {r.id for r in member.roles})


def member_can_create_mandatory(member: discord.Member) -> bool:
    """Обязательный контент: стафф или RL."""
    return member_is_staff(member) or member_is_rl(member)


def member_can_create_content(member: discord.Member, content_type: Optional[str] = None) -> bool:
    """Необязательный — любой участник сервера. Обязательный — стафф/RL."""
    if content_type == TYPE_OPTIONAL:
        return True
    if content_type == TYPE_MANDATORY:
        return member_can_create_mandatory(member)
    return member_can_create_mandatory(member)


def is_organizer_or_admin(member: discord.Member, created_by: int) -> bool:
    return member.id == created_by or member_is_staff(member)


def get_member_safe(interaction: discord.Interaction) -> Optional[discord.Member]:
    if interaction.guild is None:
        return None
    return interaction.guild.get_member(interaction.user.id)


MENTION_ID_RE = re.compile(r"<@!?(\d+)>")


def parse_user_ids_from_text(text: str) -> List[int]:
    ids, seen, out = [int(m.group(1)) for m in MENTION_ID_RE.finditer(text)], set(), []
    for uid in ids:
        if uid not in seen:
            seen.add(uid); out.append(uid)
    return out


def type_label(content_type: str) -> str:
    return "🔴 Обязательный" if content_type == TYPE_MANDATORY else "🟢 Необязательный"


def build_main_post_embed(content_row, roster, attendance_only) -> discord.Embed:
    content_id = int(content_row["id"])
    title = str(content_row["title"])
    status = str(content_row["status"])
    roles_lines = normalize_roles_lines(str(content_row["roles_text"]))
    after_text = str(content_row["after_text"]) if content_row["after_text"] else None
    hosted_by = str(content_row["hosted_by"]) if content_row["hosted_by"] else None
    start_ts = int(content_row["start_ts"]) if content_row["start_ts"] else None
    content_type = str(content_row["content_type"])
    builds_link = str(content_row["builds_link"]) if content_row["builds_link"] else None
    photo_url = str(content_row["photo_url"]) if content_row["photo_url"] else None

    by_index = {idx: uid for idx, uid in roster}
    participants_count = len({uid for _, uid in roster}) + len(attendance_only)
    is_open = status == STATUS_OPEN

    embed = discord.Embed(title=f"📋 Контент #{content_id}: {title}",
                          color=COLOR_GREEN if is_open else COLOR_RED)
    embed.add_field(name="Тип", value=type_label(content_type), inline=True)

    if start_ts and start_ts > 0:
        utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
        embed.add_field(name="⏰ Be ready by",
                        value=f"{utc_str} • {ts_discord(start_ts, 'F')} • {ts_discord(start_ts, 'R')}",
                        inline=False)
    if hosted_by:
        embed.add_field(name="👤 Content hosted by", value=hosted_by, inline=False)

    embed.add_field(name="Status", value="🟢 Open" if is_open else "🔴 Closed", inline=True)
    embed.add_field(name="Players", value=f"**{participants_count}**", inline=True)

    roles_fmt = [
        f"`{i}.` {rn} — <@{by_index[i]}>" if by_index.get(i) else f"`{i}.` {rn} —"
        for i, rn in enumerate(roles_lines, start=1)
    ]
    add_chunked_field(embed, "📝 Roles", roles_fmt)

    if attendance_only:
        start = len(roles_lines) + 1
        att_lines = [f"`{j}.` {lbl} — <@{uid}>" if lbl else f"`{j}.` <@{uid}>"
                     for j, (uid, lbl) in enumerate(attendance_only, start=start)]
        add_chunked_field(embed, "➕ Additionally", att_lines)

    if builds_link:
        embed.add_field(name="🔗 Билды", value=builds_link, inline=False)
    if after_text and after_text.strip():
        embed.add_field(name="📌 Note", value=after_text.strip(), inline=False)

    # закрыт → не показываем инструкцию записи, показываем замок (меньше шума + п.1)
    if is_open:
        embed.add_field(name="📖 В ветке", value="`1` — занять роль · `-` — выписаться", inline=False)
    else:
        embed.add_field(name="🔒 Запись закрыта", value="Самозапись недоступна.", inline=False)

    if photo_url and str(photo_url).startswith(("http://", "https://", "attachment://")):
        embed.set_image(url=str(photo_url))
    return embed


def attachment_embed_url(filename: str) -> str:
    return f"attachment://{filename}"


async def resolve_photo_url(guild: discord.Guild, stored: Optional[str]) -> Optional[str]:
    """Достаёт актуальный HTTPS URL с сообщения-хранилища (att:channel:message)."""
    if not stored:
        return None
    if stored.startswith("att:"):
        try:
            _, ch_id, msg_id = stored.split(":", 2)
            ch = guild.get_channel(int(ch_id)) or guild.get_thread(int(ch_id))
            if ch is None:
                try:
                    ch = await guild.fetch_channel(int(ch_id))
                except discord.HTTPException:
                    ch = await bot.fetch_channel(int(ch_id))
            m = await ch.fetch_message(int(msg_id))
            if m.attachments:
                return m.attachments[0].url
            if m.embeds and m.embeds[0].image and m.embeds[0].image.url:
                return m.embeds[0].image.url
        except Exception as e:
            log.warning("resolve_photo_url %s: %r", stored, e)
        return None
    if stored.startswith(("http://", "https://")):
        return stored
    return None


async def refresh_main_post(guild: discord.Guild, content_row) -> None:
    try:
        if not content_row["message_id"]:
            return
        channel = guild.get_channel(int(content_row["channel_id"]))
        if channel is None:
            try:
                channel = await guild.fetch_channel(int(content_row["channel_id"]))
            except Exception:
                return
        if not isinstance(channel, discord.TextChannel):
            return
        msg = await channel.fetch_message(int(content_row["message_id"]))
        roster = await db_get_roster(int(content_row["id"]))
        attend = await db_attend_list_labeled(int(content_row["id"]))
        roster_uids = {uid for _, uid in roster}
        attendance_only = [(uid, lbl) for uid, lbl in attend if uid not in roster_uids]
        embed = build_main_post_embed(content_row, roster, attendance_only)

        edit_kwargs = {"embed": embed}
        if msg.attachments:
            # Файл на этом же сообщении: attachment:// + явно оставить аттачи,
            # иначе после edit картинка выпадает из эмбеда наверх.
            embed.set_image(url=attachment_embed_url(msg.attachments[0].filename))
            edit_kwargs["attachments"] = list(msg.attachments)
        else:
            stored = str(content_row["photo_url"]) if content_row["photo_url"] else None
            image_url = await resolve_photo_url(guild, stored)
            if image_url:
                embed.set_image(url=image_url)

        await msg.edit(**edit_kwargs)
    except Exception as e:
        log.warning("refresh_main_post content=%s: %r", content_row["id"], e)
        return


_REFRESH_DELAY = 0.4
_refresh_tasks: Dict[Tuple[int, int], asyncio.Task] = {}
_refresh_guard = asyncio.Lock()


async def schedule_refresh(guild: discord.Guild, content_id: int) -> None:
    """Схлопывает пачку записей в один edit поста."""
    key = (guild.id, content_id)

    async def _run():
        try:
            await asyncio.sleep(_REFRESH_DELAY)
            row = await db_get_content_by_id(content_id, guild.id)
            if row is not None:
                await refresh_main_post(guild, row)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("schedule_refresh content=%s: %r", content_id, e)

    async with _refresh_guard:
        prev = _refresh_tasks.get(key)
        if prev is not None and not prev.done():
            prev.cancel()
        _refresh_tasks[key] = asyncio.create_task(_run())


def parse_thread_command(text: str) -> Tuple[str, Optional[int]]:
    t = text.strip()
    if t == "" or t.lower() == "help":
        return "help", None
    if t == "-":
        return "self_leave", None
    if t.isdigit():
        return "self_join", int(t)
    # +2 @user — назначить слот по номеру
    m = re.match(r"^\+(\d+)\s+.+$", t)
    if m:
        return "org_assign_slot", int(m.group(1))
    # +метка @user — сразу после + идёт текст метки (не пробел)
    if re.match(r"^\+\S+.*<@!?\d+>", t) and not re.match(r"^\+\d+\s", t):
        return "org_assign_label", None
    if t.startswith("+"):
        return "org_attend_add", None
    m = re.match(r"^-(\d+)$", t)
    if m:
        return "org_kick_role", int(m.group(1))
    if t.startswith("-"):
        return "org_kick_user", None
    return "unknown", None


# =========================
# LEADERBOARD VIEW
# =========================
class LeaderboardView(discord.ui.View):
    def __init__(self, rows, guild, requester_id, my_count, money: bool = False,
                 total_sum: int = 0, subtitle: Optional[str] = None):
        super().__init__(timeout=300)
        self.rows = rows
        self.guild = guild
        self.requester_id = requester_id
        self.my_count = my_count
        self.money = money
        self.total_sum = total_sum
        self.subtitle = subtitle
        self.page = 0
        self.total_pages = max(1, (len(rows) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages - 1
        self.page_label.label = f"{self.page + 1} / {self.total_pages}"

    def build_embed(self) -> discord.Embed:
        if self.money:
            title = "💰 Лидерборд по балансу"
            header = (
                f"Общий баланс (к раздаче): **{fmt_money(self.total_sum)}**\n"
                f"Держателей: **{len(self.rows)}**"
            )
        else:
            title = "🏆 Лидерборд посещаемости (текущий месяц)"
            header = self.subtitle or ""
        embed = discord.Embed(title=title, color=COLOR_GOLD)
        start_idx = self.page * LEADERBOARD_PAGE_SIZE
        page_rows = self.rows[start_idx:start_idx + LEADERBOARD_PAGE_SIZE]
        if not page_rows:
            body = "_Пока пусто_"
        else:
            lines = []
            for i, (uid, val) in enumerate(page_rows, start=start_idx + 1):
                medal = MEDALS.get(i, "🎖️" if i <= 10 else "▫️")
                member = self.guild.get_member(uid)
                name = member.display_name if member else f"<@{uid}>"
                val_str = fmt_money(val) if self.money else str(val)
                lines.append(f"{medal} **{i}.** {name} **·** {val_str}")
            body = "\n".join(lines)
        embed.description = f"{header}\n\n{body}" if header else body
        my_str = fmt_money(self.my_count) if self.money else f"{self.my_count}"
        suffix = "Ваш баланс" if self.money else "Ваш результат за месяц"
        embed.set_footer(text=f"{suffix}: {my_str}  •  Стр. {self.page + 1}/{self.total_pages}")
        return embed

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction, button):
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True)
    async def page_label(self, interaction, button):
        await interaction.response.defer()

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction, button):
        self.page = min(self.total_pages - 1, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


# =========================
# BOT
# =========================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix=commands.when_mentioned_or("!"), intents=intents)
bot.remove_command("help")

def build_help_embed() -> discord.Embed:
    e = discord.Embed(title="📖 Команды бота", color=COLOR_BLUE)
    e.add_field(name="👥 Для всех", inline=False, value=(
        "`/balance` · `!bal [@user]` — баланс\n"
        "`/att_profile` · `!att [@user]` — посещаемость\n"
        "`/att_stats` · `!lb` — лидерборд посещаемости (месяц)\n"
        "`/economy_lb` · `!economy-lb` — лидерборд балансов + сумма к раздаче\n"
        "`/content_create` необязательный — любой участник\n"
        "`/energy <ник>` · `!energy <ник>` — энергия игрока\n"
        "`/energy_lb` · `!energy-lb` — лидерборд энергии\n"
        "`/help` · `!help` — это меню · `/healthcheck` · `!healthcheck` — статус"))
    e.add_field(name="🧵 В ветке", inline=False, value="`<цифра>` — занять роль · `-` — выписаться")
    e.add_field(name="🛡️ Стафф / Орг", inline=False, value=(
        "`/content_create` обязательный — стафф / RL\n"
        "`/content_close <id>` · `/content_list` · `!content-list`\n"
        "`/content_copy <id>` · `!copy <id>` — скопировать кол\n"
        "`/add_ppl` · `/att_add <id>` · `/att_remove <id> @user`\n"
        "`/att_export_csv` · `!att-export` — выгрузка лб посещаемости\n"
        "`/role_from_content <id>` · `/role_clear <id>`"))
    e.add_field(name="💰 Деньги (стафф)", inline=False, value=(
        "`/money_add` · `!add-money @user сумма`\n"
        "`/money_sub` · `!remove-money @user сумма`\n"
        "`/money_payout_role` · `!add-money-role @role сумма`\n"
        "`/money_payout_content <id> сумма [force]`\n"
        "`/economy_stats` · `!economy-stats` — общая стата\n"
        "`/economy_export_csv` · `!economy-export [@user]` — лб + все +/-\n"
        "`/energy_import` — залить txt лог энергии"))
    e.add_field(name="🧵 В ветке (стафф)", inline=False, value=(
        "`+хилл @user` · `+2 @user` · `-2` · `- @user` · `+ @user...`"))
    return e

@bot.tree.command(name="help", description="Список всех команд бота")
async def help_slash(interaction: discord.Interaction):
    await interaction.response.send_message(embed=build_help_embed(), ephemeral=True)

@bot.command(name="help")
async def help_prefix(ctx: commands.Context):
    await ctx.reply(embed=build_help_embed(), mention_author=False)

_STARTED = False


@bot.event
async def on_ready():
    global _STARTED
    if not _STARTED:
        await db_init()
        try:
            if GUILD_ID:
                gobj = discord.Object(id=GUILD_ID)
                bot.tree.copy_global_to(guild=gobj)
                synced = await bot.tree.sync(guild=gobj)
            else:
                synced = await bot.tree.sync()
            log.info("Synced commands: %s", len(synced))
        except Exception as e:
            log.error("Sync error: %s", e)
        _STARTED = True
    log.info("Logged in as %s (id=%s)", bot.user, bot.user.id)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f"Не хватает аргумента: `{error.param.name}`.", mention_author=False)
        return
    if isinstance(error, (commands.BadArgument, commands.MemberNotFound,
                          commands.RoleNotFound, commands.UserNotFound,
                          commands.BadUnionArgument)):
        await ctx.reply("Не удалось разобрать аргумент. Проверь упоминание и число.", mention_author=False)
        return
    log.error("command_error %s: %r", ctx.command, error)
    try:
        await ctx.reply("Ошибка при выполнении команды.", mention_author=False)
    except Exception:
        pass


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    err = error.original if isinstance(error, app_commands.CommandInvokeError) else error
    log.error("app_command_error %s: %r", interaction.command, err)
    if isinstance(error, app_commands.CommandOnCooldown):
        msg = f"Кулдаун: {error.retry_after:.1f}с."
    elif isinstance(error, app_commands.MissingPermissions):
        msg = "Недостаточно прав."
    else:
        msg = "Ошибка при выполнении команды."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass


# =========================
# MODALS
# =========================
class ContentCreateModal(discord.ui.Modal, title="Создать контент"):
    title_text = discord.ui.TextInput(label="Заголовок", placeholder="Например: пути неисповедимы", max_length=150)
    roles_text = discord.ui.TextInput(
        label="Роли (каждая строка = слот)",
        placeholder="Танк\nХил\nДД\nДД\nДД\nСтоп",
        style=discord.TextStyle.paragraph, max_length=1500
    )
    start_time_input = discord.ui.TextInput(
        label="Время старта в UTC (ЧЧ:ММ)", placeholder="Например: 18:35", required=True, max_length=5
    )
    builds_link = discord.ui.TextInput(
        label="Ссылка на билды (опционально)", placeholder="https://...", required=False, max_length=400
    )
    after_text = discord.ui.TextInput(
        label="Примечание (опционально)", placeholder="Например: /join NickName", required=False, max_length=900
    )

    def __init__(self, content_type: str, photo: Optional[discord.Attachment], auto_assign_organizer: bool = False):
        super().__init__()
        self.content_type = content_type
        self.photo = photo
        self.auto_assign_organizer = auto_assign_organizer

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        member = get_member_safe(interaction)
        if member is None or not member_can_create_content(member, self.content_type):
            await interaction.followup.send(
                "Недостаточно прав. Обязательный контент создают стафф или RL.",
                ephemeral=True,
            )
            return

        roles_lines = normalize_roles_lines(str(self.roles_text))
        if not roles_lines:
            await interaction.followup.send("Нужно указать хотя бы одну роль.", ephemeral=True)
            return

        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.followup.send("Команду нужно запускать в текстовом канале.", ephemeral=True)
            return

        start_ts = parse_utc_time(str(self.start_time_input).strip())
        if start_ts is None:
            await interaction.followup.send("❌ Неверный формат времени. Используй ЧЧ:ММ, например `18:35`.", ephemeral=True)
            return

        title = str(self.title_text).strip()
        after = str(self.after_text).strip() or None
        link = str(self.builds_link).strip() or None
        hosted_by = f"<@{interaction.user.id}>"

        guild_id = int(interaction.guild_id)
        content_id = await db_create_content(
            guild_id=guild_id, channel_id=channel.id, title=title,
            roles_text="\n".join(roles_lines), after_text=after, created_by=interaction.user.id,
            content_type=self.content_type, hosted_by=hosted_by, start_ts=start_ts, builds_link=link,
        )

        photo_file = None
        photo_url = None
        if self.photo is not None:
            try:
                photo_file = await self.photo.to_file()
                photo_url = attachment_embed_url(photo_file.filename)
            except Exception:
                photo_file = None
                photo_url = None

        row0 = await db_get_content_by_id(content_id, guild_id)
        embed0 = build_main_post_embed(row0, [], []) if row0 else discord.Embed(color=COLOR_GREEN)
        if photo_url:
            embed0.set_image(url=photo_url)

        send_kwargs = {
            "content": "@everyone",
            "embed": embed0,
            "allowed_mentions": discord.AllowedMentions(everyone=True),
        }
        if photo_file is not None:
            send_kwargs["file"] = photo_file
        try:
            msg = await channel.send(**send_kwargs)
        except Exception as e:
            await db_delete_content(content_id, guild_id)
            log.error("content_create send failed CS#%s: %r", content_id, e)
            await interaction.followup.send(
                f"Не удалось отправить пост, контент не создан. ({e})", ephemeral=True)
            return
        message_id = msg.id
        if msg.attachments:
            photo_url = attachment_embed_url(msg.attachments[0].filename)

        thread_id = None
        thread_note = ""
        thread = None
        tname = thread_name_for(title, content_id)
        for dur in (10080, 4320, 1440):
            try:
                thread = await msg.create_thread(name=tname, auto_archive_duration=dur)
                break
            except (discord.Forbidden, discord.HTTPException) as e:
                log.warning("content_create thread dur=%s CS#%s: %r", dur, content_id, e)
                thread = None
        if thread is None:
            thread_note = "\n⚠️ Тред не создан — запись через ветку недоступна."
        else:
            thread_id = thread.id
            try:
                await thread.send(embed=discord.Embed(
                    title="📖 Инструкция",
                    description=(
                        "**Как записаться:**\n"
                        "➣ Напишите **цифру** чтобы занять роль\n"
                        "➣ Напишите `-` чтобы выписаться\n\n"
                        "Если запись закрыта — обратитесь к организатору."
                    ),
                    color=COLOR_BLUE
                ))
            except discord.HTTPException as e:
                log.warning("content_create thread intro CS#%s: %r", content_id, e)

        await db_set_message_thread(content_id, guild_id, message_id, thread_id, photo_url)

        if self.auto_assign_organizer and roles_lines:
            await db_assign_user(content_id, interaction.user.id, 1)

        row = await db_get_content_by_id(content_id, guild_id)
        if row and interaction.guild:
            await refresh_main_post(interaction.guild, row)

        await interaction.followup.send(
            f"✅ Контент создан: **CS#{content_id}**{thread_note}", ephemeral=True)


class AttendAddModal(discord.ui.Modal, title="Добавить людей"):
    def __init__(self, default_content_id: Optional[int] = None):
        super().__init__()
        self.content_id_input = discord.ui.TextInput(
            label="Content ID", placeholder="Например: 12", required=True, max_length=10,
            default=str(default_content_id) if default_content_id is not None else None
        )
        self.users_input = discord.ui.TextInput(
            label="Пользователи", placeholder="@user1 @user2 @user3",
            style=discord.TextStyle.paragraph, max_length=2000
        )
        self.add_item(self.content_id_input)
        self.add_item(self.users_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Guild недоступен.", ephemeral=True)
            return
        try:
            content_id = int(str(self.content_id_input).strip())
        except ValueError:
            await interaction.followup.send("Content ID должен быть числом.", ephemeral=True)
            return
        row = await db_get_content_by_id(content_id, int(interaction.guild_id))
        if row is None:
            await interaction.followup.send("Контент не найден.", ephemeral=True)
            return
        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True)
            return
        if not is_organizer_or_admin(member, int(row["created_by"])):
            await interaction.followup.send("Недостаточно прав.", ephemeral=True)
            return
        user_ids = parse_user_ids_from_text(str(self.users_input))
        if not user_ids:
            await interaction.followup.send("Нет упоминаний. Укажите пользователей через @.", ephemeral=True)
            return

        added, already = await db_attend_add_many(content_id, user_ids, interaction.user.id)

        role = interaction.guild.get_role(int(row["payout_role_id"])) if row["payout_role_id"] else None
        assigned = failed = 0
        if role is not None:
            for uid in user_ids:
                m = interaction.guild.get_member(uid)
                if m is None:
                    try:
                        m = await interaction.guild.fetch_member(uid)
                    except Exception:
                        failed += 1; continue
                try:
                    await m.add_roles(role, reason=f"CS add_ppl content {content_id}")
                    assigned += 1
                except Exception:
                    failed += 1

        if interaction.guild:
            await schedule_refresh(interaction.guild, content_id)

        embed = discord.Embed(title="✅ Участники добавлены", color=COLOR_GREEN)
        embed.add_field(name="Добавлено", value=str(added), inline=True)
        embed.add_field(name="Уже было", value=str(already), inline=True)
        if role is not None:
            embed.add_field(name="Роль выдана", value=str(assigned), inline=True)
            if failed:
                embed.add_field(name="Ошибок", value=str(failed), inline=True)
        embed.set_footer(text="Добавлено в ростер. Стату начисляет /att_add")
        await interaction.followup.send(embed=embed, ephemeral=True)


# =========================
# THREAD SIGNUP
# =========================
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None or not isinstance(message.channel, discord.Thread):
        await bot.process_commands(message)
        return

    content = await db_get_content_by_thread(message.channel.id, message.guild.id)
    if content is None:
        await bot.process_commands(message)
        return

    cmd = message.content.strip()
    if not (cmd.isdigit() or cmd in ("-", "help") or cmd.startswith("+") or cmd.startswith("-")):
        await bot.process_commands(message)
        return

    if message.channel.archived:
        try:
            await message.channel.edit(archived=False, reason="CS signup")
        except Exception:
            pass

    content_id = int(content["id"])
    status = str(content["status"])
    guild_id = int(content["guild_id"])

    member = message.guild.get_member(message.author.id)
    if member is None:
        try:
            member = await message.guild.fetch_member(message.author.id)
        except Exception:
            member = None
    is_org = isinstance(member, discord.Member) and is_organizer_or_admin(member, int(content["created_by"]))

    kind, num = parse_thread_command(cmd)

    if kind == "self_leave":
        if status != STATUS_OPEN:
            await _try_delete_command(message)
            try:
                await message.channel.send("🔒 Запись закрыта.", delete_after=5)
            except Exception:
                pass
            return
        removed_slot = await db_unassign_user(content_id, message.author.id)
        removed_att = await db_attend_remove(content_id, message.author.id)
        await message.add_reaction("✅" if (removed_slot or removed_att) else "ℹ️")
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    if kind == "help":
        await message.reply(embed=discord.Embed(
            title="📖 Команды в ветке", color=COLOR_BLUE,
            description=(
                "`<цифра>` — занять роль с указанным номером\n"
                "`-` — выписаться со своей роли\n\n"
                "**Стафф:**\n"
                "`+хилл @user` — добавить с меткой роли\n"
                "`+2 @user` — поставить @user на слот №2\n"
                "`-2` — освободить слот №2\n"
                "`- @user` — выписать @user\n"
                "`+ @user1 @user2` — добавить как доп. участников\n\n"
                "Если самозапись закрыта — обратитесь к организатору."
            )
        ), mention_author=False)
        return

    roles_lines = normalize_roles_lines(str(content["roles_text"]))
    max_slot = len(roles_lines)

    # Самозапись — только когда открыт, для всех (вкл. орга: орг ставит через +N)
    if kind == "self_join":
        if status != STATUS_OPEN:
            await _try_delete_command(message)
            try:
                await message.channel.send("🔒 Запись закрыта.", delete_after=5)
            except Exception:
                pass
            return
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False); return
        ok, txt = await db_assign_user(content_id, message.author.id, int(num))
        await message.add_reaction("✅" if ok else "⛔")
        if not ok:
            await message.reply(txt, mention_author=False)
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    # Дальше — только управляющие команды организатора (работают и при закрытом)
    if not is_org:
        await _try_delete_command(message)
        return

    if kind == "org_attend_add":
        user_ids = [m.id for m in message.mentions]
        if not user_ids:
            await message.reply("Формат: `+ @user @user ...`", mention_author=False); return
        added, already = await db_attend_add_many(content_id, user_ids, message.author.id)
        await message.add_reaction("✅")
        await message.reply(f"Добавлено: {added}. Уже были: {already}.", mention_author=False)
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    if kind == "org_assign_slot":
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False); return
        if not message.mentions:
            await message.reply("Формат: `+2 @user`", mention_author=False); return
        target = message.mentions[0]
        ok, txt = await db_assign_user(content_id, target.id, int(num))
        await message.reply(f"{target.mention}: {txt}", mention_author=False)
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    if kind == "org_assign_label":
        if not message.mentions:
            await message.reply("Формат: `+хилл @user`", mention_author=False); return
        label_match = re.match(r"^\+(.+?)\s+<@", cmd)
        label = label_match.group(1).strip() if label_match else "доп. роль"
        target = message.mentions[0]
        added, already = await db_attend_add_many(content_id, [target.id], message.author.id, label=label)
        status_txt = "метка обновлена" if already else "добавлен"
        await message.reply(f"{target.mention} — **{label}** ({status_txt})", mention_author=False)
        await message.add_reaction("\u2705")
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    if kind == "org_kick_role":
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False); return
        kicked = await db_unassign_by_role_index(content_id, int(num))
        await message.reply("Роль свободна." if kicked is None else f"Выписано с роли {num}: <@{kicked}>", mention_author=False)
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return

    if kind == "org_kick_user":
        if not message.mentions:
            await message.reply("Формат: `- @user`", mention_author=False); return
        target = message.mentions[0]
        removed_slot = await db_unassign_user(content_id, target.id)
        removed_att = await db_attend_remove(content_id, target.id)
        removed_award, _ = await db_att_remove_for_content(
            guild_id, content_id, target.id, message.author.id)
        gone = removed_slot or removed_att or removed_award
        await message.reply("Выписан." if gone else "Пользователь не записан.", mention_author=False)
        await schedule_refresh(message.guild, content_id)
        await _try_delete_command(message)
        return


async def _try_delete_command(message: discord.Message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


# =========================
# SLASH COMMANDS
# =========================
TYPE_CHOICES = [
    app_commands.Choice(name="Обязательный", value=TYPE_MANDATORY),
    app_commands.Choice(name="Необязательный", value=TYPE_OPTIONAL),
]
SCOPE_CHOICES = [
    app_commands.Choice(name="За неделю", value="week"),
    app_commands.Choice(name="Текущий месяц", value="month"),
    app_commands.Choice(name="Прошлый месяц", value="prev_month"),
    app_commands.Choice(name="Последние 3 месяца", value="3months"),
    app_commands.Choice(name="Всё время", value="all"),
]
SCOPE_LABELS = {"week": "за неделю", "month": "за текущий месяц",
                "prev_month": "за прошлый месяц", "3months": "за последние 3 месяца",
                "all": "за всё время"}


def _health_embed() -> discord.Embed:
    return discord.Embed(title="✅ Статус бота", description="Бот онлайн, БД доступна.", color=COLOR_GREEN)


async def _db_ping() -> None:
    if DB is None:
        raise RuntimeError("БД не инициализирована")
    await DB.execute("SELECT 1")


@bot.tree.command(name="healthcheck", description="Проверка статуса бота и базы данных")
async def healthcheck(interaction: discord.Interaction):
    try:
        await _db_ping()
        await interaction.response.send_message(embed=_health_embed(), ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(
            embed=discord.Embed(title="❌ Ошибка", description=str(e), color=COLOR_RED), ephemeral=True)


@bot.command(name="healthcheck")
async def healthcheck_prefix(ctx: commands.Context):
    try:
        await _db_ping()
        await ctx.reply(embed=_health_embed(), mention_author=False)
    except Exception as e:
        await ctx.reply(embed=discord.Embed(title="❌ Ошибка", description=str(e), color=COLOR_RED),
                        mention_author=False)


@bot.tree.command(name="content_create", description="Создать контент. Необязательный — все, обязательный — стафф/RL")
@app_commands.describe(type="Тип контента", photo="Картинка к посту (опционально)")
@app_commands.choices(type=TYPE_CHOICES)
async def content_create(
    interaction: discord.Interaction,
    type: app_commands.Choice[str],
    photo: Optional[discord.Attachment] = None,
):
    if photo is not None and not (photo.content_type or "").startswith("image/"):
        await interaction.response.send_message("Вложение должно быть картинкой.", ephemeral=True)
        return
    member = get_member_safe(interaction)
    if member is None:
        await interaction.response.send_message("Команду нужно запускать на сервере.", ephemeral=True)
        return
    if not member_can_create_content(member, type.value):
        await interaction.response.send_message(
            "Обязательный контент могут создавать только стафф и RL.",
            ephemeral=True,
        )
        return
    await interaction.response.send_modal(ContentCreateModal(content_type=type.value, photo=photo))


@bot.tree.command(name="add_ppl", description="Добавить людей в контент")
async def attend_add(interaction: discord.Interaction):
    default_content_id = None
    if isinstance(interaction.channel, discord.Thread) and interaction.guild_id:
        row = await db_get_content_by_thread(interaction.channel.id, int(interaction.guild_id))
        if row is not None:
            default_content_id = int(row["id"])
    await interaction.response.send_modal(AttendAddModal(default_content_id=default_content_id))


@bot.tree.command(name="content_close", description="Закрыть запись на контент")
@app_commands.describe(content_id="ID контента")
async def content_close(interaction: discord.Interaction, content_id: int):
    if interaction.guild_id is None:
        await interaction.response.send_message("Guild недоступен.", ephemeral=True); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.response.send_message("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.response.send_message("Недостаточно прав.", ephemeral=True); return
    await db_close_content(content_id, int(interaction.guild_id))
    row2 = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row2 and interaction.guild:
        await refresh_main_post(interaction.guild, row2)
    await interaction.response.send_message(
        embed=discord.Embed(title="🔴 Контент закрыт", description=f"Контент **#{content_id}** закрыт.", color=COLOR_RED),
        ephemeral=True)


STATUS_LIST_CHOICES = [
    app_commands.Choice(name="Открытые", value=STATUS_OPEN),
    app_commands.Choice(name="Закрытые", value=STATUS_CLOSED),
    app_commands.Choice(name="Все", value="all"),
]


async def build_content_list_embed(guild: discord.Guild, status: str) -> discord.Embed:
    rows = await db_list_contents(guild.id, None if status == "all" else status, limit=25)
    title_map = {
        STATUS_OPEN: "📋 Открытый контент",
        STATUS_CLOSED: "📋 Закрытый контент",
        "all": "📋 Контент сервера",
    }
    embed = discord.Embed(title=title_map.get(status, "📋 Контент"), color=COLOR_BLUE)
    if not rows:
        embed.description = "_Пусто_"
        return embed
    lines = []
    for r in rows:
        cid = int(r["id"])
        st = "🟢" if str(r["status"]) == STATUS_OPEN else "🔴"
        start_ts = int(r["start_ts"]) if r["start_ts"] else None
        when = f" · {ts_discord(start_ts, 'R')}" if start_ts else ""
        lines.append(f"{st} **#{cid}** {r['title']}{when}")
    embed.description = "\n".join(lines)
    embed.set_footer(text="Последние 25. Id для /content_close /att_add /copy")
    return embed


@bot.tree.command(name="content_list", description="Список контентов этого сервера")
@app_commands.describe(status="Фильтр по статусу")
@app_commands.choices(status=STATUS_LIST_CHOICES)
async def content_list(interaction: discord.Interaction, status: str = STATUS_OPEN):
    await interaction.response.defer(ephemeral=True)
    member = get_member_safe(interaction)
    if member is None or not member_can_create_content(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    await interaction.followup.send(
        embed=await build_content_list_embed(interaction.guild, status), ephemeral=True)


@bot.command(name="content-list")
async def prefix_content_list(ctx: commands.Context, status: str = "open"):
    """!content-list [open|closed|all] — список контентов (стафф/RL)."""
    if ctx.guild is None:
        return
    if not (isinstance(ctx.author, discord.Member) and member_can_create_content(ctx.author)):
        await ctx.reply("❌ Только стафф или RL.", mention_author=False); return
    st = (status or "open").lower()
    if st not in (STATUS_OPEN, STATUS_CLOSED, "all"):
        await ctx.reply("Статус: `open`, `closed`, `all`.", mention_author=False); return
    await ctx.reply(embed=await build_content_list_embed(ctx.guild, st), mention_author=False)


@bot.tree.command(name="role_from_content", description="Создать роль и выдать всем участникам")
@app_commands.describe(content_id="ID контента", role_name="Название роли (по умолчанию = заголовок)")
async def role_from_content(interaction: discord.Interaction, content_id: int, role_name: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден."); return
    member = guild.get_member(interaction.user.id)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав."); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников."); return

    old_role = guild.get_role(int(row["payout_role_id"])) if row["payout_role_id"] else None
    if old_role is None and row["payout_role_name"]:
        old_role = discord.utils.get(guild.roles, name=str(row["payout_role_name"]))

    base_name = role_name.strip() if role_name and role_name.strip() else str(row["title"])
    final_role_name = f"{base_name} [CS#{content_id}]"
    try:
        role = await guild.create_role(name=final_role_name, reason=f"CS payout role for content {content_id}")
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на создание роли."); return
    if old_role is not None and old_role.id != role.id:
        try:
            await old_role.delete(reason=f"CS replace payout role for content {content_id}")
        except discord.HTTPException:
            pass

    failed, assigned = [], 0
    for uid in user_ids:
        member = guild.get_member(uid)
        if member is None:
            try:
                member = await guild.fetch_member(uid)
            except Exception:
                failed.append(uid); continue
        try:
            await member.add_roles(role, reason=f"CS content {content_id} payout")
            assigned += 1
        except discord.Forbidden:
            failed.append(uid)

    await db_set_payout_role(content_id, int(interaction.guild_id), role.id, role.name)
    embed = discord.Embed(title="✅ Роль создана", color=COLOR_GREEN)
    embed.add_field(name="Роль", value=f"`{role.name}`", inline=False)
    embed.add_field(name="Выдано", value=f"{assigned}/{len(user_ids)}", inline=True)
    if failed:
        embed.add_field(name="Не удалось", value=", ".join(f"<@{u}>" for u in failed[:10]), inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="role_clear", description="Удалить payout роль контента")
@app_commands.describe(content_id="ID контента")
async def role_clear(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден."); return
    member = guild.get_member(interaction.user.id)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав."); return
    role = guild.get_role(int(row["payout_role_id"])) if row["payout_role_id"] else None
    if role is None and row["payout_role_name"]:
        role = discord.utils.get(guild.roles, name=str(row["payout_role_name"]))
    if role is None:
        await interaction.followup.send("Роль не найдена."); return
    try:
        await role.delete(reason=f"CS payout role cleanup for content {content_id}")
        await db_clear_payout_role(content_id, int(interaction.guild_id))
        await interaction.followup.send(embed=discord.Embed(title="🗑️ Роль удалена", description=f"`{role.name}`", color=COLOR_RED))
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на удаление роли.")


@bot.tree.command(name="att_add", description="Начислить аттенданс за контент всем участникам")
@app_commands.describe(content_id="ID контента")
async def att_add(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True); return
    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.", ephemeral=True); return

    awarded, already = await db_att_award_for_content(
        guild_id=int(row["guild_id"]), content_id=int(content_id), user_ids=user_ids,
        awarded_by=int(interaction.user.id),
    )
    embed = discord.Embed(title="✅ Аттенданс начислен", color=COLOR_GREEN)
    embed.add_field(name="Начислено", value=str(awarded), inline=True)
    embed.add_field(name="Уже было", value=str(already), inline=True)
    embed.add_field(name="Тип", value=type_label(str(row["content_type"])), inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="att_profile", description="Карточка посещаемости (% обяз/необяз)")
@app_commands.describe(user="Пользователь (по умолчанию — ты)", scope="Период")
@app_commands.choices(scope=SCOPE_CHOICES)
async def att_profile(interaction: discord.Interaction, user: Optional[discord.Member] = None, scope: str = "month"):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    target = user or interaction.user
    data = await db_att_profile(int(interaction.guild_id), int(target.id), scope)
    if data is None:
        await interaction.followup.send(f"{target.mention} ещё ни разу не аттендил."); return

    joined_str = datetime.datetime.fromtimestamp(data["joined_at"], tz=datetime.timezone.utc).strftime("%Y-%m-%d")
    m, o = data[TYPE_MANDATORY], data[TYPE_OPTIONAL]
    embed = discord.Embed(title=f"Посещаемость · {getattr(target, 'display_name', target.name)}",
                          description=f"Период: **{SCOPE_LABELS.get(scope, scope)}**\nВ сборах с: `{joined_str}`",
                          color=COLOR_BLUE)
    embed.add_field(
        name="🔴 Обязательные",
        value=f"Количество: {m['total']}\nПосещено: {m['attended']}\nПроцент: {m['pct']} %",
        inline=True,
    )
    embed.add_field(
        name="🟢 Не обязательные",
        value=f"Количество: {o['total']}\nПосещено: {o['attended']}\nПроцент: {o['pct']} %",
        inline=True,
    )
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="att_stats", description="Лидерборд посещаемости (текущий месяц)")
async def att_stats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild_id is None or interaction.guild is None:
        await interaction.followup.send("Guild недоступен."); return
    rows = await db_att_leaderboard_month(int(interaction.guild_id))
    my_count = next((cnt for uid, cnt in rows if uid == interaction.user.id), 0)
    view = LeaderboardView(rows=rows, guild=interaction.guild, requester_id=interaction.user.id, my_count=my_count)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="att_remove", description="Удалить аттенданс за конкретный контент у пользователя")
@app_commands.describe(content_id="ID контента", user="Пользователь")
async def att_remove(interaction: discord.Interaction, content_id: int, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True); return
    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    ok, msg_text = await db_att_remove_for_content(
        guild_id=int(row["guild_id"]), content_id=int(content_id),
        user_id=int(user.id), removed_by=int(interaction.user.id),
    )
    await interaction.followup.send(
        embed=discord.Embed(description=msg_text, color=COLOR_GREEN if ok else COLOR_RED), ephemeral=True)


async def build_att_export(guild: discord.Guild, scope: str) -> Tuple[Optional[str], Optional[discord.Embed], Optional[discord.File]]:
    """CSV лидерборда посещаемости + % обяз/необяз. (error, embed, file)."""
    guild_id = guild.id
    lb = await db_att_leaderboard(guild_id, scope)
    count_by_uid = {uid: cnt for uid, cnt in lb}

    cur = await DB.execute("SELECT user_id FROM attendance_join WHERE guild_id = ?", (guild_id,))
    user_ids = [int(r[0]) for r in await cur.fetchall()]
    if not user_ids:
        return "Нет данных для экспорта.", None, None

    rows_data = []
    for uid in user_ids:
        data = await db_att_profile(guild_id, uid, scope)
        if data is None:
            continue
        m, o = data[TYPE_MANDATORY], data[TYPE_OPTIONAL]
        gm = guild.get_member(uid)
        visits = count_by_uid.get(uid, 0)
        rows_data.append((visits, m["pct"], [
            uid,
            gm.name if gm else "Неизвестно",
            gm.display_name if gm else f"ID:{uid}",
            datetime.datetime.fromtimestamp(data["joined_at"], tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
            visits,
            m["total"], m["attended"], m["pct"],
            o["total"], o["attended"], o["pct"],
        ]))
    rows_data.sort(key=lambda x: (x[0], x[1]), reverse=True)

    csv_rows = []
    rank = 0
    for visits, _pct, r in rows_data:
        if visits > 0:
            rank += 1
            rank_val: object = rank
        else:
            rank_val = ""
        csv_rows.append([rank_val] + r)

    header = [
        "Место", "User ID", "Никнейм", "Отображаемое имя", "В сборах с",
        "Посещений (период)",
        "Обяз всего", "Обяз посещено", "Обяз %",
        "Необяз всего", "Необяз посещено", "Необяз %",
    ]
    file = make_csv_file(f"attendance_lb_{scope}_{int(time.time())}.csv", header, csv_rows)
    in_lb = sum(1 for visits, _, _ in rows_data if visits > 0)
    embed = discord.Embed(
        title="📥 Выгрузка лидерборда посещаемости",
        description=(
            f"Период: **{SCOPE_LABELS.get(scope, scope)}**\n"
            f"В лидерборде: **{in_lb}** · Всего в сборах: **{len(rows_data)}**"
        ),
        color=COLOR_BLUE,
    )
    return None, embed, file


@bot.tree.command(name="att_export_csv", description="Выгрузить лидерборд посещаемости в CSV (стафф)")
@app_commands.describe(scope="Период")
@app_commands.choices(scope=SCOPE_CHOICES)
async def att_export_csv(interaction: discord.Interaction, scope: str = "month"):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return

    err, embed, file = await build_att_export(interaction.guild, scope)
    if err:
        await interaction.followup.send(err, ephemeral=True); return
    await interaction.followup.send(embed=embed, file=file, ephemeral=True)


@bot.command(name="att-export")
async def prefix_att_export(ctx: commands.Context, scope: str = "month"):
    """!att-export [month|week|prev_month|3months|all] — выгрузка лб посещаемости (стафф)."""
    if ctx.guild is None:
        return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("❌ Только стафф.", mention_author=False); return
    if scope not in SCOPE_LABELS:
        await ctx.reply("Период: `week`, `month`, `prev_month`, `3months`, `all`.", mention_author=False); return
    err, embed, file = await build_att_export(ctx.guild, scope)
    if err:
        await ctx.reply(err, mention_author=False); return
    await ctx.reply(embed=embed, file=file, mention_author=False)


# =========================
# BALANCE COMMANDS
# =========================


@bot.command(name="bal")
async def bal_prefix(ctx: commands.Context, member: Optional[discord.Member] = None):
    """!bal — свой баланс; !bal @user — чужой (только стафф)."""
    if ctx.guild is None:
        return
    target = member or ctx.author
    if member is not None and member.id != ctx.author.id:
        if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
            await ctx.reply("Чужой баланс может смотреть только стафф.", mention_author=False)
            return
    bal = await db_balance_get(ctx.guild.id, target.id)
    embed = discord.Embed(color=COLOR_GOLD)
    embed.add_field(name=f"\U0001fa99 Баланс · {target.display_name}", value=f"**{fmt_money(bal)}**", inline=False)
    await ctx.reply(embed=embed, mention_author=False)


@bot.tree.command(name="balance", description="Показать баланс")
@app_commands.describe(user="Пользователь (по умолчанию — ты)")
async def balance(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    if interaction.guild_id is None:
        await interaction.response.send_message("Guild недоступен.", ephemeral=True); return
    target = user or interaction.user
    ephemeral = True
    if user is not None and user.id != interaction.user.id:
        member = get_member_safe(interaction)
        if member is None or not member_is_staff(member):
            await interaction.response.send_message("Чужой баланс может смотреть только стафф.", ephemeral=True)
            return
        ephemeral = False
    bal = await db_balance_get(int(interaction.guild_id), int(target.id))
    embed = discord.Embed(color=COLOR_GOLD)
    embed.add_field(name=f"\U0001fa99 Баланс · {target.display_name}", value=f"**{fmt_money(bal)}**", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=ephemeral)


@bot.tree.command(name="money_add", description="Начислить деньги пользователю (стафф)")
@app_commands.describe(user="Кому", amount="Сколько (>0)", reason="Причина (опционально)")
async def money_add(interaction: discord.Interaction, user: discord.Member, amount: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=False)
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if amount <= 0:
        await interaction.followup.send("Сумма должна быть > 0.", ephemeral=True); return
    new, _ = await db_balance_apply(int(interaction.guild_id), int(user.id), amount,
                                    "add", reason, int(interaction.user.id))
    embed = discord.Embed(title="✅ Начислено", color=COLOR_GREEN,
                          description=f"{user.mention}: +{fmt_money(amount)}\nНовый баланс: **{fmt_money(new)}**")
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="money_sub", description="Снять деньги у пользователя (стафф, минимум 0)")
@app_commands.describe(user="У кого", amount="Сколько (>0)", reason="Причина (опционально)")
async def money_sub(interaction: discord.Interaction, user: discord.Member, amount: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=False)
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if amount <= 0:
        await interaction.followup.send("Сумма должна быть > 0.", ephemeral=True); return
    new, applied = await db_balance_apply(int(interaction.guild_id), int(user.id), -amount,
                                          "sub", reason, int(interaction.user.id))
    removed = -applied  # сколько реально сняли (с учётом клампа в 0)
    embed = discord.Embed(title="✅ Снято", color=COLOR_YELLOW,
                          description=f"{user.mention}: -{fmt_money(removed)}\nНовый баланс: **{fmt_money(new)}**")
    if removed < amount:
        embed.add_field(name="⚠️", value=f"Запрошено {fmt_money(amount)}, но баланс ушёл в 0.", inline=False)
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="money_payout_role", description="Начислить деньги всем с ролью (стафф)")
@app_commands.describe(role="Роль", amount="Сколько каждому (>0)", reason="Причина (опционально)")
async def money_payout_role(interaction: discord.Interaction, role: discord.Role, amount: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=False)
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if amount <= 0:
        await interaction.followup.send("Сумма должна быть > 0.", ephemeral=True); return
    targets = [m for m in role.members if not m.bot]
    if not targets:
        await interaction.followup.send("У роли нет участников.", ephemeral=True); return
    await db_balance_payout_many(int(interaction.guild_id), [int(m.id) for m in targets], amount,
                                 "payout_role", reason or f"role:{role.name}", int(interaction.user.id))
    embed = discord.Embed(title="✅ Массовое начисление", color=COLOR_GREEN,
                          description=f"Роль {role.mention}: +{fmt_money(amount)} каждому\nПолучателей: **{len(targets)}**")
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="money_payout_content", description="Начислить деньги всем участникам контента (стафф)")
@app_commands.describe(content_id="ID контента", amount="Сколько каждому (>0)",
                       reason="Причина (опц.)", force="Повторно выплатить уже оплаченным")
async def money_payout_content(interaction: discord.Interaction, content_id: int, amount: int,
                               reason: Optional[str] = None, force: bool = False):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав. Выплата — только стафф.", ephemeral=True); return
    if amount <= 0:
        await interaction.followup.send("Сумма должна быть > 0.", ephemeral=True); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.", ephemeral=True); return
    paid, skipped = await db_payout_content_once(
        int(row["guild_id"]), int(content_id), [int(u) for u in user_ids],
        amount, int(interaction.user.id), force=force, reason=reason)
    embed = discord.Embed(title="✅ Выплата за контент", color=COLOR_GREEN,
        description=f"Контент **#{content_id}**: +{fmt_money(amount)} каждому\n"
                    f"Выплачено: **{paid}** · Пропущено (уже было): **{skipped}**")
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="content_copy", description="Скопировать кол: полное (с тегами) или упрощённое (структура)")
@app_commands.describe(
    content_id="ID контента",
    mode="full = с тегами участников, simple = только структура"
)
@app_commands.choices(mode=[
    app_commands.Choice(name="Полное (с тегами участников)", value="full"),
    app_commands.Choice(name="Упрощённое (структура без людей)", value="simple"),
])
async def content_copy(interaction: discord.Interaction, content_id: int, mode: str = "full"):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    row = await db_get_content_by_id(content_id, int(interaction.guild_id))
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return

    title = str(row["title"])
    roles_lines = normalize_roles_lines(str(row["roles_text"]))
    start_ts = int(row["start_ts"]) if row["start_ts"] else None

    lines = [f"**{title}**"]
    if start_ts:
        utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
        lines.append(f"⏰ {utc_str} • {ts_discord(start_ts, 'F')}")
    lines.append("")

    if mode == "full":
        roster = await db_get_roster(content_id)
        by_index = {idx: uid for idx, uid in roster}
        for i, rn in enumerate(roles_lines, start=1):
            uid = by_index.get(i)
            lines.append(f"`{i}.` {rn} — <@{uid}>" if uid else f"`{i}.` {rn} —")
    else:
        for i, rn in enumerate(roles_lines, start=1):
            lines.append(f"`{i}.` {rn}")

    text = "\n".join(lines)
    if len(text) <= 1900:
        await interaction.followup.send(content=text, ephemeral=True)
    else:
        buf = io.BytesIO(text.encode("utf-8"))
        await interaction.followup.send(content=f"Копия контента **#{content_id}**:",
                                        file=discord.File(buf, filename=f"content_{content_id}.txt"),
                                        ephemeral=True)
        
@bot.command(name="copy")
async def prefix_copy(ctx: commands.Context, content_id: int, mode: str = "full"):
    """!copy <id> [full|simple] — скопировать кол для вставки."""
    if ctx.guild is None: return
    row = await db_get_content_by_id(content_id, ctx.guild.id)
    if row is None:
        await ctx.reply("Контент не найден.", mention_author=False); return
    if not (isinstance(ctx.author, discord.Member) and is_organizer_or_admin(ctx.author, int(row["created_by"]))):
        await ctx.reply("❌ Только организатор или стафф.", mention_author=False); return
    roles_lines = normalize_roles_lines(str(row["roles_text"]))
    start_ts = int(row["start_ts"]) if row["start_ts"] else None
    lines = [f"**{row['title']}**"]
    if start_ts:
        utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
        lines.append(f"⏰ {utc_str} • {ts_discord(start_ts, 'F')}")
    lines.append("")
    mode_norm = (mode or "full").lower()
    if mode_norm not in ("full", "simple"):
        mode_norm = "full"
    if mode_norm == "full":
        by_index = {idx: uid for idx, uid in await db_get_roster(content_id)}
        for i, rn in enumerate(roles_lines, start=1):
            uid = by_index.get(i)
            lines.append(f"`{i}.` {rn} — <@{uid}>" if uid else f"`{i}.` {rn} —")
    else:
        for i, rn in enumerate(roles_lines, start=1):
            lines.append(f"`{i}.` {rn}")
    text = "\n".join(lines)
    if len(text) <= 1900:
        await ctx.reply(text, mention_author=False)
    else:
        buf = io.BytesIO(text.encode("utf-8"))
        await ctx.reply(
            content=f"Копия контента **#{content_id}**:",
            file=discord.File(buf, filename=f"content_{content_id}.txt"),
            mention_author=False,
        )


# =========================
# PREFIX COMMANDS — STAFF ONLY
# =========================

@bot.command(name="add-money")
async def prefix_add_money(ctx: commands.Context, member: discord.Member, amount: int, *, reason: Optional[str] = None):
    """!add-money @user <сумма> [причина] — начислить деньги (стафф)."""
    if ctx.guild is None: return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("\u274c Только стафф.", mention_author=False); return
    if amount <= 0:
        await ctx.reply("Сумма должна быть > 0.", mention_author=False); return
    new, _ = await db_balance_apply(ctx.guild.id, member.id, amount, "add", reason, ctx.author.id)
    embed = discord.Embed(title="\u2705 Начислено", color=COLOR_GREEN)
    embed.add_field(name=f"\U0001fa99 {member.display_name}", value=f"+{fmt_money(amount)}\nНовый баланс: **{fmt_money(new)}**", inline=False)
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await ctx.reply(embed=embed, mention_author=False)


@bot.command(name="remove-money")
async def prefix_remove_money(ctx: commands.Context, member: discord.Member, amount: int, *, reason: Optional[str] = None):
    """!remove-money @user <сумма> [причина] — снять деньги (стафф)."""
    if ctx.guild is None: return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("\u274c Только стафф.", mention_author=False); return
    if amount <= 0:
        await ctx.reply("Сумма должна быть > 0.", mention_author=False); return
    new, applied = await db_balance_apply(ctx.guild.id, member.id, -amount, "sub", reason, ctx.author.id)
    removed = -applied
    embed = discord.Embed(title="\u2705 Снято", color=COLOR_YELLOW)
    embed.add_field(name=f"\U0001fa99 {member.display_name}", value=f"-{fmt_money(removed)}\nНовый баланс: **{fmt_money(new)}**", inline=False)
    if removed < amount:
        embed.add_field(name="\u26a0\ufe0f", value="Баланс обнулён (было меньше запрошенного).", inline=False)
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await ctx.reply(embed=embed, mention_author=False)


@bot.command(name="add-money-role")
async def prefix_add_money_role(ctx: commands.Context, role: discord.Role, amount: int, *, reason: Optional[str] = None):
    """!add-money-role @role <сумма> [причина] — начислить всем с ролью (стафф)."""
    if ctx.guild is None: return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("\u274c Только стафф.", mention_author=False); return
    if amount <= 0:
        await ctx.reply("Сумма должна быть > 0.", mention_author=False); return
    targets = [m for m in role.members if not m.bot]
    if not targets:
        await ctx.reply("У роли нет участников.", mention_author=False); return
    await db_balance_payout_many(ctx.guild.id, [m.id for m in targets], amount,
                                 "payout_role", reason or f"role:{role.name}", ctx.author.id)
    embed = discord.Embed(title="\u2705 Массовое начисление", color=COLOR_GREEN)
    embed.add_field(name=f"\U0001fa99 {role.name}", value=f"+{fmt_money(amount)} каждому\nПолучателей: **{len(targets)}**", inline=False)
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await ctx.reply(embed=embed, mention_author=False)


# =========================
# PREFIX COMMANDS — ALL MEMBERS
# =========================

@bot.command(name="lb")
async def prefix_lb(ctx: commands.Context):
    """!lb — лидерборд посещаемости за текущий месяц."""
    if ctx.guild is None: return
    rows = await db_att_leaderboard_month(ctx.guild.id)
    my_count = next((cnt for uid, cnt in rows if uid == ctx.author.id), 0)
    view = LeaderboardView(rows=rows, guild=ctx.guild, requester_id=ctx.author.id, my_count=my_count)
    await ctx.reply(embed=view.build_embed(), view=view, mention_author=False)


@bot.command(name="att")
async def prefix_att(ctx: commands.Context, target_member: Optional[discord.Member] = None):
    """!att — своя посещаемость; !att @user — чужая (доступно всем)."""
    if ctx.guild is None: return
    target = target_member or ctx.author
    data = await db_att_profile(ctx.guild.id, int(target.id), "month")
    if data is None:
        await ctx.reply(f"{target.mention} ещё ни разу не аттендил.", mention_author=False); return
    joined_str = datetime.datetime.fromtimestamp(data["joined_at"], tz=datetime.timezone.utc).strftime("%Y-%m-%d")
    m_data, o_data = data[TYPE_MANDATORY], data[TYPE_OPTIONAL]
    embed = discord.Embed(
        title=f"\U0001f4ca Посещаемость \u00b7 {getattr(target, 'display_name', target.name)}",
        description=f"Период: **текущий месяц**\nВ сборах с: `{joined_str}`",
        color=COLOR_BLUE
    )
    embed.add_field(
        name="\U0001f534 Обязательные",
        value=f"Ивентов: {m_data['total']}\nПосещено: {m_data['attended']}\nПроцент: **{m_data['pct']} %**",
        inline=True,
    )
    embed.add_field(
        name="\U0001f7e2 Необязательные",
        value=f"Ивентов: {o_data['total']}\nПосещено: {o_data['attended']}\nПроцент: **{o_data['pct']} %**",
        inline=True,
    )
    await ctx.reply(embed=embed, mention_author=False)


async def build_economy_stats_embed(guild_id: int) -> discord.Embed:
    bank, holders = await db_balance_bank(guild_id)
    lo, hi = window_bounds("all")
    stats = await db_balance_period_stats(guild_id, lo, hi)
    embed = discord.Embed(title="🏦 Economy Stats", color=0x1a1f3c)
    embed.add_field(name="💰 Total Awarded:", value=f"**{fmt_money(stats['awarded'])}**", inline=False)
    embed.add_field(name="📉 Total Removed:", value=f"**{fmt_money(stats['removed'])}**", inline=False)
    embed.add_field(name="🏦 Total Bank (к раздаче):", value=f"**{fmt_money(bank)}**", inline=False)
    embed.add_field(name="👥 Holders:", value=f"**{holders}**", inline=False)
    return embed


async def build_economy_export(
    guild: discord.Guild, scope: str, user: Optional[discord.Member] = None,
) -> Tuple[Optional[str], Optional[discord.Embed], List[discord.File]]:
    """ЛБ балансов + транзакции за период + общая стата. Опционально фильтр по юзеру."""
    guild_id = guild.id
    lo, hi = window_bounds(scope)
    bank, holders = await db_balance_bank(guild_id)
    lb_rows = await db_balance_leaderboard(guild_id)
    guild_stats = await db_balance_period_stats(guild_id, lo, hi)
    events = await db_balance_events_range(guild_id, lo, hi, None if user is None else int(user.id))

    if not lb_rows and not events and guild_stats["count"] == 0:
        return "Нет данных для экспорта.", None, []

    def name_of(uid: int) -> str:
        gm = guild.get_member(uid)
        return gm.display_name if gm else f"ID:{uid}"

    embed = discord.Embed(title="📥 Выгрузка экономики", color=COLOR_BLUE)
    embed.add_field(name="Период", value=SCOPE_LABELS.get(scope, scope), inline=True)
    if user is not None:
        embed.add_field(name="Фильтр транзакций", value=user.mention, inline=True)
    embed.add_field(
        name="💰 Сейчас (к раздаче)",
        value=f"Общий баланс: **{fmt_money(bank)}**\nДержателей: **{holders}**",
        inline=False,
    )
    embed.add_field(
        name="📊 Движение за период (вся гильдия)",
        value=(
            f"Начислено (+): **{fmt_money(guild_stats['awarded'])}**\n"
            f"Списано (−): **{fmt_money(guild_stats['removed'])}**\n"
            f"Сальдо: **{fmt_money(guild_stats['net'])}**\n"
            f"Транзакций: **{guild_stats['count']}**"
        ),
        inline=False,
    )
    if user is not None:
        user_stats = await db_balance_period_stats(guild_id, lo, hi, int(user.id))
        user_bal = await db_balance_get(guild_id, int(user.id))
        if scope == "all":
            match = user_stats["net"] == user_bal
            recon = (
                f"\nΣ всех delta: **{fmt_money(user_stats['net'])}**"
                f"\nСверка с балансом: {'✅ сходится' if match else '⚠️ не сходится'}"
            )
        else:
            recon = "\nДля полной сверки баланса выбери период «Всё время»."
        embed.add_field(
            name=f"👤 {user.display_name}",
            value=(
                f"Текущий баланс: **{fmt_money(user_bal)}**\n"
                f"Начислено (+): **{fmt_money(user_stats['awarded'])}**\n"
                f"Списано (−): **{fmt_money(user_stats['removed'])}**\n"
                f"Сальдо периода: **{fmt_money(user_stats['net'])}**\n"
                f"Транзакций: **{user_stats['count']}**"
                f"{recon}"
            ),
            inline=False,
        )

    lb_csv_rows = []
    for i, (uid, amount) in enumerate(lb_rows, start=1):
        gm = guild.get_member(uid)
        lb_csv_rows.append([
            i, uid,
            gm.name if gm else "Неизвестно",
            gm.display_name if gm else f"ID:{uid}",
            amount,
        ])
    ts = int(time.time())
    files = [
        make_csv_file(
            f"economy_lb_{ts}.csv",
            ["Место", "User ID", "Никнейм", "Отображаемое имя", "Баланс"],
            lb_csv_rows,
        )
    ]
    tx_rows = [[
        datetime.datetime.fromtimestamp(e["created_at"], tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        e["user_id"], name_of(e["user_id"]),
        e["delta"], e["balance_after"],
        e["kind"], e["reason"],
        e["actor_id"], name_of(e["actor_id"]),
    ] for e in events]
    who = f"_{user.id}" if user is not None else ""
    files.append(make_csv_file(
        f"economy_tx_{scope}{who}_{ts}.csv",
        ["Дата (UTC)", "User ID", "Пользователь", "Дельта (+/−)", "Баланс после",
         "Тип", "Причина", "Кто начислил ID", "Кто начислил"],
        tx_rows,
    ))
    return None, embed, files


@bot.command(name="economy-stats")
async def prefix_economy_stats(ctx: commands.Context):
    """!economy-stats — общая статистика экономики гильдии (стафф)."""
    if ctx.guild is None:
        return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("❌ Только стафф.", mention_author=False); return
    await ctx.reply(embed=await build_economy_stats_embed(ctx.guild.id), mention_author=False)


@bot.tree.command(name="economy_stats", description="Общая статистика экономики (стафф)")
async def economy_stats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    await interaction.followup.send(embed=await build_economy_stats_embed(int(interaction.guild_id)), ephemeral=True)


@bot.command(name="economy-lb")
async def prefix_economy_lb(ctx: commands.Context):
    """!economy-lb — лидерборд по балансам (для всех)."""
    if ctx.guild is None:
        return
    rows = await db_balance_leaderboard(ctx.guild.id)
    my_bal = await db_balance_get(ctx.guild.id, ctx.author.id)
    bank, _holders = await db_balance_bank(ctx.guild.id)
    view = LeaderboardView(rows=rows, guild=ctx.guild, requester_id=ctx.author.id,
                           my_count=my_bal, money=True, total_sum=bank)
    await ctx.reply(embed=view.build_embed(), view=view, mention_author=False)


@bot.tree.command(name="economy_lb", description="Лидерборд по балансам")
async def economy_lb(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    gid = int(interaction.guild_id)
    rows = await db_balance_leaderboard(gid)
    my_bal = await db_balance_get(gid, int(interaction.user.id))
    bank, _holders = await db_balance_bank(gid)
    view = LeaderboardView(rows=rows, guild=interaction.guild, requester_id=interaction.user.id,
                           my_count=my_bal, money=True, total_sum=bank)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="economy_export_csv", description="Выгрузить лб и транзакции экономики (стафф)")
@app_commands.describe(
    scope="Период транзакций (по умолчанию текущий месяц)",
    user="Только транзакции этого пользователя (для сверки +/-)",
)
@app_commands.choices(scope=SCOPE_CHOICES)
async def economy_export_csv(
    interaction: discord.Interaction,
    scope: str = "month",
    user: Optional[discord.Member] = None,
):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    err, embed, files = await build_economy_export(interaction.guild, scope, user)
    if err:
        await interaction.followup.send(err, ephemeral=True); return
    await interaction.followup.send(embed=embed, files=files, ephemeral=True)


@bot.command(name="economy-export")
async def prefix_economy_export(ctx: commands.Context, member: Optional[discord.Member] = None):
    """!economy-export [@user] — выгрузка лб + транзакции за текущий месяц (стафф)."""
    if ctx.guild is None:
        return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("❌ Только стафф.", mention_author=False); return
    err, embed, files = await build_economy_export(ctx.guild, "month", member)
    if err:
        await ctx.reply(err, mention_author=False); return
    await ctx.reply(embed=embed, files=files, mention_author=False)


# =========================
# SIPHONED ENERGY COMMANDS
# =========================
def _fmt_energy(n: int) -> str:
    sign = "+" if n > 0 else ""
    return f"{sign}{n:,}".replace(",", " ")


def build_siphon_player_embed(data: dict) -> discord.Embed:
    net = data["net"]
    color = COLOR_RED if net < 0 else (COLOR_GREEN if net > 0 else COLOR_GOLD)
    status = "должен гильдии" if net < 0 else ("в плюсе" if net > 0 else "ноль")
    embed = discord.Embed(
        title=f"⚡ Энергия · {data['player']}",
        description=f"Статус: **{status}**\nТекущий баланс: **{_fmt_energy(net)}**",
        color=color,
    )
    embed.add_field(name="Сдал", value=_fmt_energy(data["deposited"]), inline=True)
    embed.add_field(name="Снял", value=_fmt_energy(-data["withdrawn"] if data["withdrawn"] else 0), inline=True)
    embed.add_field(name="Операций", value=str(data["ops"]), inline=True)
    if data["events"]:
        lines = []
        for e in data["events"][:15]:
            mark = "📥" if e["reason"] == "Deposit" else "📤"
            lines.append(f"`{e['at']}` {mark} {_fmt_energy(e['amount'])}")
        embed.add_field(name="Последние операции", value="\n".join(lines), inline=False)
    return embed


async def _import_siphon_bytes(guild_id: int, actor_id: int, raw: bytes) -> Tuple[Optional[discord.Embed], Optional[str]]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("cp1251")
        except UnicodeDecodeError:
            return None, "Не удалось прочитать файл (нужен UTF-8 или Windows-1251)."
    parsed, errors = parse_siphon_txt(text)
    if not parsed and not errors:
        return None, "В файле нет строк."
    inserted, skipped = await db_siphon_import(guild_id, parsed, actor_id) if parsed else (0, 0)
    embed = discord.Embed(title="⚡ Импорт энергии", color=COLOR_GREEN if inserted or skipped else COLOR_YELLOW)
    embed.add_field(name="Распознано", value=str(len(parsed)), inline=True)
    embed.add_field(name="Новых", value=str(inserted), inline=True)
    embed.add_field(name="Уже были", value=str(skipped), inline=True)
    if errors:
        embed.add_field(name=f"Ошибки ({len(errors)})", value="\n".join(errors[:8]), inline=False)
    return embed, None


@bot.tree.command(name="energy_import", description="Залить txt лог Siphoned Energy (стафф)")
@app_commands.describe(file="TXT из игры: Date / Player / Reason / Amount")
async def energy_import(interaction: discord.Interaction, file: discord.Attachment):
    await interaction.response.defer(ephemeral=True)
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    name = (file.filename or "").lower()
    if not (name.endswith(".txt") or name.endswith(".tsv") or name.endswith(".csv")):
        await interaction.followup.send("Нужен файл `.txt` (копия лога из игры).", ephemeral=True); return
    if file.size and file.size > 2_000_000:
        await interaction.followup.send("Файл слишком большой (лимит 2 МБ).", ephemeral=True); return
    raw = await file.read()
    embed, err = await _import_siphon_bytes(int(interaction.guild_id), int(interaction.user.id), raw)
    if err:
        await interaction.followup.send(err, ephemeral=True); return
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.command(name="energy-import")
async def prefix_energy_import(ctx: commands.Context):
    """!energy-import + вложение .txt — залить лог энергии (стафф)."""
    if ctx.guild is None:
        return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("❌ Только стафф.", mention_author=False); return
    atts = [a for a in ctx.message.attachments
            if (a.filename or "").lower().endswith((".txt", ".tsv", ".csv"))]
    if not atts:
        await ctx.reply("Прикрепи `.txt` лог к сообщению: `!energy-import`", mention_author=False); return
    raw = await atts[0].read()
    embed, err = await _import_siphon_bytes(ctx.guild.id, ctx.author.id, raw)
    if err:
        await ctx.reply(err, mention_author=False); return
    await ctx.reply(embed=embed, mention_author=False)


@bot.tree.command(name="energy", description="Статус энергии игрока по нику Albion")
@app_commands.describe(nick="Ник в Albion")
async def energy_status(interaction: discord.Interaction, nick: str):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    data = await db_siphon_player(int(interaction.guild_id), nick)
    if data is None:
        hints = await db_siphon_suggest(int(interaction.guild_id), nick)
        extra = (" Похожие: " + ", ".join(f"`{h}`" for h in hints)) if hints else ""
        await interaction.followup.send(f"Игрок `{nick}` не найден.{extra}"); return
    await interaction.followup.send(embed=build_siphon_player_embed(data))


@bot.command(name="energy")
async def prefix_energy(ctx: commands.Context, *, nick: Optional[str] = None):
    """!energy <ник> — статус энергии."""
    if ctx.guild is None:
        return
    if not nick or not nick.strip():
        await ctx.reply("Формат: `!energy НикВИгре`", mention_author=False); return
    data = await db_siphon_player(ctx.guild.id, nick.strip())
    if data is None:
        hints = await db_siphon_suggest(ctx.guild.id, nick.strip())
        extra = (" Похожие: " + ", ".join(f"`{h}`" for h in hints)) if hints else ""
        await ctx.reply(f"Игрок `{nick.strip()}` не найден.{extra}", mention_author=False); return
    await ctx.reply(embed=build_siphon_player_embed(data), mention_author=False)


def build_siphon_lb_embed(rows: List[dict], summary: dict) -> discord.Embed:
    head = []
    if summary.get("import_first") and summary.get("import_last"):
        a, b = summary["import_first"], summary["import_last"]
        if a == b:
            head.append(f"Импорт: {ts_discord(a, 'f')}")
        else:
            head.append(f"Импорт: {ts_discord(a, 'f')} → {ts_discord(b, 'f')}")
    head.append(f"Всего: **{_fmt_energy(summary['net'])}**")
    embed = discord.Embed(
        title="⚡ Энергия",
        description="\n".join(head),
        color=COLOR_GOLD,
    )
    lines = [
        f"**{i}.** {r['player']} · **{_fmt_energy(r['net'])}**"
        for i, r in enumerate(rows, start=1)
    ]
    add_chunked_field(embed, "Баланс", lines)
    embed.set_footer(text=f"{summary['players']} игроков")
    return embed


@bot.tree.command(name="energy_lb", description="Лидерборд энергии за всё время")
async def energy_lb(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен."); return
    gid = int(interaction.guild_id)
    summary = await db_siphon_summary(gid)
    if summary is None:
        await interaction.followup.send("Пока нет импортов энергии."); return
    rows = await db_siphon_leaderboard(gid)
    await interaction.followup.send(embed=build_siphon_lb_embed(rows, summary))


@bot.command(name="energy-lb")
async def prefix_energy_lb(ctx: commands.Context):
    """!energy-lb — лидерборд энергии за всё время."""
    if ctx.guild is None:
        return
    summary = await db_siphon_summary(ctx.guild.id)
    if summary is None:
        await ctx.reply("Пока нет импортов энергии.", mention_author=False); return
    rows = await db_siphon_leaderboard(ctx.guild.id)
    await ctx.reply(embed=build_siphon_lb_embed(rows, summary), mention_author=False)

# =========================
# MAIN
# =========================
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN не найден. Проверьте .env")
    bot.run(token)


if __name__ == "__main__":
    main()