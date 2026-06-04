import os
import re
import io
import csv
import time
import datetime
from typing import Optional, List, Tuple, Set

import discord
from discord import app_commands
from discord.ext import commands
import aiosqlite
from dotenv import load_dotenv

# OCR / fuzzy match (для /role_from_screenshot)
# Требует системный бинарь tesseract: apt install tesseract-ocr tesseract-ocr-rus
# pip: pillow pytesseract rapidfuzz
try:
    from PIL import Image, ImageOps
    import pytesseract
    from rapidfuzz import process as rf_process, fuzz as rf_fuzz
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# =========================
# CONFIG
# =========================
load_dotenv()
DB_PATH = "cs_helper.db"

STATUS_OPEN = "open"
STATUS_CLOSED = "closed"

TYPE_MANDATORY = "mandatory"
TYPE_OPTIONAL = "optional"

LEADERBOARD_PAGE_SIZE = 20
FUZZ_CUTOFF = 78  # порог совпадения ника при OCR

CURRENCY = "💰"  # символ валюты — поменять тут

# Роли, дающие доступ к управляющим командам (помимо admin/manage_guild/manage_roles).
# .env: STAFF_ROLE_IDS=123,456
STAFF_ROLE_IDS: Set[int] = {
    int(x) for x in os.getenv("STAFF_ROLE_IDS", "").replace(" ", "").split(",") if x.isdigit()
}

# RL роли — могут /content_create. .env: RL_ROLE_IDS=111,222
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
COLOR_PURPLE = 0x9B59B6
COLOR_GOLD   = 0xF1C40F

# Глобальный коннект к БД (инициализируется в db_init).
# aiosqlite сериализует операции через свой воркер-поток => безопасно для конкурентных await,
# а WAL + busy_timeout убирают "database is locked".
DB: Optional[aiosqlite.Connection] = None


# =========================
# DB INIT
# =========================
async def db_init() -> None:
    global DB
    DB = await aiosqlite.connect(DB_PATH)
    DB.row_factory = aiosqlite.Row
    await DB.execute("PRAGMA journal_mode=WAL")
    await DB.execute("PRAGMA busy_timeout=5000")
    await DB.execute("PRAGMA foreign_keys=ON")

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

    # Миграции для старой БД (новые колонки contents).
    migrations = [
        "ALTER TABLE contents ADD COLUMN content_type TEXT NOT NULL DEFAULT 'mandatory'",
        "ALTER TABLE contents ADD COLUMN builds_link TEXT",
        "ALTER TABLE contents ADD COLUMN photo_url TEXT",
        "ALTER TABLE contents ADD COLUMN hosted_by TEXT",
        "ALTER TABLE contents ADD COLUMN start_ts INTEGER",
    ]
    for sql in migrations:
        try:
            await DB.execute(sql)
        except Exception:
            pass

    await DB.execute("CREATE INDEX IF NOT EXISTS idx_contents_guild_type_start ON contents(guild_id, content_type, start_ts)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_awards_guild_user ON attendance_awards(guild_id, user_id)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_balevents_guild_user ON balance_events(guild_id, user_id)")
    await DB.execute("CREATE INDEX IF NOT EXISTS idx_awards_guild_awarded ON attendance_awards(guild_id, awarded_at)")

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
    await DB.commit()
    return cur.lastrowid


async def db_set_message_thread(content_id: int, message_id: int, thread_id: Optional[int], photo_url: Optional[str]) -> None:
    await DB.execute(
        "UPDATE contents SET message_id = ?, thread_id = ?, photo_url = ? WHERE id = ?",
        (message_id, thread_id, photo_url, content_id)
    )
    await DB.commit()


async def db_get_content_by_id(content_id: int):
    cur = await DB.execute("SELECT * FROM contents WHERE id = ?", (content_id,))
    return await cur.fetchone()


async def db_get_content_by_thread(thread_id: int):
    cur = await DB.execute("SELECT * FROM contents WHERE thread_id = ?", (thread_id,))
    return await cur.fetchone()


async def db_close_content(content_id: int) -> None:
    await DB.execute("UPDATE contents SET status = ? WHERE id = ?", (STATUS_CLOSED, content_id))
    await DB.commit()


async def db_set_payout_role(content_id: int, role_id: int, role_name: str) -> None:
    await DB.execute(
        "UPDATE contents SET payout_role_id = ?, payout_role_name = ? WHERE id = ?",
        (role_id, role_name, content_id)
    )
    await DB.commit()


# ---------- slots ----------
async def db_assign_user(content_id: int, user_id: int, role_index: int) -> Tuple[bool, str]:
    # BEGIN IMMEDIATE лочит запись сразу => нет гонки "двое заняли один слот".
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
    cur = await DB.execute(
        "DELETE FROM content_assignments WHERE content_id = ? AND user_id = ?",
        (content_id, user_id)
    )
    await DB.commit()
    return cur.rowcount > 0


async def db_unassign_by_role_index(content_id: int, role_index: int) -> Optional[int]:
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
async def db_attend_add_many(content_id: int, user_ids: List[int], added_by: int) -> Tuple[int, int]:
    added = already = 0
    now = int(time.time())
    for uid in user_ids:
        try:
            await DB.execute(
                "INSERT INTO content_attendance (content_id, user_id, added_by, added_at) VALUES (?, ?, ?, ?)",
                (content_id, uid, added_by, now)
            )
            added += 1
        except aiosqlite.IntegrityError:
            already += 1
    await DB.commit()
    return added, already


async def db_attend_remove(content_id: int, user_id: int) -> bool:
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
    awarded_by: int, content_start_ts: Optional[int] = None,
) -> Tuple[int, int]:
    now = int(time.time())
    awarded = already = 0
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
    return awarded, already


async def db_att_remove_for_content(guild_id: int, content_id: int, user_id: int, removed_by: int) -> Tuple[bool, str]:
    now = int(time.time())
    cur = await DB.execute(
        "DELETE FROM attendance_awards WHERE guild_id = ? AND content_id = ? AND user_id = ?",
        (guild_id, content_id, user_id)
    )
    if cur.rowcount == 0:
        await DB.rollback()
        return False, "У пользователя нет аттенданса за этот контент."
    await DB.execute(
        "INSERT INTO attendance_events (guild_id, user_id, content_id, delta, kind, actor_id, created_at) VALUES (?, ?, ?, -1, 'content_remove', ?, ?)",
        (guild_id, user_id, content_id, removed_by, now)
    )
    await DB.commit()
    return True, "Аттенданс удалён."


async def db_get_joined_at(guild_id: int, user_id: int) -> Optional[int]:
    cur = await DB.execute(
        "SELECT joined_at FROM attendance_join WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id)
    )
    row = await cur.fetchone()
    return int(row[0]) if row else None


def window_bounds(scope: str) -> Tuple[int, int]:
    """(lo, hi) unix-границы окна по awarded_at. Полуинтервал [lo, hi)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    hi = int(now.timestamp()) + 1  # +1 чтобы включить только что начисленное (awarded_at == now)
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


async def db_att_leaderboard_month(guild_id: int) -> List[Tuple[int, int]]:
    """Лидерборд за текущий календарный месяц (по awarded_at)."""
    lo, hi = window_bounds("month")
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


async def db_balance_apply(
    guild_id: int, user_id: int, delta: int,
    kind: str, reason: Optional[str], actor_id: int,
) -> Tuple[int, int]:
    """
    Применяет дельту к балансу. Клампит снизу нулём (минимум 0).
    Возвращает (new_balance, applied_delta) — applied_delta может отличаться от delta при клампе.
    """
    now = int(time.time())
    await DB.execute("BEGIN IMMEDIATE")
    try:
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
        await DB.commit()
    except Exception:
        await DB.rollback()
        raise
    return new, applied


# =========================
# HELPERS
# =========================
def ts_discord(unix_ts: int, fmt: str = "F") -> str:
    return f"<t:{unix_ts}:{fmt}>"


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
    """RL — может /content_create."""
    return bool(RL_ROLE_IDS & {r.id for r in member.roles})


def member_is_recruit(member: discord.Member) -> bool:
    """Рекрут — только мемберские команды."""
    return bool(RECRUIT_ROLE_IDS & {r.id for r in member.roles})


def member_can_create_content(member: discord.Member) -> bool:
    """Создавать контент: стафф + RL."""
    return member_is_staff(member) or member_is_rl(member)


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
    message_id = int(content_row["message_id"])
    thread_id = int(content_row["thread_id"]) if content_row["thread_id"] else None
    roles_lines = normalize_roles_lines(str(content_row["roles_text"]))
    after_text = str(content_row["after_text"]) if content_row["after_text"] else None
    hosted_by = str(content_row["hosted_by"]) if content_row["hosted_by"] else None
    start_ts = int(content_row["start_ts"]) if content_row["start_ts"] else None
    content_type = str(content_row["content_type"])
    builds_link = str(content_row["builds_link"]) if content_row["builds_link"] else None
    photo_url = str(content_row["photo_url"]) if content_row["photo_url"] else None

    by_index = {idx: uid for idx, uid in roster}
    participants_count = len({uid for _, uid in roster}) + len(attendance_only)

    color = COLOR_GREEN if status == STATUS_OPEN else COLOR_RED
    embed = discord.Embed(title=f"📋 Контент #{content_id}: {title}", color=color)

    embed.add_field(name="Тип", value=type_label(content_type), inline=True)

    if start_ts and start_ts > 0:
        utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
        embed.add_field(
            name="⏰ Be ready by",
            value=f"{utc_str} • {ts_discord(start_ts, 'F')} • {ts_discord(start_ts, 'R')}",
            inline=False
        )

    if hosted_by:
        embed.add_field(name="👤 Content hosted by", value=hosted_by, inline=False)

    status_str = "🟢 Open" if status == STATUS_OPEN else "🔴 Closed"
    embed.add_field(name="Status", value=status_str, inline=True)
    embed.add_field(name="Players", value=f"**{participants_count}**", inline=True)
    embed.add_field(name="Content ID", value=f"`{content_id}`", inline=True)

    # Роли (чанками по лимиту embed-поля).
    roles_fmt = []
    for i, role_name in enumerate(roles_lines, start=1):
        uid = by_index.get(i)
        roles_fmt.append(f"`{i}.` {role_name} — <@{uid}>" if uid else f"`{i}.` {role_name} —")
        roles_fmt.append("")  # воздух между слотами

    chunk, chunk_size, field_num = [], 0, 0
    for line in roles_fmt:
        if chunk_size + len(line) + 1 > 950 and chunk:
            embed.add_field(name="📝 Roles" if field_num == 0 else "\u200b", value="\n".join(chunk), inline=False)
            chunk, chunk_size, field_num = [], 0, field_num + 1
        chunk.append(line); chunk_size += len(line) + 1
    if chunk:
        embed.add_field(name="📝 Roles" if field_num == 0 else "\u200b", value="\n".join(chunk), inline=False)

    if attendance_only:
        start = len(roles_lines) + 1
        att_lines = [f"`{j}.` <@{uid}>" for j, uid in enumerate(attendance_only, start=start)]
        embed.add_field(name="➕ Additionally", value="\n".join(att_lines[:20]), inline=False)

    if builds_link:
        embed.add_field(name="🔗 Билды", value=builds_link, inline=False)

    if after_text and after_text.strip():
        embed.add_field(name="📌 Note", value=after_text.strip(), inline=False)

    embed.add_field(
        name="📖 В ветке",
        value="➣ `1` — занять роль (если свободна)\n➣ `-` — выписаться с роли\n➣ `+хилл @user` — добавить с меткой (стафф)",
        inline=False
    )

    if photo_url:
        embed.set_image(url=photo_url)

    return embed


async def refresh_main_post(guild: discord.Guild, content_row) -> None:
    try:
        channel = guild.get_channel(int(content_row["channel_id"]))
        if not isinstance(channel, discord.TextChannel):
            return
        msg = await channel.fetch_message(int(content_row["message_id"]))
        roster = await db_get_roster(int(content_row["id"]))
        attend = await db_attend_list(int(content_row["id"]))
        roster_uids = {uid for _, uid in roster}
        attendance_only = [uid for uid in attend if uid not in roster_uids]
        embed = build_main_post_embed(content_row, roster, attendance_only)
        # attachments не трогаем => фото (аттач этого же сообщения) остаётся валидным.
        await msg.edit(content=None, embed=embed)
    except Exception:
        return


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
    # +метка @user — доп. участник с подписью роли
    if t.startswith("+") and re.search(r"<@!?\d+>", t) and not re.match(r"^\+\d+\s", t):
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
# OCR
# =========================
def ocr_extract_names(image_bytes: bytes) -> List[str]:
    """OCR скрина -> кандидаты-ники (по строкам). Лёгкий препроцессинг под тёмную тему DC."""
    img = Image.open(io.BytesIO(image_bytes)).convert("L")
    # Апскейл мелкого текста + автоконтраст
    if max(img.size) < 1600:
        scale = 1600 / max(img.size)
        img = img.resize((int(img.width * scale), int(img.height * scale)))
    img = ImageOps.autocontrast(img)
    raw = pytesseract.image_to_string(img, lang="rus+eng")
    out, seen = [], set()
    for ln in raw.splitlines():
        s = ln.strip()
        # отбрасываем мусор: слишком коротко/только символы
        if len(s) < 2 or not re.search(r"[A-Za-zА-Яа-я0-9]", s):
            continue
        # ник дискорда обычно одно "слово"; берём самый длинный токен строки
        token = max(s.split(), key=len)
        token = token.strip("|•·.,:#@()[]")
        if len(token) >= 2 and token.lower() not in seen:
            seen.add(token.lower()); out.append(token)
    return out


def match_members(guild: discord.Guild, tokens: List[str]) -> Tuple[List[Tuple[str, discord.Member, int]], List[str]]:
    """Фуззи-матч токенов к мемберам. Возвращает (matched[(token,member,score)], unmatched_tokens)."""
    choices = {}
    for m in guild.members:
        choices[m.display_name] = m
        if m.name not in choices:
            choices[m.name] = m
    names = list(choices.keys())
    matched, unmatched, used = [], [], set()
    for tok in tokens:
        best = rf_process.extractOne(tok, names, scorer=rf_fuzz.WRatio, score_cutoff=FUZZ_CUTOFF)
        if best is None:
            unmatched.append(tok); continue
        member = choices[best[0]]
        if member.id in used:
            continue
        used.add(member.id)
        matched.append((tok, member, int(best[1])))
    return matched, unmatched


# =========================
# LEADERBOARD VIEW
# =========================
class LeaderboardView(discord.ui.View):
    def __init__(self, rows, guild, requester_id, my_count):
        super().__init__(timeout=300)
        self.rows = rows
        self.guild = guild
        self.requester_id = requester_id
        self.my_count = my_count
        self.page = 0
        self.total_pages = max(1, (len(rows) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages - 1
        self.page_label.label = f"{self.page + 1} / {self.total_pages}"

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🏆 Лидерборд посещаемости (текущий месяц)",
            color=COLOR_GOLD
        )
        start_idx = self.page * LEADERBOARD_PAGE_SIZE
        page_rows = self.rows[start_idx:start_idx + LEADERBOARD_PAGE_SIZE]
        if not page_rows:
            embed.description = "_Пока пусто_"
        else:
            lines = []
            for i, (uid, cnt) in enumerate(page_rows, start=start_idx + 1):
                medal = MEDALS.get(i, "🎖️" if i <= 10 else "▫️")
                member = self.guild.get_member(uid)
                name = member.display_name if member else f"<@{uid}>"
                lines.append(f"{medal} **{i}.** {name} **·** {cnt}")
            embed.description = "\n".join(lines)
        embed.set_footer(text=f"Ваш результат за месяц: {self.my_count}  •  Стр. {self.page + 1}/{self.total_pages}")
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
# OCR -> ROLE: confirm/edit view
# =========================
class OcrRoleEditModal(discord.ui.Modal, title="Править список"):
    def __init__(self, parent_view: "OcrRoleView"):
        super().__init__()
        self.parent_view = parent_view
        prefill = " ".join(f"<@{m.id}>" for m in parent_view.members)
        if parent_view.unmatched:
            prefill += "\n# Не распознаны: " + ", ".join(parent_view.unmatched)
        self.mentions_input = discord.ui.TextInput(
            label="Упоминания (@user @user ...)",
            style=discord.TextStyle.paragraph,
            default=prefill[:4000],
            max_length=4000,
        )
        self.add_item(self.mentions_input)

    async def on_submit(self, interaction: discord.Interaction):
        ids = parse_user_ids_from_text(str(self.mentions_input))
        members = []
        for uid in ids:
            m = interaction.guild.get_member(uid)
            if m is None:
                try:
                    m = await interaction.guild.fetch_member(uid)
                except Exception:
                    m = None
            if m:
                members.append(m)
        self.parent_view.members = members
        self.parent_view.unmatched = []
        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view)


class OcrRoleView(discord.ui.View):
    def __init__(self, members: List[discord.Member], unmatched: List[str], role_name: str, author_id: int):
        super().__init__(timeout=300)
        self.members = members
        self.unmatched = unmatched
        self.role_name = role_name
        self.author_id = author_id

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🖼️ Распознанные участники",
            description=f"Будет создана роль **`{self.role_name}`** и выдана {len(self.members)} участникам.",
            color=COLOR_BLUE,
        )
        if self.members:
            embed.add_field(
                name=f"✅ Совпало ({len(self.members)})",
                value="\n".join(m.mention for m in self.members[:30]) or "—",
                inline=False,
            )
        if self.unmatched:
            embed.add_field(
                name=f"❓ Не распознаны ({len(self.unmatched)})",
                value=", ".join(self.unmatched[:30]),
                inline=False,
            )
        embed.set_footer(text="Проверь список. «Править» — поправить вручную.")
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Это не твоё подтверждение.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Подтвердить и выдать", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        guild = interaction.guild
        if not self.members:
            await interaction.followup.send("Список пуст.", ephemeral=True)
            return
        try:
            role = await guild.create_role(name=self.role_name, reason="CS role from screenshot")
        except discord.Forbidden:
            await interaction.followup.send("Нет прав на создание роли.", ephemeral=True)
            return
        assigned, failed = 0, []
        for m in self.members:
            try:
                await m.add_roles(role, reason="CS role from screenshot")
                assigned += 1
            except discord.Forbidden:
                failed.append(m.mention)
        for item in self.children:
            item.disabled = True
        embed = discord.Embed(title="✅ Роль создана", color=COLOR_GREEN)
        embed.add_field(name="Роль", value=f"`{role.name}`", inline=False)
        embed.add_field(name="Выдано", value=f"{assigned}/{len(self.members)}", inline=True)
        if failed:
            embed.add_field(name="Не удалось", value=", ".join(failed[:10]), inline=False)
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Править", style=discord.ButtonStyle.secondary, emoji="✏️")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(OcrRoleEditModal(self))

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.danger, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Отменено.", embed=None, view=self)


# =========================
# BOT
# =========================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix=commands.when_mentioned_or("!"), intents=intents)


@bot.event
async def on_ready():
    await db_init()
    try:
        synced = await bot.tree.sync()
        print(f"Ready. Synced commands: {len(synced)}")
    except Exception as e:
        print("Sync error:", e)
    print(f"Logged in as {bot.user} (id={bot.user.id})")


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

    def __init__(self, content_type: str, photo: Optional[discord.Attachment], auto_assign_organizer: bool = True):
        super().__init__()
        self.content_type = content_type
        self.photo = photo
        self.auto_assign_organizer = auto_assign_organizer

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

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

        content_id = await db_create_content(
            guild_id=interaction.guild_id, channel_id=channel.id, title=title,
            roles_text="\n".join(roles_lines), after_text=after, created_by=interaction.user.id,
            content_type=self.content_type, hosted_by=hosted_by, start_ts=start_ts, builds_link=link,
        )

        # Шлём пост сразу с фото (аттач этого сообщения => URL не протухает при edit).
        photo_url = None
        send_kwargs = {"content": f"**Контент #{content_id}: {title}**\nСоздание…"}
        if self.photo is not None:
            try:
                send_kwargs["file"] = await self.photo.to_file()
            except Exception:
                pass
        msg = await channel.send(**send_kwargs)
        if msg.attachments:
            photo_url = msg.attachments[0].url
        message_id = msg.id

        thread_id = None
        try:
            tname = f"{title} (CS#{content_id})"
            thread = await msg.create_thread(name=tname, auto_archive_duration=1440)
            thread_id = thread.id
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
        except discord.Forbidden:
            thread_id = None

        await db_set_message_thread(content_id, message_id, thread_id, photo_url)

        if self.auto_assign_organizer and roles_lines:
            await db_assign_user(content_id, interaction.user.id, 1)

        row = await db_get_content_by_id(content_id)
        if row and interaction.guild:
            await refresh_main_post(interaction.guild, row)

        await interaction.followup.send(f"✅ Контент создан: **CS#{content_id}**", ephemeral=True)


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
        row = await db_get_content_by_id(content_id)
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

        row2 = await db_get_content_by_id(content_id)
        if row2 and interaction.guild:
            await refresh_main_post(interaction.guild, row2)

        embed = discord.Embed(title="✅ Участники добавлены", color=COLOR_GREEN)
        embed.add_field(name="Добавлено", value=str(added), inline=True)
        embed.add_field(name="Уже было", value=str(already), inline=True)
        if role is not None:
            embed.add_field(name="Роль выдана", value=str(assigned), inline=True)
            if failed:
                embed.add_field(name="Ошибок", value=str(failed), inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)


# =========================
# THREAD SIGNUP
# =========================
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None or not isinstance(message.channel, discord.Thread):
        await bot.process_commands(message)
        return

    content = await db_get_content_by_thread(message.channel.id)
    if content is None:
        await bot.process_commands(message)
        return

    cmd = message.content.strip()
    if not (cmd.isdigit() or cmd in ("-", "help") or cmd.startswith("+") or cmd.startswith("-")):
        await bot.process_commands(message)
        return

    content_id = int(content["id"])
    status = str(content["status"])

    member = message.guild.get_member(message.author.id)
    if member is None:
        try:
            member = await message.guild.fetch_member(message.author.id)
        except Exception:
            member = None
    is_org = isinstance(member, discord.Member) and is_organizer_or_admin(member, int(content["created_by"]))

    kind, num = parse_thread_command(cmd)

    if kind == "self_leave":
        removed = await db_unassign_user(content_id, message.author.id)
        await message.add_reaction("✅" if removed else "ℹ️")
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
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

    if status != STATUS_OPEN and not is_org:
        await message.reply("🔴 Запись закрыта.", mention_author=False)
        return

    roles_lines = normalize_roles_lines(str(content["roles_text"]))
    max_slot = len(roles_lines)

    if kind == "self_join":
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False)
            return
        ok, txt = await db_assign_user(content_id, message.author.id, int(num))
        await message.add_reaction("✅" if ok else "⛔")
        if not ok:
            await message.reply(txt, mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_attend_add":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False); return
        user_ids = [m.id for m in message.mentions]
        if not user_ids:
            await message.reply("Формат: `+ @user @user ...`", mention_author=False); return
        added, already = await db_attend_add_many(content_id, user_ids, message.author.id)
        await message.add_reaction("✅")
        await message.reply(f"Добавлено: {added}. Уже были: {already}.", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_assign_slot":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False); return
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False); return
        if not message.mentions:
            await message.reply("Формат: `+2 @user`", mention_author=False); return
        target = message.mentions[0]
        ok, txt = await db_assign_user(content_id, target.id, int(num))
        await message.reply(f"{target.mention}: {txt}", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_assign_label":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False); return
        if not message.mentions:
            await message.reply("Формат: `+хилл @user`", mention_author=False); return
        label_match = re.match(r"^\+(.+?)\s+<@", cmd)
        label = label_match.group(1).strip() if label_match else "доп. роль"
        target = message.mentions[0]
        added, already = await db_attend_add_many(content_id, [target.id], message.author.id)
        status_txt = "уже записан" if already else "добавлен"
        await message.reply(f"{target.mention} — **{label}** ({status_txt})", mention_author=False)
        await message.add_reaction("\u2705")
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_kick_role":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False); return
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False); return
        kicked = await db_unassign_by_role_index(content_id, int(num))
        await message.reply("Роль свободна." if kicked is None else f"Выписано с роли {num}: <@{kicked}>", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_kick_user":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False); return
        if not message.mentions:
            await message.reply("Формат: `- @user`", mention_author=False); return
        target = message.mentions[0]
        removed_slot = await db_unassign_user(content_id, target.id)
        removed_att = await db_attend_remove(content_id, target.id)
        await message.reply("Выписан." if (removed_slot or removed_att) else "Пользователь не записан.", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
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
]
SCOPE_LABELS = {"week": "за неделю", "month": "за текущий месяц",
                "prev_month": "за прошлый месяц", "3months": "за последние 3 месяца"}


@bot.tree.command(name="healthcheck", description="Проверка статуса бота и базы данных")
async def healthcheck(interaction: discord.Interaction):
    try:
        await DB.execute("SELECT 1")
        embed = discord.Embed(title="✅ Статус бота",
                              description=f"Бот онлайн, БД доступна.\nOCR: {'✅' if OCR_AVAILABLE else '❌ не установлен'}",
                              color=COLOR_GREEN)
        await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(
            embed=discord.Embed(title="❌ Ошибка", description=str(e), color=COLOR_RED), ephemeral=True)


@bot.tree.command(name="content_create", description="Создать контент через форму")
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
    if member is None or not member_can_create_content(member):
        await interaction.response.send_message("Недостаточно прав. Требуется роль стаффа или RL.", ephemeral=True)
        return
    await interaction.response.send_modal(ContentCreateModal(content_type=type.value, photo=photo))


@bot.tree.command(name="add_ppl", description="Добавить людей в контент")
async def attend_add(interaction: discord.Interaction):
    default_content_id = None
    if isinstance(interaction.channel, discord.Thread):
        row = await db_get_content_by_thread(interaction.channel.id)
        if row is not None:
            default_content_id = int(row["id"])
    await interaction.response.send_modal(AttendAddModal(default_content_id=default_content_id))


@bot.tree.command(name="content_close", description="Закрыть запись на контент")
@app_commands.describe(content_id="ID контента")
async def content_close(interaction: discord.Interaction, content_id: int):
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.response.send_message("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.response.send_message("Недостаточно прав.", ephemeral=True); return
    await db_close_content(content_id)
    row2 = await db_get_content_by_id(content_id)
    if row2 and interaction.guild:
        await refresh_main_post(interaction.guild, row2)
    await interaction.response.send_message(
        embed=discord.Embed(title="🔴 Контент закрыт", description=f"Контент **#{content_id}** закрыт.", color=COLOR_RED),
        ephemeral=True)


@bot.tree.command(name="role_from_content", description="Создать роль и выдать всем участникам")
@app_commands.describe(content_id="ID контента", role_name="Название роли (по умолчанию = заголовок)")
async def role_from_content(interaction: discord.Interaction, content_id: int, role_name: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден."); return
    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("Guild недоступен."); return
    member = guild.get_member(interaction.user.id)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав."); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников."); return

    base_name = role_name.strip() if role_name and role_name.strip() else str(row["title"])
    final_role_name = f"{base_name} [CS#{content_id}]"
    try:
        role = await guild.create_role(name=final_role_name, reason=f"CS payout role for content {content_id}")
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на создание роли."); return

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

    await db_set_payout_role(content_id, role.id, role.name)
    embed = discord.Embed(title="✅ Роль создана", color=COLOR_GREEN)
    embed.add_field(name="Роль", value=f"`{role.name}`", inline=False)
    embed.add_field(name="Выдано", value=f"{assigned}/{len(user_ids)}", inline=True)
    if failed:
        embed.add_field(name="Не удалось", value=", ".join(f"<@{u}>" for u in failed[:10]), inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="role_from_screenshot", description="OCR скрина -> создать роль и выдать распознанным")
@app_commands.describe(photo="Скрин со списком ников", role_name="Название роли")
async def role_from_screenshot(interaction: discord.Interaction, photo: discord.Attachment, role_name: str):
    await interaction.response.defer(ephemeral=True)
    if not OCR_AVAILABLE:
        await interaction.followup.send("OCR не установлен (нужны tesseract + pillow/pytesseract/rapidfuzz)."); return
    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен."); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None or not (member.guild_permissions.administrator or member.guild_permissions.manage_roles):
        await interaction.followup.send("Недостаточно прав."); return
    if not (photo.content_type or "").startswith("image/"):
        await interaction.followup.send("Вложение должно быть картинкой."); return

    try:
        img_bytes = await photo.read()
        tokens = ocr_extract_names(img_bytes)
    except Exception as e:
        await interaction.followup.send(f"Ошибка OCR: {e}"); return
    if not tokens:
        await interaction.followup.send("Не удалось распознать текст на скрине."); return

    matched, unmatched = match_members(interaction.guild, tokens)
    members = [m for _, m, _ in matched]
    if not members and not unmatched:
        await interaction.followup.send("Совпадений не найдено."); return

    view = OcrRoleView(members=members, unmatched=unmatched, role_name=role_name.strip(), author_id=interaction.user.id)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="role_clear", description="Удалить payout роль контента")
@app_commands.describe(content_id="ID контента")
async def role_clear(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден."); return
    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("Guild недоступен."); return
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
        await interaction.followup.send(embed=discord.Embed(title="🗑️ Роль удалена", description=f"`{role.name}`", color=COLOR_RED))
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на удаление роли.")


@bot.tree.command(name="att_add", description="Начислить аттенданс за контент всем участникам")
@app_commands.describe(content_id="ID контента")
async def att_add(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True); return
    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.", ephemeral=True); return

    awarded, already = await db_att_award_for_content(
        guild_id=int(interaction.guild_id), content_id=int(content_id), user_ids=user_ids,
        awarded_by=int(interaction.user.id),
        content_start_ts=int(row["start_ts"]) if row["start_ts"] else None,
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
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True); return
    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    ok, msg_text = await db_att_remove_for_content(
        guild_id=int(interaction.guild_id), content_id=int(content_id),
        user_id=int(user.id), removed_by=int(interaction.user.id),
    )
    await interaction.followup.send(
        embed=discord.Embed(description=msg_text, color=COLOR_GREEN if ok else COLOR_RED), ephemeral=True)


@bot.tree.command(name="att_export_csv", description="Выгрузить аттенданс в CSV (с % посещений)")
@app_commands.describe(scope="Период")
@app_commands.choices(scope=SCOPE_CHOICES)
async def att_export_csv(interaction: discord.Interaction, scope: str = "month"):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True); return
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True); return
    perms = member.guild_permissions
    if not (perms.administrator or perms.manage_guild or perms.manage_roles):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return

    # Все, у кого есть joined (хоть раз аттендили).
    cur = await DB.execute("SELECT user_id FROM attendance_join WHERE guild_id = ?", (int(interaction.guild_id),))
    user_ids = [int(r[0]) for r in await cur.fetchall()]
    if not user_ids:
        await interaction.followup.send("Нет данных для экспорта.", ephemeral=True); return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "User ID", "Никнейм", "Отображаемое имя", "В сборах с",
        "Обяз всего", "Обяз посещено", "Обяз %",
        "Необяз всего", "Необяз посещено", "Необяз %",
    ])
    rows_data = []
    for uid in user_ids:
        data = await db_att_profile(int(interaction.guild_id), uid, scope)
        if data is None:
            continue
        m, o = data[TYPE_MANDATORY], data[TYPE_OPTIONAL]
        gm = interaction.guild.get_member(uid)
        rows_data.append((m["pct"], [
            uid,
            gm.name if gm else "Неизвестно",
            gm.display_name if gm else f"ID:{uid}",
            datetime.datetime.fromtimestamp(data["joined_at"], tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
            m["total"], m["attended"], m["pct"],
            o["total"], o["attended"], o["pct"],
        ]))
    rows_data.sort(key=lambda x: x[0], reverse=True)  # по обяз % убыв.
    for _, r in rows_data:
        writer.writerow(r)

    output.seek(0)
    csv_bytes = output.getvalue().encode("utf-8-sig")
    file = discord.File(fp=io.BytesIO(csv_bytes), filename=f"attendance_{scope}_{int(time.time())}.csv")
    embed = discord.Embed(
        title="📥 Экспорт аттенданса",
        description=f"Период: **{SCOPE_LABELS.get(scope, scope)}** — **{len(rows_data)}** участников.",
        color=COLOR_BLUE
    )
    await interaction.followup.send(embed=embed, file=file, ephemeral=True)


# =========================
# BALANCE COMMANDS
# =========================
def fmt_money(n: int) -> str:
    return f"{n:,}".replace(",", " ") + f" {CURRENCY}"


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
    for m in targets:
        await db_balance_apply(int(interaction.guild_id), int(m.id), amount,
                               "payout_role", reason or f"role:{role.name}", int(interaction.user.id))
    embed = discord.Embed(title="✅ Массовое начисление", color=COLOR_GREEN,
                          description=f"Роль {role.mention}: +{fmt_money(amount)} каждому\nПолучателей: **{len(targets)}**")
    if reason:
        embed.add_field(name="Причина", value=reason, inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="money_payout_content", description="Начислить деньги всем участникам контента (стафф)")
@app_commands.describe(content_id="ID контента", amount="Сколько каждому (>0)", reason="Причина (опционально)")
async def money_payout_content(interaction: discord.Interaction, content_id: int, amount: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=False)
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return
    if amount <= 0:
        await interaction.followup.send("Сумма должна быть > 0.", ephemeral=True); return
    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.", ephemeral=True); return
    for uid in user_ids:
        await db_balance_apply(int(interaction.guild_id), int(uid), amount,
                               "payout_content", reason or f"content:{content_id}", int(interaction.user.id))
    embed = discord.Embed(title="✅ Выплата за контент", color=COLOR_GREEN,
                          description=f"Контент **#{content_id}**: +{fmt_money(amount)} каждому\nПолучателей: **{len(user_ids)}**")
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
async def content_copy(interaction: discord.Interaction, content_id: int, mode: str = "simple"):
    await interaction.response.defer(ephemeral=True)
    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True); return
    member = get_member_safe(interaction)
    if member is None or not member_is_staff(member):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True); return

    title = str(row["title"])
    roles_lines = normalize_roles_lines(str(row["roles_text"]))
    start_ts = int(row["start_ts"]) if row["start_ts"] else None
    after_text = str(row["after_text"]).strip() if row["after_text"] else None
    builds_link = str(row["builds_link"]) if row["builds_link"] else None
    content_type = str(row["content_type"])

    if mode == "full":
        roster = await db_get_roster(content_id)
        attend = await db_attend_list(content_id)
        by_index = {idx: uid for idx, uid in roster}
        roster_uids = {uid for _, uid in roster}
        attendance_only = [uid for uid in attend if uid not in roster_uids]
        lines = [f"**Контент: {title}**", f"Тип: {type_label(content_type)}"]
        if start_ts:
            utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
            lines.append(f"\u23f0 Be ready by: {utc_str} ({ts_discord(start_ts, 'F')})")
        if builds_link:
            lines.append(f"\U0001f517 Билды: {builds_link}")
        lines.extend(["", "**\U0001f4dd Роли:**"])
        for i, role_name in enumerate(roles_lines, start=1):
            uid = by_index.get(i)
            lines.append(f"`{i}.` {role_name} — <@{uid}>" if uid else f"`{i}.` {role_name} —")
        if attendance_only:
            lines.extend(["", "**\u2795 Дополнительно:**"])
            for uid in attendance_only:
                lines.append(f"\u2022 <@{uid}>")
        if after_text:
            lines.extend(["", f"\U0001f4cc {after_text}"])
    else:
        lines = [f"**Контент: {title}**", f"Тип: {type_label(content_type)}"]
        if start_ts:
            utc_str = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc).strftime("%H:%M UTC")
            lines.append(f"\u23f0 Be ready by: {utc_str} ({ts_discord(start_ts, 'F')})")
        if builds_link:
            lines.append(f"\U0001f517 Билды: {builds_link}")
        lines.extend(["", "**\U0001f4dd Роли:**"])
        for i, role_name in enumerate(roles_lines, start=1):
            lines.append(f"`{i}.` {role_name}")
        if after_text:
            lines.extend(["", f"\U0001f4cc {after_text}"])

    text = "\n".join(lines)
    if len(text) <= 1900:
        await interaction.followup.send(content=text, ephemeral=True)
    else:
        buf = io.BytesIO(text.encode("utf-8"))
        file = discord.File(buf, filename=f"content_{content_id}_copy.txt")
        await interaction.followup.send(
            content=f"Копия контента **#{content_id}**:",
            file=file, ephemeral=True
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
    for m in targets:
        await db_balance_apply(ctx.guild.id, m.id, amount, "payout_role", reason or f"role:{role.name}", ctx.author.id)
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


@bot.command(name="economy-stats")
async def prefix_economy_stats(ctx: commands.Context):
    """!economy-stats — общая статистика экономики гильдии (стафф)."""
    if ctx.guild is None: return
    if not (isinstance(ctx.author, discord.Member) and member_is_staff(ctx.author)):
        await ctx.reply("\u274c Только стафф.", mention_author=False); return

    # Текущие балансы на руках (сумма всего, что сейчас у участников)
    cur = await DB.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM balances WHERE guild_id = ?",
        (ctx.guild.id,)
    )
    total_bank = int((await cur.fetchone())[0])

    # Всего начислено за всё время (сумма всех положительных delta)
    cur = await DB.execute(
        "SELECT COALESCE(SUM(delta), 0) FROM balance_events WHERE guild_id = ? AND delta > 0",
        (ctx.guild.id,)
    )
    total_awarded = int((await cur.fetchone())[0])

    # Всего участников с ненулевым балансом
    cur = await DB.execute(
        "SELECT COUNT(*) FROM balances WHERE guild_id = ? AND amount > 0",
        (ctx.guild.id,)
    )
    holders = int((await cur.fetchone())[0])

    embed = discord.Embed(
        title="\U0001f3e6  Economy Stats",
        color=0x1a1f3c
    )
    embed.add_field(
        name="\U0001f4b0 Total Awarded:",
        value=f"**{fmt_money(total_awarded)}**",
        inline=False
    )
    embed.add_field(
        name="\U0001f3e6 Total Bank:",
        value=f"**{fmt_money(total_bank)}**",
        inline=False
    )
    embed.add_field(
        name="\U0001f465 Holders:",
        value=f"**{holders}**",
        inline=False
    )
    await ctx.reply(embed=embed, mention_author=False)


# =========================
# MAIN
# =========================
def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN не найден. Проверьте .env")
    bot.run(token)


if __name__ == "__main__":
    main()