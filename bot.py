import os
import re
import io
import csv
import time
from typing import Optional, List, Tuple, Set

import discord
from discord import app_commands
from discord.ext import commands
import aiosqlite
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
load_dotenv()
DB_PATH = "cs_helper.db"

STATUS_OPEN = "open"
STATUS_CLOSED = "closed"

LEADERBOARD_PAGE_SIZE = 20  # Кол-во участников на странице лидерборда

# Медали для топ-3
MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

# Цвета для embed
COLOR_BLUE   = 0x5865F2   # Discord Blurple
COLOR_GREEN  = 0x57F287
COLOR_RED    = 0xED4245
COLOR_YELLOW = 0xFEE75C
COLOR_PURPLE = 0x9B59B6
COLOR_GOLD   = 0xF1C40F


# =========================
# DB
# =========================
async def db_init() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS contents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            message_id INTEGER,
            thread_id INTEGER,
            title TEXT NOT NULL,
            roles_text TEXT NOT NULL,
            after_text TEXT,
            ends_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_by INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            payout_role_id INTEGER,
            payout_role_name TEXT,
            hosted_by TEXT,
            start_ts INTEGER
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS content_assignments (
            content_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_index INTEGER NOT NULL,
            assigned_at INTEGER NOT NULL,
            PRIMARY KEY (content_id, user_id),
            UNIQUE (content_id, role_index)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS content_attendance (
            content_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            added_by INTEGER NOT NULL,
            added_at INTEGER NOT NULL,
            PRIMARY KEY (content_id, user_id)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS attendance_stats (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            all_time INTEGER NOT NULL DEFAULT 0,
            week INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS attendance_awards (
            guild_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            awarded_by INTEGER NOT NULL,
            awarded_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, content_id, user_id)
        );
        """)

        await db.execute("""
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

        await db.execute("""
        CREATE TABLE IF NOT EXISTS attendance_weekly_resets (
            guild_id INTEGER NOT NULL,
            reset_at INTEGER NOT NULL,
            reset_by INTEGER NOT NULL
        );
        """)

        # =========================
        # МИГРАЦИИ: добавляем колонки если их нет (совместимость со старой БД)
        # =========================
        migrations_contents = [
            "ALTER TABLE contents ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE contents ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE contents ADD COLUMN message_id INTEGER",
            "ALTER TABLE contents ADD COLUMN thread_id INTEGER",
            "ALTER TABLE contents ADD COLUMN after_text TEXT",
            "ALTER TABLE contents ADD COLUMN payout_role_id INTEGER",
            "ALTER TABLE contents ADD COLUMN payout_role_name TEXT",
            "ALTER TABLE contents ADD COLUMN hosted_by TEXT",
            "ALTER TABLE contents ADD COLUMN start_ts INTEGER",
        ]
        for sql in migrations_contents:
            try:
                await db.execute(sql)
            except Exception:
                pass  # колонка уже существует — игнорируем

        await db.commit()


async def db_create_content(
    guild_id: int,
    channel_id: int,
    title: str,
    roles_text: str,
    after_text: Optional[str],
    ends_at: int,
    created_by: int,
    hosted_by: Optional[str] = None,
    start_ts: Optional[int] = None,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO contents (guild_id, channel_id, title, roles_text, after_text, ends_at, status, created_by, created_at, hosted_by, start_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (guild_id, channel_id, title, roles_text, after_text, ends_at, STATUS_OPEN, created_by, int(time.time()), hosted_by, start_ts)
        )
        await db.commit()
        return cur.lastrowid


async def db_set_message_thread(content_id: int, message_id: int, thread_id: Optional[int]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE contents SET message_id = ?, thread_id = ? WHERE id = ?",
            (message_id, thread_id, content_id)
        )
        await db.commit()


async def db_get_content_by_id(content_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM contents WHERE id = ?", (content_id,))
        return await cur.fetchone()


async def db_get_content_by_thread(thread_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM contents WHERE thread_id = ?", (thread_id,))
        return await cur.fetchone()


async def db_close_content(content_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE contents SET status = ? WHERE id = ?", (STATUS_CLOSED, content_id))
        await db.commit()


async def db_set_payout_role(content_id: int, role_id: int, role_name: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE contents SET payout_role_id = ?, payout_role_name = ? WHERE id = ?",
            (role_id, role_name, content_id)
        )
        await db.commit()


# ---------- slots ----------
async def db_assign_user(content_id: int, user_id: int, role_index: int) -> Tuple[bool, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM content_assignments WHERE content_id = ? AND role_index = ?",
            (content_id, role_index)
        )
        row = await cur.fetchone()
        if row is not None and int(row[0]) != int(user_id):
            return False, "Роль занята."

        await db.execute(
            "DELETE FROM content_assignments WHERE content_id = ? AND user_id = ?",
            (content_id, user_id)
        )

        await db.execute(
            "INSERT INTO content_assignments (content_id, user_id, role_index, assigned_at) VALUES (?, ?, ?, ?)",
            (content_id, user_id, role_index, int(time.time()))
        )
        await db.commit()

    return True, f"Записан на роль {role_index}."


async def db_unassign_user(content_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "DELETE FROM content_assignments WHERE content_id = ? AND user_id = ?",
            (content_id, user_id)
        )
        await db.commit()
        return cur.rowcount > 0


async def db_unassign_by_role_index(content_id: int, role_index: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM content_assignments WHERE content_id = ? AND role_index = ?",
            (content_id, role_index)
        )
        row = await cur.fetchone()
        if row is None:
            return None

        user_id = int(row[0])
        await db.execute(
            "DELETE FROM content_assignments WHERE content_id = ? AND role_index = ?",
            (content_id, role_index)
        )
        await db.commit()
        return user_id


async def db_get_roster(content_id: int) -> List[Tuple[int, int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT role_index, user_id FROM content_assignments WHERE content_id = ? ORDER BY role_index ASC",
            (content_id,)
        )
        rows = await cur.fetchall()
        return [(int(r[0]), int(r[1])) for r in rows]


# ---------- attendance ----------
async def db_attend_add_many(content_id: int, user_ids: List[int], added_by: int) -> Tuple[int, int]:
    added = 0
    already = 0
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        for uid in user_ids:
            try:
                await db.execute(
                    "INSERT INTO content_attendance (content_id, user_id, added_by, added_at) VALUES (?, ?, ?, ?)",
                    (content_id, uid, added_by, now)
                )
                added += 1
            except aiosqlite.IntegrityError:
                already += 1
        await db.commit()
    return added, already


async def db_attend_remove(content_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "DELETE FROM content_attendance WHERE content_id = ? AND user_id = ?",
            (content_id, user_id)
        )
        await db.commit()
        return cur.rowcount > 0


async def db_attend_list(content_id: int) -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM content_attendance WHERE content_id = ? ORDER BY added_at ASC",
            (content_id,)
        )
        rows = await cur.fetchall()
        return [int(r[0]) for r in rows]


# ---------- union ----------
async def db_get_all_participants(content_id: int) -> List[int]:
    roster = await db_get_roster(content_id)
    attend = await db_attend_list(content_id)
    s: List[int] = []
    seen: Set[int] = set()
    for _, uid in roster:
        if uid not in seen:
            seen.add(uid)
            s.append(uid)
    for uid in attend:
        if uid not in seen:
            seen.add(uid)
            s.append(uid)
    return s


# =========================
# ATTENDANCE LEADERBOARD DB
# =========================
async def db_att_award_for_content(
    guild_id: int,
    content_id: int,
    user_ids: List[int],
    awarded_by: int,
) -> Tuple[int, int]:
    now = int(time.time())
    awarded = 0
    already = 0

    async with aiosqlite.connect(DB_PATH) as db:
        for uid in user_ids:
            await db.execute("""
                INSERT OR IGNORE INTO attendance_stats (guild_id, user_id, all_time, week, updated_at)
                VALUES (?, ?, 0, 0, ?)
            """, (guild_id, uid, now))

            try:
                await db.execute("""
                    INSERT INTO attendance_awards (guild_id, content_id, user_id, awarded_by, awarded_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (guild_id, content_id, uid, awarded_by, now))
            except aiosqlite.IntegrityError:
                already += 1
                continue

            await db.execute("""
                UPDATE attendance_stats
                SET all_time = all_time + 1,
                    week = week + 1,
                    updated_at = ?
                WHERE guild_id = ? AND user_id = ?
            """, (now, guild_id, uid))

            await db.execute("""
                INSERT INTO attendance_events (guild_id, user_id, content_id, delta, kind, actor_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (guild_id, uid, content_id, 1, "content_award", awarded_by, now))

            awarded += 1

        await db.commit()

    return awarded, already


async def db_att_remove_for_content(
    guild_id: int,
    content_id: int,
    user_id: int,
    removed_by: int,
) -> Tuple[bool, str]:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT 1 FROM attendance_awards
            WHERE guild_id = ? AND content_id = ? AND user_id = ?
        """, (guild_id, content_id, user_id))
        row = await cur.fetchone()
        if row is None:
            return False, "У пользователя нет аттенданса за этот контент."

        await db.execute("""
            DELETE FROM attendance_awards
            WHERE guild_id = ? AND content_id = ? AND user_id = ?
        """, (guild_id, content_id, user_id))

        await db.execute("""
            INSERT OR IGNORE INTO attendance_stats (guild_id, user_id, all_time, week, updated_at)
            VALUES (?, ?, 0, 0, ?)
        """, (guild_id, user_id, now))

        await db.execute("""
            UPDATE attendance_stats
            SET all_time = CASE WHEN all_time > 0 THEN all_time - 1 ELSE 0 END,
                week = CASE WHEN week > 0 THEN week - 1 ELSE 0 END,
                updated_at = ?
            WHERE guild_id = ? AND user_id = ?
        """, (now, guild_id, user_id))

        await db.execute("""
            INSERT INTO attendance_events (guild_id, user_id, content_id, delta, kind, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (guild_id, user_id, content_id, -1, "content_remove", removed_by, now))

        await db.commit()

    return True, "Аттенданс удалён."


async def db_att_leaderboard_full(guild_id: int, scope: str = "week") -> List[Tuple[int, int]]:
    """Возвращает полный лидерборд (все игроки у которых score > 0)."""
    col = "week" if scope == "week" else "all_time"
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            SELECT user_id, {col}
            FROM attendance_stats
            WHERE guild_id = ? AND {col} > 0
            ORDER BY {col} DESC, updated_at ASC
        """, (guild_id,))
        rows = await cur.fetchall()
        return [(int(r[0]), int(r[1])) for r in rows]


async def db_att_get_user(guild_id: int, user_id: int) -> Tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT all_time, week FROM attendance_stats
            WHERE guild_id = ? AND user_id = ?
        """, (guild_id, user_id))
        row = await cur.fetchone()
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])


async def db_att_weekly_reset(guild_id: int, reset_by: int) -> Tuple[int, List[Tuple[int, int]]]:
    now = int(time.time())
    snapshot_top = await db_att_leaderboard_full(guild_id, scope="week")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT COUNT(*) FROM attendance_stats
            WHERE guild_id = ? AND week > 0
        """, (guild_id,))
        cnt_row = await cur.fetchone()
        affected = int(cnt_row[0]) if cnt_row else 0

        await db.execute("""
            UPDATE attendance_stats
            SET week = 0, updated_at = ?
            WHERE guild_id = ?
        """, (now, guild_id))

        await db.execute("""
            INSERT INTO attendance_weekly_resets (guild_id, reset_at, reset_by)
            VALUES (?, ?, ?)
        """, (guild_id, now, reset_by))

        await db.execute("""
            INSERT INTO attendance_events (guild_id, user_id, content_id, delta, kind, actor_id, created_at)
            VALUES (?, ?, NULL, 0, 'weekly_reset', ?, ?)
        """, (guild_id, reset_by, reset_by, now))

        await db.commit()

    return affected, snapshot_top


# =========================
# HELPERS
# =========================
def ts_discord(unix_ts: int, fmt: str = "F") -> str:
    return f"<t:{unix_ts}:{fmt}>"


import datetime

def parse_utc_time(time_str: str) -> Optional[int]:
    """
    Парсит строку "ЧЧ:ММ" как время в UTC сегодня и возвращает unix timestamp.
    Если время уже прошло — берёт следующий день.
    """
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


def is_organizer_or_admin(member: discord.Member, created_by: int) -> bool:
    if member.id == created_by:
        return True
    perms = member.guild_permissions
    return perms.administrator or perms.manage_guild or perms.manage_roles


MENTION_ID_RE = re.compile(r"<@!?(\d+)>")


def parse_user_ids_from_text(text: str) -> List[int]:
    ids: List[int] = []
    for m in MENTION_ID_RE.finditer(text):
        ids.append(int(m.group(1)))
    seen = set()
    out = []
    for uid in ids:
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


# _ends_at_line удалена (таймер дедлайна убран)


def _deadline_expired(ends_at: int) -> bool:
    return bool(ends_at and ends_at > 0 and int(time.time()) > int(ends_at))


def build_main_post_embed(
    content_id: int,
    title: str,
    status: str,
    ends_at: int,
    message_id: int,
    thread_id: Optional[int],
    roles_lines: List[str],
    roster: List[Tuple[int, int]],
    attendance_only: List[int],
    after_text: Optional[str],
    hosted_by: Optional[str] = None,
    start_ts: Optional[int] = None,
) -> discord.Embed:
    by_index = {idx: uid for idx, uid in roster}
    participants_count = len({uid for _, uid in roster}) + len(attendance_only)
    expired = _deadline_expired(int(ends_at))

    color = COLOR_GREEN if status == STATUS_OPEN and not expired else COLOR_RED

    embed = discord.Embed(
        title=f"📋 Контент #{content_id}: {title}",
        color=color
    )

    # Be ready by — время старта
    if start_ts and start_ts > 0:
        import datetime as _dt
        utc_dt = _dt.datetime.fromtimestamp(start_ts, tz=_dt.timezone.utc)
        utc_str = utc_dt.strftime("%H:%M UTC")
        # <t:...:F> = локальная дата/время зрителя, <t:...:R> = через сколько
        ready_val = f"{utc_str} • {ts_discord(start_ts, 'F')} • {ts_discord(start_ts, 'R')}"
        embed.add_field(name="⏰ Be ready by", value=ready_val, inline=False)

    # Организатор
    if hosted_by:
        embed.add_field(name="👤 Content hosted by", value=hosted_by, inline=False)

    # Статус
    status_str = "🟢 Open" if status == STATUS_OPEN else "🔴 Closed"
    if expired and status == STATUS_OPEN:
        status_str = "🟡 Timer expired"
    embed.add_field(name="Status", value=status_str, inline=True)
    embed.add_field(name="Players", value=f"**{participants_count}**", inline=True)
    embed.add_field(name="ID", value=f"`{message_id}`", inline=True)

    # Таймер дедлайна убран

    # Ветка
    if thread_id:
        embed.add_field(name="💬 Thread", value=f"<#{thread_id}>", inline=True)

    # Роли — все слоты, занятые с тегом, свободные просто с тире
    roles_lines_fmt = []
    for i, role_name in enumerate(roles_lines, start=1):
        uid = by_index.get(i)
        if uid:
            roles_lines_fmt.append(f"`{i}.` {role_name} — <@{uid}>")
        else:
            roles_lines_fmt.append(f"`{i}.` {role_name} —")

    # Разбиваем на чанки если ролей много (лимит поля embed 1024 символа)
    chunk = []
    chunk_size = 0
    field_num = 0
    for line in roles_lines_fmt:
        if chunk_size + len(line) + 1 > 950 and chunk:
            embed.add_field(
                name="📝 Roles" if field_num == 0 else "\u200b",
                value="\n".join(chunk),
                inline=False
            )
            chunk = []
            chunk_size = 0
            field_num += 1
        chunk.append(line)
        chunk_size += len(line) + 1

    if chunk:
        embed.add_field(
            name="📝 Roles" if field_num == 0 else "\u200b",
            value="\n".join(chunk),
            inline=False
        )

    # Дополнительные участники
    if attendance_only:
        start = len(roles_lines) + 1
        att_lines = [f"`{j}.` <@{uid}>" for j, uid in enumerate(attendance_only, start=start)]
        embed.add_field(name="➕ Additionally", value="\n".join(att_lines[:20]), inline=False)

    # Примечание
    if after_text and after_text.strip():
        embed.add_field(name="📌 Note", value=after_text.strip(), inline=False)

    # Инструкция
    embed.add_field(
        name="📖 В ветке",
        value="➣ `1` — занять роль (если свободна)\n➣ `-` — выписаться с роли",
        inline=False
    )

    return embed


async def refresh_main_post(guild: discord.Guild, content_row) -> None:
    try:
        channel = guild.get_channel(int(content_row["channel_id"]))
        if not isinstance(channel, discord.TextChannel):
            return

        msg = await channel.fetch_message(int(content_row["message_id"]))
        roles_lines = normalize_roles_lines(str(content_row["roles_text"]))
        roster = await db_get_roster(int(content_row["id"]))
        attend = await db_attend_list(int(content_row["id"]))

        roster_uids = {uid for _, uid in roster}
        attendance_only = [uid for uid in attend if uid not in roster_uids]

        embed = build_main_post_embed(
            content_id=int(content_row["id"]),
            title=str(content_row["title"]),
            status=str(content_row["status"]),
            ends_at=int(content_row["ends_at"]),
            message_id=int(content_row["message_id"]),
            thread_id=int(content_row["thread_id"]) if content_row["thread_id"] else None,
            roles_lines=roles_lines,
            roster=roster,
            attendance_only=attendance_only,
            after_text=str(content_row["after_text"]) if content_row["after_text"] else None,
            hosted_by=str(content_row["hosted_by"]) if content_row["hosted_by"] else None,
            start_ts=int(content_row["start_ts"]) if content_row["start_ts"] else None,
        )

        await msg.edit(content=None, embed=embed)
    except Exception:
        return


def parse_thread_command(text: str) -> Tuple[str, Optional[int]]:
    t = text.strip()

    if t == "" or t.lower() in ("help",):
        return "help", None

    if t == "-":
        return "self_leave", None

    if t.isdigit():
        return "self_join", int(t)

    m = re.match(r"^\+(\d+)\s+.+$", t)
    if m:
        return "org_assign_slot", int(m.group(1))

    if t.startswith("+"):
        return "org_attend_add", None

    m = re.match(r"^-(\d+)$", t)
    if m:
        return "org_kick_role", int(m.group(1))

    if t.startswith("-"):
        return "org_kick_user", None

    return "unknown", None


# =========================
# LEADERBOARD PAGINATION VIEW
# =========================
class LeaderboardView(discord.ui.View):
    def __init__(
        self,
        rows: List[Tuple[int, int]],
        scope: str,
        guild: discord.Guild,
        requester_id: int,
        my_all: int,
        my_week: int,
    ):
        super().__init__(timeout=300)
        self.rows = rows
        self.scope = scope
        self.guild = guild
        self.requester_id = requester_id
        self.my_all = my_all
        self.my_week = my_week
        self.page = 0
        self.total_pages = max(1, (len(rows) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages - 1
        self.page_label.label = f"{self.page + 1} / {self.total_pages}"

    def build_embed(self) -> discord.Embed:
        scope_label = "за неделю" if self.scope == "week" else "за всё время"
        emoji_scope = "📅" if self.scope == "week" else "🏆"

        embed = discord.Embed(
            title=f"{emoji_scope} Лидерборд посещаемости ({scope_label})",
            description=f"Топ участников {scope_label}:",
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

        my_score = self.my_week if self.scope == "week" else self.my_all
        embed.set_footer(text=f"Ваш результат: {my_score}  •  Страница {self.page + 1}/{self.total_pages}")
        return embed

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.total_pages - 1, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


# =========================
# WEEKLY RESET PAGINATION VIEW
# =========================
class WeeklyResetView(discord.ui.View):
    def __init__(self, rows: List[Tuple[int, int]], guild: discord.Guild, affected: int):
        super().__init__(timeout=300)
        self.rows = rows
        self.guild = guild
        self.affected = affected
        self.page = 0
        self.total_pages = max(1, (len(rows) + LEADERBOARD_PAGE_SIZE - 1) // LEADERBOARD_PAGE_SIZE)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages - 1
        self.page_label.label = f"{self.page + 1} / {self.total_pages}"

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="📊 Итоги недели",
            description=f"Топ посещаемости за прошедшую неделю:\n*(обнулено: {self.affected} участников)*",
            color=COLOR_PURPLE
        )

        start_idx = self.page * LEADERBOARD_PAGE_SIZE
        page_rows = self.rows[start_idx:start_idx + LEADERBOARD_PAGE_SIZE]

        if not page_rows:
            embed.add_field(name="\u200b", value="_Пока пусто_", inline=False)
        else:
            lines = []
            for i, (uid, cnt) in enumerate(page_rows, start=start_idx + 1):
                medal = MEDALS.get(i, "🎖️" if i <= 10 else "▫️")
                member = self.guild.get_member(uid)
                name = member.display_name if member else f"<@{uid}>"
                lines.append(f"{medal} **{i}.** {name} **·** {cnt}")
            embed.add_field(name="🏅 Топ посещаемых игроков", value="\n".join(lines), inline=False)

        embed.set_footer(text=f"Страница {self.page + 1}/{self.total_pages}")
        return embed

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.total_pages - 1, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


# =========================
# BOT
# =========================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)


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
    title_text = discord.ui.TextInput(
        label="Заголовок",
        placeholder="Например: пути неисповедимы",
        max_length=150
    )
    roles_text = discord.ui.TextInput(
        label="Роли (каждая строка = слот)",
        placeholder="Танк\nХил\nДД\nДД\nДД\nСтоп",
        style=discord.TextStyle.paragraph,
        max_length=1500
    )
    start_time_input = discord.ui.TextInput(
        label="Время старта в UTC (ЧЧ:ММ)",
        placeholder="Например: 18:35",
        required=True,
        max_length=5
    )
    after_text = discord.ui.TextInput(
        label="Примечание (опционально)",
        placeholder="Например: /join NickName",
        required=False,
        max_length=900
    )
    thread_name = discord.ui.TextInput(
        label="Имя ветки (опционально)",
        placeholder="Если пусто — будет как заголовок",
        required=False,
        max_length=100
    )

    def __init__(self, duration_minutes: int, create_thread: bool, auto_assign_organizer: bool = True):
        super().__init__()
        self.duration_minutes = duration_minutes
        self.create_thread = create_thread
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

        # Парсим время старта в UTC
        start_ts = parse_utc_time(str(self.start_time_input).strip())
        if start_ts is None:
            await interaction.followup.send(
                "❌ Неверный формат времени. Используй ЧЧ:ММ, например `18:35`.",
                ephemeral=True
            )
            return

        ends_at = 0  # таймер дедлайна убран

        title = str(self.title_text).strip()
        after = str(self.after_text).strip() if str(self.after_text).strip() else None
        # Организатор = автоматически тег создателя
        hosted_by = f"<@{interaction.user.id}>"

        content_id = await db_create_content(
            guild_id=interaction.guild_id,
            channel_id=channel.id,
            title=title,
            roles_text="\n".join(roles_lines),
            after_text=after,
            ends_at=ends_at,
            created_by=interaction.user.id,
            hosted_by=hosted_by,
            start_ts=start_ts,
        )

        # Временная заглушка
        msg = await channel.send(content=f"**Контент #{content_id}: {title}**\nСоздание…")
        message_id = msg.id

        thread_id = None
        if self.create_thread:
            try:
                tname = str(self.thread_name).strip() if str(self.thread_name).strip() else f"{title} (CS#{content_id})"
                thread = await msg.create_thread(name=tname, auto_archive_duration=1440)
                thread_id = thread.id
                thread_embed = discord.Embed(
                    title="📖 Инструкция",
                    description=(
                        "**Как записаться:**\n"
                        "➣ Напишите **цифру** чтобы занять соответствующую роль\n"
                        "➣ Напишите `-` чтобы выписаться\n\n"
                        "Если запись закрыта — обратитесь к организатору."
                    ),
                    color=COLOR_BLUE
                )
                await thread.send(embed=thread_embed)
            except discord.Forbidden:
                thread_id = None

        await db_set_message_thread(content_id, message_id, thread_id)

        if self.auto_assign_organizer and len(roles_lines) >= 1:
            await db_assign_user(content_id, interaction.user.id, 1)

        row = await db_get_content_by_id(content_id)
        if row and interaction.guild:
            await refresh_main_post(interaction.guild, row)

        await interaction.followup.send(f"✅ Контент создан: **CS#{content_id}**", ephemeral=True)


class AttendAddModal(discord.ui.Modal, title="Добавить людей"):
    def __init__(self, default_content_id: Optional[int] = None):
        super().__init__()
        self.content_id_input = discord.ui.TextInput(
            label="Content ID",
            placeholder="Например: 12",
            required=True,
            max_length=10,
            default=str(default_content_id) if default_content_id is not None else None
        )
        self.users_input = discord.ui.TextInput(
            label="Пользователи",
            placeholder="@user1 @user2 @user3",
            style=discord.TextStyle.paragraph,
            max_length=2000
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

        role = None
        if row["payout_role_id"]:
            role = interaction.guild.get_role(int(row["payout_role_id"]))

        assigned = 0
        failed = 0
        if role is not None:
            for uid in user_ids:
                m = interaction.guild.get_member(uid)
                if m is None:
                    try:
                        m = await interaction.guild.fetch_member(uid)
                    except Exception:
                        failed += 1
                        continue
                try:
                    await m.add_roles(role, reason=f"CS add_ppl content {content_id}")
                    assigned += 1
                except Exception:
                    failed += 1

        if interaction.guild:
            row2 = await db_get_content_by_id(content_id)
            if row2:
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
    if message.author.bot:
        return
    if message.guild is None:
        return
    if not isinstance(message.channel, discord.Thread):
        return

    content = await db_get_content_by_thread(message.channel.id)
    if content is None:
        return

    cmd = message.content.strip()
    if not (cmd.isdigit() or cmd in ("-", "help") or cmd.startswith("+") or cmd.startswith("-")):
        return

    content_id = int(content["id"])
    ends_at = int(content["ends_at"])
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
        await message.add_reaction("✅") if removed else await message.add_reaction("ℹ️")
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "help":
        help_embed = discord.Embed(
            title="📖 Команды в ветке",
            color=COLOR_BLUE,
            description=(
                "`<цифра>` — занять роль с указанным номером\n"
                "`-` — выписаться со своей роли\n\n"
                "Если самозапись закрыта — обратитесь к организатору контента."
            )
        )
        await message.reply(embed=help_embed, mention_author=False)
        return

    if status != STATUS_OPEN and not is_org:
        await message.reply("🔴 Запись закрыта.", mention_author=False)
        return

    if kind == "self_join" and _deadline_expired(ends_at) and not is_org:
        await message.reply("⏰ Время записи истекло. Обратитесь к организатору.", mention_author=False)
        await _try_delete_command(message)
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
            await message.reply("Недостаточно прав.", mention_author=False)
            return
        user_ids = [m.id for m in message.mentions]
        if not user_ids:
            await message.reply("Формат: `+ @user @user ...`", mention_author=False)
            return
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
            await message.reply("Недостаточно прав.", mention_author=False)
            return
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False)
            return
        if not message.mentions:
            await message.reply("Формат: `+2 @user`", mention_author=False)
            return
        target = message.mentions[0]
        ok, txt = await db_assign_user(content_id, target.id, int(num))
        await message.reply(f"{target.mention}: {txt}", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_kick_role":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False)
            return
        if num is None or num < 1 or num > max_slot:
            await message.reply(f"Неверный номер. Допустимо: 1..{max_slot}", mention_author=False)
            return
        kicked = await db_unassign_by_role_index(content_id, int(num))
        if kicked is None:
            await message.reply("Роль свободна.", mention_author=False)
        else:
            await message.reply(f"Выписано с роли {num}: <@{kicked}>", mention_author=False)
        row2 = await db_get_content_by_id(content_id)
        if row2:
            await refresh_main_post(message.guild, row2)
        await _try_delete_command(message)
        return

    if kind == "org_kick_user":
        if not is_org:
            await message.reply("Недостаточно прав.", mention_author=False)
            return
        if not message.mentions:
            await message.reply("Формат: `- @user`", mention_author=False)
            return
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
@bot.tree.command(name="healthcheck", description="Проверка статуса бота и базы данных")
async def healthcheck(interaction: discord.Interaction):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("SELECT 1")
        embed = discord.Embed(title="✅ Статус бота", description="Бот онлайн, БД доступна.", color=COLOR_GREEN)
        await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        embed = discord.Embed(title="❌ Ошибка", description=str(e), color=COLOR_RED)
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="content_create", description="Создать контент через форму")
@app_commands.describe(
    duration_minutes="Время записи в минутах (0 = без таймера)",
    create_thread="Создать ветку автоматически"
)
async def content_create(interaction: discord.Interaction, duration_minutes: int = 180, create_thread: bool = True):
    if duration_minutes < 0 or duration_minutes > 24 * 60:
        await interaction.response.send_message("duration_minutes должно быть в диапазоне 0..1440.", ephemeral=True)
        return
    await interaction.response.send_modal(ContentCreateModal(duration_minutes=duration_minutes, create_thread=create_thread))


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
        await interaction.response.send_message("Контент не найден.", ephemeral=True)
        return
    await db_close_content(content_id)
    row2 = await db_get_content_by_id(content_id)
    if row2 and interaction.guild:
        await refresh_main_post(interaction.guild, row2)
    embed = discord.Embed(title="🔴 Контент закрыт", description=f"Контент **#{content_id}** закрыт.", color=COLOR_RED)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="role_from_content", description="Создать роль и выдать всем участникам")
@app_commands.describe(content_id="ID контента", role_name="Название роли (по умолчанию = заголовок)")
async def role_from_content(interaction: discord.Interaction, content_id: int, role_name: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)

    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.")
        return

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("Guild недоступен.")
        return

    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.")
        return

    base_name = role_name.strip() if role_name and role_name.strip() else str(row["title"])
    final_role_name = f"{base_name} [CS#{content_id}]"

    try:
        role = await guild.create_role(name=final_role_name, reason=f"CS payout role for content {content_id}")
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на создание роли.")
        return

    failed = []
    assigned = 0
    for uid in user_ids:
        member = guild.get_member(uid)
        if member is None:
            try:
                member = await guild.fetch_member(uid)
            except Exception:
                failed.append(uid)
                continue
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
        embed.add_field(name="Не удалось", value=", ".join([f"<@{u}>" for u in failed[:10]]), inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="role_clear", description="Удалить payout роль контента")
@app_commands.describe(content_id="ID контента")
async def role_clear(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)

    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.")
        return

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("Guild недоступен.")
        return

    role_id = row["payout_role_id"]
    role_name = row["payout_role_name"]

    role = None
    if role_id:
        role = guild.get_role(int(role_id))
    if role is None and role_name:
        role = discord.utils.get(guild.roles, name=str(role_name))

    if role is None:
        await interaction.followup.send("Роль не найдена.")
        return

    try:
        await role.delete(reason=f"CS payout role cleanup for content {content_id}")
        embed = discord.Embed(title="🗑️ Роль удалена", description=f"`{role.name}`", color=COLOR_RED)
        await interaction.followup.send(embed=embed)
    except discord.Forbidden:
        await interaction.followup.send("Нет прав на удаление роли.")


@bot.tree.command(name="att_add", description="Начислить аттенданс за контент всем участникам")
@app_commands.describe(content_id="ID контента")
async def att_add(interaction: discord.Interaction, content_id: int):
    await interaction.response.defer(ephemeral=True)

    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True)
        return

    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True)
        return

    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True)
        return

    user_ids = await db_get_all_participants(content_id)
    if not user_ids:
        await interaction.followup.send("Нет участников.", ephemeral=True)
        return

    awarded, already = await db_att_award_for_content(
        guild_id=int(interaction.guild_id),
        content_id=int(content_id),
        user_ids=user_ids,
        awarded_by=int(interaction.user.id),
    )

    embed = discord.Embed(title="✅ Аттенданс начислен", color=COLOR_GREEN)
    embed.add_field(name="Начислено", value=str(awarded), inline=True)
    embed.add_field(name="Уже было", value=str(already), inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="att_stats", description="Лидерборд посещаемости")
@app_commands.describe(scope="Период: week (неделя) или all_time (всё время)")
@app_commands.choices(scope=[
    app_commands.Choice(name="За неделю", value="week"),
    app_commands.Choice(name="Всё время", value="all_time"),
])
async def att_stats(interaction: discord.Interaction, scope: str = "week"):
    await interaction.response.defer(ephemeral=False)

    if interaction.guild_id is None or interaction.guild is None:
        await interaction.followup.send("Guild недоступен.")
        return

    rows = await db_att_leaderboard_full(int(interaction.guild_id), scope=scope)
    my_all, my_week = await db_att_get_user(int(interaction.guild_id), int(interaction.user.id))

    view = LeaderboardView(
        rows=rows,
        scope=scope,
        guild=interaction.guild,
        requester_id=interaction.user.id,
        my_all=my_all,
        my_week=my_week,
    )

    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="att_remove", description="Удалить аттенданс за конкретный контент у пользователя")
@app_commands.describe(content_id="ID контента", user="Пользователь")
async def att_remove(interaction: discord.Interaction, content_id: int, user: discord.Member):
    await interaction.response.defer(ephemeral=True)

    row = await db_get_content_by_id(content_id)
    if row is None:
        await interaction.followup.send("Контент не найден.", ephemeral=True)
        return

    if interaction.guild is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True)
        return

    if not is_organizer_or_admin(member, int(row["created_by"])):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True)
        return

    ok, msg_text = await db_att_remove_for_content(
        guild_id=int(interaction.guild_id),
        content_id=int(content_id),
        user_id=int(user.id),
        removed_by=int(interaction.user.id),
    )
    color = COLOR_GREEN if ok else COLOR_RED
    embed = discord.Embed(description=msg_text, color=color)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="att_week_reset", description="Показать итоги недели и обнулить weekly аттенданс")
async def att_week_reset(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)

    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.")
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.")
        return
    perms = member.guild_permissions
    if not (perms.administrator or perms.manage_guild):
        await interaction.followup.send("Недостаточно прав.")
        return

    affected, snapshot_top = await db_att_weekly_reset(int(interaction.guild_id), int(interaction.user.id))

    view = WeeklyResetView(rows=snapshot_top, guild=interaction.guild, affected=affected)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="att_export_csv", description="Выгрузить аттенданс в CSV файл (перед обнулением)")
@app_commands.describe(scope="Период: week (неделя) или all_time (всё время)")
@app_commands.choices(scope=[
    app_commands.Choice(name="За неделю", value="week"),
    app_commands.Choice(name="Всё время", value="all_time"),
])
async def att_export_csv(interaction: discord.Interaction, scope: str = "week"):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild недоступен.", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.followup.send("Не удалось получить данные участника.", ephemeral=True)
        return
    perms = member.guild_permissions
    if not (perms.administrator or perms.manage_guild or perms.manage_roles):
        await interaction.followup.send("Недостаточно прав.", ephemeral=True)
        return

    rows = await db_att_leaderboard_full(int(interaction.guild_id), scope=scope)

    if not rows:
        await interaction.followup.send("Нет данных для экспорта.", ephemeral=True)
        return

    # Собираем CSV в памяти
    output = io.StringIO()
    writer = csv.writer(output)
    scope_col = "week" if scope == "week" else "all_time"
    writer.writerow(["Место", "User ID", "Никнейм", "Отображаемое имя", scope_col])

    for i, (uid, cnt) in enumerate(rows, start=1):
        guild_member = interaction.guild.get_member(uid)
        username = guild_member.name if guild_member else "Неизвестно"
        display_name = guild_member.display_name if guild_member else f"ID:{uid}"
        writer.writerow([i, uid, username, display_name, cnt])

    output.seek(0)
    csv_bytes = output.getvalue().encode("utf-8-sig")  # utf-8-sig для корректного открытия в Excel
    file = discord.File(fp=io.BytesIO(csv_bytes), filename=f"attendance_{scope}_{int(time.time())}.csv")

    scope_label = "за неделю" if scope == "week" else "за всё время"
    embed = discord.Embed(
        title="📥 Экспорт аттенданса",
        description=f"Аттенданс {scope_label} — **{len(rows)}** участников.",
        color=COLOR_BLUE
    )
    embed.set_footer(text="Файл сохранён. После сохранения можно делать сброс.")

    await interaction.followup.send(embed=embed, file=file, ephemeral=True)


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