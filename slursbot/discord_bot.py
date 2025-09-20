# discord_bot.py — OZF lookups with paginated embeds and per-command time windows
#
# Commands:
#   !<ozfid> [words…] [p=<page>] [s=<window>]
#   !team <id> [words…] [p=<page>] [s=<window>]
#   !t<id> / !teams <id> … (aliases)
#
# Window formats for s=<window> (or loose token): Nd / Nw / Nm / Ny   (180d, 6m, 2w, 1y)
#
# Env:
#   BOT_TOKEN=...                         (required)
#   ALLOWED_CHANNEL_IDS=123,456           (optional; comma/semicolon separated)
#   PAGE_SIZE=20                          (optional; messages per page; default 20)

from __future__ import annotations

import os
import re
import logging
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta, timezone

import requests
import discord
from discord import ui

# ---------- NEW: load env via env_loader (public -> secrets) ----------
from env_loader import load as load_env
load_env()

import db  # your existing db.get_conn()

# ---------- logging ----------
logger = logging.getLogger("slursbot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger.info("discord.py version: %s", getattr(discord, "__version__", "unknown"))

# ---------- env ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set in .env")

ALLOWED_CHANNEL_IDS: List[int] = [
    int(x) for x in os.getenv("ALLOWED_CHANNEL_IDS", "").replace(";", ",").split(",") if x.strip().isdigit()
]

def _int_env(key: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(os.getenv(key, str(default)))
        return max(lo, min(hi, v))
    except Exception:
        return default

PAGE_SIZE = _int_env("PAGE_SIZE", 20, 5, 100)  # messages per page

# ---------- constants ----------
TEXT_SNIPPET_LIMIT   = 160
ABS_MAX_TOTAL_LINES  = 200000  # safety guard
HTTP_HEADERS = {
    "User-Agent": "slursbot/2.1 (+ozfortress command bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://ozfortress.com/",
}
RE_TEAM_MEMBER = re.compile(r'href="/users/(\d+)"[^>]*>([^<]+)</a>', re.I)

# ---------- discord client ----------
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# ---------- utils ----------
def _escape(s: str) -> str:
    return s.replace("`", "'").replace("*", "\\*").replace("_", "\\_").replace("~~", "~")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _parse_window_to_days(token: str) -> Optional[int]:
    """Accepts Nd, Nw, Nm, Ny (case-insensitive). Returns day count int, or None."""
    m = re.fullmatch(r"(?i)(\d+)\s*([d w m y])", token.replace(" ", ""))
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    if unit == "d": return n
    if unit == "w": return n * 7
    if unit == "m": return n * 30
    if unit == "y": return n * 365
    return None

def _cutoff_from_days(days: Optional[int]) -> Optional[datetime]:
    if not days or days <= 0:
        return None
    return _now_utc() - timedelta(days=days)

def _chunks(lst: List, size: int) -> List[List]:
    return [lst[i:i + size] for i in range(0, len(lst), max(1, size))] or [[]]

async def safe_send_content(message: discord.Message, content: str) -> None:
    allowed = discord.AllowedMentions.none()
    if len(content) > 1900:
        content = content[:1900] + "\n…"
    try:
        await message.channel.send(content, allowed_mentions=allowed)
    except Exception as e:
        logger.warning("discord send failed: %s", e)

# ---------- DB helpers ----------
def get_player_by_ozid(conn, oz_id: int) -> Optional[Dict]:
    """oz_id -> {oz_id, steamid64, current_name} using your tables."""
    with conn.cursor() as cur:
        # Primary
        try:
            cur.execute(
                """
                SELECT TOP (1) steamid64, current_name
                FROM kian.oz.players
                WHERE oz_id = ?
                ORDER BY CONVERT(datetime2(0), updated_at) DESC
                """,
                oz_id,
            )
            row = cur.fetchone()
            if row and row[0]:
                try:
                    sid = int(row[0])
                except Exception:
                    sid = int(str(row[0]).strip())
                return {"oz_id": oz_id, "steamid64": sid, "current_name": row[1]}
        except Exception as e:
            logger.debug("players lookup failed (oz_id=%s): %s", oz_id, e)

        # Fallback
        try:
            cur.execute(
                """
                SELECT TOP (1) steamid64_bigint, current_name
                FROM kian.oz.v_players_clean
                WHERE oz_id = ?
                ORDER BY CONVERT(datetime2(0), updated_at) DESC
                """,
                oz_id,
            )
            row = cur.fetchone()
            if row and row[0]:
                return {"oz_id": oz_id, "steamid64": int(row[0]), "current_name": row[1]}
        except Exception as e:
            logger.debug("v_players_clean lookup failed (oz_id=%s): %s", oz_id, e)
    return None

def _cutoff_sql(view_col: str, since_days: Optional[int]) -> Tuple[str, List]:
    if since_days and since_days > 0:
        return f" AND {view_col} >= ? ", [_cutoff_from_days(since_days)]
    return "", []

def fetch_messages_player(conn, steam64: int, contains: Optional[str], since_days: Optional[int]) -> List[Dict]:
    """Fetch ALL messages for one player, optionally filtered by text and time window."""
    text_sql = ""
    params_v: List = [int(steam64)]
    if contains:
        text_sql = " AND LOWER([text]) LIKE ?"
        params_v.append(f"%{contains.lower()}%")
    cutoff_sql_v, cutoff_params_v = _cutoff_sql("msg_time_utc_dt2", since_days)
    params_v += cutoff_params_v

    sql_view = f"""
        SELECT
            msg_time_utc_dt2 AS msg_time_utc,
            [text],
            logid
        FROM kiancat.dbo.v_slurs_msg_safe
        WHERE steamid64 = ?{text_sql}{cutoff_sql_v}
        ORDER BY msg_time_utc_dt2 DESC, hash_key DESC
    """

    # Fallback (cast)
    params_c: List = [int(steam64)]
    if contains:
        params_c.append(f"%{contains.lower()}%")
    cutoff_sql_c, cutoff_params_c = _cutoff_sql("CONVERT(datetime2(0), msg_time_utc)", since_days)
    params_c += cutoff_params_c

    sql_cast = f"""
        SELECT
            CONVERT(datetime2(0), msg_time_utc) AS msg_time_utc,
            [text],
            logid
        FROM kiancat.dbo.slurs_msg
        WHERE steamid64 = ?{text_sql}{cutoff_sql_c}
        ORDER BY msg_time_utc DESC, hash_key DESC
    """

    rows: List[Dict] = []
    with conn.cursor() as cur:
        try:
            cur.execute(sql_view, params_v)
        except Exception:
            cur.execute(sql_cast, params_c)
        for r in cur.fetchall():
            rows.append({"utc": r[0], "text": r[1], "logid": r[2]})
    return rows[:ABS_MAX_TOTAL_LINES]

def fetch_team_members(team_id: int, timeout: int = 20) -> List[Dict]:
    """Scrape https://ozfortress.com/teams/<team_id> to list players on that team."""
    url = f"https://ozfortress.com/teams/{team_id}"
    r = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    html = r.text or ""
    seen = set()
    members: List[Dict] = []
    for m in RE_TEAM_MEMBER.finditer(html):
        oz = int(m.group(1))
        if oz in seen:
            continue
        seen.add(oz)
        name = re.sub(r"\s+", " ", m.group(2)).strip()
        members.append({"oz_id": oz, "name": name, "url": f"https://ozfortress.com/users/{oz}"})
    return members

def resolve_team_players(conn, team_members: List[Dict]) -> Tuple[List[int], Dict[int, Dict]]:
    """Map scraped team members (oz_id) -> steamid64/name/url for fast formatting."""
    steamids: List[int] = []
    idx: Dict[int, Dict] = {}
    for m in team_members:
        oz = int(m["oz_id"])
        rec = get_player_by_ozid(conn, oz)
        if rec and rec.get("steamid64"):
            sid = int(rec["steamid64"])
            steamids.append(sid)
            idx[sid] = {
                "oz_id": oz,
                "oz_url": f"https://ozfortress.com/users/{oz}",
                "name": rec.get("current_name") or m.get("name") or f"OZF {oz}",
            }
    steamids = sorted(set(steamids))
    return steamids, idx

def fetch_messages_team(conn, steamids: List[int], contains: Optional[str], since_days: Optional[int]) -> List[Dict]:
    """Fetch ALL messages across steamids, optionally filtered by text and time window."""
    if not steamids:
        return []
    placeholders = ",".join("?" * len(steamids))
    params_v: List = list(map(int, steamids))
    contains_sql = ""
    if contains:
        contains_sql = " AND LOWER([text]) LIKE ? "
        params_v.append(f"%{contains.lower()}%")
    cutoff_sql_v, cutoff_params_v = _cutoff_sql("msg_time_utc_dt2", since_days)
    params_v += cutoff_params_v

    sql_view = f"""
        SELECT
            msg_time_utc_dt2 AS msg_time_utc,
            steamid64,
            [text],
            logid
        FROM kiancat.dbo.v_slurs_msg_safe
        WHERE steamid64 IN ({placeholders}) {contains_sql} {cutoff_sql_v}
        ORDER BY msg_time_utc_dt2 DESC, hash_key DESC
    """

    params_c: List = list(map(int, steamids))
    if contains:
        params_c.append(f"%{contains.lower()}%")
    cutoff_sql_c, cutoff_params_c = _cutoff_sql("CONVERT(datetime2(0), msg_time_utc)", since_days)
    params_c += cutoff_params_c

    sql_cast = f"""
        SELECT
            CONVERT(datetime2(0), msg_time_utc) AS msg_time_utc,
            steamid64,
            [text],
            logid
        FROM kiancat.dbo.slurs_msg
        WHERE steamid64 IN ({placeholders}) {contains_sql} {cutoff_sql_c}
        ORDER BY msg_time_utc DESC, hash_key DESC
    """

    rows: List[Dict] = []
    with conn.cursor() as cur:
        try:
            cur.execute(sql_view, params_v)
        except Exception:
            cur.execute(sql_cast, params_c)
        for r in cur.fetchall():
            rows.append({"utc": r[0], "steamid64": int(r[1]), "text": r[2], "logid": r[3]})
    return rows[:ABS_MAX_TOTAL_LINES]

# ---------- formatting ----------
def to_line_player(row: Dict) -> str:
    ts = (str(row["utc"])[:19] + "Z") if row["utc"] else "?"
    text = _escape((row.get("text") or "").replace("\n", " ").strip())
    if len(text) > TEXT_SNIPPET_LIMIT:
        text = text[:TEXT_SNIPPET_LIMIT] + "…"
    log_link = f"[log {row['logid']}](https://logs.tf/{row['logid']})" if row.get("logid") else ""
    return f"`{ts}` — {text} {log_link}".strip()

def to_line_team(row: Dict, meta: Dict[int, Dict]) -> str:
    ts = (str(row["utc"])[:19] + "Z") if row["utc"] else "?"
    info = meta.get(row["steamid64"], {})
    name = _escape(info.get("name") or str(row["steamid64"]))
    oz_url = info.get("oz_url")
    who = f"[{name}]({oz_url})" if oz_url else name
    text = _escape((row.get("text") or "").replace("\n", " ").strip())
    if len(text) > TEXT_SNIPPET_LIMIT:
        text = text[:TEXT_SNIPPET_LIMIT] + "…"
    log_link = f"[log {row['logid']}](https://logs.tf/{row['logid']})" if row.get("logid") else ""
    return f"`{ts}` — **{who}** — {text} {log_link}".strip()

def build_player_pages(oz_id: int, name: str, rows: List[Dict], page_size: int) -> Tuple[List[List[str]], str, str]:
    lines = [to_line_player(r) for r in rows]
    title = name
    url   = f"https://ozfortress.com/users/{oz_id}"
    return (_chunks(lines, page_size), title, url)

def build_team_pages(team_id: int, rows: List[Dict], meta: Dict[int, Dict], page_size: int) -> Tuple[List[List[str]], str, Optional[str]]:
    # Group by player (most -> least), then flatten
    by_player: Dict[int, List[Dict]] = {}
    for r in rows:
        by_player.setdefault(r["steamid64"], []).append(r)
    sorted_players = sorted(by_player.keys(), key=lambda sid: len(by_player[sid]), reverse=True)
    flat: List[str] = []
    for sid in sorted_players:
        for r in by_player[sid]:
            flat.append(to_line_team(r, meta))
    title = f"Team {team_id} — grouped by player (most → least)"
    return (_chunks(flat, page_size), title, None)

def embed_for_page(title: str, url: Optional[str], lines: List[str], page_idx: int, total_pages: int, totals_hint: str, color: int = 0x5865F2) -> discord.Embed:
    desc = "\n".join(lines) if lines else "(no results)"
    if len(desc) > 4000:
        desc = desc[:4000] + "\n…"
    kwargs = dict(title=title, description=desc, color=color)
    if url:
        kwargs["url"] = url  # avoid discord.Embed.Empty for older libraries
    e = discord.Embed(**kwargs)
    e.set_footer(text=f"Page {page_idx}/{total_pages} • {totals_hint}")
    return e

# ---------- paginator ----------
class Paginator(ui.View):
    def __init__(self, author_id: int, render_embed_fn, total_pages: int, page: int, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.render_embed_fn = render_embed_fn
        self.total_pages = max(1, total_pages)
        self.page = max(1, min(page, self.total_pages))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    async def on_timeout(self):
        for child in self.children:
            if isinstance(child, ui.Button):
                child.disabled = True

    async def _refresh(self, interaction: discord.Interaction):
        self.page = max(1, min(self.page, self.total_pages))
        self.first.disabled = self.page <= 1
        self.prev.disabled  = self.page <= 1
        self.next.disabled  = self.page >= self.total_pages
        self.last.disabled  = self.page >= self.total_pages
        await interaction.response.edit_message(embed=self.render_embed_fn(self.page), view=self)

    @ui.button(emoji="⏮", style=discord.ButtonStyle.secondary)
    async def first(self, interaction: discord.Interaction, button: ui.Button):
        self.page = 1
        await self._refresh(interaction)

    @ui.button(emoji="◀", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: ui.Button):
        self.page -= 1
        await self._refresh(interaction)

    @ui.button(emoji="▶", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: ui.Button):
        self.page += 1
        await self._refresh(interaction)

    @ui.button(emoji="⏭", style=discord.ButtonStyle.secondary)
    async def last(self, interaction: discord.Interaction, button: ui.Button):
        self.page = self.total_pages
        await self._refresh(interaction)

# ---------- parsing ----------
def parse_command(content: str) -> Tuple[str, Dict]:
    """
    Returns (cmd, kwargs)
      cmd in {"player","team","unknown"}
      kwargs:
        - For player: ozfid:int, contains:Optional[str], page:int, since_days:Optional[int]
        - For team:  team_id:int, contains:Optional[str], page:int, since_days:Optional[int]
    """
    s = (content or "").strip()
    if not s.startswith("!"):
        return "unknown", {}

    def split_rest(rest: str):
        tokens = rest.split() if rest else []
        page = 1
        since_days: Optional[int] = None
        words: List[str] = []
        for tok in tokens:
            if tok.lower().startswith("p=") and tok[2:].isdigit():
                page = max(1, int(tok[2:]))
                continue
            if tok.lower().startswith("s="):
                d = _parse_window_to_days(tok[2:])
                if d is not None:
                    since_days = d
                    continue
            if tok.isdigit():
                page = max(1, int(tok))
                continue
            d = _parse_window_to_days(tok)
            if d is not None:
                since_days = d
                continue
            words.append(tok)
        contains = " ".join(words).strip() or None
        return contains, page, since_days

    # !t<id> [..]
    m_t_short = re.match(r"^!t(\d+)(?:\s+(.*))?$", s, re.I)
    if m_t_short:
        team_id = int(m_t_short.group(1))
        contains, page, since_days = split_rest(m_t_short.group(2) or "")
        return "team", {"team_id": team_id, "contains": contains, "page": page, "since_days": since_days}

    # !team(s) <id> [..]
    m_team = re.match(r"^!(?:team|teams)\s+(\d+)(?:\s+(.*))?$", s, re.I)
    if m_team:
        team_id = int(m_team.group(1))
        contains, page, since_days = split_rest(m_team.group(2) or "")
        return "team", {"team_id": team_id, "contains": contains, "page": page, "since_days": since_days}

    # !<ozfid> [..]
    m_player = re.match(r"^!(\d+)(?:\s+(.*))?$", s)
    if m_player:
        ozfid = int(m_player.group(1))
        contains, page, since_days = split_rest(m_player.group(2) or "")
        return "player", {"ozfid": ozfid, "contains": contains, "page": page, "since_days": since_days}

    return "unknown", {}

# ---------- handlers ----------
async def handle_player(message: discord.Message, ozfid: int, contains: Optional[str], page: int, since_days: Optional[int]) -> None:
    try:
        with db.get_conn() as conn:
            player = get_player_by_ozid(conn, ozfid)
            if not player or not player.get("steamid64"):
                await safe_send_content(message, f"OZF {ozfid}: no Steam64 mapping found.")
                return
            rows = fetch_messages_player(conn, player["steamid64"], contains, since_days)
        name = player.get("current_name") or f"OZF {ozfid}"
        pages, title, url = build_player_pages(ozfid, name, rows, PAGE_SIZE)
        total_pages = max(1, len(pages))
        page = min(max(1, page), total_pages)

        def render(p: int) -> discord.Embed:
            win_txt = f"{since_days}d" if since_days else "ALL"
            totals_hint = f"{len(rows)} messages • window={win_txt}"
            return embed_for_page(title, url, pages[p-1], p, total_pages, totals_hint, color=0x5865F2)

        view = Paginator(author_id=message.author.id, render_embed_fn=render, total_pages=total_pages, page=page, timeout=180)
        await message.channel.send(embed=render(page), view=view, allowed_mentions=discord.AllowedMentions.none())

    except Exception as e:
        logger.exception("player query failed")
        await safe_send_content(message, f"Error looking up OZF {ozfid}: {e}")

async def handle_team(message: discord.Message, team_id: int, contains: Optional[str], page: int, since_days: Optional[int]) -> None:
    try:
        members = await client.loop.run_in_executor(None, fetch_team_members, team_id)
        if not members:
            await safe_send_content(message, f"Team {team_id}: not found or empty roster.")
            return
        with db.get_conn() as conn:
            steamids, idx = resolve_team_players(conn, members)
            if not steamids:
                await safe_send_content(message, f"Team {team_id}: no players with Steam64 mapping.")
                return
            rows = fetch_messages_team(conn, steamids, contains, since_days)

        pages, title, url = build_team_pages(team_id, rows, idx, PAGE_SIZE)
        total_pages = max(1, len(pages))
        page = min(max(1, page), total_pages)

        def render(p: int) -> discord.Embed:
            players_hit = len({r["steamid64"] for r in rows})
            win_txt = f"{since_days}d" if since_days else "ALL"
            totals_hint = f"{len(rows)} msgs • {players_hit} players • window={win_txt}"
            return embed_for_page(title, url, pages[p-1], p, total_pages, totals_hint, color=0x57F287)

        view = Paginator(author_id=message.author.id, render_embed_fn=render, total_pages=total_pages, page=page, timeout=240)
        await message.channel.send(embed=render(page), view=view, allowed_mentions=discord.AllowedMentions.none())

    except Exception as e:
        logger.exception("team query failed")
        await safe_send_content(message, f"Error fetching team {team_id}: {e}")

# ---------- events ----------
@client.event
async def on_ready():
    logger.info("Bot ready as %s (%s)", client.user, client.user.id)

@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if ALLOWED_CHANNEL_IDS and message.channel.id not in ALLOWED_CHANNEL_IDS:
        return

    cmd, kw = parse_command(message.content)
    if cmd == "player":
        await handle_player(message, kw["ozfid"], kw["contains"], kw["page"], kw["since_days"])
    elif cmd == "team":
        await handle_team(message, kw["team_id"], kw["contains"], kw["page"], kw["since_days"])
    else:
        return

# ---------- entry ----------
if __name__ == "__main__":
    client.run(BOT_TOKEN)
