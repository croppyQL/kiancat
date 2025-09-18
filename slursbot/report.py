# report.py — OZF-only CSV/HTML reports + daily Excel workbook
from __future__ import annotations

import os
import time
import math
import logging
import pandas as pd
from typing import Optional, Tuple, List
from datetime import datetime, timedelta, timezone, time as dtime

logger = logging.getLogger("slursbot")

# -----------------------
# Adelaide time helpers
# -----------------------
def _tzname() -> str:
    return os.getenv("DISPLAY_TZ", "Australia/Adelaide")

def _adelaide():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(_tzname())
    except Exception:
        return timezone.utc

def _adelaide_window_22h() -> Tuple[datetime, datetime]:
    """Local window [22:00 yesterday → 22:00 today] returned as UTC datetimes."""
    ADL = _adelaide()
    now_local = datetime.now(ADL)
    anchor = datetime.combine(now_local.date(), dtime(22, 0, 0), ADL)
    if now_local < anchor:
        anchor -= timedelta(days=1)
    start_local = anchor - timedelta(days=1)
    return (start_local.astimezone(timezone.utc), anchor.astimezone(timezone.utc))

def _adelaide_date_str(dt_utc: datetime) -> str:
    ADL = _adelaide()
    try:
        return dt_utc.astimezone(ADL).strftime("%Y-%m-%d")
    except Exception:
        return str(dt_utc)[:10]

# -----------------------
# Atomic file helpers
# -----------------------
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _atomic_replace(src_tmp: str, dst_final: str, tries: int = 6) -> str:
    """Replace dst with src_tmp; retry if target is locked (e.g. Excel open)."""
    for i in range(tries):
        try:
            os.replace(src_tmp, dst_final)
            return dst_final
        except PermissionError:
            if i == tries - 1:
                return src_tmp
            time.sleep(2 ** i)  # 1,2,4,8,16,32
    return src_tmp

def _safe_write_text(text: str, out_path: str) -> str:
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    return _atomic_replace(tmp, out_path)

def _safe_write_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> Tuple[str, str]:
    """Write timestamped and 'latest' CSV; return (timestamped_path, final_latest_path_used)."""
    _ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts_path = os.path.join(out_dir, f"{base_name}_{ts}.csv")
    latest_path = os.path.join(out_dir, f"{base_name}.csv")
    tmp_path = ts_path + ".tmp"
    df.to_csv(tmp_path, index=False, encoding="utf-8", lineterminator="\n")
    os.replace(tmp_path, ts_path)
    # refresh latest (best-effort)
    try:
        tmp_latest = latest_path + ".tmp"
        df.to_csv(tmp_latest, index=False, encoding="utf-8", lineterminator="\n")
        _atomic_replace(tmp_latest, latest_path)
        used = latest_path
    except PermissionError:
        used = ts_path
    return ts_path, used

# -----------------------
# SQL helpers (OZF-only)
# -----------------------
def _fetch_counts_master(conn, since_utc: datetime, before_utc: datetime) -> pd.DataFrame:
    """
    Counts per OZF player across multiple windows:
      c1 (given window), c7, c31, c180, c_all, plus first/last seen.
    Restricts strictly to roster by joining kian.oz.v_players_clean (steamid64_bigint).
    """
    sql = """
    DECLARE @since  DATETIME2(3) = ?;
    DECLARE @before DATETIME2(3) = ?;
    DECLARE @now    DATETIME2(3) = SYSUTCDATETIME();

    WITH base AS (
      SELECT v.steamid64_bigint AS steamid64, v.player_id, v.oz_id, v.current_name
      FROM kian.oz.v_players_clean AS v
      WHERE v.steamid64_bigint IS NOT NULL
    ),
    c1 AS (
      SELECT m.steamid64, COUNT(*) AS c1
      FROM kiancat.dbo.slurs_msg AS m
      JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
      WHERE m.msg_time_utc >= @since AND m.msg_time_utc < @before
      GROUP BY m.steamid64
    ),
    c7 AS (
      SELECT m.steamid64, COUNT(*) AS c7
      FROM kiancat.dbo.slurs_msg AS m
      JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
      WHERE m.msg_time_utc >= DATEADD(DAY,-7,@now) AND m.msg_time_utc < @now
      GROUP BY m.steamid64
    ),
    c31 AS (
      SELECT m.steamid64, COUNT(*) AS c31
      FROM kiancat.dbo.slurs_msg AS m
      JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
      WHERE m.msg_time_utc >= DATEADD(DAY,-31,@now) AND m.msg_time_utc < @now
      GROUP BY m.steamid64
    ),
    c180 AS (
      SELECT m.steamid64, COUNT(*) AS c180
      FROM kiancat.dbo.slurs_msg AS m
      JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
      WHERE m.msg_time_utc >= DATEADD(DAY,-180,@now) AND m.msg_time_utc < @now
      GROUP BY m.steamid64
    ),
    call AS (
      SELECT m.steamid64, COUNT(*) AS c_all,
             CAST(MIN(m.msg_time_utc) AS DATETIME2(3)) AS first_hit_utc,
             CAST(MAX(m.msg_time_utc) AS DATETIME2(3)) AS last_hit_utc
      FROM kiancat.dbo.slurs_msg AS m
      JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
      GROUP BY m.steamid64
    )
    SELECT b.steamid64, b.player_id, b.oz_id, b.current_name,
           ISNULL(c1.c1,0)   AS c1,
           ISNULL(c7.c7,0)   AS c7,
           ISNULL(c31.c31,0) AS c31,
           ISNULL(c180.c180,0) AS c180,
           ISNULL(call.c_all,0) AS c_all,
           call.first_hit_utc,
           call.last_hit_utc
    FROM base AS b
    LEFT JOIN c1   ON c1.steamid64 = b.steamid64
    LEFT JOIN c7   ON c7.steamid64 = b.steamid64
    LEFT JOIN c31  ON c31.steamid64 = b.steamid64
    LEFT JOIN c180 ON c180.steamid64 = b.steamid64
    LEFT JOIN call ON call.steamid64 = b.steamid64
    """
    df = pd.read_sql(sql, conn, params=[since_utc, before_utc])
    # Ensure proper dtypes
    for c in ("c1","c7","c31","c180","c_all"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df

def _fetch_messages_1d(conn, since_utc: datetime, before_utc: datetime) -> pd.DataFrame:
    """
    Messages in the 1-day Adelaide window (UTC bounds passed in).
    Includes links: logs.tf. Restricted to OZF roster.
    """
    sql = """
    SELECT
      CAST(m.msg_time_utc AS DATETIME2(0)) AS msg_time_utc,
      v.current_name,
      v.player_id,
      v.oz_id,
      m.steamid64,
      m.text,
      m.logid
    FROM kiancat.dbo.slurs_msg AS m
    JOIN kian.oz.v_players_clean AS v ON v.steamid64_bigint = m.steamid64
    WHERE m.msg_time_utc >= ? AND m.msg_time_utc < ?
    ORDER BY m.msg_time_utc ASC
    """
    df = pd.read_sql(sql, conn, params=[since_utc, before_utc])
    # Derive convenience columns
    ADL = _adelaide()
    try:
        dt = pd.to_datetime(df["msg_time_utc"], utc=True)
        df["date_local"] = dt.dt.tz_convert(ADL).dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        df["date_local"] = df["msg_time_utc"].astype(str)
    df["logs.tf"] = df["logid"].apply(lambda x: f"https://logs.tf/{int(x)}" if pd.notna(x) else "")
    df.rename(columns={"current_name":"player_name", "text":"message_text"}, inplace=True)
    return df[["date_local","player_name","player_id","oz_id","steamid64","message_text","logs.tf"]]

# -----------------------
# HTML rendering
# -----------------------
_CSS = """
<style>
  :root { --bg:#fff; --ink:#111827; --muted:#6b7280; --line:#e5e7eb; --th:#f3f4f6; }
  body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; padding:16px; color:var(--ink); background:var(--bg); }
  h1 { margin:0 0 8px 0; font-size:22px; }
  .meta { color: var(--muted); font-size: 12px; margin-bottom: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid var(--line); padding: 6px 8px; text-align: left; }
  th { background: var(--th); position: sticky; top: 0; }
  tr:nth-child(even) { background: #fafafa; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .muted { color: var(--muted); }
</style>
"""

def _player_links(row) -> str:
    name = str(row.get("current_name") or row.get("player_name") or "")
    ozid = row.get("oz_id")
    sid  = row.get("steamid64")
    oz   = f"https://ozfortress.com/users/{int(ozid)}" if pd.notna(ozid) else None
    sl   = f"https://slurs.tf/steamid/{int(sid)}" if pd.notna(sid) else None
    st   = f"https://steamcommunity.com/profiles/{int(sid)}" if pd.notna(sid) else None
    links = []
    if oz: links.append(f'<a href="{oz}" target="_blank">ozf</a>')
    if sl: links.append(f'<a href="{sl}" target="_blank">slurs</a>')
    if st: links.append(f'<a href="{st}" target="_blank">steam</a>')
    suffix = " <span class='muted'>(" + " · ".join(links) + ")</span>" if links else ""
    return f"{name}{suffix}"

def _render_html_summary(counts: pd.DataFrame, title: str, rank_col: str) -> str:
    df = counts.copy()
    if rank_col not in df.columns:
        rank_col = "c_all"
    # Only show rows with >0 in the chosen window
    df = df[df[rank_col] > 0].copy()
    df.sort_values([rank_col, "current_name"], ascending=[False, True], inplace=True)

    rows = []
    for _, r in df.iterrows():
        rows.append(
            "<tr>"
            f"<td>{_player_links(r)}</td>"
            f"<td class='num'>{int(r.get('c1',0))}</td>"
            f"<td class='num'>{int(r.get('c7',0))}</td>"
            f"<td class='num'>{int(r.get('c31',0))}</td>"
            f"<td class='num'>{int(r.get('c180',0))}</td>"
            f"<td class='num'>{int(r.get('c_all',0))}</td>"
            "</tr>"
        )
    table = (
        "<table>"
        "<thead><tr>"
        "<th>Player</th><th>1d</th><th>7d</th><th>31d</th><th>180d</th><th>All</th>"
        "</tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody>"
        "</table>"
    )
    now_txt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"<!doctype html><html><head><meta charset='utf-8'>{_CSS}</head><body>" \
           f"<h1>{title}</h1><div class='meta'>Generated {now_txt}</div>{table}</body></html>"

def _render_html_messages(msgs_1d: pd.DataFrame, title: str) -> str:
    rows = []
    for _, r in msgs_1d.iterrows():
        logs = r["logs.tf"]
        logs_html = f'<a href="{logs}" target="_blank">logs.tf</a>' if logs else ""
        rows.append(
            "<tr>"
            f"<td>{r['date_local']}</td>"
            f"<td>{_player_links({'current_name': r['player_name'], 'oz_id': r['oz_id'], 'steamid64': r['steamid64']})}</td>"
            f"<td>{(r['message_text'] or '').replace('<','&lt;').replace('>','&gt;')}</td>"
            f"<td>{logs_html}</td>"
            "</tr>"
        )
    table = (
        "<table>"
        "<thead><tr><th>Local Time</th><th>Player</th><th>Message</th><th>Log</th></tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table>"
    )
    now_txt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"<!doctype html><html><head><meta charset='utf-8'>{_CSS}</head><body>" \
           f"<h1>{title}</h1><div class='meta'>Generated {now_txt}</div>{table}</body></html>"

# -----------------------
# PUBLIC: HTML reports
# -----------------------
def _mode_to_rank_col(mode: str) -> str:
    m = str(mode).strip().lower()
    if m in ("1", "day", "1d"): return "c1"
    if m in ("7", "7d", "week"): return "c7"
    if m in ("31", "31d", "month"): return "c31"
    if m in ("180", "180d"): return "c180"
    return "c_all"

def _mode_title(mode: str) -> str:
    m = str(mode).strip().lower()
    lookup = {"1":"1 Day", "7":"7 Days", "31":"31 Days", "180":"180 Days", "all":"All Time"}
    return lookup.get(m, m)

def make_reports(conn, out_dir: str, mode: str) -> List[str]:
    """
    Build CSV + HTML in out_dir for the requested mode.
    HTML files:
      - slurs_summary_<mode>.html
      - slurs_messages_1d.html (only for mode 1)
    Returns list of written file paths.
    """
    _ensure_dir(out_dir)

    # For the counts we need a 1-day window argument; c1 uses it, others use NOW windows.
    if str(mode).lower() == "1":
        since_utc, before_utc = _adelaide_window_22h()
    else:
        # Still compute a plausible day window so c1 has a reference point.
        since_utc, before_utc = _adelaide_window_22h()

    counts = _fetch_counts_master(conn, since_utc, before_utc)
    written = []

    # CSV (timestamped + latest) for summary counts
    _safe_write_csv(counts, out_dir, base_name="summary_counts_ozf")

    # HTML summary (sorted by chosen mode)
    rank_col = _mode_to_rank_col(mode)
    title = f"OZF Slurs — Summary ({_mode_title(mode)})"
    html = _render_html_summary(counts, title=title, rank_col=rank_col)
    path_sum = os.path.join(out_dir, f"slurs_summary_{str(mode).lower()}.html")
    written.append(_safe_write_text(html, path_sum))

    # If mode is "1", also write per-message 1-day table
    if str(mode).lower() == "1":
        msgs = _fetch_messages_1d(conn, since_utc, before_utc)
        # CSV set for messages_1d
        _safe_write_csv(msgs, out_dir, base_name="messages_1d_ozf")
        htmlm = _render_html_messages(msgs, title="OZF Slurs — Messages (Last Adelaide Day)")
        path_msg = os.path.join(out_dir, "slurs_messages_1d.html")
        written.append(_safe_write_text(htmlm, path_msg))

    logger.info("HTML reports (mode=%s) built in %s", mode, out_dir)
    logger.info("HTML reports written to %s (mode=%s)", out_dir, mode)
    return written

# -----------------------
# Excel writer helpers
# -----------------------
def _xlsx_formats(writer):
    book = writer.book
    fmt_header = book.add_format({"bold": True, "bg_color": "#f3f4f6", "border": 1})
    fmt_num    = book.add_format({"num_format": "0", "border": 1})
    fmt_text   = book.add_format({"border": 1})
    return fmt_header, fmt_num, fmt_text

def _write_counts_sheet(writer, name: str, counts: pd.DataFrame, by_col: str):
    df = counts.copy()
    if by_col not in df.columns:
        by_col = "c_all"
    df.sort_values([by_col, "current_name"], ascending=[False, True], inplace=True)
    cols = ["current_name","player_id","oz_id","steamid64","c1","c7","c31","c180","c_all"]
    for c in cols:
        if c not in df.columns:
            df[c] = 0
    df = df[cols]

    df.to_excel(writer, sheet_name=name, index=False, startrow=1, header=False)
    ws = writer.book.get_worksheet_by_name(name)
    fmt_header, _, _ = _xlsx_formats(writer)
    for i, h in enumerate(cols):
        ws.write(0, i, h, fmt_header)
    # widths
    ws.set_column(0, 0, 26)  # current_name
    ws.set_column(1, 1, 10)  # player_id
    ws.set_column(2, 2, 10)  # oz_id
    ws.set_column(3, 3, 18)  # steamid64
    ws.set_column(4, 8, 8)   # counts
    ws.freeze_panes(1, 0)

def _write_messages_1d_pages(writer, base_name: str, msgs: pd.DataFrame, page_size: int = 50000):
    n = len(msgs)
    if n == 0:
        df = pd.DataFrame(columns=["date_local","player_name","player_id","oz_id","steamid64","message_text","logs.tf"])
        df.to_excel(writer, sheet_name=base_name, index=False)
        return

    pages = math.ceil(n / page_size)
    for p in range(pages):
        lo = p * page_size
        hi = min((p + 1) * page_size, n)
        sl = msgs.iloc[lo:hi]
        name = base_name if p == 0 else f"{base_name}_{p+1}"

        cols = ["date_local","player_name","player_id","oz_id","steamid64","message_text","logs.tf"]
        sl = sl[cols]
        sl.to_excel(writer, sheet_name=name, index=False, startrow=1, header=False)

        ws = writer.book.get_worksheet_by_name(name)
        fmt_header, _, _ = _xlsx_formats(writer)
        for i, h in enumerate(cols):
            ws.write(0, i, h, fmt_header)

        widths = {"date_local":12, "player_name":26, "player_id":10, "oz_id":10,
                  "steamid64":18, "message_text":80, "logs.tf":16}
        for i, c in enumerate(cols):
            ws.set_column(i, i, widths.get(c, 12))

        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0 + len(sl), len(cols) - 1)

        # Make logs.tf clickable
        log_col = cols.index("logs.tf")
        for ridx in range(len(sl)):
            url = sl.iloc[ridx]["logs.tf"]
            if url:
                ws.write_url(1 + ridx, log_col, url, string="logs.tf")

# -----------------------
# PUBLIC: Excel workbook
# -----------------------
def make_excel_daily(conn, out_dir: str,
                     tz_name: Optional[str] = None,
                     retention_days: int = 30) -> str:
    """
    Create a single Excel workbook for today's Adelaide local day:
      Tabs: Summary, 1d, 7d, 31d, 180d, All, Messages_1d (split into 50k chunks)
      Links: player_name page has ozf/slurs/steam in HTML; Excel has logs.tf links
      Returns: path to the dated workbook (also refreshes ozf_daily_latest.xlsx if not locked).
    """
    _ensure_dir(out_dir)
    since_utc, before_utc = _adelaide_window_22h()
    day_str = _adelaide_date_str(before_utc - timedelta(seconds=1))

    counts = _fetch_counts_master(conn, since_utc, before_utc)
    msgs_1d = _fetch_messages_1d(conn, since_utc, before_utc)

    dated_path  = os.path.join(out_dir, f"ozf_daily_{day_str}.xlsx")
    latest_path = os.path.join(out_dir, "ozf_daily_latest.xlsx")
    tmp_path    = dated_path + ".tmp"

    with pd.ExcelWriter(tmp_path, engine="xlsxwriter") as writer:
        # Summary tab (top offenders today by c1)
        try:
            _write_counts_sheet(writer, "Summary", counts[counts["c1"] > 0], by_col="c1")
        except Exception:
            _write_counts_sheet(writer, "Summary", counts.head(0), by_col="c1")
        # Other windows
        _write_counts_sheet(writer, "1d",   counts, by_col="c1")
        _write_counts_sheet(writer, "7d",   counts, by_col="c7")
        _write_counts_sheet(writer, "31d",  counts, by_col="c31")
        _write_counts_sheet(writer, "180d", counts, by_col="c180")
        _write_counts_sheet(writer, "All",  counts, by_col="c_all")

        # Messages (1 day) with logs.tf hyperlinks
        _write_messages_1d_pages(writer, "Messages_1d", msgs_1d, page_size=50000)

    os.replace(tmp_path, dated_path)
    # Best-effort latest
    try:
        tmp2 = latest_path + ".tmp"
        with pd.ExcelWriter(tmp2, engine="xlsxwriter") as w:
            # small pointer sheet
            pd.DataFrame({"see": [os.path.basename(dated_path)]}).to_excel(w, index=False)
        _atomic_replace(tmp2, latest_path)
    except PermissionError:
        pass

    logger.info("Excel daily written: %s", dated_path)
    return dated_path
