# report.py
# Generates CSV + HTML reports from kiancat.dbo.slurs_msg with robust ozf joins.
# - Casts DATETIMEOFFSET to ISO text at SQL layer (avoids ODBC -155)
# - Joins to kian.oz.players via TRY_CONVERT(BIGINT, steamid64) for resilience
# - Adelaide local time display for humans
# - Atomic file updates; writes timestamped files and best-effort "latest" files

import os
import time
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Display timezone for human readable timestamps
LOCAL_TZ = ZoneInfo("Australia/Adelaide")

# Lightweight CSS for HTML report
CSS = """
<style>
  :root { --bg:#ffffff; --ink:#111827; --muted:#6b7280; --line:#e5e7eb; --th:#f3f4f6; }
  body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
         padding: 16px; color: var(--ink); background: var(--bg); }
  h1 { margin: 0 0 6px 0; font-size: 22px; }
  h2 { margin: 20px 0 8px 0; font-size: 18px; }
  .meta { color: var(--muted); font-size: 12px; margin-bottom: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid var(--line); padding: 6px 8px; text-align: left; }
  th { background: var(--th); position: sticky; top: 0; }
  tr:nth-child(even) { background: #fafafa; }
  .muted { color: var(--muted); }
  .notice { padding:8px 10px; background:#fff7ed; border:1px solid #fed7aa; border-radius:6px; margin:8px 0; }
</style>
"""

def _atomic_replace(src_tmp: str, dst_final: str, tries: int = 6) -> str:
    """
    Atomically replace dst_final with src_tmp.
    If dst is locked (e.g., Excel), retry with backoff. If still locked, keep the timestamped file.
    Returns the path that ended up containing the content (dst_final if success, else src_tmp).
    """
    for i in range(tries):
        try:
            os.replace(src_tmp, dst_final)
            return dst_final
        except PermissionError:
            if i == tries - 1:
                return src_tmp
            time.sleep(2 ** i)  # 1,2,4,8,16,32
    return src_tmp

def _safe_write_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> tuple[str, str]:
    """
    Write a timestamped CSV (base_YYYYmmdd_HHMMSS.csv) and try to refresh base.csv atomically.
    Returns (timestamped_path, final_path_used_for_latest).
    """
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts_path = os.path.join(out_dir, f"{base_name}_{ts}.csv")
    latest_path = os.path.join(out_dir, f"{base_name}.csv")
    tmp_path = ts_path + ".tmp"

    # lineterminator avoids extra blank lines on Windows when opened in Notepad/Excel
    df.to_csv(tmp_path, index=False, encoding="utf-8", lineterminator="\n")
    # Ensure the timestamped artifact exists
    final_ts = _atomic_replace(tmp_path, ts_path)
    # Best-effort update of the "latest" alias (ignore if locked)
    _atomic_replace(final_ts, latest_path)
    return ts_path, final_ts

def _safe_write_text(text: str, out_dir: str, base_name: str) -> tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts_path = os.path.join(out_dir, f"{base_name}_{ts}.html")
    latest_path = os.path.join(out_dir, f"{base_name}.html")
    tmp_path = ts_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
    final_ts = _atomic_replace(tmp_path, ts_path)
    _atomic_replace(final_ts, latest_path)
    return ts_path, final_ts

def make_reports(sql_conn, out_dir: str, tz_name: str, window_days: int):
    """
    Build matches.csv, summary.csv, report.html into out_dir.
    Returns (matches_csv_timestamped_path, summary_csv_timestamped_path).
    """
    os.makedirs(out_dir, exist_ok=True)

    # -------------------- SQL QUERIES --------------------
    # Use OUTER APPLY + TRY_CONVERT on roster steamid64 for robustness
    q_matches = """
    SELECT
        m.steamid64,
        p.oz_id,
        p.player_id,
        p.current_name,
        CONVERT(VARCHAR(33), m.msg_time_utc, 127) AS msg_time_utc_iso,
        m.text,
        m.logid,
        m.message_id
    FROM dbo.slurs_msg AS m
    OUTER APPLY (
        SELECT TOP (1)
               TRY_CONVERT(BIGINT, pl.steamid64) AS steamid64_big,
               pl.oz_id, pl.player_id, pl.current_name,
               pl.updated_at, pl.last_checked_at, pl.created_at
        FROM kian.oz.players AS pl
        WHERE TRY_CONVERT(BIGINT, pl.steamid64) = m.steamid64
        ORDER BY pl.updated_at DESC, pl.last_checked_at DESC, pl.created_at DESC
    ) AS p
    WHERE m.msg_time_utc >= DATEADD(DAY, -?, SYSUTCDATETIME())
    ORDER BY m.msg_time_utc DESC;
    """

    def count_since(days: int) -> pd.DataFrame:
        q = f"""
        SELECT m.steamid64, COUNT(*) AS c{days}
        FROM dbo.slurs_msg AS m
        WHERE m.msg_time_utc >= DATEADD(DAY, -{days}, SYSUTCDATETIME())
        GROUP BY m.steamid64;
        """
        return pd.read_sql(q, sql_conn)

    q_bounds = """
    SELECT
        steamid64,
        CONVERT(VARCHAR(33), MIN(msg_time_utc), 127) AS first_hit_utc_iso,
        CONVERT(VARCHAR(33), MAX(msg_time_utc), 127) AS last_hit_utc_iso
    FROM dbo.slurs_msg
    GROUP BY steamid64;
    """

    # Global oz mapping so summary always gets best-known roster info
    q_map = """
    SELECT DISTINCT
        m.steamid64,
        pl.oz_id,
        pl.player_id,
        pl.current_name
    FROM dbo.slurs_msg AS m
    OUTER APPLY (
        SELECT TOP (1) oz_id, player_id, current_name
        FROM kian.oz.players p2
        WHERE TRY_CONVERT(BIGINT, p2.steamid64) = m.steamid64
        ORDER BY p2.updated_at DESC, p2.last_checked_at DESC, p2.created_at DESC
    ) AS pl;
    """

    # -------------------- EXECUTE --------------------
    with sql_conn as conn:
        df_matches = pd.read_sql(q_matches, conn, params=[window_days])
        s24  = count_since(1)
        s7   = count_since(7)
        s30  = count_since(30)
        s180 = count_since(180)
        bounds = pd.read_sql(q_bounds, conn)
        ozmap  = pd.read_sql(q_map, conn)

    # -------------------- TRANSFORM: MATCHES --------------------
    if not df_matches.empty:
        dt_utc = pd.to_datetime(df_matches["msg_time_utc_iso"], utc=True, errors="coerce")
        df_matches["msg_time_local"] = dt_utc.dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        # preferred column order
        df_matches = df_matches[
            ["steamid64", "oz_id", "player_id", "current_name",
             "msg_time_local", "msg_time_utc_iso",
             "text", "logid", "message_id"]
        ]
        # tidy types
        for col in ["oz_id", "player_id"]:
            if col in df_matches.columns:
                df_matches[col] = pd.to_numeric(df_matches[col], errors="coerce").astype("Int64")
        if "current_name" in df_matches.columns:
            df_matches["current_name"] = df_matches["current_name"].astype("string").fillna("")

    # -------------------- TRANSFORM: SUMMARY --------------------
    # Merge count windows + bounds + oz mapping (do not zero oz fields)
    summary = (
        s180.merge(s30, how="outer", on="steamid64")
            .merge(s7,  how="outer", on="steamid64")
            .merge(s24, how="outer", on="steamid64")
            .merge(bounds, how="left", on="steamid64")
            .merge(ozmap,  how="left", on="steamid64")
    )

    # Fill only numeric counts; keep oz fields nullable
    for col in [c for c in summary.columns if c.startswith("c")]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).astype("Int64")

    for col in ["oz_id", "player_id"]:
        if col in summary.columns:
            summary[col] = pd.to_numeric(summary[col], errors="coerce").astype("Int64")
    if "current_name" in summary.columns:
        summary["current_name"] = summary["current_name"].astype("string")

    # Sort by 180d count (then 30d, 7d)
    sort_cols = [c for c in ["c180", "c30", "c7", "c1"] if c in summary.columns]
    if sort_cols:
        summary = summary.sort_values(sort_cols, ascending=[False] * len(sort_cols))

    # SteamIDs lacking oz mapping (for operator follow-up)
    missing_map = summary[summary["oz_id"].isna()][["steamid64"]].copy()
    missing_map["note"] = "No ozf mapping found (normalize kian.oz.players.steamid64 or add player)"

    # For nicer CSVs (optional): empty strings instead of <NA> in text-ish fields
    for col in ["current_name"]:
        if col in summary.columns:
            summary[col] = summary[col].astype("string").fillna("")

    # -------------------- WRITE FILES --------------------
    matches_ts, _ = _safe_write_csv(df_matches, out_dir, "matches")
    summary_ts, _ = _safe_write_csv(summary,   out_dir, "summary")

    # also emit a timestamped missing roster CSV (no "latest" alias)
    ts_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    missing_csv = os.path.join(out_dir, f"missing_roster_{ts_stamp}.csv")
    missing_map.to_csv(missing_csv, index=False, encoding="utf-8", lineterminator="\n")

    # -------------------- HTML REPORT --------------------
    now_local = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>slurs summary</title>",
        CSS,
        "</head><body>",
        "<h1>slurs summary</h1>",
        f"<div class='meta'>Generated: {now_local} • Window: last {window_days} days</div>",
    ]

    # quick topline stats
    total_players = len(summary) if isinstance(summary, pd.DataFrame) else 0
    total_msgs_24 = int(summary["c1"].sum()) if "c1" in summary.columns else 0
    parts += [
        f"<div class='notice'>Players in summary: <b>{total_players}</b> &nbsp;•&nbsp; "
        f"Messages in last 24h: <b>{total_msgs_24}</b></div>"
    ]

    parts += ["<h2>Summary (per SteamID)</h2>", summary.to_html(index=False, escape=False)]
    parts += ["<h2>Recent matches</h2>"]
    if df_matches.empty:
        parts += ["<div class='muted'>No matches in the configured window.</div>"]
    else:
        parts += [df_matches.head(1000).to_html(index=False, escape=False)]
    if not missing_map.empty:
        parts += [
            "<h2>Missing ozfortress mappings</h2>",
            "<p class='muted'>These SteamIDs were found in messages but had no oz_id/current_name in "
            "<code>kian.oz.players</code>. Fix the roster table and this will populate on the next run.</p>",
            missing_map.to_html(index=False, escape=False),
        ]
    parts += ["</body></html>"]
    _safe_write_text("".join(parts), out_dir, "report")

    return matches_ts, summary_ts
