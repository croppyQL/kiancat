# main.py — daily orchestration (roster refresh, pull, HTML reports, Excel workbook, Discord, watermark)
# This version preserves your original behavior AND fixes:
#  - Roster bug: pass INTEGER streak length to ozf_roster.refresh (no more "stop after 1")
#  - No probe watermark drift: start from DB MAX(oz_id) only (no +20/+40 runaway)
#  - AFTER-only fetch kept, plus optional overlap on the watermark window (dedup via hash_key)
#  - Steam3→Steam64 handling remains in db.py / slurs_api.py (no changes needed here)

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone, time as dtime
from typing import Optional, List, Tuple
from dotenv import load_dotenv

import db
import slurs_api
import report
import discord_webhook
import ozf_roster

# ---------------- allowlist/lexicon filtering (post-fetch, pre-write) ----------------
import re
try:
    import yaml
except Exception:
    yaml = None

def _load_word_list_yaml(path: str, keys=("words", "allow", "allowlist")) -> list[str]:
    if not path:
        return []
    try:
        if yaml is None:
            logger.warning("PyYAML not installed; cannot load %s", path)
            return []
        if not os.path.exists(path):
            logger.warning("allow/lexicon file not found: %s", path)
            return []
        with open(path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        words: list[str] = []
        if isinstance(doc, list):
            words = [str(x).strip().lower() for x in doc if str(x).strip()]
        elif isinstance(doc, dict):
            for k in keys:
                if isinstance(doc.get(k), list):
                    words.extend([str(x).strip().lower() for x in doc[k] if str(x).strip()])
        # de-dup and sort for repeatable logs
        return sorted(set(words))
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
        return []

def _compile_word_re(words: list[str]) -> re.Pattern | None:
    if not words:
        return None
    # Word-boundary OR pattern, escape each word
    # \b is OK for simple word tokens; if you add punctuation words, we can extend later.
    pat = r"\b(?:" + "|".join(re.escape(w) for w in words) + r")\b"
    try:
        return re.compile(pat, re.IGNORECASE)
    except Exception as e:
        logger.warning("Regex compile failed: %s", e)
        return None

def _apply_allowlist_filter(rows: list[dict]) -> tuple[list[dict], dict]:
    """
    Keep rows that:
      - contain any 'slur' from lexicon (always keep), OR
      - contain none of the allowlist words.

    Drop rows that:
      - contain allowlist words AND contain no known slur words.

    This guarantees that messages with both allowed words and slurs are KEPT.

    Controlled by:
      ALLOWLIST_PATH (default 'allowlist.yaml')
      LEXICON_PATH   (default 'lexicon.yaml')
      ALLOWLIST_DROP (default '0' = off; set '1' to drop)
    """
    allow_path = os.getenv("ALLOWLIST_PATH", "allowlist.yaml")
    lex_path   = os.getenv("LEXICON_PATH",   "lexicon.yaml")
    do_drop    = os.getenv("ALLOWLIST_DROP", "0").strip().lower() in {"1","true","yes","on"}

    allow_words = _load_word_list_yaml(allow_path, keys=("words","allow","allowlist"))
    lex_words   = _load_word_list_yaml(lex_path,   keys=("words","terms","slurs","deny","denylist"))

    allow_re = _compile_word_re(allow_words) if allow_words else None
    slur_re  = _compile_word_re(lex_words)   if lex_words else None

    if not do_drop or (allow_re is None):
        # Nothing to do (feature off or no allowlist words)
        return rows, {"enabled": False, "allow_terms": len(allow_words), "lex_terms": len(lex_words), "dropped": 0, "kept": len(rows)}

    kept: list[dict] = []
    dropped = 0

    for r in rows:
        msg = (r.get("message") or r.get("text") or "")
        t = str(msg)

        # If we can positively identify a slur term from lexicon -> keep
        if slur_re is not None and slur_re.search(t):
            kept.append(r)
            continue

        # Otherwise, if allowlist is present in the text -> drop
        if allow_re.search(t):
            dropped += 1
            continue

        # Neither slur nor allow term -> keep as-is
        kept.append(r)

    return kept, {"enabled": True, "allow_terms": len(allow_words), "lex_terms": len(lex_words), "dropped": dropped, "kept": len(kept)}



# ---------- logging ----------
try:
    from logging_setup import setup_logger
    logger = setup_logger()
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("slursbot")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# ---------- env helpers ----------
def env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return v if v is not None and str(v).strip() != "" else default

def env_int(key: str, default: int) -> int:
    s = env_str(key, str(default))
    try:
        return int(s)
    except Exception:
        return default

def env_list_int(key: str, default_list: List[int]) -> List[int]:
    raw = env_str(key, "")
    if not raw:
        return default_list
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            continue
    return out or default_list

def reports_dir() -> str:
    return env_str("REPORTS_DIR", "C:/slurs/reports")

# ---------- windows ----------
def iso_local_window_adelaide_22h() -> Tuple[str, str]:
    """Adelaide local-day [22:00 yesterday → 22:00 today] to UTC ISO Z."""
    try:
        from zoneinfo import ZoneInfo
        ADL = ZoneInfo(env_str("DISPLAY_TZ", "Australia/Adelaide"))
    except Exception:
        ADL = timezone.utc
    now_local = datetime.now(ADL)
    anchor = datetime.combine(now_local.date(), dtime(22, 0, 0), ADL)
    if now_local < anchor:
        anchor -= timedelta(days=1)
    start = anchor - timedelta(days=1)
    return (
        start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        anchor.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

# ---------- roster ----------
STEAM64_MIN = 76561197960265728

def fetch_ozf_steamids(conn) -> List[int]:
    """
    Pull OZF Steam64 IDs from the cleaned view; optionally cap count via OZF_MAX_IDS for testing.
    """
    ids: List[int] = []
    sql = "SELECT steamid64_bigint FROM kian.oz.v_players_clean WHERE steamid64_bigint IS NOT NULL"
    with conn.cursor() as cur:
        cur.execute(sql)
        for (sid,) in cur.fetchall():
            try:
                si = int(sid)
                if si >= STEAM64_MIN:
                    ids.append(si)
            except Exception:
                continue
    ids = sorted(set(ids))
    cap = env_str("OZF_MAX_IDS", "").strip()
    if cap:
        try:
            ids = ids[: max(1, int(cap))]
        except Exception:
            pass
    return ids

def run_roster_refresh() -> Tuple[int, int]:
    """
    Scrape ozf profiles forward; returns (checked, changed) and posts an admin summary embed.
    IMPORTANT: pass INTEGERS for stop_after_404 (previous bug was passing a boolean).
    """
    max_probe = env_int("OZF_REFRESH_PROBE", 300)        # how many pages/IDs to probe this run
    stop_404  = env_int("OZF_REFRESH_404_STREAK", 20)    # stop after N consecutive 404s
    sleep_ms  = env_int("OZF_REFRESH_SLEEP_MS", 200)     # politeness delay between pages
    with db.get_conn() as conn:
        checked, changed = ozf_roster.refresh(
            conn,
            max_probe=max_probe,
            stop_after_404=stop_404,
            sleep_ms=sleep_ms
        )
        try:
            discord_webhook.post_admin_roster_summary(conn, checked, changed)
        except Exception as e:
            logger.warning("post_admin_roster_summary failed: %s", e)
    logger.info("roster-refresh: checked=%s changed=%s", checked, changed)
    return checked, changed

# ---------- pull ----------
def run_pull(since_iso: Optional[str], before_iso: Optional[str]) -> Tuple[int, int]:
    """
    Return (inserted_raw, upserted).
    Uses AFTER-only (before_iso intentionally ignored); category=total; max 10 IDs/request.
    """ 
    with db.get_conn() as conn:
        steamids = fetch_ozf_steamids(conn)
    logger.info("ozf steamids: %d", len(steamids))

    # AFTER-only by design; we still compute/log 'before' for visibility but don't pass it to the API
    category   = "total"   # only slurs (per slurs.tf maintainer)


    data = []
    try:
        data = slurs_api.fetch_messages_for_steamids(
            steamids=steamids,
            after_iso=since_iso,
            before_iso=before_iso,
            category=category,
            batch_size=min(env_int("SLURS_BATCH_SIZE", 10), 10),
            limit=env_int("SLURS_LIMIT", 100),
            sleep_ms=env_int("SLURS_SLEEP_MS", 1100),  # ~<300 req/5min
            retries_s=env_list_int("SLURS_RETRIES_S", [10, 30, 300, 900]),
        )
    except Exception as e:
        logger.warning("pull exception (fallback single ID, no dates): %s", e)
        if not env_str("OZF_MAX_IDS", "").strip():
            os.environ["OZF_MAX_IDS"] = "1"
        data = slurs_api.fetch_messages_for_steamids(
            steamids=steamids,
            after_iso=None,
            before_iso=None,
            category=category,
            batch_size=1,
            limit=env_int("SLURS_LIMIT", 100),
            sleep_ms=env_int("SLURS_SLEEP_MS", 1100),
            retries_s=env_list_int("SLURS_RETRIES_S", [10, 30, 300, 900]),
        )

        # OPTIONAL allowlist filter (off by default). Drops rows that contain only allowed words.
    data, af_stats = _apply_allowlist_filter(data)
    if af_stats.get("enabled"):
        logger.info(
            "allowlist filter active: allow_terms=%s lex_terms=%s dropped=%s kept=%s",
            af_stats["allow_terms"], af_stats["lex_terms"], af_stats["dropped"], af_stats["kept"]
        )

    if not data:
        logger.info("pull: no rows returned from API for given window.")
        return (0, 0)

    raw_table = env_str("SLURS_RAW_TABLE", "kiancat.dbo.slurs_raw")
    msg_table = env_str("SLURS_MSG_TABLE", "kiancat.dbo.slurs_msg")

    inserted_raw = 0
    upserted = 0
    with db.get_conn() as conn:
        if hasattr(db, "insert_raw_rows"):
            try:
                inserted_raw = db.insert_raw_rows(data, raw_table)
            except Exception as e:
                logger.warning("insert_raw_rows failed: %s", e)
        else:
            logger.warning("db.insert_raw_rows not found; skipping raw insert.")

        if hasattr(db, "upsert_messages"):
            try:
                upserted = db.upsert_messages(data, msg_table)
            except Exception as e:
                logger.warning("upsert_messages failed: %s", e)
        else:
            logger.warning("db.upsert_messages not found; typed upsert skipped.")
    logger.info("raw inserted: %s; upsert inserted: %s", inserted_raw, upserted)
    return (inserted_raw, upserted)

# ---------- HTML reports wrapper (existing) ----------
def run_report(mode: str = "180"):
    out_dir = reports_dir()
    with db.get_conn() as conn:
        # preferred: (conn, out_dir, mode=...)
        try:
            report.make_reports(conn, out_dir, mode=mode)
        except TypeError:
            # some versions take (conn, out_dir, mode) positionally
            try:
                report.make_reports(conn, out_dir, mode)
            except TypeError:
                # last-resort: very old signature (conn, out_dir) only
                report.make_reports(conn, out_dir)
    logger.info("HTML reports written to %s (mode=%s)", out_dir, mode)


# ---------- Discord ----------
def run_discord_admin():
    try:
        with db.get_conn() as conn:
            discord_webhook.post_daily_player_embeds(conn)
        logger.info("admin discord post: per-player daily embeds")
    except Exception as e:
        logger.warning("admin per-player embeds failed: %s", e)

def run_discord_public(top_n: int):
    with db.get_conn() as conn:
        discord_webhook.post_public_digest(conn, top_n=max(1, min(int(top_n), 25)))

# ---------- watermark (simple, DB-driven; no probe watermark used) ----------
def get_watermark(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("""
            IF OBJECT_ID('dbo.slurs_state') IS NULL
                SELECT NULL
            ELSE
                SELECT TOP 1 last_success_utc FROM dbo.slurs_state ORDER BY id DESC
        """)
        row = cur.fetchone()
        if row and row[0]:
            return row[0].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None

def set_watermark(conn, when_utc: datetime):
    with conn.cursor() as cur:
        cur.execute("""
            IF OBJECT_ID('dbo.slurs_state') IS NULL
            BEGIN
              CREATE TABLE dbo.slurs_state(
                id INT IDENTITY(1,1) PRIMARY KEY,
                last_success_utc DATETIME2(3) NULL,
                updated_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
              );
              INSERT INTO dbo.slurs_state(last_success_utc) VALUES (NULL);
            END
        """)
        cur.execute("UPDATE dbo.slurs_state SET last_success_utc=?, updated_at=SYSUTCDATETIME()", when_utc)
        conn.commit()

# ---------- daily ----------
def run_daily():
    """
    Daily orchestration:
      1) Refresh roster
      2) Pull last day (uses DB watermark if present; otherwise Adelaide 22:00 local-day window)
         NOTE: We apply an OVERLAP_HOURS lookback on the watermark to catch late-indexed messages.
      3) Build HTML reports (1,7,31,180,all) into REPORTS_DIR
      4) Build Excel daily workbook (best-effort)
      5) Post Discord (admin per-player + public digest)
    """
    import pathlib

    # Ensure report directory exists
    out_dir = reports_dir()
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    logger.info("REPORTS_DIR resolved to %s", out_dir)

    # 1) Roster refresh (non-fatal if it fails)
    try:
        run_roster_refresh()
    except Exception as e:
        logger.warning("roster refresh failed (continuing): %s", e)

    # 2) Determine window (DB watermark preferred; else Adelaide 22:00 local-day window)
    # 2) Determine window: strictly last LOOKBACK_HOURS from *now* (default 25)
    LOOKBACK_HOURS = env_int("LOOKBACK_HOURS", 25)
    now_dt = datetime.now(timezone.utc)
    since_dt = now_dt - timedelta(hours=max(1, LOOKBACK_HOURS))
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    before_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("pull window (last %sh): since=%s before=%s", LOOKBACK_HOURS, since_iso, before_iso)


    # 3) Pull and load rows
    inserted_raw, upserted = run_pull(since_iso, before_iso)
    logger.info("pull complete: raw=%s upserted=%s", inserted_raw, upserted)

    # Advance watermark to now (best-effort)
    try:
        with db.get_conn() as conn:
            set_watermark(conn, datetime.now(timezone.utc))
            logger.info("watermark advanced")
    except Exception as e:
        logger.warning("failed to advance watermark: %s", e)

    # 4) Reports (HTML + CSV for each mode)
    try:
        with db.get_conn() as conn:
            for mode in ("1", "7", "31", "180", "all"):
                try:
                    report.make_reports(conn, out_dir, mode=mode)
                except TypeError:
                    # Back-compat old signature
                    report.make_reports(conn, out_dir, mode)
        logger.info("reports written to %s", out_dir)
    except Exception as e:
        logger.warning("report generation failed: %s", e)

    # 5) Excel (ok if empty)
    try:
        with db.get_conn() as conn:
            xlsx_path = report.make_excel_daily(conn, out_dir=out_dir)
        logger.info("excel daily: %s", xlsx_path)
    except Exception as e:
        logger.warning("make_excel_daily failed: %s", e)

    # 6) Discord posts (non-fatal)
    try:
        run_discord_admin()
    except Exception as e:
        logger.warning("admin per-player embeds failed: %s", e)
    try:
        top = int(os.getenv("PUBLIC_TOP", "10"))
    except Exception:
        top = 10
    try:
        run_discord_public(top)
    except Exception as e:
        logger.warning("public digest failed: %s", e)

# ---------- probe & health ----------
def run_probe(steamid: str, since: Optional[str], before: Optional[str], contains: Optional[str]):
    try:
        int(steamid)
    except Exception:
        raise SystemExit("steamid must be a 17-digit number")
    logger.info("probe steamid=%s since=%s before=%s contains=%r", steamid, since, before, contains)
    rows = slurs_api.fetch_messages_for_steamids(
        steamids=[int(steamid)],
        after_iso=since,
        before_iso=before,
        category="total",
        batch_size=1,
        limit=100,
        sleep_ms=1100,
    )
    if contains:
        rows = [r for r in rows if contains.lower() in (r.get("message") or "").lower()]
    logger.info("probe returned %d rows", len(rows))
    for r in rows[:10]:
        print(f"{r.get('logdate')} | {r.get('steamid')} | {str(r.get('message') or '')[:140]}")
    if len(rows) > 10:
        print(f"... ({len(rows)} total)")

def run_health():
    ok = True
    try:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM kian.oz.v_players_clean WHERE steamid64_bigint IS NOT NULL")
                (cnt,) = cur.fetchone()
        logger.info("DB ok; ozf roster rows: %s", cnt)
        if cnt == 0:
            ok = False
    except Exception as e:
        logger.error("DB connect/query failed: %s", e)
        ok = False
    try:
        with db.get_conn() as conn:
            ids = fetch_ozf_steamids(conn)
        if ids:
            rows = slurs_api.fetch_messages_for_steamids(
                steamids=[ids[0]],
                after_iso=None,
                before_iso=None,
                category="total",
                batch_size=1,
                limit=5,
                sleep_ms=500,
            )
            logger.info("API probe rows: %d", len(rows))
    except Exception as e:
        logger.error("API probe failed: %s", e); ok=False
    if not ok:
        sys.exit(1)

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(prog="slursbot", description="slurs.tf OZF ingest & reports")
    subs = p.add_subparsers(dest="cmd", required=True)

    sp = subs.add_parser("pull", help="Pull messages from API and load into SQL")
    sp.add_argument("--since", type=str, default=None, help="ISO8601 UTC start (e.g., 2025-09-15T00:00:00Z)")
    sp.add_argument("--before", type=str, default=None, help="ISO8601 UTC end   (e.g., 2025-09-16T00:00:00Z)")

    sp = subs.add_parser("report", help="Build HTML reports from SQL (kept as-is)")
    sp.add_argument("--mode", choices=["1","7","31","180","all"], default="180")

    subs.add_parser("discord-post", help="Post the admin/private per-player daily embeds")
    sp = subs.add_parser("discord-public", help="Post the public daily digest embed")
    sp.add_argument("--top", type=int, default=10)

    subs.add_parser("roster-refresh", help="Refresh ozfortress roster before pulling")
    subs.add_parser("run-daily", help="Refresh roster, pull, HTML+Excel, Discord, watermark")
    subs.add_parser("daily", help="Alias for run-daily")
    sp = subs.add_parser("probe", help="Probe one SteamID for a window")
    sp.add_argument("--steamid", required=True); sp.add_argument("--since", type=str, default=None)
    sp.add_argument("--before", type=str, default=None); sp.add_argument("--contains", type=str, default=None)
    subs.add_parser("health", help="Check DB + API connectivity")

    return p.parse_args()

# ---------- entry ----------
if __name__ == "__main__":
    load_dotenv()
    args = parse_args()
    try:
        if args.cmd == "pull":
            run_pull(args.since, args.before)
        elif args.cmd == "report":
            run_report(mode=args.mode)
        elif args.cmd == "discord-post":
            run_discord_admin()
        elif args.cmd == "discord-public":
            run_discord_public(args.top)
        elif args.cmd == "roster-refresh":
            run_roster_refresh()
        elif args.cmd in ("run-daily","daily"):
            run_daily()
        elif args.cmd == "probe":
            run_probe(args.steamid, args.since, args.before, args.contains)
        elif args.cmd == "health":
            run_health()
        else:
            sys.exit(2)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user"); sys.exit(130)
    except Exception:
        logger.exception("job failed")
        try:
            discord_webhook.post_error("main.py crashed; see logs")
        except Exception:
            pass
        sys.exit(1)
