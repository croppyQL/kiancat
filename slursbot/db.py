import os
import pyodbc
from hashlib import sha256
from dotenv import load_dotenv
import re
import os
import logging
from hashlib import sha256
from typing import Iterable, List, Dict, Any, Optional

import pyodbc

STEAM64_BASE = 76561197960265728

# db.py — DB helpers for slursbot

logger = logging.getLogger("slursbot")

# ---- env loading early (so importers see vars) ----
def _load_env():
    try:
        from env_loader import load as _load
        _load()
    except Exception:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass

_load_env()

# ---- connection string resolution ----
def _resolve_conn_str() -> str:
    conn = os.getenv("SQLSERVER_CONN_STR")
    if conn:
        return conn
    # Fallback: build from pieces if provided
    server   = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    if all([server, database, user, password]):
        return (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={server};Database={database};"
            f"UID={user};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )
    raise RuntimeError(
        "Missing SQL connection info. Set SQLSERVER_CONN_STR or DB_SERVER/DB_DATABASE/DB_USER/DB_PASSWORD "
        "in .env.secrets"
    )

CONN_STR = _resolve_conn_str()

def get_conn():
    return pyodbc.connect(CONN_STR)

# ---- roster helpers (some code uses this) ----
def get_ozf_steamids() -> List[str]:
    """
    Legacy helper: returns steamid64 strings from kian.oz.players.
    """
    sql = "SELECT steamid64 FROM kian.oz.players WHERE steamid64 IS NOT NULL"
    good: List[str] = []
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for (sid,) in cur.fetchall():
            s = str(sid).strip()
            if s.isdigit() and len(s) == 17:
                good.append(s)
    return good

# ---- raw ingest ----
def insert_raw_rows(rows: List[Dict[str, Any]], table: str = "dbo.slurs_raw") -> int:
    """
    Insert original API rows into a raw table for auditing.
    Expected columns in the target table:
      source nvarchar, message_id nvarchar, steamid64 bigint, logid bigint,
      logdate_txt nvarchar, text nvarchar, payload_json nvarchar(max)
    """
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table}
      (source, message_id, steamid64, logid, logdate_txt, text, payload_json)
    VALUES (?,?,?,?,?,?,?)
    """
    inserted = 0
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        for r in rows:
            source   = r.get("source", "slurs.tf")
            message_id = r.get("message_id")
            steamid64  = r.get("steamid") or r.get("steamid64")
            logid      = r.get("logid")
            logdate    = r.get("logdate") or r.get("msg_time_iso") or r.get("messagedate")
            text       = r.get("message") or r.get("text")
            payload    = r.get("payload_json")

            cur.execute(
                sql,
                source,
                message_id,
                int(steamid64) if steamid64 is not None and str(steamid64).strip().isdigit() else None,
                int(logid) if logid is not None and str(logid).strip().isdigit() else None,
                str(logdate) if logdate is not None else None,
                text,
                payload
            )
            inserted += 1
        conn.commit()
    return inserted

# ---- typed upsert (dedupe by hash_key) ----
def upsert_messages(rows: List[Dict[str, Any]], table: str = "dbo.slurs_msg") -> int:
    """
    Accepts either:
      - {'steamid','message','logdate','logid'}  (slurs_api shape)
      - {'steamid64','text','msg_time_iso','logid'} (older shape)
    Uses SHA256(steamid64|iso|text) as dedupe key stored in hash_key.
    """
    if not rows:
        return 0

    import logging
    logger = logging.getLogger("slursbot")

    sql = f"""
    IF NOT EXISTS (SELECT 1 FROM {table} WHERE hash_key = ?)
    INSERT INTO {table} (message_id, steamid64, logid, msg_time_utc, text, hash_key)
    VALUES (?,?,?,?,?,?)
    """
    inserted = 0
    skipped  = 0

    with get_conn() as conn, conn.cursor() as cur:
        for r in rows:
            sid = r.get("steamid64") or r.get("steamid")
            iso = (r.get("msg_time_iso") or r.get("logdate") or r.get("messagedate") or "").strip()
            text = r.get("text") or r.get("message")
            sid_str = str(sid).strip() if sid is not None else ""

            if not (sid_str.isdigit() and len(sid_str) == 17):
                skipped += 1
                logger.warning("Skipping row: invalid steamid64=%r", sid)
                continue
            if not iso or not text:
                skipped += 1
                logger.warning("Skipping row: missing timestamp/text (steamid64=%s)", sid_str)
                continue

            logid_raw = r.get("logid")
            try:
                logid = int(logid_raw) if logid_raw is not None and str(logid_raw).strip().isdigit() else None
            except Exception:
                logid = None

            sid_int = int(sid_str)
            hk = sha256(f"{sid_str}|{iso}|{text}".encode("utf-8")).hexdigest()

            cur.execute(sql,
                        hk,                         # WHERE hash_key = ?
                        r.get("message_id"),
                        sid_int,
                        logid,
                        iso,                        # DATETIMEOFFSET-compatible ISO
                        text,
                        hk)
            if cur.rowcount:
                inserted += 1

        conn.commit()

    if skipped:
        logger.info("Upsert: inserted=%d, skipped_invalid=%d", inserted, skipped)
    else:
        logger.info("Upsert: inserted=%d", inserted)

    return inserted

# ---- ozfortress players upsert ----
def get_max_oz_id(conn) -> int:
    """
    Returns MAX(oz_id) from kian.oz.players, or 0 if table empty.
    """
    sql = "SELECT ISNULL(MAX(CAST(oz_id AS BIGINT)), 0) FROM kian.oz.players"
    with conn.cursor() as cur:
        cur.execute(sql)
        (mx,) = cur.fetchone()
        try:
            return int(mx or 0)
        except Exception:
            return 0

def upsert_oz_players(conn, rows: List[Dict[str, Any]]) -> int:
    """
    rows: list of dicts with:
      {'oz_id':str|int, 'steamid64':str(17) or None,
       'current_name':str|None, 'oz_profile_url':str|None, 'steam_profile_url':str|None}

    Upserts on oz_id. Ensures current_name is NEVER NULL (uses 'ozf_user_<oz_id>' fallback).
    Skips rows without a steamid64.
    """
    if not rows:
        return 0

    sql = """
    MERGE kian.oz.players AS tgt
    USING (SELECT
             CAST(? AS BIGINT)  AS oz_id,
             ?                   AS steamid64,
             ?                   AS current_name,
             ?                   AS oz_profile_url,
             ?                   AS steam_profile_url
           ) AS src
    ON (tgt.oz_id = src.oz_id)
    WHEN MATCHED THEN
      UPDATE SET
        tgt.steamid64         = COALESCE(src.steamid64, tgt.steamid64),
        tgt.current_name      = CASE WHEN NULLIF(LTRIM(RTRIM(src.current_name)),'') IS NOT NULL
                                     THEN LTRIM(RTRIM(src.current_name))
                                     ELSE tgt.current_name END,
        tgt.oz_profile_url    = COALESCE(src.oz_profile_url, tgt.oz_profile_url),
        tgt.steam_profile_url = COALESCE(src.steam_profile_url, tgt.steam_profile_url),
        tgt.updated_at        = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (oz_id, steamid64, current_name, oz_profile_url, steam_profile_url, created_at, updated_at)
      VALUES (src.oz_id,
              src.steamid64,
              LTRIM(RTRIM(COALESCE(NULLIF(src.current_name,''), CONCAT('ozf_user_', CAST(src.oz_id AS NVARCHAR(32)))))),
              src.oz_profile_url,
              src.steam_profile_url,
              SYSUTCDATETIME(), SYSUTCDATETIME());
    """

    count = 0
    with conn.cursor() as cur:
        try:
            cur.fast_executemany = True
        except Exception:
            pass

        for r in rows:
            oz_id = r.get("oz_id")
            steamid = r.get("steamid64")
            if not steamid:
                # No steam ID, nothing to store (skip)
                continue

            nm = r.get("current_name") or ""
            name = nm.strip()
            if not name:
                name = f"ozf_user_{oz_id}"

            cur.execute(
                sql,
                str(oz_id),
                str(steamid),
                name,
                (r.get("oz_profile_url") or None),
                (r.get("steam_profile_url") or None),
            )
            count += 1

        conn.commit()

    return count
