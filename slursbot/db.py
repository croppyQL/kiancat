import os
import pyodbc
from hashlib import sha256
from dotenv import load_dotenv

load_dotenv()
CONN_STR = os.environ["SQLSERVER_CONN_STR"]

def get_conn():
    return pyodbc.connect(CONN_STR)

def get_ozf_steamids():
    sql = "SELECT steamid64 FROM kian.oz.players WHERE steamid64 IS NOT NULL"
    good = []
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for (sid,) in cur.fetchall():
            s = str(sid).strip()
            if s.isdigit() and len(s) == 17:
                good.append(s)
    return good

# --- add this whole function to db.py ---
def get_sql_conn():
    """
    Open a SQL Server connection using the .env value SQLSERVER_CONN_STR.
    Returns a live pyodbc connection. Raises a clear error if the variable is missing.
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()  # lets us read .env without exporting env vars in the shell
    conn_str = os.environ.get("SQLSERVER_CONN_STR")
    if not conn_str:
        raise RuntimeError(
            "SQLSERVER_CONN_STR is not set. Put your full ODBC connection string in .env, e.g.\n"
            'SQLSERVER_CONN_STR=DRIVER={ODBC Driver 18 for SQL Server};SERVER=server;DATABASE=db;UID=user;PWD=pass;Encrypt=yes;TrustServerCertificate=yes'
        )
    import pyodbc
    # autocommit False lets your existing code manage transactions if it already does
    return pyodbc.connect(conn_str, autocommit=False)
# --- end add ---


def insert_raw_rows(rows, table="dbo.slurs_raw"):
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table}
      (source, message_id, steamid64, logid, logdate_txt, text, payload_json)
    VALUES (?,?,?,?,?,?,?)
    """
    with get_conn() as conn, conn.cursor() as cur:
        try: cur.fast_executemany = True
        except Exception: pass
        for r in rows:
            source = r.get("source", "slurs.tf")
            message_id = r.get("message_id")
            steamid64 = r.get("steamid") or r.get("steamid64")
            logid = r.get("logid")
            logdate = r.get("logdate") or r.get("msg_time_iso")
            text = r.get("message") or r.get("text")
            payload_json = r.get("payload_json")
            cur.execute(sql,
                        source,
                        message_id,
                        str(steamid64) if steamid64 is not None else None,
                        str(logid) if logid is not None else None,
                        str(logdate) if logdate is not None else None,
                        text,
                        payload_json)
        conn.commit()
        return len(rows)

def upsert_messages(rows, table="dbo.slurs_msg"):
    """
    Accepts either:
      - {'steamid','message','logdate','logid'}  (shape from slurs_api)
      - {'steamid64','text','msg_time_iso','logid'} (older shape)
    Uses SHA256(steamid64|iso|text) as dedupe key.
    """
    import logging
    logger = logging.getLogger("slursbot")
    if not rows:
        return 0

    sql = f"""
    IF NOT EXISTS (SELECT 1 FROM {table} WHERE hash_key = ?)
    INSERT INTO {table} (message_id, steamid64, logid, msg_time_utc, text, hash_key)
    VALUES (?,?,?,?,?,?)
    """

    inserted = 0
    skipped = 0

    with get_conn() as conn, conn.cursor() as cur:
        for r in rows:
            sid = r.get("steamid64") or r.get("steamid")
            iso = (r.get("msg_time_iso") or r.get("logdate") or "").strip()
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

# ---------------------- OZF ROSTER HELPERS ----------------------

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

def upsert_oz_players(conn, rows):
    """
    rows: list of dicts with:
      {'oz_id':str/int, 'steamid64':str(17) or None,
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
        -- only overwrite name when we have a non-empty one
        tgt.current_name      = CASE WHEN NULLIF(LTRIM(RTRIM(src.current_name)),'') IS NOT NULL
                                     THEN LTRIM(RTRIM(src.current_name))
                                     ELSE tgt.current_name END,
        tgt.oz_profile_url    = COALESCE(src.oz_profile_url, tgt.oz_profile_url),
        tgt.steam_profile_url = COALESCE(src.steam_profile_url, tgt.steam_profile_url),
        tgt.updated_at        = SYSUTCDATETIME(),
        tgt.last_checked_at   = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (oz_id, steamid64, current_name, oz_profile_url, steam_profile_url, created_at, updated_at, last_checked_at)
      VALUES (src.oz_id, src.steamid64, src.current_name, src.oz_profile_url, src.steam_profile_url,
              SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME());
    """

    count = 0
    with conn.cursor() as cur:
        try:
            cur.fast_executemany = True
        except Exception:
            pass

        for r in rows:
            oz_id_raw = r.get("oz_id")
            steamid = r.get("steamid64")
            if not steamid:
                # skip profiles without a Steam link
                continue

            # Ensure oz_id is a simple int
            try:
                oz_id = int(str(oz_id_raw).strip())
            except Exception:
                continue

            # NEVER pass NULL for current_name (table is NOT NULL)
            name = (r.get("current_name") or "").strip()
            if not name:
                name = f"ozf_user_{oz_id}"

            cur.execute(
                sql,
                str(oz_id),
                str(steamid),
                name,                              # guaranteed non-empty string
                (r.get("oz_profile_url") or None),
                (r.get("steam_profile_url") or None),
            )
            # rowcount can be 0 for MATCH without change; we still consider it processed
            count += 1

        conn.commit()
    return count
