# slurs_api.py
# Robust slurs.tf client with paging, retries, category fallback, and safe filtering.
from __future__ import annotations

import logging
import os
import time
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

try:
    import yaml  # for lexicon.yaml
except Exception:  # pragma: no cover
    yaml = None

try:
    from dotenv import load_dotenv  # optional, but handy
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger("slursbot")

# -------------------------
# Config
# -------------------------
BASE = os.getenv("SLURS_API_BASE", "https://slurs.tf").rstrip("/")
API_MESSAGES = f"{BASE}/api/messages"

# polite identification
DEFAULT_HEADERS = {
    "User-Agent": f"ozf-slursbot/{os.getenv('SLURSBOT_VERSION', 'dev')}",
}

# default knobs (can be overridden by caller or env)
DEFAULT_TIMEOUT_S = float(os.getenv("SLURS_HTTP_TIMEOUT_S", "25"))
DEFAULT_LIMIT = int(os.getenv("SLURS_LIMIT", "100"))
DEFAULT_BATCH_SIZE = min(int(os.getenv("SLURS_BATCH_SIZE", "10")), 10)
DEFAULT_SLEEP_MS = int(os.getenv("SLURS_SLEEP_MS", "1100"))
DEFAULT_RETRIES_S = [
    int(x.strip()) for x in os.getenv("SLURS_RETRIES_S", "10,30,300,900").split(",") if x.strip()
]


# -------------------------
# Helpers
# -------------------------
STEAM64_BASE = 76561197960265728


def _steam3_to_steam64(s: str) -> Optional[int]:
    """
    Convert Steam3 like '[U:1:33844719]' or 'U:1:33844719' to Steam64 (76561197960265728 + 33844719).
    Returns None if conversion fails.
    """
    if not s:
        return None
    # grab the last group of digits
    import re

    m = re.findall(r"(\d+)", str(s))
    if not m:
        return None
    try:
        account_id = int(m[-1])
    except Exception:
        return None
    return STEAM64_BASE + account_id


def _chunk(seq: Sequence[int], size: int) -> Iterable[List[int]]:
    size = max(1, int(size))
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def _env_bool(k: str, default: bool) -> bool:
    v = os.getenv(k)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _get_timeout() -> float:
    try:
        return float(os.getenv("SLURS_HTTP_TIMEOUT_S", str(DEFAULT_TIMEOUT_S)))
    except Exception:
        return DEFAULT_TIMEOUT_S


def _load_lexicon_words(path: str) -> List[str]:
    """
    Load a simple list of lowercase 'words' from lexicon.yaml.
    We accept several shapes:
      - a YAML list: ["word1","word2",...]
      - {words: [...]} or {terms: [...]}
    Empty/missing file => returns [] (and fallback will fail-closed).
    """
    words: List[str] = []
    p = os.getenv("LEXICON_PATH", path)
    try:
        if yaml is None:
            logger.warning("PyYAML not available; lexicon disabled.")
            return []
        if not os.path.exists(p):
            logger.warning("lexicon file not found at %s", p)
            return []
        with open(p, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if isinstance(doc, list):
            words = [str(x).strip().lower() for x in doc if str(x).strip()]
        elif isinstance(doc, dict):
            if "words" in doc and isinstance(doc["words"], list):
                words = [str(x).strip().lower() for x in doc["words"] if str(x).strip()]
            elif "terms" in doc and isinstance(doc["terms"], list):
                words = [str(x).strip().lower() for x in doc["terms"] if str(x).strip()]
            else:
                # flatten any list-like values
                for v in doc.values():
                    if isinstance(v, list):
                        words.extend([str(x).strip().lower() for x in v if str(x).strip()])
        words = sorted(set(words))
    except Exception as e:  # pragma: no cover
        logger.warning("Failed parsing lexicon: %s", e)
        return []
    return words


def _text_contains_any(text: str, words: List[str]) -> bool:
    if not text or not words:
        return False
    t = text.lower()
    return any(w in t for w in words)


def _normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize slurs.tf payload row to a stable dict for DB layer.
    We keep original keys too (so db.py can pick what it wants).
    """
    out: Dict[str, Any] = dict(r)  # keep original
    # time
    iso = (r.get("msg_time_iso") or r.get("logdate") or r.get("messagedate") or "").strip()
    if not iso and isinstance(r.get("time"), str):
        iso = r["time"].strip()
    if iso:
        out["msg_time_iso"] = iso

    # message/text
    msg = r.get("message")
    if msg is not None and "text" not in out:
        out["text"] = msg

    # logid (string)
    if "logid" in r and r.get("logid") is not None:
        out["logid"] = str(r["logid"])

    # steam id forms
    sid_any = r.get("steamid64") or r.get("steamid")
    if sid_any is not None:
        out["steamid"] = str(sid_any)  # keep original name for compatibility
    # add steamid64 if we can
    if "steamid64" not in out:
        s3 = str(out.get("steamid", ""))
        if s3.isdigit() and len(s3) == 17:
            out["steamid64"] = s3
        else:
            sid64 = _steam3_to_steam64(s3)
            if sid64 is not None:
                out["steamid64"] = str(sid64)

    return out


# -------------------------
# HTTP
# -------------------------
def _get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout_s: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """
    GET url and parse JSON.
    Returns:
      dict on 2xx (attempting JSON; if non-JSON, returns {'success': False, '__status__': 'non_json'})
      {'success': False, '__status__': 'timeout'/'conn_err'/HTTPcode/..} on soft errors
      None on unexpected/irrecoverable
    """
    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    t = timeout_s if timeout_s is not None else _get_timeout()

    try:
        r = requests.get(url, headers=hdrs, timeout=t)
        if 200 <= r.status_code < 300:
            try:
                return r.json()
            except Exception:
                snippet = r.text[:200].replace("\n", " ")
                logger.info("Non-JSON 2xx at %s CT=%s Body~%s", url, r.headers.get("Content-Type", ""), snippet)
                return {"success": False, "__status__": "non_json"}
        else:
            body_snip = r.text[:200].replace("\n", " ")
            logger.info("HTTP %s on %s Body~%s", r.status_code, url, body_snip)
            return {"success": False, "__status__": str(r.status_code)}
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
        logger.warning("Timeout on %s (%.1fs): %s", url, t, e)
        return {"success": False, "__status__": "timeout"}
    except requests.exceptions.ConnectionError as e:
        logger.warning("Connection error on %s: %s", url, e)
        return {"success": False, "__status__": "conn_err"}
    except Exception as e:
        logger.warning("Unexpected error on %s: %s", url, e)
        return {"success": False, "__status__": "unknown_err"}


def _build_url(ids_chunk: List[int], include_category: bool, limit: int, offset: int, after_iso: Optional[str], before_iso: Optional[str]) -> str:
    q: List[str] = []
    for sid in ids_chunk:
        q.append(f"steamid={sid}")
    if include_category:
        q.append("category=total")
    if after_iso:
        q.append(f"after={urllib.parse.quote_plus(after_iso)}")
    if before_iso:
        q.append(f"before={urllib.parse.quote_plus(before_iso)}")
    q.append(f"limit={int(limit)}")
    q.append(f"offset={int(offset)}")
    return f"{API_MESSAGES}?{'&'.join(q)}"


def _page_request(ids_chunk: List[int], offset: int, include_category: bool, limit: int, after_iso: Optional[str], before_iso: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = _build_url(ids_chunk, include_category, limit, offset, after_iso, before_iso)
    logger.info("REQUEST %s", url)
    resp = _get_json(url)
    if isinstance(resp, dict) and resp.get("success") is False:
        logger.info("RESPONSE soft-fail: %s (offset=%s)", resp.get("__status__"), offset)
        return None, str(resp.get("__status__"))
    if not resp:
        logger.info("RESPONSE empty/None (offset=%s)", offset)
        return None, "empty"
    data = resp.get("data")
    if data is None:
        logger.info("RESPONSE missing 'data' key (offset=%s)", offset)
        return {"data": []}, None
    logger.info("RESPONSE ok: %s items (offset=%s)", len(data), offset)
    return {"data": data}, None


def _paginate(ids_chunk: List[int], include_category: bool, *, limit: int, after_iso: Optional[str], before_iso: Optional[str], sleep_ms: int, retries_s: List[int]) -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
    """
    Pull pages until fewer than 'limit' returned.
    On soft failures (timeout/conn), retry with backoff; if still failing, return (False, partial_rows, last_status).
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    last_status: Optional[str] = None

    while True:
        resp, status = _page_request(ids_chunk, offset, include_category, limit, after_iso, before_iso)
        if resp is None:
            # soft failure; backoff/retry this same offset
            last_status = status
            for s in retries_s:
                logger.info("paginate retry in %ss (offset=%s)", s, offset)
                time.sleep(s)
                resp, status = _page_request(ids_chunk, offset, include_category, limit, after_iso, before_iso)
                if resp is not None:
                    break
            if resp is None:
                logger.warning("paginate giving up at offset=%s (soft errors)", offset)
                return False, out, last_status

        rows = resp.get("data", [])
        if not isinstance(rows, list):
            rows = []
        out.extend(rows)

        # throttle between pages
        if sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)

        if len(rows) < limit:
            return True, out, None
        offset += limit


def _fetch_chunk(ids_chunk: List[int], *, category: Optional[str], limit: int, after_iso: Optional[str], before_iso: Optional[str], sleep_ms: int, retries_s: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch one chunk of steamids, using category if provided.
    On server errors with category (e.g., 500), retry without category and then **filter locally** using lexicon.yaml.
    If lexicon is empty/missing, we **fail-closed** and skip that fallback payload.
    """
    use_cat = bool(category)
    ok, rows, last_status = _paginate(
        ids_chunk=ids_chunk,
        include_category=use_cat,
        limit=limit,
        after_iso=after_iso,
        before_iso=before_iso,
        sleep_ms=sleep_ms,
        retries_s=retries_s,
    )
    if ok:
        return rows

    # Category fallback for server-side issues (timeout/500/etc.)
    serverish = last_status in {"500", "502", "503", "504", "timeout", "conn_err", "non_json", "empty"}
    if use_cat and serverish:
        logger.info("FALLBACK: retrying WITHOUT category for ids=%s", ",".join(str(x) for x in ids_chunk))
        ok2, rows2, _ = _paginate(
            ids_chunk=ids_chunk,
            include_category=False,
            limit=limit,
            after_iso=after_iso,
            before_iso=before_iso,
            sleep_ms=sleep_ms,
            retries_s=retries_s,
        )
        if ok2:
            # Require lexicon words to filter, else fail-closed.
            lex_words = _load_lexicon_words(os.getenv("LEXICON_PATH", "lexicon.yaml"))
            if not lex_words:
                logger.warning("lexicon empty/missing; skipping fallback ingestion for ids=%s", ",".join(map(str, ids_chunk)))
                return []
            filtered = [r for r in rows2 if _text_contains_any(str(r.get("message", "")), lex_words)]
            logger.info("FALLBACK filtered %s/%s rows by lexicon", len(filtered), len(rows2))
            return filtered

    # Otherwise return what we have (partial rows may be >0)
    return rows


# -------------------------
# Public API
# -------------------------
def fetch_messages_for_steamids(
    *,
    steamids: Sequence[int],
    after_iso: Optional[str],
    before_iso: Optional[str],
    category: Optional[str] = "total",
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: int = DEFAULT_LIMIT,
    sleep_ms: int = DEFAULT_SLEEP_MS,
    retries_s: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch slur-flagged messages for the given Steam64 IDs.

    Args:
      steamids: sequence of Steam64 ints
      after_iso: ISO8601 UTC lower bound (inclusive) or None
      before_iso: ISO8601 UTC upper bound (exclusive) or None
      category: "total" to use server-side slur classification; or None to omit
      batch_size: max IDs per request (API tolerated up to ~10)
      limit: page size (default 100)
      sleep_ms: delay between pages to be gentle
      retries_s: backoff schedule for soft failures

    Returns:
      List of normalized rows. Each row contains at least:
        - message/message text in 'message' and 'text'
        - 'logid' (stringified)
        - 'msg_time_iso' (ISO8601)
        - 'steamid' (original from API) and 'steamid64' when derivable
    """
    if not steamids:
        return []

    if retries_s is None:
        retries_s = DEFAULT_RETRIES_S

    # Ensure ints
    ids: List[int] = [int(x) for x in steamids if str(x).isdigit()]

    all_rows: List[Dict[str, Any]] = []

    for chunk in _chunk(ids, batch_size):
        try:
            raw_rows = _fetch_chunk(
                chunk,
                category=category,
                limit=limit,
                after_iso=after_iso,
                before_iso=before_iso,
                sleep_ms=sleep_ms,
                retries_s=retries_s,
            )
        except Exception as e:
            logger.warning("Chunk fetch failed for ids=%s: %s", ",".join(map(str, chunk)), e)
            raw_rows = []

        # Normalize each row and add convenience fields
        for r in raw_rows:
            # slurs.tf "data" rows typically look like:
            # { "steamid":"[U:1:33844719]", "message":"...", "messagedate":"2025-09-17T12:35:34.000Z", "logid":"3934184", "logdate":"2025-09-17T12:35:34.000Z" }
            # We keep compatibility for db.upsert_messages (text/message; msg_time_iso/logdate/messagedate; steamid/steamid64).
            # Standardize an ISO field so the DB layer doesn't guess.
            if "msg_time_iso" not in r:
                if r.get("logdate"):
                    r["msg_time_iso"] = str(r["logdate"])
                elif r.get("messagedate"):
                    r["msg_time_iso"] = str(r["messagedate"])
            if r.get("message") is not None and r.get("text") is None:
                r["text"] = r["message"]

            # add steamid64 when possible (helps DB layer and joins)
            sid_any = r.get("steamid64") or r.get("steamid")
            if sid_any is not None and not str(sid_any).isdigit():
                sid64 = _steam3_to_steam64(str(sid_any))
                if sid64 is not None:
                    r["steamid64"] = str(sid64)

            all_rows.append(_normalize_row(r))

        # Be gentle between chunks too
        if sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)

    return all_rows
