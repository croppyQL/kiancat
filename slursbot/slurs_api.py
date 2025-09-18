# slurs_api.py — robust fetcher with fallback for slurs.tf 500 on category=total
from __future__ import annotations
from urllib.parse import urlencode
from dotenv import load_dotenv
import os
import time
import json
import logging
import requests
from typing import List, Optional, Dict, Any, Tuple, Iterable

# -------------------------------------------------------------------
# Config / globals
# -------------------------------------------------------------------
load_dotenv()
logger = logging.getLogger("slursbot")

BASE = os.getenv("SLURS_API_BASE", "https://slurs.tf").rstrip("/")
API_MESSAGES = f"{BASE}/api/messages"

# Fallback lexicon path (client-side filter when we cannot use category=total)
LEXICON_PATH = os.getenv("LEXICON_PATH", "lexicon.yaml")

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _load_lexicon_words() -> set[str]:
    """
    Load a simple lexicon YAML if present, return a set of lowercase words/patterns.
    If file is missing or invalid, return empty set (fallback will then pass-all).
    """
    try:
        import yaml  # type: ignore
        if not os.path.exists(LEXICON_PATH):
            logger.info("lexicon not found at %s; fallback filter will pass-all", LEXICON_PATH)
            return set()
        with open(LEXICON_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        words: set[str] = set()
        # expect schema like:
        # generic: [word1, word2]
        # racial:  [word3]
        # bigotry: [word4]
        for _, arr in (data.items() if isinstance(data, dict) else []):
            if isinstance(arr, list):
                for w in arr:
                    if isinstance(w, str) and w.strip():
                        words.add(w.strip().lower())
        return words
    except Exception as e:
        logger.warning("lexicon load failed (%s); fallback filter will pass-all", e)
        return set()

def _text_contains_any(hay: str, needles: Iterable[str]) -> bool:
    h = (hay or "").lower()
    return any(n in h for n in needles)

def _params_for(ids: List[int],
                after_iso: Optional[str],
                before_iso: Optional[str],
                category: Optional[str],
                limit: int,
                offset: int,
                include_category: bool) -> List[Tuple[str, str]]:
    params: List[Tuple[str, str]] = []
    for sid in ids:
        params.append(("steamid", str(int(sid))))
    if after_iso:
        params.append(("after", after_iso))
    if before_iso:
        params.append(("before", before_iso))
    if include_category and category:
        params.append(("category", category))
    params.append(("limit", str(int(limit))))
    params.append(("offset", str(int(offset))))
    return params

def _get_json(url: str, headers: Dict[str, str] | None = None, timeout_s: float = 15.0) -> Dict[str, Any] | None:
    r = requests.get(url, headers=headers or {}, timeout=timeout_s)
    # 2xx -> attempt json
    if 200 <= r.status_code < 300:
        try:
            return r.json()
        except Exception:
            # Not JSON; log a short snippet for debugging
            snippet = r.text[:200].replace("\n", " ")
            logger.info("Non-JSON 2xx at %s CT=%s Body~%s", url, r.headers.get("Content-Type",""), snippet)
            return None
    # non-2xx
    body_snip = r.text[:200].replace("\n", " ")
    logger.info("HTTP %s on %s Body~%s", r.status_code, url, body_snip)
    # Return a marker so caller can inspect status
    return {"__status__": r.status_code, "__non_json_body__": body_snip}

def _is_server_500(resp: Dict[str, Any] | None) -> bool:
    return isinstance(resp, dict) and resp.get("__status__") == 500

# -------------------------------------------------------------------
# Public function used by main.py
# -------------------------------------------------------------------
def fetch_messages_for_steamids(
    steamids: List[int],
    after_iso: Optional[str] = None,
    before_iso: Optional[str] = None,
    category: str = "",
    batch_size: int = 10,
    limit: int = 100,
    sleep_ms: int = 500,
    retries_s: Tuple[int, ...] | List[int] = (10, 30, 300, 900),
) -> List[Dict[str, Any]]:
    """
    Fetch messages for given SteamIDs with safe defaults:
      - Respects slurs.tf rule: up to 10 steamid params per request.
      - Tries with 'category' first (e.g., 'total').
      - On server 500 (the 'dic.words is not iterable' path), retries the SAME request
        WITHOUT 'category' and then client-filters messages with the lexicon so only
        slur-like messages remain.
      - Paginates with limit/offset until the page returns <limit rows.

    Returns a list of rows with at least:
      steamid, message, logid, logdate (ISO/UTC string as per API)
    """
    # normalize inputs
    batch_size = max(1, min(int(batch_size), 10))  # API allows up to 10 steamids
    limit = max(1, min(int(limit), 100))
    sleep_ms = max(0, int(sleep_ms))
    retry_delays = tuple(int(x) for x in (retries_s if isinstance(retries_s, (list, tuple)) else [])) or (10, 30, 300, 900)
    ids = [int(s) for s in steamids if s and int(s) >= 76561197960265728]

    # load lexicon once for fallback
    lex_words = _load_lexicon_words()

    def page_request(ids_chunk: List[int], offset: int, include_category: bool) -> Dict[str, Any] | None:
        params = _params_for(ids_chunk, after_iso, before_iso, category, limit, offset, include_category)
        url = f"{API_MESSAGES}?{urlencode(params)}"
        logger.info("REQUEST %s", url)
        return _get_json(url)

    def fetch_one_chunk(ids_chunk: List[int]) -> List[Dict[str, Any]]:
        """
        Fetch all pages for this ids_chunk. Try with category first; if 500 -> fallback (no category + client filter).
        """
        out: List[Dict[str, Any]] = []

        def paginate(include_category: bool) -> Tuple[bool, List[Dict[str, Any]]]:
            rows_all: List[Dict[str, Any]] = []
            offset = 0
            while True:
                resp = page_request(ids_chunk, offset, include_category)
                # Network/format/500 handling
                if resp is None:
                    # non-json 2xx -> treat as empty and stop
                    break
                if "__status__" in resp and resp["__status__"] != 200:
                    # non-2xx => return failure flag
                    return False, []
                # normal JSON? expect {"success": true, "data": [...]}
                data = resp.get("data") if isinstance(resp, dict) else None
                if not isinstance(data, list):
                    # unexpected shape — stop
                    break
                logger.info("RESPONSE ok: %d items (offset=%d)", len(data), offset)
                rows_all.extend(data)
                if len(data) < limit:
                    break
                offset += limit
                time.sleep(sleep_ms / 1000.0)
            return True, rows_all

        # 1) Try with category (if provided)
        use_cat = bool(category)
        ok, rows = paginate(include_category=use_cat)
        if ok:
            out.extend(rows)
            return out

        # 2) If the failure looked like a 500, fallback without category
        #    (This bypasses the buggy code path on their server.)
        # We do a small one-shot probe to confirm it is a 500:
        probe = page_request(ids_chunk, 0, include_category=use_cat)
        if _is_server_500(probe):
            logger.info("FALLBACK: retrying without category for ids=%s", ",".join(str(x) for x in ids_chunk))
            ok2, rows2 = paginate(include_category=False)
            if ok2:
                # If we removed category, client-filter with lexicon terms (if any)
                if lex_words:
                    rows2 = [r for r in rows2 if _text_contains_any(r.get("message",""), lex_words)]
                out.extend(rows2)
                return out

        # 3) If still failed (or not a 500), apply backoff retries, then give up for this chunk
        for sec in retry_delays:
            logger.info("RETRY in %ss ...", sec)
            time.sleep(sec)
            ok3, rows3 = paginate(include_category=use_cat)
            if ok3:
                out.extend(rows3)
                return out

        # give up; log and return empty so the caller can continue with other chunks
        sid_str = ",".join(str(x) for x in ids_chunk)
        logger.warning("Giving up on ids=[%s] for this window due to repeated errors.", sid_str)
        return out

    # iterate in batches of ≤ batch_size (≤10)
    all_rows: List[Dict[str, Any]] = []
    for i in range(0, len(ids), batch_size):
        chunk = ids[i:i+batch_size]
        all_rows.extend(fetch_one_chunk(chunk))
        time.sleep(sleep_ms / 1000.0)

    return all_rows
