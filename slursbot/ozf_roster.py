# ozf_roster.py — refresh kian.oz.players from ozfortress.com/users/<id>
# - Finds current max oz_id in DB
# - Probes forward until N consecutive 404s
# - Extracts SteamID64 from the profile HTML (steamcommunity profiles link)
# - Upserts into kian.oz.players (minimal fields + timestamps/URLs)

import re
import time
import logging
from typing import Optional, List, Dict

import requests

logger = logging.getLogger("slursbot")

RE_STEAM = re.compile(r'https?://steamcommunity\.com/profiles/(\d{17})', re.I)
RE_NAME  = re.compile(r'<h1[^>]*>(.*?)</h1>', re.I | re.S)

def _get(url: str, timeout: int = 30) -> requests.Response:
    hdrs = {
        "User-Agent": "slursbot/1.1 (+ozfortress roster refresh)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://ozfortress.com/",
    }
    r = requests.get(url, headers=hdrs, timeout=timeout)
    return r

def probe_user(user_id: int) -> Dict[str, Optional[str]]:
    """
    Returns dict with oz_id, steamid64 (or None if missing),
    current_name (best-effort), oz_profile_url, steam_profile_url.
    If 404, returns {'oz_id': id, 'steamid64': None} (signal to caller).
    """
    url = f"https://ozfortress.com/users/{user_id}"
    r = _get(url)
    if r.status_code == 404:
        return {"oz_id": str(user_id), "steamid64": None}
    r.raise_for_status()

    html = r.text or ""
    m_sid = RE_STEAM.search(html)
    steamid64 = m_sid.group(1) if m_sid else None

    m_name = RE_NAME.search(html)
    raw_name = (m_name.group(1).strip() if m_name else "") or ""
    # crude strip of HTML tags
    current_name = re.sub(r"<[^>]+>", "", raw_name).strip()

    steam_url = f"https://steamcommunity.com/profiles/{steamid64}" if steamid64 else None
    return {
        "oz_id": str(user_id),
        "steamid64": steamid64,
        "current_name": current_name or None,
        "oz_profile_url": url,
        "steam_profile_url": steam_url,
    }

def refresh(conn,
            max_probe: int = 300,
            stop_after_404: int = 20,
            sleep_ms: int = 200):
    """
    Probes forward from MAX(oz_id) in DB up to max_probe pages,
    stopping early after 'stop_after_404' consecutive 404s.
    Upserts any pages that expose a SteamID64.

    Returns: (checked_count, inserted_or_updated_count)
    """
    from db import get_max_oz_id, upsert_oz_players  # local import to avoid cycles

    base = get_max_oz_id(conn) or 0
    checked = 0
    changed = 0
    streak_404 = 0

    logger.info("Roster refresh: starting from oz_id=%s", base)
    for i in range(1, int(max_probe) + 1):
        oz_id = base + i
        rec = probe_user(oz_id)
        checked += 1

        if rec.get("steamid64"):
            streak_404 = 0
            changed += upsert_oz_players(conn, [rec])
            logger.info("oz_id=%s steamid64=%s name=%s", oz_id, rec.get("steamid64"), (rec.get("current_name") or "")[:48])
        else:
            streak_404 += 1
            logger.info("oz_id=%s → 404 (streak=%s)", oz_id, streak_404)

        if streak_404 >= int(stop_after_404):
            logger.info("Stopping after %s consecutive 404s.", streak_404)
            break

        time.sleep(max(0, int(sleep_ms)) / 1000.0)

    logger.info("Roster refresh: checked=%s changed=%s", checked, changed)
    return checked, changed
