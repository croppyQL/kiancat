# discord_webhook.py — admin/public embeds: daily offenders, no-offenders notice, and roster summary
import os, time, logging, requests

logger = logging.getLogger("slursbot.discord")

def _get_env(k, d=""):
    v = os.getenv(k)
    return v if v is not None and str(v).strip() != "" else d

def _admin_url():
    for k in ("ADMIN_WEBHOOK","DISCORD_ADMIN_WEBHOOK","DISCORD_WEBHOOK_URL"):
        u = _get_env(k,"")
        if u: return u
    return ""

def _public_url():
    for k in ("PUBLIC_WEBHOOK","DISCORD_PUBLIC_WEBHOOK"):
        u = _get_env(k,"")
        if u: return u
    return ""

def _post(url, payload):
    if not url:
        logger.warning("Discord webhook URL missing; skipping post.")
        return
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code >= 300:
            logger.warning("Discord webhook %s -> HTTP %s body=%s", url, r.status_code, r.text[:300])
        time.sleep(0.25)
    except Exception as e:
        logger.warning("Discord webhook post failed: %s", e)

# ---------- colors ----------
def _blue():  return int("0x7DD3FC",16)   # sky-300
def _green(): return int("0x86EFAC",16)   # green-300
def _red():   return int("0xFCA5A5",16)   # red-300

# ---------- Admin: roster summary after refresh ----------
def post_admin_roster_summary(conn, checked: int, changed: int):
    """
    Posts an admin embed like:
      "OZF roster refresh"
      Profiles checked: N
      New users discovered: M
      (lists up to 10 newest today with links)
    """
    url = _admin_url()
    if not url:
        logger.warning("ADMIN webhook missing; set ADMIN_WEBHOOK in .env")
        return

    sql = """
    SELECT TOP 10 oz_id, current_name, created_at
    FROM kian.oz.players
    WHERE created_at >= DATEADD(DAY,-1,SYSUTCDATETIME())
    ORDER BY created_at DESC, oz_id DESC;
    """
    new_list = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            for (oz_id, name, created_at) in cur.fetchall():
                nm = name or "(unknown)"
                new_list.append((int(oz_id), nm))
    except Exception as e:
        logger.warning("roster summary: query failed: %s", e)

    desc = f"**Profiles checked:** {checked}\n**New users discovered:** {changed}"
    if changed > 0 and new_list:
        lines = []
        for oz_id, nm in new_list:
            lines.append(f"- [{nm}](https://ozfortress.com/users/{oz_id})")
        desc += "\n\n**Newest today:**\n" + "\n".join(lines)

    payload = {"embeds": [{
        "title":"OZF roster refresh",
        "description": desc,
        "color": _blue()
    }]}
    _post(url, payload)

# ---------- Daily offenders helpers ----------
def _fetch_daily_offenders(conn):
    sql = """
    WITH day_rows AS (
      SELECT m.steamid64,
             CAST(m.msg_time_utc AS datetime2(3)) AS msg_time_utc,
             m.text,
             m.logid
      FROM kiancat.dbo.slurs_msg AS m
      WHERE m.msg_time_utc >= DATEADD(DAY,-1,SYSUTCDATETIME())
    ),
    agg_day AS (
      SELECT steamid64, COUNT(*) AS c1
      FROM day_rows
      GROUP BY steamid64
    ),
    agg_180 AS (
      SELECT steamid64, COUNT(*) AS c180
      FROM kiancat.dbo.slurs_msg
      WHERE msg_time_utc >= DATEADD(DAY,-180,SYSUTCDATETIME())
      GROUP BY steamid64
    )
    SELECT v.current_name, v.oz_id, a.steamid64, a.c1, ISNULL(b.c180,0) AS c180
    FROM agg_day a
    JOIN kian.oz.v_players_clean AS v
      ON v.steamid64_bigint = a.steamid64
    LEFT JOIN agg_180 b
      ON b.steamid64 = a.steamid64
    WHERE a.c1 > 0
    ORDER BY a.c1 DESC, v.current_name ASC;
    """
    out = []
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            out.append({cols[i]: row[i] for i in range(len(cols))})
    return out

def _fetch_daily_messages_for(conn, steamid64: int):
    sql = """
    SELECT CAST(m.msg_time_utc AS datetime2(3)) AS msg_time_utc, m.text, m.logid
    FROM kiancat.dbo.slurs_msg AS m
    WHERE m.steamid64 = ? AND m.msg_time_utc >= DATEADD(DAY,-1,SYSUTCDATETIME())
    ORDER BY m.msg_time_utc DESC;
    """
    rows = []
    with conn.cursor() as cur:
        cur.execute(sql, (int(steamid64),))
        for (dt, text, logid) in cur.fetchall():
            try:
                dt_str = dt.strftime("%Y-%m-%d")
            except Exception:
                dt_str = str(dt)[:10]
            rows.append({"date": dt_str, "text": text or "", "logid": logid})
    return rows

def _ellipsize(s, n):
    if s is None: return ""
    s = str(s);  return s if len(s)<=n else s[:n-1]+"…"

def _lines(msgs):
    """
    Build bullet lines for Discord.
    If a logid exists, we ALWAYS include a link: https://logs.tf/<logid>
    """
    out=[]
    for m in msgs:
        t=_ellipsize(m.get("text",""), 300)
        lg=m.get("logid")
        if lg:
            out.append(f"• **{m.get('date','')}** — {t}  [[logs.tf/{lg}]](https://logs.tf/{lg})")
        else:
            out.append(f"• **{m.get('date','')}** — {t}")
    return out

def _chunk(base, lines, limit=3900):
    embeds=[]; buf=[]; L=0
    for ln in lines:
        add=len(ln)+1
        if buf and L+add>limit:
            e=dict(base); e["description"]="\n".join(buf); embeds.append(e)
            buf=[ln]; L=len(ln)+1
        else:
            buf.append(ln); L+=add
    if buf:
        e=dict(base); e["description"]="\n".join(buf); embeds.append(e)
    return embeds

# ---------- Admin daily per-player ----------
def post_daily_player_embeds(conn):
    url=_admin_url()
    if not url:
        logger.warning("ADMIN webhook missing; set ADMIN_WEBHOOK")
        return
    offenders=_fetch_daily_offenders(conn)
    if not offenders:
        _post(url, {"embeds":[{
            "title":"OZF — Daily Report",
            "description":"No OZF players recorded any flagged messages in the last 24 hours.",
            "color": _green()
        }]})
        return

    for o in offenders:
        name=o["current_name"] or "(unknown)"
        ozid=o["oz_id"]; sid=o["steamid64"]
        c1=int(o["c1"]); c180=int(o["c180"])
        msgs=_fetch_daily_messages_for(conn, sid)
        lines=_lines(msgs)
        oz=f"https://ozfortress.com/users/{ozid}" if ozid else "#"
        st=f"https://slurs.tf/player?steamid={sid}"
        base={
            "title": name,
            "url": oz,
            "color": _blue(),
            "fields":[
                {"name":"Today (c1)","value":str(c1),"inline":True},
                {"name":"Last 180d","value":str(c180),"inline":True},
                {"name":"Links","value":f"[slurs.tf]({st}) • [ozf]({oz})","inline":False},
            ],
            "footer":{"text":f"steamid64: {sid}"}
        }
        for em in _chunk(base, lines):
            _post(url, {"embeds":[em]})

# ---------- Public digest ----------
def post_public_digest(conn, top_n: int = 10):
    url = _public_url()
    offenders = _fetch_daily_offenders(conn)
    if not offenders:
        logger.info("public digest: no offenders; skipping post.")
        return
    offenders = offenders[: max(1, min(int(top_n), 25))]
    lines=[]
    for o in offenders:
        name=o["current_name"] or "(unknown)"
        ozid=o["oz_id"]; c1=int(o["c1"]); c180=int(o["c180"])
        oz=f"https://ozfortress.com/users/{ozid}" if ozid else "#"
        st=f"https://slurs.tf/player?steamid={o['steamid64']}"
        lines.append(f"[{name}]({oz}) — **{c1}** today • **{c180}** in 180d · [slurs.tf]({st})")
    _post(url, {"embeds":[{"title":"OZF — Daily Top Offenders","description":"\n".join(lines),"color":_blue()}]})

def post_error(text: str):
    url=_admin_url() or _public_url()
    if not url: return
    if not text: text="(no details)"
    max_len=1900
    s = text if len(text)<=max_len else text[:max_len-1]+"…"
    _post(url, {"embeds":[{"title":"Error","description":s,"color":_red()}]})


# --- image report posting helpers ---
import os, requests #alr in but no point not having it again.

def _choose_webhook(channel: str = "public") -> str:
    """
    channel: 'public' or 'admin'
    """
    if channel.lower() == "admin":
        url = os.getenv("DISCORD_ADMIN_WEBHOOK_URL")
    else:
        url = os.getenv("DISCORD_PUBLIC_WEBHOOK_URL")
    if not url:
        raise RuntimeError(f"Missing webhook for channel={channel}. "
                           f"Set DISCORD_PUBLIC_WEBHOOK_URL or DISCORD_ADMIN_WEBHOOK_URL.")
    return url

def post_report_images_local(png_paths, channel: str = "public", message: str | None = None) -> None:
    """
    Upload one or more local PNGs to Discord so they render inline.
    """
    url = _choose_webhook(channel)
    # Discord: one message can carry multiple files; keep it modest (<=8)
    png_paths = list(png_paths)[:8]
    if not png_paths:
        return

    # Upload in a single message with optional text
    files = []
    for i, path in enumerate(png_paths):
        files.append(("files[%d]" % i, (os.path.basename(path), open(path, "rb"), "image/png")))
    data = {"content": message or ""}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()

def post_report_image_urls(image_urls, channel: str = "public", message: str | None = None) -> None:
    """
    Post external image URLs (e.g., raw GitHub PNGs) as embeds so Discord displays them inline.
    """
    url = _choose_webhook(channel)
    image_urls = list(image_urls)[:10]
    for img in image_urls:
        data = {
            "content": message or "",
            "embeds": [{"image": {"url": img}}]
        }
        r = requests.post(url, json=data, timeout=30)
        r.raise_for_status()
