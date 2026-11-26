#!/usr/bin/env python3
"""
Flight alerts -> ntfy. Designed to run on GitHub Actions every ~5 minutes.
Stdlib only, so the workflow needs no pip install.

Three tiers, so the bot stays worth reading:
  RED    - emergencies and diversions. Sent instantly, always.
  YELLOW - interesting but not urgent. Accumulates, flushed roughly hourly.
  GREEN  - daily roundup, once every 24h.

Every run first proves the data feed is alive. If it isn't we say so rather than
reporting a quiet sky - a silently dead feed is the one failure that makes a
monitor worthless.
"""
import html
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import tracks

BASE = "https://api.adsb.lol"
NTFY_BASE = os.environ.get("NTFY_BASE", "https://ntfy.sh").rstrip("/")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")
STATE_F = "state.json"
UA = "flight-alerts/1.0 (hobby aviation monitor)"

YELLOW_INTERVAL = 55 * 60      # flush the digest roughly hourly
GREEN_INTERVAL = 24 * 60 * 60  # daily roundup
SEEN_TTL = 6 * 60 * 60         # forget an aircraft after 6h
SCAN_INTERVAL = 5 * 60         # expensive regional scan: only every 5 min
STALE_MAX = 15 * 60            # never compare against a position older than this
DIGEST_IMAGES = 10             # max images per digest flush
SCAN_RADIUS_NM = 250           # radius per region query in nautical miles

# Flight-school and light-GA types orbit constantly on training circuits and
# were ~150 false hits per run in testing. A circling C172 is not a story.
TRAINERS = {"C172", "C152", "C162", "C182", "PA28", "PA38", "PA44", "P28A", "P28B",
            "SR20", "SR22", "DA40", "DA42", "DV20", "BE33", "BE35", "BE36", "BE9T",
            "AA5", "GROB", "T206", "C206", "C208", "RV7", "RV8", "RV10", "EVOP",
            "G115", "TECN", "SLG2", "SLG4", "ULAC", "GLID", "B06", "R44", "R22",
            "H500", "C150", "C120", "P32R", "PA32", "BE76", "C72R", "C77R"}
ROUTE_BUDGET = 40              # max NEW route lookups per scan (cache makes repeats free)
ROUTE_CACHE_MAX = 5000         # routes are static, so the cache compounds

# Airline ICAO prefix -> home country, for fifth-freedom detection.
AIRLINE_CC = {
    "CES": "CN", "CCA": "CN", "CSN": "CN", "CHH": "CN", "CXA": "CN", "UAE": "AE",
    "ETD": "AE", "QTR": "QA", "SVA": "SA", "KAC": "KW", "SIA": "SG", "ETH": "ET",
    "MSR": "EG", "RJA": "JO", "ELY": "IL", "THY": "TR", "PGT": "TR", "AJT": "TR",
    "SXS": "TR", "FHY": "TR", "AFL": "RU", "AAL": "US", "UAL": "US", "DAL": "US",
    "BAW": "GB", "VIR": "GB", "AFR": "FR", "DLH": "DE", "KLM": "NL", "AZA": "IT",
    "ITY": "IT", "IBE": "ES", "TAP": "PT", "SWR": "CH", "ANA": "JP", "JAL": "JP",
    "KAL": "KR", "AAR": "KR", "CPA": "HK", "EVA": "TW", "CAL": "TW", "THA": "TH",
    "MAS": "MY", "GIA": "ID", "QFA": "AU", "ANZ": "NZ", "LAN": "CL", "TAM": "BR",
    "GLO": "BR", "AZU": "BR", "ARG": "AR", "AVA": "CO", "ACA": "CA", "AIC": "IN",
    "IGO": "IN", "GTI": "US", "FDX": "US", "UPS": "US", "CLX": "LU", "MPH": "NL",
    "NCA": "JP", "BOX": "DE", "TAY": "BE", "SAS": "SE", "FIN": "FI", "AUA": "AT",
    "LOT": "PL", "AEE": "GR", "RAM": "MA", "KQA": "KE", "SAA": "ZA", "HVN": "VN",
    "ABW": "RU", "AZG": "AZ", "UZB": "UZ", "PIA": "PK", "IRA": "IR", "IRM": "IR",
    "IAW": "IQ", "MEA": "LB", "RBG": "JO", "GFA": "BH", "OMA": "OM", "KUW": "KW",
}

# 7600 (radio failure) deliberately omitted: in testing every instance was a
# non-event - a taxiing airliner, a light trainer, a manufacturer's test jet.
SQUAWKS = {"7700": "GENERAL EMERGENCY", "7500": "HIJACK"}

# Priority regions - scanned on EVERY scan cycle (Turkey + Middle East).
PRIORITY_REGIONS = [
    (41.01, 28.98, "Marmara & Istanbul"),
    (41.00, 39.72, "Trabzon & E Black Sea"),
    (38.42, 27.14, "Aegean & Izmir"),
    (39.93, 32.86, "Central Anatolia & Ankara"),
    (36.90, 30.71, "S Coast & Antalya"),
    (33.85, 35.86, "Levant & E Med"),
    (33.31, 44.37, "Iraq"),
    (34.50, 50.00, "Iran"),
    (25.25, 55.30, "Persian Gulf"),
    (24.71, 46.68, "Saudi Arabia"),
]

# Rotating global scan regions - full sweep every ~6 scans.
REGIONS = [
    (51.5, 0.0, "W Europe"), (40.7, -74.0, "US Northeast"), (34.0, -118.2, "US West"),
    (1.35, 103.8, "SE Asia"), (35.7, 139.7, "Japan"), (-33.9, 151.2, "Australia"),
    (-23.5, -46.6, "Brazil"), (19.4, -99.1, "Mexico"), (-26.1, 28.2, "S Africa"),
    (55.7, 37.6, "Russia"), (28.6, 77.2, "India"),
]


# ---------------------------------------------------------------- plumbing
def get(path, timeout=25):
    req = urllib.request.Request(BASE + path,
                                 headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        print(f"GET {path} -> HTTP {e.code} | retry-after={e.headers.get('Retry-After')} | {e.reason}")
        return None
    except Exception as e:
        print(f"GET {path} -> {type(e).__name__}: {e}")
        return None


def fetch_route(cs):
    """One network lookup. Routes are static, so results are cached forever."""
    req = urllib.request.Request(f"{BASE}/api/0/route/{cs}",
                                 headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
            aps = d.get("_airports") or []
            if aps:
                return {"iata": d.get("_airport_codes_iata"),
                        "airline": (d.get("airline_code") or "")[:3],
                        "legs": [{"iata": a.get("iata"), "cc": a.get("countryiso2"),
                                  "lat": a.get("lat"), "lon": a.get("lon")} for a in aps]}
    except Exception:
        pass
    return None


def resolve_routes(callsigns, cache, budget):
    """Resolve many callsigns in parallel, filling `cache` in place."""
    todo = []
    for cs in callsigns:
        cs = (cs or "").strip().upper()
        if len(cs) >= 4 and cs not in cache and cs not in todo:
            todo.append(cs)
    todo = todo[:budget]
    if not todo:
        return 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for cs, val in zip(todo, ex.map(fetch_route, todo)):
            cache[cs] = val
    return len(todo)


def route_for(cs, cache=None):
    cs = (cs or "").strip().upper()
    if len(cs) < 4:
        return None
    if cache is not None and cs in cache:
        return cache[cs]
    val = fetch_route(cs)
    if cache is not None:
        cache[cs] = val
    return val


def nm(lat1, lon1, lat2, lon2):
    R = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def bearing(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


# ---------------------------------------------------------------- ntfy
def _html_to_plain(s):
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s)


def _ascii_header(s):
    return (s or "").encode("ascii", "ignore").decode("ascii").strip()


def send(text, fr24=None, photo=None, title="Flight Alerts", priority="default"):
    plain = _html_to_plain(text)
    if not NTFY_TOPIC:
        print(f"[dry-run] ntfy ({title}): {plain[:200]}")
        return True

    url = f"{NTFY_BASE}/{urllib.parse.quote(NTFY_TOPIC)}"
    headers = {
        "Title": _ascii_header(title)[:200] or "Flight Alerts",
        "Priority": priority,
        "User-Agent": UA,
    }
    if fr24:
        headers["Click"] = fr24
        plain = f"{plain}\n\n{fr24}"

    try:
        if photo:
            with open(photo, "rb") as f:
                blob = f.read()
            headers["Filename"] = "track.png"
            msg_line = _ascii_header(plain.replace("\n", " | "))[:800]
            if msg_line:
                headers["X-Message"] = msg_line
            req = urllib.request.Request(url, data=blob, headers=headers, method="PUT")
        else:
            req = urllib.request.Request(url, data=plain.encode("utf-8"), headers=headers, method="POST")

        with urllib.request.urlopen(req, timeout=30) as r:
            return 200 <= r.status < 300
    except Exception as e:
        print(f"ntfy send failed: {e}", file=sys.stderr)
        return False


# ---------------------------------------------------------------- helpers
MIL_HINT = ("RRR", "RCH", "PAT", "CNV", "EVAC", "SPAR", "SAM", "NATO", "CFC",
            "ASY", "IAM", "GAF", "FAB", "BRS", "AME", "HKY", "TROY", "DOOM",
            "TRK", "HVY")


def is_noise(a):
    """True for private and flight-school traffic - the Cessnas and Pipers."""
    typ = (a.get("t") or "").upper()
    cs = (a.get("flight") or "").strip().upper()
    reg = (a.get("r") or "").strip().upper()

    if a.get("category") == "B6":
        return False
    if any(cs.startswith(m) for m in MIL_HINT):
        return False
    if reg and reg[:1].isdigit() and "-" in reg:
        return False
    if reg.startswith(("ZM", "ZZ", "ZJ", "ZK", "ZH")):
        return False
    if reg.isdigit() and len(reg) in (4, 6):
        return False

    if len(cs) >= 4 and cs[:3].isalpha() and cs[3:4].isdigit():
        return False

    looks_private = (not cs) or cs == reg.replace("-", "") or cs == reg
    return looks_private or typ in TRAINERS


def fr24_link(a):
    cs = (a.get("flight") or "").strip()
    if cs:
        return f"https://www.flightradar24.com/{cs}"
    if a.get("lat") is not None and a.get("lon") is not None:
        return f"https://www.flightradar24.com/{a['lat']:.2f},{a['lon']:.2f}/11"
    return "https://www.flightradar24.com/"


def label(a):
    return (a.get("flight") or a.get("r") or a.get("hex") or "?").strip()


def fmt_alt(a):
    v = a.get("alt_baro")
    return "on ground" if isinstance(v, str) else (f"{v:,} ft" if isinstance(v, int) else "? ft")


def fifth_freedom(route):
    if not route or not route.get("legs"):
        return []
    home = AIRLINE_CC.get(route.get("airline", ""))
    if not home:
        return []
    out, legs = [], route["legs"]
    for i in range(len(legs) - 1):
        a, b = legs[i], legs[i + 1]
        if a.get("cc") and b.get("cc") and home not in (a["cc"], b["cc"]):
            out.append(f"{a['iata']} ({a['cc']}) → {b['iata']} ({b['cc']})")
    return out


def diversion_check(a, route):
    """Is this aircraft heading somewhere other than where it filed?"""
    if not route or not route.get("legs") or a.get("lat") is None:
        return None
    dest = route["legs"][-1]
    if dest.get("lat") is None:
        return None

    alt = a.get("alt_baro")
    if not isinstance(alt, int) or alt < 100:
        return None

    if (a.get("gs") or 0) < 100:
        return None

    rate = a.get("baro_rate")
    if isinstance(rate, (int, float)) and rate > 200:
        return None

    pts = [(nm(a["lat"], a["lon"], L["lat"], L["lon"]), L.get("iata") or "?")
           for L in route["legs"] if L.get("lat") is not None]
    if not pts:
        return None
    d, near = min(pts)

    # Catches aircraft descending low well away from any filed route airport
    # (including regional diversions where an aircraft diverts mid-route).
    if alt < 9500 and d > 110:
        return (f"descending through {alt:,} ft, {d:,.0f} nm from the nearest "
                f"airport on its route ({near})")

    trk = a.get("track")
    if trk is not None and alt < 22000 and d > 180:
        nearest_leg = min((L for L in route["legs"] if L.get("lat") is not None),
                          key=lambda L: nm(a["lat"], a["lon"], L["lat"], L["lon"]))
        brg = bearing(a["lat"], a["lon"], nearest_leg["lat"], nearest_leg["lon"])
        off = abs((trk - brg + 180) % 360 - 180)
        if off > 110:
            return (f"tracking {off:.0f}\u00b0 away from {near}, "
                    f"{d:,.0f} nm out at {alt:,} ft")
    return None


def load_state():
    try:
        with open(STATE_F) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(s):
    with open(STATE_F, "w") as f:
        json.dump(s, f, indent=1, sort_keys=True)


# ---------------------------------------------------------------- main
def hello():
    if not NTFY_TOPIC:
        print("[dry-run] startup ping suppressed - no NTFY_TOPIC set")
        return 0
    send("\u2708\ufe0f <b>Flight alerts is live</b>\n\n"
         "Watching for:\n"
         "\U0001f6a8 emergency squawks (7700 / 7500) - sent instantly\n"
         "\u26a0\ufe0f possible diversions (Turkey, Middle East & tracked zones) - instant\n"
         "\U0001f30d unusual routings, drones, rare types & track shapes - hourly digest\n"
         "\U0001f305 a roundup every 24h\n\n"
         "<i>Polling every 60 seconds. Most of the time you will hear nothing, "
         "which is the point.</i>",
         title="Flight Alerts Live")
    print("startup ping sent")
    return 0


def test_alert():
    if not NTFY_TOPIC:
        print("no NTFY_TOPIC set")
        return 1

    d = get("/v2/point/41.01/28.98/250") or get("/v2/point/51.5/0.0/250") or {}
    ac = [x for x in d.get("ac", [])
          if x.get("hex") and x.get("lat") is not None and not is_noise(x)]
    if not ac:
        send("\U0001f9ea <b>Test</b>\n\nNo suitable aircraft airborne to demo with, "
             "but the pipeline works - you are reading this.",
             title="Flight Alerts Test")
        return 0

    a = ac[0]
    cs = (a.get("flight") or "").strip()
    route = route_for(cs, {}) if cs else None
    img, cx = tracks.render(a["hex"], f"/tmp/test_{a['hex']}.png")

    send("\U0001f9ea <b>TEST — this is not a real emergency</b>\n\n"
         "\U0001f6a8 <b>SQUAWK 7700 — GENERAL EMERGENCY</b>\n\n"
         f"<b>{label(a)}</b> — {a.get('t','?')} ({a.get('r','?')})\n"
         f"Altitude: {fmt_alt(a)}   Speed: {a.get('gs','?')} kt\n"
         + (f"Route: {route['iata']}\n" if route else "")
         + f"Position: {a['lat']:.3f}, {a['lon']:.3f}\n\n"
         "<i>A real one looks exactly like this, with the aircraft's track "
         "rendered above.</i>",
         fr24_link(a), img, title="Test Emergency Alert", priority="high")

    others = ac[1:4]
    body = "\n\n".join(
        f"\U0001f30d <b>{label(x)}</b> ({x.get('t','?')}, {x.get('r','?')}) "
        f"at {fmt_alt(x)}" for x in others)
    send("\U0001f9ea <b>TEST — sample hourly digest</b>\n\n"
         f"\U0001f4e1 <b>Worth a look</b> — {len(others)} find(s)\n\n{body}\n\n"
         "<i>The real digest carries fifth-freedom routings, drones, uncommon "
         "types and odd track shapes, with up to 10 images ranked by how "
         "convoluted the path is.</i>",
         title="Sample Hourly Digest")
    print(f"test alerts sent (demo aircraft: {label(a)}, shape score {cx:.1f})")
    return 0


def main():
    now = datetime.now(timezone.utc)
    ts = now.timestamp()
    st = load_state()
    seen = {k: v for k, v in st.get("seen", {}).items() if ts - v < SEEN_TTL}
    queue = st.get("queue", [])
    counts = st.get("type_counts", {})
    stats = st.get("stats", {"emergencies": 0, "diversions": 0, "finds": 0})
    routes = st.get("routes", {})
    prev = st.get("prev", {})
    bad_streak = st.get("feed_bad_streak", 0)
    if not st:
        st["last_yellow"] = st["last_green"] = ts

    # 1. LIVENESS CONTROL ---------------------------------------------------
    ctrl = get("/v2/point/51.5/0.0/250")
    n_ctrl = (ctrl or {}).get("total", 0)
    if not ctrl or n_ctrl < 50:
        bad_streak += 1
        st["feed_bad_streak"] = bad_streak
        print(f"FEED DEGRADED (streak {bad_streak}) - control returned {n_ctrl} (expected 300+)")
        last_warn = st.get("last_feed_warn", 0)
        if bad_streak >= 2 and ts - last_warn > 3600:
            send(f"⚠️ <b>Feed degraded</b>\nControl query returned {n_ctrl} "
                 f"aircraft over London (expected 300+). Not reporting an all-clear — "
                 f"the data source is unreliable right now.",
                 title="Feed Degraded")
            st["last_feed_warn"] = ts
        save_state(st)
        return 0
    bad_streak = 0
    st["feed_bad_streak"] = 0

    # 2. RED TIER - emergencies, sent instantly -----------------------------
    for code, meaning in SQUAWKS.items():
        d = get(f"/v2/sqk/{code}") or {}
        for a in d.get("ac", []):
            hexid = a.get("hex")
            key = f"sqk{code}:{hexid}"
            if not hexid or key in seen:
                continue
            seen[key] = ts
            stats["emergencies"] += 1
            cs = (a.get("flight") or "").strip()
            route = route_for(cs, routes)
            img, _ = tracks.render(hexid, f"/tmp/{hexid}.png")
            lines = [f"\U0001f6a8 <b>SQUAWK {code} — {meaning}</b>", "",
                     f"<b>{label(a)}</b> — {a.get('t','?')} ({a.get('r','?')})",
                     f"Altitude: {fmt_alt(a)}   Speed: {a.get('gs','?')} kt"]
            if route:
                lines.append(f"Route: {route['iata']}")
                div = diversion_check(a, route)
                if div:
                    lines.append(f"⚠️ <b>Possible diversion</b> — {div}")
                    stats["diversions"] += 1
            if a.get("lat") is not None:
                lines.append(f"Position: {a['lat']:.3f}, {a['lon']:.3f}")
            if a.get("emergency") and a["emergency"] != "none":
                lines.append(f"Transponder reports: <code>{a['emergency']}</code>")
            send("\n".join(lines), fr24_link(a), img,
                 title=f"SQUAWK {code} - {label(a)}", priority="urgent")

    # 3. YELLOW TIER + DIVERSIONS -----------------------------------------
    run = st.get("run", 0) + 1
    st["run"] = run
    fetched = 0
    do_scan = ts - st.get("last_scan", 0) > SCAN_INTERVAL
    if not do_scan:
        st.update({"seen": seen, "queue": queue[-400:], "type_counts": counts,
                   "routes": routes, "prev": prev, "stats": stats,
                   "feed_bad_streak": bad_streak, "last_run": now.isoformat()})
        save_state(st)
        print(f"[{now:%H:%M:%S}] fast tick | feed OK ({n_ctrl}) | queued={len(queue)}")
        return 0

    st["last_scan"] = ts
    global_picked = [REGIONS[(run + i) % len(REGIONS)] for i in range(2)]
    picked = list(PRIORITY_REGIONS) + global_picked
    snapshots = []
    for lat, lon, rname in picked:
        d = get(f"/v2/point/{lat}/{lon}/{SCAN_RADIUS_NM}") or {}
        snapshots.append((rname, d.get("ac", [])))

    cands = [(a.get("flight") or "").strip().upper()
             for _, acs in snapshots for a in acs
             if len((a.get("flight") or "").strip()) >= 4]
    fetched = resolve_routes([c for c in cands if c not in routes],
                             routes, ROUTE_BUDGET)

    for rname, acs in snapshots:
        for a in acs:
            hexid, typ = a.get("hex"), (a.get("t") or "").upper()
            if not hexid:
                continue
            if typ:
                counts[typ] = counts.get(typ, 0) + 1

            alt = a.get("alt_baro")
            if not is_noise(a) and a.get("category") == "B6" and isinstance(alt, int) and alt > 5000:
                key = f"uav:{hexid}"
                if key not in seen:
                    seen[key] = ts
                    queue.append({"icon": "\U0001f6f8", "kind": "UAV",
                                  "hex": hexid, "photo": True,
                                  "text": f"<b>{label(a)}</b> - unmanned aircraft "
                                          f"at {fmt_alt(a)} over {rname}",
                                  "fr24": fr24_link(a)})

            total = sum(counts.values())
            if not is_noise(a) and typ and total > 4000 and counts.get(typ, 0) <= max(2, total // 4000):
                key = f"rare:{hexid}"
                if key not in seen:
                    seen[key] = ts
                    queue.append({"icon": "\u2728", "kind": "RARE",
                                  "hex": hexid, "photo": True,
                                  "text": f"<b>{typ}</b> - {label(a)} ({a.get('r','?')}) "
                                          f"over {rname}. Seen {counts.get(typ,0)}x in "
                                          f"{total:,} observations",
                                  "fr24": fr24_link(a)})

            cs_d = (a.get("flight") or "").strip().upper()
            if cs_d and routes.get(cs_d) and f"div:{cs_d}" not in seen:
                why = diversion_check(a, routes[cs_d])
                if why:
                    seen[f"div:{cs_d}"] = ts
                    stats["diversions"] += 1
                    r = routes[cs_d]
                    img, _ = tracks.render(hexid, f"/tmp/{hexid}.png")
                    send("\u26a0\ufe0f <b>POSSIBLE DIVERSION</b>\n\n"
                         f"<b>{cs_d}</b> - {typ or '?'} ({a.get('r','?')}) [{rname}]\n"
                         f"Filed: {r['iata']}\n"
                         f"{why.capitalize()}\n"
                         f"Position: {a['lat']:.3f}, {a['lon']:.3f}\n"
                         f"Speed: {a.get('gs','?')} kt",
                         fr24_link(a), img,
                         title=f"Possible Diversion - {cs_d}", priority="high")

            if (a.get("lat") is not None and isinstance(alt, int)
                    and not is_noise(a)):
                p = prev.get(hexid)
                if p and len(p) == 3 and ts - p[2] <= STALE_MAX:
                    dist = nm(p[0], p[1], a["lat"], a["lon"])
                    gs = a.get("gs") or 0
                    mins = max(0.5, (ts - p[2]) / 60.0)
                    expected = gs * (mins / 60.0)
                    stale = a.get("seen_pos")
                    fresh = dist >= 0.75 and not (
                        isinstance(stale, (int, float)) and stale > 45)
                    if fresh and expected >= 5:
                        ratio = dist / expected
                        if ratio < 0.35:
                            kind = ("LOW-LEVEL ORBIT" if alt < 12000
                                    else "HOLDING / CIRCLING")
                            key = f"shape:{hexid}:{int(ts // 1800)}"
                            if key not in seen:
                                seen[key] = ts
                                queue.append({
                                    "icon": "\U0001f300", "kind": "SHAPE",
                                    "hex": hexid, "photo": True,
                                    "text": f"<b>{label(a)}</b> ({typ or '?'}, "
                                            f"{a.get('r','?')}) - {kind.lower()}\n"
                                            f"    {dist:.1f} nm of an expected "
                                            f"{expected:.0f} ({ratio:.0%}) at "
                                            f"{alt:,} ft over {rname}",
                                    "fr24": fr24_link(a)})
                prev[hexid] = [a["lat"], a["lon"], ts]

            cs = (a.get("flight") or "").strip().upper()
            if cs and f"ff:{cs}" not in seen and cs in routes:
                r = routes[cs]
                ff = fifth_freedom(r)
                if ff:
                    seen[f"ff:{cs}"] = ts
                    queue.append({"icon": "\U0001f30d", "kind": "ROUTE",
                                  "hex": None, "photo": False,
                                  "text": f"<b>{cs}</b> {r['iata']} ({a.get('t','?')})\n"
                                          f"    foreign-to-foreign: {'; '.join(ff)}",
                                  "fr24": fr24_link(a)})

    # 4. Flush the digest if it's due ---------------------------------------
    if queue and ts - st.get("last_yellow", 0) > YELLOW_INTERVAL:
        stats["finds"] += len(queue)

        head = f"\U0001f4e1 <b>Worth a look</b> - {len(queue)} find(s)\n\n"
        chunk, chunks = head, []
        for q in queue:
            line = f"{q['icon']} {q['text']}\n\n"
            if len(chunk) + len(line) > 3800:
                chunks.append(chunk)
                chunk = ""
            chunk += line
        if chunk.strip():
            chunks.append(chunk)
        for c in chunks:
            send(c, title="Flight Alerts Digest")

        cands = [q for q in queue if q.get("photo") and q.get("hex")][-25:]
        scored = []
        for q in cands:
            img, cx = tracks.render(q["hex"], f"/tmp/{q['hex']}.png")
            if img:
                scored.append((cx, q, img))
        scored.sort(key=lambda t: -t[0])
        for cx, q, img in scored[:DIGEST_IMAGES]:
            send(f"{q['icon']} {q['text']}\n<i>shape score {cx:.1f}x</i>",
                 q.get("fr24"), img, title=f"Track: {q['kind']}")

        print(f"digest sent: {len(queue)} finds, {len(scored[:DIGEST_IMAGES])} images")
        queue = []
        st["last_yellow"] = ts

    # 5. Daily roundup -------------------------------------------------------
    if ts - st.get("last_green", 0) > GREEN_INTERVAL:
        top = sorted(counts.items(), key=lambda kv: -kv[1])[:5]
        send("\U0001f305 <b>Daily roundup</b>\n\n"
             f"Emergencies caught: <b>{stats['emergencies']}</b>\n"
             f"Possible diversions: <b>{stats['diversions']}</b>\n"
             f"Finds queued: <b>{stats['finds']}</b>\n"
             f"Aircraft observed: <b>{sum(counts.values()):,}</b>\n\n"
             "Most common types seen:\n"
             + "\n".join(f"  {t} — {c:,}" for t, c in top),
             title="Daily Flight Roundup")
        st["last_green"] = ts
        stats = {"emergencies": 0, "diversions": 0, "finds": 0}

    # 6. Persist -------------------------------------------------------------
    if len(counts) > 800:
        counts = dict(sorted(counts.items(), key=lambda kv: -kv[1])[:600])
    if len(routes) > ROUTE_CACHE_MAX:
        routes = dict(list(routes.items())[-ROUTE_CACHE_MAX:])
    if len(prev) > 8000:
        prev = dict(list(prev.items())[-5000:])
    st.update({"seen": seen, "queue": queue[-400:], "type_counts": counts,
               "routes": routes, "prev": prev, "stats": stats,
               "feed_bad_streak": bad_streak, "last_run": now.isoformat()})
    save_state(st)
    print(f"[{now:%Y-%m-%d %H:%M UTC}] run #{run} | feed OK ({n_ctrl}) | "
          f"queued={len(queue)} | routes cached={len(routes)} (+{fetched}) | "
          f"types={len(counts)}")
    return 0


if __name__ == "__main__":
    if "--hello" in sys.argv:
        sys.exit(hello())
    if "--test" in sys.argv:
        sys.exit(test_alert())
    sys.exit(main())
