"""Build the CFM (Moldova) GTFS feed from the cfm.md schedule widget.

Usage:
  python build_cfm.py FEED_DIR [--workers N] [--cache DIR]

The widget only answers "trains from station A to station B", so every ordered pair
of stations from the widget's station list is queried. From the answers each train is
rebuilt:
  * departure at A / arrival at B / travel time A->B / days of operation
  * stations are ordered by travel time from the first station, so night trains and
    trains running past midnight get times > 24:00:00
  * "??" instead of a time -> 3 min after the arrival (departure) or 3 min before the
    departure (arrival) at the same station
  * days: 7 boxes Mon..Sun, or 2 boxes even/odd days (pare/impare)
Stations that are not in stops.txt (most foreign stations) are left out. Extra stops
that exist only in the feed (e.g. 4000001 on the Kiev line) are kept at the same
offset from the previous station. Shapes are cut from an existing shape when the trip
lies on it, otherwise routed with brouter.de (profile rail).

Things that need a human (a new Moldovan station without coordinates, an unknown
day pattern) are written to cfm_alert.txt; the feed is still built.
"""

import concurrent.futures as cf
import csv
import html as htmlmod
import json
import math
import os
import re
import sys
import time
import zipfile
from datetime import date, timedelta

URL = "http://cfm.md/widgets/schedule/?v=1&from={a}&to={b}"
SEED = ("2300000", "2300145")
VALID_DAYS = 120
BROUTER = "https://brouter.de/brouter"
NAMED_MASKS = {"1111111": "daily", "0000111": "weekend"}


# ---------------------------------------------------------------- download

def get(session, a, b, cache):
    path = os.path.join(cache, f"{a}_{b}.html") if cache else None
    if path and os.path.exists(path):
        return open(path, encoding="utf-8").read()
    for attempt in range(4):
        try:
            r = session.get(URL.format(a=a, b=b), timeout=30)
            r.raise_for_status()
            r.encoding = "utf-8"
            if path:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(r.text)
            return r.text
        except Exception:
            if attempt == 3:
                raise
            time.sleep(3 * (attempt + 1))


def stations(page):
    from bs4 import BeautifulSoup
    sel = BeautifulSoup(page, "html.parser").find("select", id="timetable_from")
    if not sel:
        raise SystemExit("Widget nevrátil seznam stanic. Začátek odpovědi:\n" + page[:1500])
    return {o["value"]: o.get_text(strip=True) for o in sel.find_all("option") if o["value"].isdigit()}


def parse_time(s):
    m = re.match(r"\s*(\d{1,2}):(\d{2})", s)
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def parse_duration(s):
    s = re.sub(r"<[^>]+>", " ", s)
    d = re.search(r"(\d+)\s*(zi|zile)", s)
    h = re.search(r"(\d+)\s*or", s)
    m = re.search(r"(\d+)\s*min", s)
    if not (h or m or d):
        return None
    return (int(d.group(1)) * 1440 if d else 0) + (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


def parse_pair(page):
    """Return dicts: train, route, dep, arr, dur, mask."""
    from bs4 import BeautifulSoup
    out = []
    table = BeautifulSoup(page, "html.parser").find("table", class_="rasp")
    if not table:
        return out
    rows = [tr for tr in table.find_all("tr") if tr.find_parent("table") is table]
    for head, body in zip(rows[0::2], rows[1::2]):
        b = head.find("b")
        train = b.get_text(strip=True)
        route = re.sub(r"\s+", " ", head.get_text(" ", strip=True)[len(train):]).strip()
        cells = [td for td in body.find_all("td") if td.find_parent("tr") is body]
        times = [x.get_text(strip=True) for x in cells[0].find_all("b")]
        dur = parse_duration(cells[1].decode_contents()) if len(cells) > 1 else None
        cur = body.find("table", class_="cur")
        mask = "".join("1" if "c" in (td.get("class") or []) else "0"
                       for td in cur.find_all("td")) if cur else ""
        dep = parse_time(times[0]) if times else None
        arr = parse_time(times[1]) if len(times) > 1 else None
        if dep is None or arr is None or dur is None or dur > 3 * 1440:
            dur = None          # with "??" the widget prints a nonsense travel time
        out.append({"train": train, "route": route, "dep": dep, "arr": arr, "dur": dur, "mask": mask})
    return out


def crawl(workers, cache):
    import requests
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (gtfs-rehost; +https://github.com/hoermalmeister/gtfs-rehost)"
    if cache:
        os.makedirs(cache, exist_ok=True)
    st = stations(get(s, *SEED, cache))
    ids = list(st)
    pairs = [(a, b) for a in ids for b in ids if a != b]
    obs = []

    def job(p):
        return p, parse_pair(get(s, p[0], p[1], cache))

    with cf.ThreadPoolExecutor(workers) as ex:
        for (a, b), rows in ex.map(job, pairs):
            for r in rows:
                r["a"], r["b"] = a, b
                obs.append(r)
    return st, obs


# ---------------------------------------------------------------- rebuild trains

def rebuild(obs, warnings, notes=None):
    notes = [] if notes is None else notes
    by_train = {}
    for o in obs:
        by_train.setdefault(o["train"], []).append(o)
    trains = {}
    for num, rows in by_train.items():
        dep_at, arr_at = {}, {}
        for r in rows:
            if r["dep"] is not None:
                dep_at[r["a"]] = r["dep"]
            if r["arr"] is not None:
                arr_at[r["b"]] = r["arr"]
        sts = {r["a"] for r in rows} | {r["b"] for r in rows}
        # "??" fallbacks
        for x in sts:
            if x not in dep_at and x in arr_at and any(r["a"] == x for r in rows):
                dep_at[x] = (arr_at[x] + 3) % 1440
            if x not in arr_at and x in dep_at and any(r["b"] == x for r in rows):
                arr_at[x] = (dep_at[x] - 3) % 1440
        # offsets relative to an arbitrary station, propagated along every observed pair:
        #   arr(b) = dep(a) + travel time,   dep(x) = arr(x) + dwell(x)
        dwell = {x: (dep_at[x] - arr_at[x]) % 1440 if x in dep_at and x in arr_at else 0 for x in sts}
        edges = []
        for r in rows:
            d = r["dur"]
            if d is None and r["b"] in arr_at and r["a"] in dep_at:
                d = (arr_at[r["b"]] - dep_at[r["a"]]) % 1440
            if d is not None:
                edges.append((r["a"], r["b"], d))
        if not edges:
            warnings.append(f"Spoj {num}: žádné použitelné časy – vynechán")
            continue
        deg = {}
        for x, y, _ in edges:
            deg[x] = deg.get(x, 0) + 1
            deg[y] = deg.get(y, 0) + 1
        ref = max(deg, key=deg.get)
        dep_off = {ref: 0}
        changed = True
        while changed:
            changed = False
            for x, y, d in edges:
                if x in dep_off and y not in dep_off:
                    dep_off[y] = dep_off[x] + d + dwell[y]
                    changed = True
                elif y in dep_off and x not in dep_off:
                    dep_off[x] = dep_off[y] - dwell[y] - d
                    changed = True
        lost = sts - set(dep_off)
        if lost:
            notes.append(f"Spoj {num}: stanice {', '.join(sorted(lost))} bez použitelného času (jen ??) – vynechány")
        arr_off = {x: dep_off[x] - dwell[x] for x in dep_off}
        first = min(dep_off, key=lambda x: arr_off[x])
        if first not in dep_at:
            warnings.append(f"Spoj {num}: chybí čas odjezdu z první stanice {first} – vynechán")
            continue
        base = dep_at[first] - dep_off[first]
        seq = sorted(dep_off, key=lambda x: (arr_off[x], dep_off[x]))
        stop_list = [(x, base + arr_off[x], base + dep_off[x]) for x in seq]
        mask = next((r["mask"] for r in rows if r["a"] == first and r["mask"]),
                    next((r["mask"] for r in rows if r["mask"]), ""))
        route = next((r["route"] for r in rows if r["route"]), "")
        trains[num] = {"stops": stop_list, "mask": mask, "route": route}
    return trains


def service_for(num, mask, warnings):
    if len(mask) == 7:
        if mask == "0000000":
            warnings.append(f"Spoj {num}: prázdný kalendář (žádný den není vyznačen) – beru jako denní")
            return "daily"
        return NAMED_MASKS.get(mask, "w" + mask)
    if mask == "01":
        return "pare"
    if mask == "10":
        return "impare"
    warnings.append(f"Spoj {num}: neznámý zápis dnů provozu „{mask}“ – beru jako denní")
    return "daily"


# ---------------------------------------------------------------- shapes

def dist(a, b):
    lat = math.radians((a[0] + b[0]) / 2)
    return math.hypot(a[0] - b[0], (a[1] - b[1]) * math.cos(lat))


def fit_shape(points, coords, tol=0.006):
    """Indices of the stops along `points` in order, or None if the trip is not on it."""
    idx, start = [], 0
    for c in coords:
        best = min(range(start, len(points)), key=lambda k: dist(points[k], c), default=None)
        if best is None or dist(points[best], c) > tol:
            return None
        idx.append(best)
        start = best
    return idx


def brouter(coords):
    import requests
    lonlats = "|".join(f"{lon:.6f},{lat:.6f}" for lat, lon in coords)
    r = requests.get(BROUTER, params={"lonlats": lonlats, "profile": "rail",
                                      "alternativeidx": 0, "format": "geojson"}, timeout=120)
    r.raise_for_status()
    return [(round(p[1], 6), round(p[0], 6)) for p in r.json()["features"][0]["geometry"]["coordinates"]]


# ---------------------------------------------------------------- gtfs io

def read(feed, name):
    p = os.path.join(feed, name)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write(feed, name, fields, rows):
    path = os.path.join(feed, name)
    eol = "\n"
    if os.path.exists(path):
        with open(path, "rb") as f:
            eol = "\r\n" if b"\r\n" in f.read(4096) else "\n"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator=eol, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def fmt(m):
    return f"{m // 60:02d}:{m % 60:02d}:00"


def tmin(s):
    h, m, _ = s.split(":")
    return int(h) * 60 + int(m)


# ---------------------------------------------------------------- main

def build(feed, st_names, obs):
    warnings, report = [], []
    stops = read(feed, "stops.txt")
    coord = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in stops}
    old_trips = {t["trip_id"]: t for t in read(feed, "trips.txt")}
    old_st = {}
    for r in read(feed, "stop_times.txt"):
        old_st.setdefault(r["trip_id"], []).append(r)
    for v in old_st.values():
        v.sort(key=lambda r: int(r["stop_sequence"]))
    shapes = {}
    for p in read(feed, "shapes.txt"):
        shapes.setdefault(p["shape_id"], []).append(p)
    shapes = {k: [(float(p["shape_pt_lat"]), float(p["shape_pt_lon"]))
                  for p in sorted(v, key=lambda p: int(p["shape_pt_sequence"]))] for k, v in shapes.items()}

    if not obs:
        raise SystemExit("Widget nevrátil žádný spoj – nic neměním.")
    trains = rebuild(obs, warnings, report)

    missing = set()
    trips, st_rows, routes, services = [], [], [], set()
    for num in sorted(trains, key=lambda n: trains[n]["stops"][0][2]):
        t = trains[num]
        tid = f"trip_{num}"
        kept = []
        for sid, arr, dep in t["stops"]:
            if sid in coord:
                kept.append([sid, arr, dep])
            else:
                missing.add(sid)
        # extra stops known only to the feed (not in the widget's station list)
        old = old_st.get(tid, [])
        for i, r in enumerate(old):
            if r["stop_id"] in st_names or r["stop_id"] not in coord:
                continue
            prev = next((old[j] for j in range(i - 1, -1, -1) if old[j]["stop_id"] in st_names), None)
            anchor = next((k for k in kept if prev and k[0] == prev["stop_id"]), None)
            if anchor:
                delta = anchor[2] - tmin(prev["departure_time"])
                kept.append([r["stop_id"], tmin(r["arrival_time"]) + delta, tmin(r["departure_time"]) + delta])
        kept.sort(key=lambda k: (k[1], k[2]))
        if len(kept) < 2:
            continue
        # first/last stop: arrival = departure
        kept[0][1] = kept[0][2]
        kept[-1][2] = kept[-1][1]
        # keep times < 24:00 at the start of the trip
        shift = (kept[0][2] // 1440) * 1440
        for k in kept:
            k[1] -= shift
            k[2] -= shift

        service = service_for(num, t["mask"], warnings)
        services.add(service)
        a, _, b = t["route"].partition(" -")
        routes.append({"route_id": num, "agency_id": "CFM",
                       "route_long_name": f"{a.strip()} - {b.strip()}" if b else t["route"],
                       "route_type": "2", "route_short_name": num})

        seq = [k[0] for k in kept]
        cs = [coord[s] for s in seq]
        shape_id = None
        if tid in old_trips and old_trips[tid].get("shape_id") in shapes \
                and fit_shape(shapes[old_trips[tid]["shape_id"]], cs):
            shape_id = old_trips[tid]["shape_id"]
        if not shape_id:
            for sid_, pts in list(shapes.items()):
                idx = fit_shape(pts, cs)
                if idx:
                    if idx[0] <= 2 and idx[-1] >= len(pts) - 3:
                        shape_id = sid_
                    else:
                        shape_id = f"{seq[0]}-{seq[-1]}"
                        if shape_id not in shapes:
                            shapes[shape_id] = pts[idx[0]:idx[-1] + 1]
                            report.append(f"  shape {shape_id} vyříznut z {sid_}")
                    break
        if not shape_id:
            shape_id = f"{seq[0]}-{seq[-1]}"
            shapes[shape_id] = brouter(cs)
            report.append(f"  shape {shape_id} z brouter.de")

        if tid not in old_trips:
            report.append(f"Nový spoj {num} {t['route']} ({service})")
        else:
            old_times = [(r["stop_id"], tmin(r["arrival_time"]), tmin(r["departure_time"])) for r in old]
            if old_times != [tuple(k) for k in kept] or old_trips[tid]["service_id"] != service:
                report.append(f"Změna spoje {num} {t['route']}")
        trips.append({"route_id": num, "service_id": service, "trip_id": tid, "direction_id": "",
                      "shape_id": shape_id})
        for n, (sid, arr, dep) in enumerate(kept, 1):
            st_rows.append({"trip_id": tid, "arrival_time": fmt(arr), "departure_time": fmt(dep),
                            "stop_id": sid, "stop_sequence": str(n)})
    for tid in old_trips:
        if tid not in {t["trip_id"] for t in trips}:
            report.append(f"Spoj {tid[5:]} už widget nevrací – odstraněn")

    foreign = sorted(s for s in missing if not s.startswith("23"))
    domestic = sorted(s for s in missing if s.startswith("23"))
    if foreign:
        report.append("Vynechané stanice bez souřadnic (zahraničí): " + ", ".join(f"{s} {st_names.get(s, '')}" for s in foreign))
    if domestic:
        warnings.append("Moldavské stanice bez souřadnic ve stops.txt (vynechány, doplňte je): "
                        + ", ".join(f"{s} {st_names.get(s, '')}" for s in domestic))

    # calendar
    today = date.today()
    start, end = today - timedelta(days=7), today + timedelta(days=VALID_DAYS)
    ds, de = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    cal, cal_dates = [], []
    for s in sorted(services):
        if s in ("pare", "impare"):
            mask = "0000000"
        elif s == "daily":
            mask = "1111111"
        elif s == "weekend":
            mask = "0000111"
        else:
            mask = s[1:]
        cal.append(dict(zip(["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
                             "saturday", "sunday"], [s, *mask]), start_date=ds, end_date=de))
        if s in ("pare", "impare"):
            d = start
            while d <= end:
                if (d.day % 2 == 0) == (s == "pare"):
                    cal_dates.append({"service_id": s, "date": d.strftime("%Y%m%d"), "exception_type": "1"})
                d += timedelta(days=1)
    fi = read(feed, "feed_info.txt")
    for f in fi:
        f.update(feed_start_date=ds, feed_end_date=de, feed_version=today.strftime("%Y%m%d"))

    used = {t["shape_id"] for t in trips}
    used_stops = {r["stop_id"] for r in st_rows}
    write(feed, "routes.txt", ["route_id", "agency_id", "route_long_name", "route_type", "route_short_name"], routes)
    write(feed, "trips.txt", ["route_id", "service_id", "trip_id", "direction_id", "shape_id"], trips)
    write(feed, "stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"], st_rows)
    write(feed, "calendar.txt", ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
                                 "saturday", "sunday", "start_date", "end_date"], cal)
    write(feed, "calendar_dates.txt", ["service_id", "date", "exception_type"], cal_dates)
    write(feed, "feed_info.txt", list(fi[0].keys()), fi)
    write(feed, "shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
          [{"shape_id": k, "shape_pt_lat": f"{la:.6f}".rstrip("0").rstrip("."),
            "shape_pt_lon": f"{lo:.6f}".rstrip("0").rstrip("."), "shape_pt_sequence": str(n)}
           for k in shapes if k in used for n, (la, lo) in enumerate(shapes[k], 1)])
    files = ["agency.txt", "calendar.txt", "calendar_dates.txt", "feed_info.txt", "routes.txt",
             "shapes.txt", "stop_times.txt", "stops.txt", "trips.txt"]
    with zipfile.ZipFile(os.path.join(feed, "cfm.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for n in files:
            z.write(os.path.join(feed, n), n)
    unused = [s["stop_id"] for s in stops if s["stop_id"] not in used_stops]
    if unused:
        report.append(f"Zastávek ve stops.txt bez spoje: {len(unused)}")
    return report, warnings


def main():
    feed = sys.argv[1]
    workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 6
    cache = sys.argv[sys.argv.index("--cache") + 1] if "--cache" in sys.argv else None
    ws = os.environ.get("GITHUB_WORKSPACE", ".")
    st_names, obs = crawl(workers, cache)
    with open(os.path.join(ws, "cfm_observations.json"), "w", encoding="utf-8") as f:
        json.dump(obs, f, ensure_ascii=False)
    report, warnings = build(feed, st_names, obs)
    text = "\n".join(report) or "Beze změn v jízdním řádu."
    print(text)
    with open(os.path.join(ws, "cfm_report.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    if warnings:
        print("\n".join(warnings))
        for num in sorted({m.group(1) for w in warnings for m in [re.match(r"Spoj (\S+):", w)] if m}):
            print(f"--- raw answers for {num} (from to dep arr dur mask)")
            for o in obs:
                if o["train"] == num:
                    print(o["a"], o["b"], o["dep"], o["arr"], o["dur"], o["mask"])
        with open(os.path.join(ws, "cfm_alert.txt"), "w", encoding="utf-8") as f:
            f.write("⚠️ Aktualizace CFM GTFS – co je potřeba zkontrolovat:\n\n" + "\n".join(f"- {w}" for w in warnings) + "\n")


if __name__ == "__main__":
    main()
