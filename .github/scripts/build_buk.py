#!/usr/bin/env python3
"""BUK Bystrzyca Kłodzka – GTFS from https://www.bukbystrzyca.pl/rozklad-jazdy-probny/

The page has no timetable API; the whole timetable is embedded in its JS bundle
(assets/page-*.js) as JSON.parse(`...`) blocks plus a list of lines with colours.

Hand-maintained in the feed folder:
  stops.txt        stop positions (the source's positions are often wrong – never used)
  stops_map.json   source stop name -> stop_id (unknown names stop the build with an alert)
  calendar.json    school calendar (dolnośląskie), holidays excluded by 'd'
  agency.txt, LICENSE.txt

Service codes: S school days, K workdays without school, D Mon–Fri except holidays,
C Sat+Sun+holidays, digits = weekdays (1 = Mon), d = except listed holidays,
w = not on Corpus Christi. Letters are joined (KC = K or C), digits narrow them
(2,5K = Tue/Fri without school).

Stops within a trip are ordered by time (the source order is sometimes mixed).
Shapes: brouter.de (car-fast) through the stops, reused while the stop pattern is unchanged.

Usage: build_buk.py <feed folder> [timetable.js-or-json]
"""
import csv
import hashlib
import io
import json
import os
import re
import sys
import zipfile
from datetime import date, timedelta

BASE = "https://www.bukbystrzyca.pl/rozklad-jazdy-probny/"
BROUTER = "https://brouter.de/brouter"
UA = {"User-Agent": "Mozilla/5.0 (gtfs-rehost; +https://github.com/hoermalmeister/gtfs-rehost)"}
report, alerts = [], []


def log(*a):
    print(*a, flush=True)
    report.append(" ".join(str(x) for x in a))


# ------------------------------------------------------------------ source

def fetch_bundle():
    import requests
    s = requests.Session()
    s.headers.update(UA)
    html = s.get(BASE, timeout=60)
    html.raise_for_status()
    m = re.search(r'assets/(page-[\w-]+\.js)', html.text)
    if not m:  # fall back to start.js which imports the page module
        start = s.get(BASE + "start.js", timeout=60).text
        m = re.search(r'(page-[\w-]+\.js)', start)
    if not m:
        raise SystemExit("page-*.js not found on " + BASE)
    url = BASE + "assets/" + m.group(1)
    log("bundle:", url)
    js = s.get(url, timeout=60)
    js.raise_for_status()
    return js.text


def template_json(body):
    # JS template literal -> plain string
    return json.loads(re.sub(r'\\(.)', lambda m: {'n': '\n', 't': '\t'}.get(m.group(1), m.group(1)), body))


def parse_bundle(js):
    blocks = [template_json(b) for b in re.findall(r'JSON\.parse\(`(.*?)`\)', js, re.S)]
    timetable = next(b for b in blocks if all(isinstance(v, dict) and "directions" in v for v in b.values()))
    meta = {}
    for m in re.finditer(r'\{number:`([^`]+)`,places:\[([^\]]*)\],color:`(#[0-9a-fA-F]{6})`', js):
        meta[m.group(1)] = {"places": re.findall(r'`([^`]*)`', m.group(2)), "color": m.group(3)[1:].upper()}
    return timetable, meta


# ---------------------------------------------------------------- calendar

def easter(y):
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mo = (h + l - 7 * m + 114) // 31
    return date(y, mo, (h + l - 7 * m + 114) % 31 + 1)


def holidays(y):
    e = easter(y)
    fixed = [(1, 1), (1, 6), (5, 1), (5, 3), (8, 15), (11, 1), (11, 11), (12, 24), (12, 25), (12, 26)]
    return {date(y, mo, d) for mo, d in fixed} | {e, e + timedelta(1), e + timedelta(49), e + timedelta(60)}


def iso(s):
    return date.fromisoformat(s)


class Cal:
    def __init__(self, cfg, years):
        self.hol = set().union(*(holidays(y) for y in years))
        self.cc = {easter(y) + timedelta(60) for y in years}
        self.years = [(iso(a), iso(b)) for a, b in cfg["school_years"]]
        self.off = [(iso(a), iso(b)) for a, b in cfg["no_school"]]
        self.dex = set()
        for y in years:
            for x in cfg["d_excluded"]:
                self.dex.add(easter(y) if x == "easter" else date(y, int(x[:2]), int(x[3:])))

    def school(self, d):
        return (d.weekday() < 5 and d not in self.hol and any(a <= d <= b for a, b in self.years)
                and not any(a <= d <= b for a, b in self.off))

    def runs(self, code, d):
        c = code.replace(" ", "")
        digits = {int(x) - 1 for x in re.findall(r'[1-7]', c)}
        letters = re.sub(r'[\d,]', '', c)
        wd, hol = d.weekday(), d in self.hol
        base = {
            "S": self.school(d),
            "K": wd < 5 and not hol and not self.school(d),
            "D": wd < 5 and not hol,
            "C": wd >= 5 or hol,
        }
        main = [x for x in letters if x in base]
        unknown = [x for x in letters if x not in base and x not in "dw"]
        if unknown:
            raise ValueError(f"unknown service code {code!r}")
        ok = any(base[x] for x in main) if main else True
        if digits:
            ok = ok and wd in digits
        if "d" in letters and d in self.dex:
            ok = False
        if "w" in letters and d in self.cc:
            ok = False
        return ok


def service_id(code):
    return re.sub(r'[^0-9A-Za-z]', '', code)


# ------------------------------------------------------------------ shapes

def dist(a, b):
    import math
    k = math.cos(math.radians((a[0] + b[0]) / 2))
    return math.hypot(a[0] - b[0], (a[1] - b[1]) * k) * 111320


def brouter(coords):
    import requests
    lonlats = "|".join(f"{lon:.6f},{lat:.6f}" for lat, lon in coords)
    r = requests.get(BROUTER, params={"lonlats": lonlats, "profile": "car-fast", "alternativeidx": 0,
                                      "format": "geojson"}, timeout=120, headers=UA)
    r.raise_for_status()
    return [(round(p[1], 6), round(p[0], 6)) for p in r.json()["features"][0]["geometry"]["coordinates"]]


# --------------------------------------------------------------------- io

def read(feed, name):
    p = os.path.join(feed, name)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write(feed, name, fields, rows):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    with open(os.path.join(feed, name), "w", encoding="utf-8", newline="") as f:
        f.write(b.getvalue())


def hms(t):
    h, m = t.strip().split(":")
    return f"{int(h):02d}:{int(m):02d}:00"


# ------------------------------------------------------------------- main

def build(feed, timetable, meta):
    stops = read(feed, "stops.txt")
    coord = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in stops}
    sname = {s["stop_id"]: s["stop_name"] for s in stops}
    smap = json.load(open(os.path.join(feed, "stops_map.json"), encoding="utf-8"))
    cfg = json.load(open(os.path.join(feed, "calendar.json"), encoding="utf-8"))

    missing = sorted({s["name"] for v in timetable.values() for d in v["directions"] for s in d["stops"]
                      if s["name"] not in smap})
    bad = sorted({v for v in smap.values() if v not in coord})
    if missing or bad:
        if missing:
            alerts.append("Neznámé zastávky ze zdroje – doplň do buk/stops_map.json:\n" +
                          "\n".join(f"- `{n}`" for n in missing))
        if bad:
            alerts.append("stop_id ze stops_map.json chybí ve stops.txt: " + ", ".join(bad))
        raise SystemExit("unmapped stops")

    known = set(json.load(open(os.path.join(feed, "lines.json"), encoding="utf-8"))) \
        if os.path.exists(os.path.join(feed, "lines.json")) else set()
    new_lines = sorted(set(timetable) - known)
    gone = sorted(known - set(timetable))
    if known and new_lines:
        alerts.append("Nové linky ve zdroji: " + ", ".join(new_lines))
    if gone:
        log("lines no longer in source (dropped):", ", ".join(gone))

    # ---- period
    start = max(iso(v["validFrom"]) for v in timetable.values())
    end = date(iso(cfg["school_years"][-1][1]).year, 8, 31) + timedelta(cfg.get("feed_end_extra_days", 0))
    cal = Cal(cfg, range(start.year, end.year + 1))
    days = [start + timedelta(k) for k in range((end - start).days + 1)]

    routes, trips, st, codes, patterns = [], [], [], {}, {}
    for line, v in timetable.items():
        m = meta.get(line, {})
        places = m.get("places") or []
        for di, dd in enumerate(v["directions"], 1):
            rid = f"{line}-{di}"
            pl = places if di == 1 else places[::-1]
            routes.append({"route_id": rid, "agency_id": "BUK", "route_short_name": line,
                           "route_long_name": " - ".join(pl), "route_type": "3",
                           "route_color": m.get("color", "2F7D4A"), "route_text_color": "FFFFFF"})
            for k, code in enumerate(dd["codes"]):
                seq = []
                for order, s in enumerate(dd["stops"]):
                    t = s["times"][k] if k < len(s["times"]) else None
                    if t:
                        seq.append((hms(t), order, smap[s["name"]]))
                if len(seq) < 2:
                    continue
                if [x[1] for x in sorted(seq)] != [x[1] for x in seq]:
                    log(f"{rid} trip {k + 1}: stop order fixed by time")
                seq.sort()
                # drop a stop repeated right after itself (two source names for one stop_id)
                clean = [seq[0]]
                for x in seq[1:]:
                    if x[2] == clean[-1][2]:
                        clean[-1] = (clean[-1][0], clean[-1][1], x[2], x[0])
                    else:
                        clean.append(x)
                tid = f"{rid}-{k + 1}"
                ids = tuple(x[2] for x in clean)
                shp = f"{rid}-" + hashlib.md5("|".join(ids).encode()).hexdigest()[:6]
                patterns[shp] = ids
                sid = service_id(code)
                codes.setdefault(sid, code)
                trips.append({"route_id": rid, "service_id": sid, "trip_id": tid, "direction_id": str(di - 1),
                              "trip_headsign": sname[ids[-1]], "shape_id": shp})
                for n, x in enumerate(clean, 1):
                    st.append({"trip_id": tid, "arrival_time": x[0], "departure_time": x[3] if len(x) > 3 else x[0],
                               "stop_id": x[2], "stop_sequence": str(n), "shape_dist_traveled": ""})

    # ---- calendar
    calendar, cdates = [], []
    for sid, code in sorted(codes.items()):
        on = {d for d in days if cal.runs(code, d)}
        if not on:
            log(f"service {code}: no days in {start}..{end}")
        mask = []
        for wd in range(7):
            ds = [d for d in days if d.weekday() == wd]
            mask.append("1" if sum(d in on for d in ds) * 2 > len(ds) else "0")
        calendar.append(dict(zip(["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                                  "sunday", "start_date", "end_date"],
                                 [sid, *mask, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")])))
        for d in days:
            if (mask[d.weekday()] == "1") != (d in on):
                cdates.append({"service_id": sid, "date": d.strftime("%Y%m%d"),
                               "exception_type": "1" if d in on else "2"})

    # ---- shapes (reuse unchanged ones)
    old = {}
    for r in read(feed, "shapes.txt"):
        old.setdefault(r["shape_id"], []).append(r)
    shapes, dist_at = [], {}
    for shp, ids in patterns.items():
        pts = None
        if shp in old and len(old[shp]) > len(ids):  # straight-line fallbacks are retried
            pts = [(float(r["shape_pt_lat"]), float(r["shape_pt_lon"]))
                   for r in sorted(old[shp], key=lambda r: int(r["shape_pt_sequence"]))]
        else:
            try:
                pts = brouter([coord[s] for s in ids])
                log(f"shape {shp}: brouter, {len(pts)} points")
            except Exception as e:  # noqa: BLE001
                pts = [coord[s] for s in ids]
                log(f"shape {shp}: brouter failed ({e}), straight lines")
        acc, prev, dd = 0.0, None, []
        for p in pts:
            acc += dist(prev, p) if prev else 0
            dd.append(acc)
            prev = p
        for n, (p, a) in enumerate(zip(pts, dd), 1):
            shapes.append({"shape_id": shp, "shape_pt_lat": f"{p[0]:.6f}", "shape_pt_lon": f"{p[1]:.6f}",
                           "shape_pt_sequence": str(n), "shape_dist_traveled": f"{a:.1f}"})
        # stop distances: nearest point, moving forward only
        j, res = 0, []
        for s in ids:
            c = coord[s]
            best = min(range(j, len(pts)), key=lambda i: dist(pts[i], c))
            j = best
            res.append(dd[best])
        dist_at[shp] = res
    by_trip = {t["trip_id"]: t["shape_id"] for t in trips}
    for r in st:
        r["shape_dist_traveled"] = f"{dist_at[by_trip[r['trip_id']]][int(r['stop_sequence']) - 1]:.1f}"

    used = {r["stop_id"] for r in st}
    write(feed, "routes.txt", list(routes[0]), routes)
    write(feed, "trips.txt", list(trips[0]), trips)
    write(feed, "stop_times.txt", list(st[0]), st)
    write(feed, "calendar.txt", list(calendar[0]), calendar)
    write(feed, "calendar_dates.txt", ["service_id", "date", "exception_type"], cdates)
    write(feed, "shapes.txt", list(shapes[0]), shapes)
    write(feed, "feed_info.txt", ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version",
                                  "feed_start_date", "feed_end_date", "feed_contact_email"],
          [{"feed_publisher_name": "Petr Novak", "feed_publisher_url": BASE, "feed_lang": "pl",
            "feed_version": date.today().isoformat(), "feed_start_date": start.strftime("%Y%m%d"),
            "feed_end_date": end.strftime("%Y%m%d"), "feed_contact_email": "novakp@wo.cz"}])
    json.dump(sorted(timetable), open(os.path.join(feed, "lines.json"), "w"), indent=1)
    with open(os.path.join(feed, "source.json"), "w", encoding="utf-8") as f:
        json.dump({"lines": meta, "timetable": timetable}, f, ensure_ascii=False, indent=1)

    files = ["agency.txt", "calendar.txt", "calendar_dates.txt", "feed_info.txt", "routes.txt", "shapes.txt",
             "stop_times.txt", "stops.txt", "trips.txt", "LICENSE.txt"]
    with zipfile.ZipFile(os.path.join(feed, "buk.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for n in files:
            if os.path.exists(os.path.join(feed, n)):
                z.write(os.path.join(feed, n), n)
    log(f"{len(routes)} routes, {len(trips)} trips, {len(st)} stop_times, {len(shapes)} shape points, "
        f"{start}..{end}")
    unused = sorted(set(coord) - used)
    if unused:
        log("stops without service:", ", ".join(unused))


def main():
    feed = sys.argv[1] if len(sys.argv) > 1 else "buk"
    try:
        if len(sys.argv) > 2:
            txt = open(sys.argv[2], encoding="utf-8").read()
            if txt.lstrip().startswith("{"):
                timetable, meta = json.loads(txt), {}
                old = os.path.join(feed, "source.json")
                if os.path.exists(old):
                    meta = json.load(open(old, encoding="utf-8"))["lines"]
            else:
                timetable, meta = parse_bundle(txt)
        else:
            timetable, meta = parse_bundle(fetch_bundle())
        if not timetable:
            raise SystemExit("empty timetable")
        build(feed, timetable, meta)
    finally:
        with open("buk_report.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(report[-60:]))
        if alerts:
            with open("buk_alert.txt", "w", encoding="utf-8") as f:
                f.write("\n\n".join(alerts))


if __name__ == "__main__":
    main()
