"""Build the ŽFBH part of the zfbih GTFS feed from https://www.zfbh.ba/en/time-table/.

Usage:
  python build_zfbih.py FEED_DIR [--workers N] [--cache DIR]

The timetable form only answers "trains between station A and station B" (both
directions in one answer), so every unordered pair of stations is queried. Trains are
rebuilt like in build_cfm.py (shared helpers are imported from there).

  * the overview table on the page gives train type, relation and the times at the
    first/last station; termini outside the form's station list (Ploče) come from it
  * stations known only to the feed (Metković) are put back between the neighbouring
    stations, at the same relative position as in an existing trip
  * Ploče – Metković is in the HŽPP feed: trains to/from Ploče start/end in Metković,
    timed from the HŽPP feed (see hzpp_metkovic.py)
  * days of operation come from the notes (* ** *** ...) below the overview table,
    interpreted by FEED_DIR/notes.json; a note that is not in notes.json raises an issue
    and the trains concerned keep their previous calendar

Trips of other agencies in the feed (ŽRS) are left untouched.
"""

import concurrent.futures as cf
import html as htmlmod
import json
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_cfm import (brouter, dist, fit_shape, fmt, read, rebuild, tmin,  # noqa: E402
                       write)
import hzpp_metkovic  # noqa: E402

URL = "https://www.zfbh.ba/en/time-table/"
AGENCY = "FBH"
VALID_DAYS = 120
TYPES = {"fast train": "B", "local train": "P", "brzi": "B", "putnicki": "P"}
ALIASES = {"lješevo": "LJSE", "ljesevo": "LJSE"}


def norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def clean(s):
    return re.sub(r"\s+", " ", htmlmod.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def parse_time(s):
    m = re.search(r"(\d{1,2})[:.](\d{2})", s)
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


# ---------------------------------------------------------------- page parsing

def parse_overview(page):
    """Stations of the form, overview table and notes."""
    sel = re.search(r'<select name="redvoznje_pol".*?</select>', page, re.S)
    if not sel:
        raise SystemExit("Stránka nevrátila formulář se stanicemi. Začátek odpovědi:\n" + page[:1500])
    stations = [clean(o) for o in re.findall(r"<option>(.*?)</option>", sel.group(0), re.S)]

    overview = {}
    tab = re.search(r'<table class="red_voz">(.*?)</table>', page, re.S)
    if tab:
        for row in re.findall(r"<tr>(.*?)</tr>", tab.group(1), re.S):
            tds = [clean(t) for t in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
            if len(tds) < 5:
                continue
            m = re.match(r"(\d+)\s*(\**)", tds[0])
            if not m:
                continue
            parts = re.split(r"\s+[-–]\s+", tds[2], maxsplit=1)
            overview[m.group(1)] = {"stars": len(m.group(2)), "type": tds[1],
                                    "origin": parts[0], "dest": parts[-1],
                                    "dep": parse_time(tds[3]), "arr": parse_time(tds[4])}

    notes, marker = [], 0
    box = re.search(r'<div class="alert alert-warning[^"]*">(.*?)</div>', page, re.S)
    if box:
        for a in re.findall(r"<a[^>]*>(.*?)</a>", box.group(1), re.S):
            text = clean(a)
            if not text:
                continue
            m = re.match(r"(\*+)", text)
            if m:
                marker = len(m.group(1))
            notes.append((marker, text))
    return stations, overview, notes


def parse_pair(page):
    """Rows of both answer tables: train, from, to, dep, arr."""
    out = []
    for chunk in re.split(r"<tr class=['\"]relacija['\"]>", page)[1:]:
        chunk = re.split(r"</table>", chunk)[0]
        tds = [clean(t) for t in re.findall(r"<td[^>]*>(.*?)</td>", chunk, re.S)][:5]
        if len(tds) < 5:
            continue
        num = re.sub(r"\D", "", tds[0])
        out.append({"train": num, "from": tds[1], "to": tds[2],
                    "dep": parse_time(tds[3]), "arr": parse_time(tds[4])})
    return out


# ---------------------------------------------------------------- download

def session():
    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (gtfs-rehost; +https://github.com/hoermalmeister/gtfs-rehost)",
                      "Referer": URL, "Origin": "https://www.zfbh.ba"})
    s.cookies.set("wpglobus-language", "en", domain="www.zfbh.ba")
    return s


def fetch(s, a=None, b=None, cache=None):
    key = f"{norm(a)}__{norm(b)}" if a else "overview"
    path = os.path.join(cache, key + ".html") if cache else None
    if path and os.path.exists(path):
        return open(path, encoding="utf-8").read()
    for attempt in range(4):
        try:
            if a:
                r = s.post(URL, data={"redvoznje_pol": a, "redvoznje_dol": b}, timeout=60)
            else:
                r = s.get(URL, timeout=60)
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


def crawl(workers, cache):
    s = session()
    if cache:
        os.makedirs(cache, exist_ok=True)
    stations, overview, notes = parse_overview(fetch(s, cache=cache))
    pairs = [(a, b) for i, a in enumerate(stations) for b in stations[i + 1:]]
    rows = []
    with cf.ThreadPoolExecutor(workers) as ex:
        for res in ex.map(lambda p: parse_pair(fetch(s, p[0], p[1], cache)), pairs):
            rows += res
    return stations, overview, notes, rows


# ---------------------------------------------------------------- calendar

def easter(year, orthodox=False):
    """Easter Sunday: Gregorian (Catholic) or Julian (Orthodox, returned as a Gregorian date)."""
    if orthodox:                                  # Meeus, Julian calendar
        a, b, c = year % 4, year % 7, year % 19
        d = (19 * c + 15) % 30
        e = (2 * a + 4 * b - d + 34) % 7
        month, day = divmod(d + e + 114, 31)
        julian = date(year, month, day + 1)
        return julian + timedelta(days=year // 100 - year // 400 - 2)
    a = year % 19                                  # anonymous Gregorian algorithm
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def holiday_set(cfg, start, end):
    """Fixed holidays (MM-DD), single dates and days around Easter:
    "easter_orthodox" / "easter_catholic" = offsets from Easter Sunday, e.g. [-2, -1, 0, 1]
    for Good Friday .. Easter Monday."""
    out = set(cfg.get("holiday_dates", []))
    for y in range(start.year, end.year + 1):
        out |= {f"{y}-{md}" for md in cfg.get("holidays", [])}
        for key, orth in (("easter_orthodox", True), ("easter_catholic", False)):
            for off in cfg.get(key, []):
                out.add((easter(y, orth) + timedelta(days=off)).isoformat())
    return out


def dates_for(periods, start, end, holidays):
    out = set()
    d = start
    while d <= end:
        iso = d.isoformat()
        on = False
        for p in periods:
            if p.get("from", "0000") <= iso <= p.get("until", "9999"):
                on = p["days"][d.weekday()] == "1" and (p.get("holidays", True) or iso not in holidays)
        if on:
            out.add(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return frozenset(out)


def calendar_rows(sid, dates, start, end):
    """Best-fit weekly pattern + exceptions."""
    by_wd = {i: [0, 0] for i in range(7)}
    d = start
    while d <= end:
        by_wd[d.weekday()][1] += 1
        by_wd[d.weekday()][0] += d.strftime("%Y%m%d") in dates
        d += timedelta(days=1)
    mask = "".join("1" if on * 2 > tot else "0" for on, tot in (by_wd[i] for i in range(7)))
    cal = dict(zip(["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                    "sunday", "start_date", "end_date"],
                   [sid, *mask, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")]))
    exc = []
    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        base = mask[d.weekday()] == "1"
        if base and ds not in dates:
            exc.append({"service_id": sid, "date": ds, "exception_type": "2"})
        elif not base and ds in dates:
            exc.append({"service_id": sid, "date": ds, "exception_type": "1"})
        d += timedelta(days=1)
    return cal, exc


# ---------------------------------------------------------------- main

def build(feed, stations, overview, notes, rows):
    warnings, report = [], []
    if not rows:
        raise SystemExit("Formulář nevrátil žádný spoj – nic neměním.")
    stops = read(feed, "stops.txt")
    coord = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in stops}
    names = {s["stop_id"]: s["stop_name"] for s in stops}
    by_name = {norm(s["stop_name"]): s["stop_id"] for s in stops}
    by_name.update({norm(k): v for k, v in ALIASES.items()})

    def sid_of(name):
        return by_name.get(norm(name))

    form_ids = {sid_of(n) for n in stations if sid_of(n)}
    unknown = sorted({n for r in rows for n in (r["from"], r["to"]) if not sid_of(n)})
    for n in unknown:
        tr = sorted({r["train"] for r in rows if n in (r["from"], r["to"])}, key=int)
        warnings.append(f"Stanice „{n}“ z formuláře není ve stops.txt – vynechána, doplňte ji "
                        f"(stop_id, název, souřadnice). Staví v ní vlaky: {', '.join(tr)}")

    old_trips = {t["trip_id"]: t for t in read(feed, "trips.txt")}
    old_routes = {r["route_id"]: r for r in read(feed, "routes.txt")}
    old_st = {}
    for r in read(feed, "stop_times.txt"):
        old_st.setdefault(r["trip_id"], []).append(r)
    for v in old_st.values():
        v.sort(key=lambda r: int(r["stop_sequence"]))
    old_cal = {c["service_id"]: c for c in read(feed, "calendar.txt")}
    old_cd = {}
    for c in read(feed, "calendar_dates.txt"):
        old_cd.setdefault(c["service_id"], []).append(c)
    shapes = {}
    for p in read(feed, "shapes.txt"):
        shapes.setdefault(p["shape_id"], []).append(p)
    shapes = {k: [(float(p["shape_pt_lat"]), float(p["shape_pt_lon"]))
                  for p in sorted(v, key=lambda p: int(p["shape_pt_sequence"]))] for k, v in shapes.items()}
    other = {tid for tid, t in old_trips.items()
             if old_routes.get(t["route_id"], {}).get("agency_id", AGENCY) != AGENCY}

    # ---- trains from the pair answers
    obs = []
    for r in rows:
        a, b = sid_of(r["from"]), sid_of(r["to"])
        if a and b:
            obs.append({"train": r["train"], "route": "", "dep": r["dep"], "arr": r["arr"], "dur": None,
                        "mask": "", "a": a, "b": b})
    trains = rebuild(obs, warnings, report)

    # ---- notes
    cfg = json.load(open(os.path.join(feed, "notes.json"), encoding="utf-8"))
    known = {norm(n["text"]): n for n in cfg["notes"]}
    rules, unknown_markers = {}, set()
    for marker, text in notes:
        n = known.get(norm(text))
        if not n:
            warnings.append(f"Nová / změněná poznámka na webu – doplňte ji do zfbih/notes.json: „{text}“")
            unknown_markers.add(marker)
            continue
        rules.setdefault(marker, []).append(n)
    web_texts = {norm(t) for _, t in notes}
    for n in cfg["notes"]:
        if norm(n["text"]) not in web_texts:
            report.append(f"Poznámka z notes.json už na webu není: „{n['text']}“")

    today = date.today()
    start, end = today - timedelta(days=7), today + timedelta(days=VALID_DAYS)
    hol = holiday_set(cfg, start, end)
    daily = dates_for([{"days": "1111111"}], start, end, hol)
    workdays = dates_for([{"days": "1111100", "holidays": False}], start, end, hol)

    services = {}          # dates -> service_id
    kept_services = set()  # old service ids reused unchanged

    def service_of(num):
        ov = overview.get(num)
        stars = ov["stars"] if ov else 0
        if not ov:
            warnings.append(f"Vlak {num} není v přehledové tabulce – beru jako denní")
        if stars == 0:
            return "AL", daily, None
        cand = [n for n in rules.get(stars, []) if num in n.get("trains", [])] or \
               [n for n in rules.get(stars, []) if not n.get("trains")]
        if not cand:
            if stars not in unknown_markers:
                warnings.append(f"Vlak {num} ({'*' * stars}): žádná poznámka z notes.json se ho netýká")
            old = old_trips.get(num)
            if old and old["service_id"] in old_cal:
                return old["service_id"], None, "keep"
            return "AL", daily, None
        dates = dates_for(cand[0]["periods"], start, end, hol)
        return f"N{stars}" + ("_" + num if cand[0].get("trains") else ""), dates, None

    try:
        hz = hzpp_metkovic.load_events()
        report.append("HŽPP Metković: vlaky " + ", ".join(hz["trains"]))
    except Exception as e:  # noqa: BLE001
        hz = None
        warnings.append(f"Feed HŽPP se nepodařilo načíst ({e}) – úsek Čapljina–Metković beze změny")

    trips, st_rows, routes = [], [], []
    for num in sorted(trains, key=lambda n: trains[n]["stops"][0][2]):
        kept = [list(k) for k in trains[num]["stops"] if k[0] in coord]
        ov = overview.get(num)
        # termini outside the form's station list, times from the overview table
        if ov:
            o, d = sid_of(ov["origin"]), sid_of(ov["dest"])
            if o and o in coord and o not in form_ids and o not in [k[0] for k in kept] and ov["dep"] is not None:
                t = ov["dep"]
                while t > kept[0][1]:
                    t -= 1440
                while kept[0][1] - t >= 1440:
                    t += 1440
                kept.insert(0, [o, t, t])
            if d and d in coord and d not in form_ids and d not in [k[0] for k in kept] and ov["arr"] is not None:
                t = ov["arr"]
                while t < kept[-1][2]:
                    t += 1440
                while t - kept[-1][2] >= 1440:
                    t -= 1440
                kept.append([d, t, t])
        # feed-only stations between two consecutive stops, taken from any existing trip
        full = [kept[0]]
        for x, y in zip(kept, kept[1:]):
            for tid, ol in old_st.items():
                ids = [r["stop_id"] for r in ol]
                if x[0] in ids and y[0] in ids and ids.index(x[0]) < ids.index(y[0]):
                    i, j = ids.index(x[0]), ids.index(y[0])
                    between = [r for r in ol[i + 1:j] if r["stop_id"] not in form_ids and r["stop_id"] in coord]
                    if between:
                        p0, p1 = tmin(ol[i]["departure_time"]), tmin(ol[j]["arrival_time"])
                        f = (y[1] - x[2]) / (p1 - p0) if p1 > p0 else 0
                        for r in between:
                            full.append([r["stop_id"], round(x[2] + (tmin(r["arrival_time"]) - p0) * f),
                                         round(x[2] + (tmin(r["departure_time"]) - p0) * f)])
                    if between or j == i + 1:
                        break
            full.append(y)
        kept = full
        if len(kept) < 2:
            continue
        kept[0][1] = kept[0][2]
        kept[-1][2] = kept[-1][1]
        via_metk = False
        if hz:
            ends = (kept[0][0], kept[-1][0])
            kept, note = hzpp_metkovic.adjust(kept, hz)
            via_metk = hzpp_metkovic.METK in (kept[0][0], kept[-1][0])
            if note and not via_metk:
                warnings.append(f"Vlak {num}: {note}")
            elif hzpp_metkovic.PLOC in ends:
                report.append(f"Vlak {num}: zkrácen do Metkoviće (Ploče–Metković je ve feedu HŽPP)")
        shift = (kept[0][2] // 1440) * 1440
        for k in kept:
            k[1] -= shift
            k[2] -= shift

        sid, dates, mode = service_of(num)
        if mode == "keep":
            kept_services.add(sid)
            report.append(f"Vlak {num}: ponechán dosavadní kalendář {sid}")
        else:
            if not dates:
                report.append(f"Vlak {num} v období feedu nejede – vynechán")
                continue
            if dates == daily:
                sid = "AL"
            elif dates == workdays:
                sid = "WD"
            if dates in services:
                sid = services[dates]
            else:
                while sid in services.values():
                    sid += "x"
                services[dates] = sid

        typ = TYPES.get(ov["type"].lower(), "P") if ov else "P"
        long_name = (f"{ov['origin']} – {ov['dest']}" if ov and not via_metk
                     else f"{names[kept[0][0]]} – {names[kept[-1][0]]}")
        routes.append({"route_id": num, "agency_id": AGENCY, "route_short_name": f"{typ} {num}",
                       "route_long_name": long_name, "route_type": "2"})

        seq = [k[0] for k in kept]
        cs = [coord[s] for s in seq]
        shape_id = None
        if num in old_trips and old_trips[num].get("shape_id") in shapes \
                and fit_shape(shapes[old_trips[num]["shape_id"]], cs):
            shape_id = old_trips[num]["shape_id"]
        if not shape_id:
            fits = [(sh, pts, fit_shape(pts, cs)) for sh, pts in list(shapes.items())]
            fits = [f for f in fits if f[2]]
            whole = [f for f in fits if f[2][0] <= 2 and f[2][-1] >= len(f[1]) - 3]
            if whole:                                   # a shape that matches end to end
                shape_id = whole[0][0]
            elif fits:                                  # otherwise cut the shortest one that covers it
                sh, pts, idx = min(fits, key=lambda f: len(f[1]))
                shape_id = f"{seq[0]}-{seq[-1]}"
                if shape_id not in shapes:
                    shapes[shape_id] = pts[idx[0]:idx[-1] + 1]
                    report.append(f"  shape {shape_id} vyříznut z {sh}")
        if not shape_id:
            shape_id = f"{seq[0]}-{seq[-1]}"
            shapes[shape_id] = brouter(cs)
            report.append(f"  shape {shape_id} z brouter.de")

        old = old_st.get(num)
        if num not in old_trips:
            report.append(f"Nový vlak {typ} {num} {long_name}")
        elif old and [(r["stop_id"], tmin(r["arrival_time"]), tmin(r["departure_time"])) for r in old] \
                != [tuple(k) for k in kept]:
            report.append(f"Změna jízdního řádu vlaku {num} {long_name}")
        trips.append({"route_id": num, "service_id": sid, "trip_id": num,
                      "trip_headsign": names[kept[-1][0]], "shape_id": shape_id, "bikes_allowed": "1"})
        for n, (s_, arr, dep) in enumerate(kept, 1):
            st_rows.append({"trip_id": num, "arrival_time": fmt(arr), "departure_time": fmt(dep),
                            "stop_id": s_, "stop_sequence": str(n), "timepoint": "1"})

    new_ids = {t["trip_id"] for t in trips}
    for tid in old_trips:
        if tid not in new_ids and tid not in other:
            report.append(f"Vlak {tid} už web nevrací – odstraněn")

    # ---- other agencies (ŽRS) untouched
    other_services = set()
    for tid in sorted(other):
        trips.append(old_trips[tid])
        other_services.add(old_trips[tid]["service_id"])
        st_rows += old_st.get(tid, [])
    rid = {t["route_id"] for t in trips}
    routes += [r for k, r in old_routes.items() if k in rid and r["agency_id"] != AGENCY]

    cal, cd = [], []
    for dates, sid in sorted(services.items(), key=lambda x: x[1]):
        c, e = calendar_rows(sid, dates, start, end)
        cal.append(c)
        cd += e
    for sid in sorted(kept_services | other_services):
        if sid in old_cal and sid not in services.values():
            cal.append(old_cal[sid])
            cd += old_cd.get(sid, [])

    fi = read(feed, "feed_info.txt")
    for f in fi:
        f.update(feed_start_date=start.strftime("%Y%m%d"), feed_end_date=end.strftime("%Y%m%d"),
                 feed_version=today.strftime("%Y%m%d"))

    cal_fields = ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                  "sunday", "start_date", "end_date"]
    write(feed, "routes.txt", ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"], routes)
    write(feed, "trips.txt", ["route_id", "service_id", "trip_id", "trip_headsign", "shape_id", "bikes_allowed"], trips)
    write(feed, "stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
                                   "timepoint"], st_rows)
    write(feed, "calendar.txt", cal_fields, cal)
    write(feed, "calendar_dates.txt", ["service_id", "date", "exception_type"], cd)
    write(feed, "feed_info.txt", list(fi[0].keys()), fi)
    # all shapes are kept: the ŽRS ones are needed for the second source
    write(feed, "shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
          [{"shape_id": k, "shape_pt_lat": f"{la:.6f}".rstrip("0").rstrip("."),
            "shape_pt_lon": f"{lo:.6f}".rstrip("0").rstrip("."), "shape_pt_sequence": str(n)}
           for k in shapes for n, (la, lo) in enumerate(shapes[k], 1)])
    files = ["agency.txt", "calendar.txt", "calendar_dates.txt", "feed_info.txt", "routes.txt",
             "shapes.txt", "stop_times.txt", "stops.txt", "trips.txt"]
    with zipfile.ZipFile(os.path.join(feed, "zfbih.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for n in files:
            z.write(os.path.join(feed, n), n)
        if os.path.exists(os.path.join(feed, "LICENSE.txt")):
            z.write(os.path.join(feed, "LICENSE.txt"), "LICENSE.txt")
    return report, warnings


def main():
    feed = sys.argv[1]
    workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 6
    cache = sys.argv[sys.argv.index("--cache") + 1] if "--cache" in sys.argv else None
    ws = os.environ.get("GITHUB_WORKSPACE", ".")
    stations, overview, notes, rows = crawl(workers, cache)
    with open(os.path.join(ws, "zfbih_observations.json"), "w", encoding="utf-8") as f:
        json.dump({"stations": stations, "overview": overview, "notes": notes, "rows": rows}, f, ensure_ascii=False)
    report, warnings = build(feed, stations, overview, notes, rows)
    text = "\n".join(report) or "Beze změn v jízdním řádu."
    print(text)
    with open(os.path.join(ws, "zfbih_report.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    if warnings:
        print("\n".join(warnings))
        with open(os.path.join(ws, "zfbih_alert.txt"), "w", encoding="utf-8") as f:
            f.write("⚠️ Aktualizace ŽFBH (zfbih) – co je potřeba zkontrolovat:\n\n"
                    + "\n".join(f"- {w}" for w in warnings) + "\n")


if __name__ == "__main__":
    main()
