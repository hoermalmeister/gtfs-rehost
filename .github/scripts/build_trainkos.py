"""Build the Trainkos GTFS feed from https://www.trainkos.com/orari.

Usage:
  python build_trainkos.py FEED_DIR                    # download pages from trainkos.com
  python build_trainkos.py FEED_DIR --html-dir DIR     # use saved pages (orari.html, detajet_1.html, ...)

What it does
  * reads the list of lines (orari/detajet/N) from the overview page
  * active line  -> its trips are rebuilt from the page (service WD, daily)
  * line "Out Of Service" -> the trips already in the feed are kept as they are,
    with service IN (inactive)
  * a new line or a new trip is added automatically; a trip whose stop sequence is
    not covered by an existing shape gets a shape cut from an existing shape, or,
    if none covers it, a route from brouter.de (profile rail)
  * calendar / feed_info validity is rolled forward on every run
  * anything that needs a human (unknown station, unexpected page layout) is written
    to trainkos_alert.txt in the workspace root and the script exits with code 1
    without touching the feed

Which trips belong to which line is remembered in FEED_DIR/lines.json.
"""

import csv
import io
import json
import math
import os
import re
import sys
import unicodedata
import zipfile
from datetime import date, timedelta

BASE = "https://www.trainkos.com"
VALID_DAYS = 120          # feed validity from the day of the run
BROUTER = "https://brouter.de/brouter"

# Station names as shown on the website (EN / SQ / SR) -> stop_id
ALIASES = {
    "PRIS": ["Pristina", "Prishtina", "Prishtinë", "Priština"],
    "EKON": ["Sh. Ekonomike", "Shkolla Ekonomike", "Ekonomska škola", "Ekonomska skola"],
    "FKOS": ["Kosovo Polje", "Fushë Kosova", "Fushë Kosovë", "Fushe Kosove", "Kosovo Polje"],
    "BARD": ["Bardh", "Bardh i Madh", "Veliki Belaćevac"],
    "MJEK": ["Mjekaj"],
    "DRIT": ["Dritan"],
    "DREN": ["Drenas", "Glogovac"],
    "DAMA": ["Damanek"],
    "LUGD": ["Lugdren"],
    "GRYK": ["Grykas"],
    "QARR": ["Qarrat"],
    "AQAR": ["Aqarevë", "Aqarevä", "Aqareva"],
    "UJEM": ["Ujëmir", "Ujemir"],
    "GURK": ["Gurkat"],
    "KLIN": ["Klina", "Klinë", "Kline"],
    "SHEN": ["Shëngjergj", "Shängjergj", "Shengjergj"],
    "BUDI": ["Budisalc", "Budisavci"],
    "ARBA": ["Arbanë", "Arbana"],
    "SEPE": ["Seperant"],
    "PEJE": ["Peja", "Pejë", "Peć", "Pec"],
    "MIRA": ["Miradi", "Miradi e Epërme"],
    "LIPJ": ["Lipjan", "Lipljan"],
    "FUSH": ["Fushlot"],
    "BABL": ["Bablak"],
    "KODR": ["Kodrion"],
    "FERI": ["Ferizaj", "Uroševac"],
    "GURE": ["Gurëz", "Gurez"],
    "KIVJ": ["Kaçanik i Vjetër", "Kacanik i Vjeter"],
    "STAG": ["Stagovë", "Stagovo"],
    "KACA": ["Kaçanik", "Kacanik"],
    "HANI": ["Han i Elezit", "Elez Han"],
    "BLAC": ["Blace"],
    "VOLK": ["Volkovo"],
    "GJOR": ["Gjorče Petrov", "Gjorce Petrov", "Đorče Petrov"],
    "SKSE": ["Skopje Sever", "Shkup Veri"],
    "ZELE": ["Železarnica", "Zelezarnica"],
    "SKOP": ["Skopje", "Shkup", "Shkupi"],
}


def norm(s):
    s = unicodedata.normalize("NFKD", s.replace("ä", "ë")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


NAME_TO_ID = {norm(n): sid for sid, names in ALIASES.items() for n in names}


class NeedsHuman(Exception):
    pass


# ---------------------------------------------------------------- pages

def fetch_pages(html_dir):
    """Return (overview_html, {line_id: html})."""
    if html_dir:
        with open(os.path.join(html_dir, "orari.html"), encoding="utf-8") as f:
            overview = f.read()
        ids = line_ids(overview)
        pages = {}
        for i in ids:
            with open(os.path.join(html_dir, f"detajet_{i}.html"), encoding="utf-8") as f:
                pages[i] = f.read()
        return overview, pages

    import requests
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (gtfs-rehost; +https://github.com/hoermalmeister/gtfs-rehost)"
    s.cookies.set(".AspNetCore.Culture", "c=en-GB|uic=en-GB", domain="www.trainkos.com")
    r = s.get(f"{BASE}/orari", timeout=60)
    r.raise_for_status()
    overview = r.text
    pages = {}
    for i in line_ids(overview):
        r = s.get(f"{BASE}/orari/detajet/{i}", timeout=60)
        r.raise_for_status()
        pages[i] = r.text
    dump = os.path.join(os.environ.get("GITHUB_WORKSPACE", "."), "trainkos_pages")
    os.makedirs(dump, exist_ok=True)                 # saved for debugging (artifact)
    with open(os.path.join(dump, "orari.html"), "w", encoding="utf-8") as f:
        f.write(overview)
    for i, h in pages.items():
        with open(os.path.join(dump, f"detajet_{i}.html"), "w", encoding="utf-8") as f:
            f.write(h)
    return overview, pages


def line_ids(overview):
    ids = sorted({int(x) for x in re.findall(r"/orari/detajet/(\d+)", overview)})
    if not ids:
        raise NeedsHuman("Na stránce /orari jsem nenašel žádný odkaz orari/detajet/N – změnil se web?")
    return ids


def parse_line(html, line_id):
    """Return (title, out_of_service, [trip dict])."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one(".page-hero h1")
    title = h1.get_text(strip=True) if h1 else f"line {line_id}"
    cards = soup.select(".sched-card")
    text = norm(soup.get_text(" "))
    oos = bool(soup.select_one(".jr-oos")) or any(
        norm(p) in text for p in ("Out Of Service", "Jashtë Shërbimit", "jashtë funksionit",
                                  "Van saobraćaja", "Van funkcije", "Izvan saobraćaja"))
    if oos and not cards:
        return title, True, []
    if not cards:
        main = soup.find("main")
        print(f"--- detajet/{line_id} text ---\n" + (main or soup).get_text(" ", strip=True)[:1500])
        raise NeedsHuman(f"Linka {line_id} ({title}) není označená jako mimo provoz, ale nemá žádný jízdní řád.")

    headings = [h.get_text(" ", strip=True) for h in soup.select(".section-title h2")]
    odd = [h for h in headings if "regular" not in h.lower() and "rregullt" not in h.lower()
           and "redovn" not in h.lower()]
    if odd:
        raise NeedsHuman(f"Linka {line_id} ({title}) má na webu další sekci jízdního řádu: {odd}. "
                         "Skript zatím umí jen pravidelný (denní) jízdní řád.")

    trips = {}
    for card in cards:
        for row in card.select(".sched-row"):
            name = row.select_one(".sched-station").get_text(strip=True)
            sid = NAME_TO_ID.get(norm(name))
            if not sid:
                raise NeedsHuman(f"Neznámá stanice „{name}“ na lince {line_id} ({title}). "
                                 "Doplňte ji do stops.txt a do ALIASES v .github/scripts/build_trainkos.py.")
            for chip in row.select(".sched-chip"):
                t = chip.find("b").get_text(strip=True)
                small = chip.find("small")
                label = small.get_text(" ", strip=True) if small else ""
                m = re.match(r"(\d+)\s*·\s*(\S+)", label)
                if not m:
                    raise NeedsHuman(f"Nečitelné číslo vlaku „{label}“ na lince {line_id}.")
                num, cat = m.groups()
                trips.setdefault(num, {"trip_id": num, "category": cat, "stops": []})
                trips[num]["stops"].append((sid, t))
    out = []
    for tr in trips.values():
        # times along the card order must grow; fix midnight crossing
        prev, day = -1, 0
        fixed = []
        for sid, t in tr["stops"]:
            h, m = map(int, t.split(":"))
            mins = h * 60 + m + day
            if mins < prev:
                day += 1440
                mins += 1440
            prev = mins
            fixed.append((sid, mins))
        tr["stops"] = fixed
        out.append(tr)
    return title, False, out


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
    if os.path.exists(path):                       # keep the file's existing line endings
        with open(path, "rb") as f:
            eol = "\r\n" if b"\r\n" in f.read(4096) else "\n"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator=eol, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def fmt(mins):
    return f"{mins // 60}:{mins % 60:02d}:00"


# ---------------------------------------------------------------- shapes

def dist(a, b):
    lat = math.radians((a[0] + b[0]) / 2)
    return math.hypot((a[0] - b[0]), (a[1] - b[1]) * math.cos(lat))


def cut_shape(points, start, end):
    """Cut a polyline between the points nearest to start and end (in this order)."""
    i = min(range(len(points)), key=lambda k: dist(points[k], start))
    if i >= len(points) - 1:
        return None
    j = min(range(i + 1, len(points)), key=lambda k: dist(points[k], end))
    if dist(points[i], start) > 0.005 or dist(points[j], end) > 0.005:   # ~500 m
        return None
    return points[i:j + 1]


def brouter(coords):
    import requests
    lonlats = "|".join(f"{lon:.6f},{lat:.6f}" for lat, lon in coords)
    r = requests.get(BROUTER, params={"lonlats": lonlats, "profile": "rail",
                                      "alternativeidx": 0, "format": "geojson"}, timeout=120)
    r.raise_for_status()
    pts = r.json()["features"][0]["geometry"]["coordinates"]
    return [(round(p[1], 6), round(p[0], 6)) for p in pts]


def is_subsequence(small, big):
    """True if `small` appears in `big` in order (stops may be skipped)."""
    it = iter(big)
    return all(s in it for s in small)


# ---------------------------------------------------------------- main

def build(feed, html_dir=None):
    overview, pages = fetch_pages(html_dir)

    stops = read(feed, "stops.txt")
    coord = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in stops}
    routes = {r["route_id"]: r for r in read(feed, "routes.txt")}
    trips = {t["trip_id"]: t for t in read(feed, "trips.txt")}
    st_rows = read(feed, "stop_times.txt")
    shapes = {}
    for p in read(feed, "shapes.txt"):
        shapes.setdefault(p["shape_id"], []).append(p)
    shapes = {k: [(float(p["shape_pt_lat"]), float(p["shape_pt_lon"]))
                  for p in sorted(v, key=lambda p: int(p["shape_pt_sequence"]))]
              for k, v in shapes.items()}
    seq_of = {}
    for r in sorted(st_rows, key=lambda r: (r["trip_id"], int(r["stop_sequence"]))):
        seq_of.setdefault(r["trip_id"], []).append(r["stop_id"])

    lines_path = os.path.join(feed, "lines.json")
    lines = json.load(open(lines_path, encoding="utf-8")) if os.path.exists(lines_path) else {}

    report = []
    new_trips, new_st = {}, []
    owned = set()
    for lid, html in pages.items():
        key = str(lid)
        title, oos, web = parse_line(html, lid)
        old_ids = lines.get(key, {}).get("trips", [])
        if key not in lines:
            report.append(f"Nová linka {lid}: {title}" + (" (mimo provoz)" if oos else ""))
        if oos:
            for tid in old_ids:
                if tid in trips:
                    new_trips[tid] = dict(trips[tid], service_id="IN")
                    new_st += [r for r in st_rows if r["trip_id"] == tid]
            lines[key] = {"title": title, "status": "out_of_service", "trips": old_ids}
            owned.update(old_ids)
            continue

        ids = []
        for tr in web:
            tid = tr["trip_id"]
            seq = [s for s, _ in tr["stops"]]
            first, last = seq[0], seq[-1]
            ids.append(tid)
            if tid not in old_ids:
                report.append(f"Linka {lid}: nový spoj {tr['category']} {tid} "
                              f"({first} {fmt(tr['stops'][0][1])[:-3]} → {last})")
            # route: one per train, like the rest of the feed
            name_of = {s["stop_id"]: s["stop_name"] for s in stops}
            routes[tid] = {"route_id": tid, "agency_id": "KOS",
                           "route_short_name": f"{tr['category']} {tid}",
                           "route_long_name": f"{name_of[first]} - {name_of[last]}",
                           "route_type": "2"}
            # shape: reuse, cut from an existing one, or ask brouter
            shape_id = None
            old = trips.get(tid)
            if old and seq_of.get(tid) == seq and old.get("shape_id") in shapes:
                shape_id = old["shape_id"]
            if not shape_id:
                for t2, s2 in seq_of.items():
                    sid2 = trips.get(t2, {}).get("shape_id")
                    if s2 == seq and sid2 in shapes:
                        shape_id = sid2
                        break
            if not shape_id:
                cand = f"{first}-{last}"
                if cand in shapes:
                    shape_id = cand
                else:
                    pts = None
                    for t2, s2 in seq_of.items():
                        sid2 = trips.get(t2, {}).get("shape_id")
                        if sid2 in shapes and is_subsequence(seq, s2):
                            pts = cut_shape(shapes[sid2], coord[first], coord[last])
                            if pts:
                                report.append(f"  shape {cand} vyříznut z {sid2}")
                                break
                    if not pts:
                        pts = brouter([coord[s] for s in seq])
                        report.append(f"  shape {cand} z brouter.de")
                    shapes[cand] = pts
                    shape_id = cand
            new_trips[tid] = {"route_id": tid, "service_id": "WD", "trip_id": tid,
                              "trip_headsign": name_of[last], "shape_id": shape_id,
                              "bikes_allowed": "1"}
            for n, (sid, mins) in enumerate(tr["stops"], 1):
                new_st.append({"trip_id": tid, "arrival_time": fmt(mins), "departure_time": fmt(mins),
                               "stop_id": sid, "stop_sequence": str(n), "timepoint": "1"})
            if old and seq_of.get(tid):
                old_t = [r["departure_time"] for r in st_rows if r["trip_id"] == tid]
                new_t = [fmt(m) for _, m in tr["stops"]]
                if old_t != new_t and tid in old_ids:
                    report.append(f"Linka {lid}: změněné časy spoje {tid}")
        for tid in old_ids:
            if tid not in ids:
                report.append(f"Linka {lid}: spoj {tid} už na webu není – odstraněn")
        lines[key] = {"title": title, "status": "active", "trips": ids}
        owned.update(ids)

    for key in list(lines):
        if int(key) not in pages:
            report.append(f"Linka {key} ({lines[key]['title']}) zmizela z webu – její spoje ponechány jako neaktivní")
            for tid in lines[key]["trips"]:
                if tid in trips:
                    new_trips[tid] = dict(trips[tid], service_id="IN")
                    new_st += [r for r in st_rows if r["trip_id"] == tid]
            owned.update(lines[key]["trips"])

    # trips not assigned to any line (manual additions) stay untouched
    for tid, t in trips.items():
        if tid not in owned:
            new_trips[tid] = t
            new_st += [r for r in st_rows if r["trip_id"] == tid]

    used_routes = {t["route_id"] for t in new_trips.values()}
    used_shapes = {t["shape_id"] for t in new_trips.values()}
    today = date.today()
    end = (today + timedelta(days=VALID_DAYS)).strftime("%Y%m%d")
    cal = read(feed, "calendar.txt")
    for c in cal:
        c["end_date"] = end
    fi = read(feed, "feed_info.txt")
    for f in fi:
        f["feed_end_date"] = end
        f["feed_publisher_url"] = f"{BASE}/orari"
        f["feed_version"] = today.strftime("%Y%m%d")
    ag = read(feed, "agency.txt")
    for a in ag:
        a["agency_url"] = f"{BASE}/orari"

    order = {tid: i for i, tid in enumerate(t for l in lines.values() for t in l["trips"])}
    trip_list = sorted(new_trips.values(), key=lambda t: order.get(t["trip_id"], 1e9))
    write(feed, "agency.txt", list(ag[0].keys()), ag)
    write(feed, "calendar.txt", list(cal[0].keys()), cal)
    write(feed, "feed_info.txt", list(fi[0].keys()), fi)
    write(feed, "routes.txt", ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
          [routes[t["route_id"]] for t in trip_list if t["route_id"] in used_routes])
    write(feed, "trips.txt", ["route_id", "service_id", "trip_id", "trip_headsign", "shape_id", "bikes_allowed"],
          trip_list)
    new_st.sort(key=lambda r: (order.get(r["trip_id"], 1e9), int(r["stop_sequence"])))
    write(feed, "stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
                                   "timepoint"], new_st)
    shape_rows = [{"shape_id": k, "shape_pt_lat": f"{la:.6f}".rstrip("0").rstrip("."),
                   "shape_pt_lon": f"{lo:.6f}".rstrip("0").rstrip("."),
                   "shape_pt_sequence": str(n)}
                  for k in shapes if k in used_shapes for n, (la, lo) in enumerate(shapes[k], 1)]
    write(feed, "shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], shape_rows)
    with open(lines_path, "w", encoding="utf-8") as f:
        json.dump(lines, f, ensure_ascii=False, indent=2)
        f.write("\n")

    files = ["agency.txt", "calendar.txt", "feed_info.txt", "routes.txt", "shapes.txt",
             "stop_times.txt", "stops.txt", "trips.txt"]
    with zipfile.ZipFile(os.path.join(feed, "trainkos.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for n in files:
            z.write(os.path.join(feed, n), n)
        if os.path.exists(os.path.join(feed, "LICENSE.txt")):
            z.write(os.path.join(feed, "LICENSE.txt"), "LICENSE.txt")
    return report


def main():
    feed = sys.argv[1]
    html_dir = sys.argv[sys.argv.index("--html-dir") + 1] if "--html-dir" in sys.argv else None
    workspace = os.environ.get("GITHUB_WORKSPACE", ".")
    try:
        report = build(feed, html_dir)
    except NeedsHuman as e:
        with open(os.path.join(workspace, "trainkos_alert.txt"), "w", encoding="utf-8") as f:
            f.write(f"⚠️ Aktualizace Trainkos GTFS se zastavila – feed nebyl změněn.\n\n{e}\n")
        print(e)
        sys.exit(1)
    text = "\n".join(report) if report else "Beze změn v jízdním řádu."
    print(text)
    with open(os.path.join(workspace, "trainkos_report.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")


if __name__ == "__main__":
    main()
