"""Čapljina – Metković – Ploče for the zfbih feed.

The section Ploče – Metković is in the HŽPP feed, so ŽFBH trains to/from Ploče start or
end in Metković here. Every HŽPP train at Metković continues to Bosnia, so its Metković
time is taken as given (calendars are not compared):

  towards Bosnia:  HŽPP arrival in Metković .. ŽFBH departure from Čapljina
  towards Croatia: ŽFBH arrival in Čapljina .. HŽPP departure from Metković

Metković – Čapljina takes RUN minutes; the rest of the interval is split into equally long
stops in both stations.

Standalone: python hzpp_metkovic.py FEED_DIR [hzpp_gtfs.zip]  (rewrites the current feed)
"""
import csv
import io
import os
import sys
import zipfile
from datetime import date

HZPP_URL = ("https://gitlab.com/api/v4/projects/vekejsn%2Fgtfs-generators/packages/generic/"
            "hzpp-gtfs/latest/hzpp_gtfs.zip")
CAPL, METK, PLOC = "CAPL", "METK", "PLOC"
RUN = 6
WINDOW = 120   # longest accepted Čapljina <-> Metković interval, minutes


def _tmin(s):
    h, m, *_ = s.split(":")
    return int(h) * 60 + int(m)


def load_events(src=None):
    """{'north': [Metković arrivals from Ploče], 'south': [Metković departures to Ploče]}"""
    if src and os.path.exists(src):
        data = open(src, "rb").read()
    else:
        import requests
        r = requests.get(HZPP_URL, timeout=180)
        r.raise_for_status()
        data = r.content
    z = zipfile.ZipFile(io.BytesIO(data))

    def rows(name):
        return csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8-sig"))

    names = {r["stop_id"]: r["stop_name"].strip() for r in rows("stops.txt")}
    m_ids = {k for k, v in names.items() if v == "Metković"}
    p_ids = {k for k, v in names.items() if v == "Ploče"}
    today = date.today().strftime("%Y%m%d")
    live = {r["service_id"] for r in rows("calendar.txt") if r["end_date"] >= today}
    if "calendar_dates.txt" in z.namelist():
        live |= {r["service_id"] for r in rows("calendar_dates.txt")
                 if r["exception_type"] == "1" and r["date"] >= today}
    trips = {r["trip_id"]: r for r in rows("trips.txt") if r["service_id"] in live}
    at = {}
    for r in rows("stop_times.txt"):
        if r["trip_id"] in trips and (r["stop_id"] in m_ids or r["stop_id"] in p_ids):
            at.setdefault(r["trip_id"], {})["M" if r["stop_id"] in m_ids else "P"] = r
    ev = {"north": set(), "south": set(), "trains": set()}
    for tid, d in at.items():
        if "M" not in d or "P" not in d:
            continue
        m, p = d["M"], d["P"]
        if int(p["stop_sequence"]) < int(m["stop_sequence"]):
            ev["north"].add(_tmin(m["arrival_time"]) % 1440)
        else:
            ev["south"].add(_tmin(m["departure_time"]) % 1440)
        ev["trains"].add(trips[tid].get("trip_short_name") or tid)
    return {k: sorted(v) for k, v in ev.items()}


def adjust(kept, ev):
    """kept: [[stop_id, arr, dep], ...] in minutes (may exceed 1440). Returns (kept, note)."""
    # drop Ploče (and Metković, rebuilt below) at either end
    while len(kept) > 2 and kept[0][0] in (PLOC, METK):
        kept = kept[1:]
    while len(kept) > 2 and kept[-1][0] in (PLOC, METK):
        kept = kept[:-1]
    if kept[0][0] == CAPL and ev["north"]:
        c = kept[0][2]
        cand = [c - ((c - a) % 1440) for a in ev["north"]]
        cand = [a for a in cand if RUN <= c - a <= WINDOW]
        if cand:
            a = max(cand)
            rest = c - a - RUN
            dm = rest // 2
            kept = [[METK, a, a + dm], [CAPL, a + dm + RUN, c]] + kept[1:]
            return kept, f"z Metkoviće {a % 1440 // 60:02d}:{a % 60:02d}"
        return kept, "žádný navazující vlak HŽPP v Metkoviću"
    if kept[-1][0] == CAPL and ev["south"]:
        c = kept[-1][1]
        cand = [c + ((d - c) % 1440) for d in ev["south"]]
        cand = [d for d in cand if RUN <= d - c <= WINDOW]
        if cand:
            d = min(cand)
            rest = d - c - RUN
            dc = rest // 2
            kept = kept[:-1] + [[CAPL, c, c + dc], [METK, c + dc + RUN, d]]
            return kept, f"do Metkoviće, odjezd {d % 1440 // 60:02d}:{d % 60:02d}"
        return kept, "žádný navazující vlak HŽPP v Metkoviću"
    return kept, None


# ------------------------------------------------------------- standalone use

def main():
    feed = sys.argv[1]
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from build_cfm import fit_shape, fmt, read, write

    ev = load_events(sys.argv[2] if len(sys.argv) > 2 else None)
    print("HŽPP:", ev)
    trips = read(feed, "trips.txt")
    st = read(feed, "stop_times.txt")
    stops = {r["stop_id"]: r for r in read(feed, "stops.txt")}
    coord = {k: (float(v["stop_lat"]), float(v["stop_lon"])) for k, v in stops.items()}
    shapes = {}
    for r in read(feed, "shapes.txt"):
        shapes.setdefault(r["shape_id"], []).append(r)
    pts = {k: [(float(r["shape_pt_lat"]), float(r["shape_pt_lon"]))
               for r in sorted(v, key=lambda r: int(r["shape_pt_sequence"]))] for k, v in shapes.items()}
    by = {}
    for r in st:
        by.setdefault(r["trip_id"], []).append(r)
    routes = {r["route_id"]: r for r in read(feed, "routes.txt")}
    out = []
    for t in trips:
        rows = sorted(by[t["trip_id"]], key=lambda r: int(r["stop_sequence"]))
        kept = [[r["stop_id"], _tmin(r["arrival_time"]), _tmin(r["departure_time"])] for r in rows]
        if CAPL not in (kept[0][0], kept[-1][0]) and not ({METK, PLOC} & {kept[0][0], kept[-1][0]}):
            out += rows
            continue
        new, note = adjust(kept, ev)
        if note:
            print(t["trip_id"], note)
        if new != kept:
            seq = [k[0] for k in new]
            fits = [(sh, p, fit_shape(p, [coord[s] for s in seq])) for sh, p in pts.items()]
            fits = [f for f in fits if f[2]]
            if fits:
                sh, p, idx = min(fits, key=lambda f: len(f[1]))
                sid = f"{seq[0]}-{seq[-1]}"
                if sid not in pts:
                    pts[sid] = p[idx[0]:idx[-1] + 1]
                t["shape_id"] = sid
            t["trip_headsign"] = stops[seq[-1]]["stop_name"]
            r0 = routes.get(t["route_id"])
            if r0:
                r0["route_long_name"] = f"{stops[seq[0]]['stop_name']} – {stops[seq[-1]]['stop_name']}"
        for n, (s, a, d) in enumerate(new, 1):
            out.append({**rows[0], "arrival_time": fmt(a), "departure_time": fmt(d), "stop_id": s,
                        "stop_sequence": str(n)})
    shp = [r for v in shapes.values() for r in v]
    for k, p in pts.items():
        if k not in shapes:
            shp += [{"shape_id": k, "shape_pt_lat": str(la), "shape_pt_lon": str(lo), "shape_pt_sequence": str(i)}
                    for i, (la, lo) in enumerate(p, 1)]
    write(feed, "trips.txt", list(trips[0]), trips)
    write(feed, "stop_times.txt", list(st[0]), out)
    write(feed, "routes.txt", list(next(iter(routes.values()))), list(routes.values()))
    write(feed, "shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], shp)


if __name__ == "__main__":
    main()
