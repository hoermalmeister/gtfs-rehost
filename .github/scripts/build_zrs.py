"""Build the ŽRS part of the zfbih GTFS feed from https://zrs-rs.com/red-voznje.

Usage:
  python build_zrs.py FEED_DIR [--obs FILE]      # --obs: build from saved observations, no browser

The page is a Blazor Server app (no plain HTTP API), so it is driven with Playwright:
  * a search = start station, end station, date; the date picker only allows a window of
    about 17 days, and each answer is for that one day -> days of operation are observed,
    there are no notes to interpret
  * a result shows train number and stops only after its row is expanded, and only for
    the part between the searched stations; one result can consist of several trains
    (transfers), each leg has its own train number
Plan of one run (FEED_DIR/zrs.json):
  1. sweep (first weekday of the window): every station against the hubs of its line
     (all hubs for a station no hub-to-hub train serves), both ways, to find trains
     that do not run between two hubs
  2. every day of the window: all hub pairs + first/last station of every train found;
     a result row is expanded only if that row (train, times) has not been seen yet
  3. per train, days with the same times form one trip; the weekly pattern seen in the
     window is extended to extend_days (holidays: see zrs.json)
Trips of other agencies in the feed (ŽFBH) are left untouched.
"""

import json
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_cfm import brouter, fit_shape, fmt, read, tmin, write  # noqa: E402
from build_zfbih import calendar_rows, holiday_set  # noqa: E402

URL = "https://zrs-rs.com/red-voznje"
AGENCY = "RSR"
TYPES = {"lokalni": "P", "regionalni": "R", "brzi": "B", "ubrzani": "U", "intercity": "IC"}

JS_HELPERS = r"""
window.__zrs = {
  sleep: ms => new Promise(r => setTimeout(r, ms)),
  txt: e => (e ? e.textContent.replace(/ /g, ' ').replace(/\s+/g, ' ').trim() : ''),
  async pick(inputName, label) {
    const dd = document.querySelector(`input[name="${inputName}"]`).closest('.rz-dropdown');
    if (this.txt(dd.querySelector('.rz-dropdown-label')) === label) return;
    dd.click();
    const list = () => document.getElementById(dd.getAttribute('aria-controls'));
    for (let k = 0; k < 30 && !(list() && list().querySelector('li[role="option"]')); k++) await this.sleep(100);
    await this.sleep(200);
    const panel = list().closest('.rz-dropdown-panel');
    const search = panel && panel.querySelector('input[type="text"]');
    if (search) {
      search.focus(); search.value = label;
      search.dispatchEvent(new Event('input', {bubbles: true}));
      for (let k = 0; k < 40; k++) {
        await this.sleep(150);
        if ([...list().querySelectorAll('li[role="option"]')].some(l => (l.getAttribute('aria-label') || '').trim() === label)) break;
      }
    }
    const li = [...list().querySelectorAll('li[role="option"]')].find(l => (l.getAttribute('aria-label') || '').trim() === label);
    if (!li) throw new Error('station not in list: ' + label);
    li.click();
    for (let k = 0; k < 50; k++) {
      await this.sleep(100);
      if (this.txt(dd.querySelector('.rz-dropdown-label')) === label) return;
    }
    throw new Error('could not select ' + label);
  },
  stations() {
    const dd = document.querySelector('input[name="PocetnaStanicaDropDown"]').closest('.rz-dropdown');
    return [...document.getElementById(dd.getAttribute('aria-controls')).querySelectorAll('li[role="option"]')]
      .map(l => (l.getAttribute('aria-label') || '').trim()).filter(Boolean);
  },
  dates() {
    const out = new Set();
    document.querySelectorAll('td[id*="-day-"]').forEach(td => {
      if (td.getAttribute('aria-disabled') !== 'true') out.add(td.id.split('-day-')[1]);
    });
    return [...out].sort();
  },
  async search(a, b, d) {
    await this.pick('PocetnaStanicaDropDown', a);
    await this.pick('KrajnjaStanicaDropDown', b);
    const inp = document.querySelector('input[placeholder="Datum polaska"]');
    inp.focus(); inp.value = d;
    inp.dispatchEvent(new Event('input', {bubbles: true}));
    inp.dispatchEvent(new Event('change', {bubbles: true}));
    inp.blur(); await this.sleep(300);
    document.querySelectorAll('.rz-alert').forEach(x => x.setAttribute('data-old', '1'));
    [...document.querySelectorAll('button')].find(x => x.textContent.includes('Pretraži')).click();
    const t0 = Date.now();
    for (let k = 0; k < 60; k++) {
      await this.sleep(200);
      const dt = document.querySelector('.red-voznje-datum');
      const head = this.txt(document.querySelector('.red-voznje-result-smijer h5'));
      const alert = [...document.querySelectorAll('.rz-alert')].find(x => /ne postoji voz/.test(x.textContent));
      // "Za odabranu relaciju ne postoji voz": a new alert, or the old one still there after 1.5 s
      if (alert && !dt && (!alert.hasAttribute('data-old') || Date.now() - t0 > 1500)) return [];
      if (dt && this.txt(dt).includes(d) && head === a + b) {
        await this.sleep(300);
        return [...document.querySelectorAll('.accordion-item button[data-bs-target^="#voz_"]')].map((btn, i) => {
          const c = [...btn.querySelectorAll('.rv-accordion-header-col')].map(x => this.txt(x));
          return {i, train: btn.getAttribute('data-bs-target').slice(5), from: c[0], dep: c[1], to: c[2], arr: c[3]};
        });
      }
    }
    return null;   // no answer (no connection or the page did not react)
  },
  async expand(i) {
    const btn = [...document.querySelectorAll('.accordion-item button[data-bs-target^="#voz_"]')][i];
    const item = btn.closest('.accordion-item');
    const body = () => item.querySelector('.accordion-body');
    if (!body().querySelector('.red-voznje-voz-details')) btn.click();
    for (let k = 0; k < 50 && !body().querySelector('.red-voznje-voz-details'); k++) await this.sleep(200);
    await this.sleep(200);
    const legs = []; let leg = null;
    body().querySelectorAll('.red-voznje-voz-details, .rz-timeline-item').forEach(n => {
      if (n.classList.contains('red-voznje-voz-details')) {
        const t = this.txt(n);
        leg = {train: (t.match(/Broj voza\s*:\s*(\S+)/) || [])[1], type: ((t.match(/Vrsta voza\s*:\s*(.+)$/) || [])[1] || '').trim(), stops: []};
        legs.push(leg); return;
      }
      const c = n.querySelector('.rz-timeline-content-end');
      if (!c || !leg) return;
      leg.stops.push({name: this.txt(c).replace(/\d{1,2}:\d{2}/g, '').trim(), times: c.textContent.match(/\d{1,2}:\d{2}/g) || []});
    });
    return legs;
  }
};
true
"""


def norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


# ---------------------------------------------------------------- crawl

class Browser:
    def __init__(self):
        from playwright.sync_api import sync_playwright
        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch()
        self.page = None
        self.open()

    def open(self):
        if self.page:
            try:
                self.page.close()
            except Exception:
                pass
        self.page = self.browser.new_page(locale="sr-Latn", viewport={"width": 1280, "height": 1400})
        self.page.goto(URL, wait_until="networkidle", timeout=90000)
        self.page.wait_for_selector('input[name="PocetnaStanicaDropDown"]', state="attached", timeout=60000)
        time.sleep(1.5)
        self.page.evaluate(JS_HELPERS)

    def call(self, expr, *args, retries=2):
        for attempt in range(retries + 1):
            try:
                return self.page.evaluate(expr, list(args))
            except Exception as e:
                if attempt == retries:
                    raise
                print(f"  opakuji po chybě: {str(e)[:120]}", flush=True)
                self.open()

    def close(self):
        self.browser.close()
        self.pw.stop()


def crawl(cfg):
    b = Browser()
    stations = b.call("() => __zrs.stations()")
    dates = b.call("() => __zrs.dates()")
    if not stations or not dates:
        raise SystemExit("Stránka nevrátila seznam stanic nebo dnů.")
    print(f"{len(stations)} stanic, dny {dates[0]} … {dates[-1]}", flush=True)
    hubs = [h for h in cfg["hubs"] if h in stations]
    seen = {}            # row key -> legs ([] = skipped, nothing new)
    known = {}           # train -> station -> (arr, dep) as text
    obs = []             # {date, from, to, rows: [{key}], }
    n_search = n_expand = 0

    t_start = time.time()
    budget = cfg.get("max_minutes", 150) * 60
    state = {"planned": 0}

    def progress(force=False):
        if force or n_search % 25 == 0:
            el = time.time() - t_start
            left = max(state["planned"] - n_search, 0)
            eta = el / n_search * left if n_search else 0
            print(f"  [{el / 60:5.1f} min] hledání {n_search}/{state['planned']}, rozbaleno {n_expand}, "
                  f"vlaků {len({l['train'] for v in seen.values() for l in v})}, zbývá cca {eta / 60:.0f} min", flush=True)

    def save():
        ws = os.environ.get("GITHUB_WORKSPACE", ".")
        with open(os.path.join(ws, "zrs_observations.json"), "w", encoding="utf-8") as f:
            json.dump({"stations": stations, "dates": sorted({o["date"] for o in obs}), "seen": seen, "obs": obs},
                      f, ensure_ascii=False)

    def run(a, b_, d):
        nonlocal n_search, n_expand
        dd = datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m.%Y")
        rows = b.call("([a, b, d]) => __zrs.search(a, b, d)", a, b_, dd)
        n_search += 1
        if rows is None:
            return []
        out = []
        for r in rows:
            key = f"{r['train']}|{r['from']}|{r['dep']}|{r['to']}|{r['arr']}"
            if key not in seen:
                st = known.get(r["train"], {})
                if st.get(r["from"], (None, None))[1] == r["dep"] and st.get(r["to"], (None, None))[0] == r["arr"]:
                    seen[key] = []      # nothing new: same train, same times at both stations
                else:
                    seen[key] = b.call("([i]) => __zrs.expand(i)", r["i"])
                    n_expand += 1
                    for leg in seen[key]:
                        for s_ in leg["stops"]:
                            t = s_["times"]
                            if t:
                                k = known.setdefault(leg["train"], {})
                                a0, d0 = k.get(s_["name"], (None, None))
                                k[s_["name"]] = (a0 or t[0], d0 or t[-1])
            out.append(key)
        obs.append({"date": d, "from": a, "to": b_, "rows": out})
        progress()
        return out

    def extents():
        """(first, last) station of every train seen so far, from all its known segments."""
        ex = {}
        for legs in seen.values():
            for leg in legs:
                for n, a, d in leg_minutes(leg["stops"]):
                    a = a if a is not None else d
                    d = d if d is not None else a
                    lo, hi = ex.setdefault(leg["train"], [(n, a), (n, d)])
                    if a < lo[1]:
                        ex[leg["train"]][0] = (n, a)
                    if d > hi[1]:
                        ex[leg["train"]][1] = (n, d)
        return {(lo[0], hi[0]) for lo, hi in ex.values() if lo[0] != hi[0]}

    hub_pairs = [(x, y) for x in hubs for y in hubs if x != y]
    sweep_day = next((d for d in dates if date.fromisoformat(d).weekday() < 5), dates[0])
    if cfg.get("sweep", True):
        t0 = time.time()
        print(f"Fáze 1: hledání vlaků (den {sweep_day})", flush=True)
        state["planned"] = len(hub_pairs) + 6 * len(stations) + len(dates) * 40
        for a, b_ in hub_pairs:
            run(a, b_, sweep_day)
        # hubs on the same line as each station (from the trains found between hubs)
        line_hubs = {}
        for legs in seen.values():
            for leg in legs:
                names_ = [s_["name"] for s_ in leg["stops"]]
                hs = [h for h in hubs if h in names_]
                for n in names_:
                    line_hubs.setdefault(n, set()).update(hs)
        for s in stations:
            if s in hubs:
                continue
            for h in sorted(line_hubs.get(s) or hubs):
                run(s, h, sweep_day)
                run(h, s, sweep_day)
        print(f"Fáze 1 hotová: {n_search} hledání, {n_expand} rozbalení, {(time.time() - t0) / 60:.1f} min", flush=True)
        save()
    pairs = sorted((set(hub_pairs) | extents()) & {(x, y) for x in stations for y in stations})
    state["planned"] = n_search + len(pairs) * len(dates)
    print(f"Fáze 2: {len(dates)} dní × {len(pairs)} dvojic", flush=True)
    done_dates = []
    for d in dates:
        if time.time() - t_start > budget:
            print(f"Překročen časový rozpočet {budget / 60:.0f} min – zbylé dny vynechány", flush=True)
            break
        t0 = time.time()
        done = {(o["from"], o["to"]) for o in obs if o["date"] == d}
        for a, b_ in pairs:
            if (a, b_) not in done:
                run(a, b_, d)
        done_dates.append(d)
        save()
        print(f"  den {d} hotový za {(time.time() - t0) / 60:.1f} min", flush=True)
        progress(force=True)
    b.close()
    return {"stations": stations, "dates": done_dates, "seen": seen, "obs": [o for o in obs if o["date"] in done_dates],
            "incomplete": len(done_dates) < len(dates)}


# ---------------------------------------------------------------- build

def leg_minutes(stops):
    """[(name, arr, dep)] in minutes, growing through midnight; the first stop of a leg has
    no arrival and the last one no departure (None) - the leg says nothing about them."""
    out, last, day = [], None, 0
    for i, s in enumerate(stops):
        vals = []
        for t in ((s["times"][:1] + s["times"][-1:]) if s["times"] else []):
            v = tmin(t + ":00") + day
            if last is not None and v < last:
                day += 1440
                v += 1440
            last = v
            vals.append(v)
        if not vals:
            continue
        arr, dep = vals[0], vals[-1]
        if i == 0 and len(s["times"]) < 2:
            arr = None
        if i == len(stops) - 1 and len(s["times"]) < 2:
            dep = None
        out.append((s["name"], arr, dep))
    return out


def build(feed, cfg, data):
    warnings, report = [], []
    stops = read(feed, "stops.txt")
    coord = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in stops}
    names = {s["stop_id"]: s["stop_name"] for s in stops}
    by_name = {norm(s["stop_name"]): s["stop_id"] for s in stops}
    by_name.update({norm(k): v for k, v in cfg.get("aliases", {}).items()})

    def sid_of(n):
        return by_name.get(norm(n))

    seen, obs, window = data["seen"], data["obs"], data["dates"]
    if data.get("incomplete"):
        warnings.append(f"Procházení ŽRS nestihlo celé okno v časovém rozpočtu – zpracováno jen {len(window)} dní "
                        f"({window[0]} … {window[-1]}).")
    if not obs:
        raise SystemExit("Web nevrátil žádné výsledky – nic neměním.")

    # ---- per day and train: merged stop list
    runs = {}      # (date, train) -> {sid: (arr, dep)}
    types = {}
    unknown = {}
    for o in obs:
        for key in o["rows"]:
            for leg in seen.get(key, []):
                if not leg.get("train") or len(leg["stops"]) < 2:
                    continue
                types[leg["train"]] = leg.get("type", "")
                ms = leg_minutes(leg["stops"])
                cur = runs.setdefault((o["date"], leg["train"]), {})
                # align with what is already known for this train on this day
                shift = 0
                for n, a, d in ms:
                    s = sid_of(n)
                    if s in cur:
                        known_t = next(x for x in (cur[s][1], cur[s][0]) if x is not None)
                        shift = round((known_t - next(x for x in (d, a) if x is not None)) / 1440) * 1440
                        break
                for n, a, d in ms:
                    s = sid_of(n)
                    if not s or s not in coord:
                        unknown.setdefault(n, set()).add(leg["train"])
                        continue
                    old_a, old_d = cur.get(s, (None, None))
                    cur[s] = (old_a if old_a is not None else (a + shift if a is not None else None),
                              old_d if old_d is not None else (d + shift if d is not None else None))
    for n, tr in sorted(unknown.items()):
        warnings.append(f"Stanice „{n}“ (ŽRS) není ve stops.txt nebo nemá souřadnice – vynechána, doplňte ji "
                        f"nebo alias do zfbih/zrs.json. Staví v ní vlaky: {', '.join(sorted(tr))}")

    # ---- variants: days with identical times
    variants = {}  # train -> {signature: set(dates)}
    for (d, train), st in runs.items():
        st = {k: (a if a is not None else dp, dp if dp is not None else a) for k, (a, dp) in st.items()}
        if len(st) < 2:
            continue
        seq = sorted(st.items(), key=lambda x: (x[1][0], x[1][1]))
        base = (seq[0][1][1] // 1440) * 1440
        sig = tuple((s, a - base, dp - base) for s, (a, dp) in seq)
        variants.setdefault(train, {}).setdefault(sig, set()).add(d)

    today = date.today()
    w0, w1 = date.fromisoformat(window[0]), date.fromisoformat(window[-1])
    end = max(w1, today + timedelta(days=cfg.get("extend_days", 60)))
    hol = holiday_set(cfg, w0, end)

    def extend(days):
        ds = sorted(date.fromisoformat(x) for x in days)
        out = {x.strftime("%Y%m%d") for x in ds}
        if (w1 - ds[-1]).days > 6:          # not seen in the last week of the window: ended
            return out
        first = ds[0]
        mask = []
        for wd in range(7):
            occ = [first + timedelta(days=k) for k in range((w1 - first).days + 1)
                   if (first + timedelta(days=k)).weekday() == wd]
            on = sum(1 for x in occ if x in ds)
            mask.append(bool(occ) and on * 2 >= len(occ) and on > 0)
        workday_only = not mask[5] and not mask[6]
        d = w1 + timedelta(days=1)
        while d <= end:
            if mask[d.weekday()] and not (workday_only and d.isoformat() in hol):
                out.add(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return out

    old_trips = {t["trip_id"]: t for t in read(feed, "trips.txt")}
    old_routes = {r["route_id"]: r for r in read(feed, "routes.txt")}
    old_st = {}
    for r in read(feed, "stop_times.txt"):
        old_st.setdefault(r["trip_id"], []).append(r)
    old_cal = {c["service_id"]: c for c in read(feed, "calendar.txt")}
    old_cd = {}
    for c in read(feed, "calendar_dates.txt"):
        old_cd.setdefault(c["service_id"], []).append(c)
    shapes = {}
    for p in read(feed, "shapes.txt"):
        shapes.setdefault(p["shape_id"], []).append(p)
    shapes = {k: [(float(p["shape_pt_lat"]), float(p["shape_pt_lon"]))
                  for p in sorted(v, key=lambda p: int(p["shape_pt_sequence"]))] for k, v in shapes.items()}
    keep = {tid for tid, t in old_trips.items()
            if old_routes.get(t["route_id"], {}).get("agency_id") != AGENCY}

    services, trips, st_rows, routes = {}, [], [], []
    for train in sorted(variants, key=lambda t: min(s[0][2] for s in variants[t])):
        vs = sorted(variants[train].items(), key=lambda x: -len(x[1]))
        typ = TYPES.get(norm(types.get(train, "")), (types.get(train, "P")[:1] or "P").upper())
        for k, (sig, days) in enumerate(vs):
            tid = train if k == 0 else f"{train}_{k + 1}"
            dates_ = frozenset(extend(days))
            sid = services.get(dates_)
            if not sid:
                sid = f"R{len(services) + 1}"
                services[dates_] = sid
            first, last = sig[0][0], sig[-1][0]
            if not any(r["route_id"] == train for r in routes):
                routes.append({"route_id": train, "agency_id": AGENCY, "route_short_name": f"{typ} {train}",
                               "route_long_name": f"{names[first]} – {names[last]}", "route_type": "2"})
            seq = [s for s, _, _ in sig]
            cs = [coord[s] for s in seq]
            shape_id = None
            if tid in old_trips and old_trips[tid].get("shape_id") in shapes \
                    and fit_shape(shapes[old_trips[tid]["shape_id"]], cs):
                shape_id = old_trips[tid]["shape_id"]
            if not shape_id:
                fits = [(sh, pts, fit_shape(pts, cs)) for sh, pts in list(shapes.items())]
                fits = [f for f in fits if f[2]]
                whole = [f for f in fits if f[2][0] <= 2 and f[2][-1] >= len(f[1]) - 3]
                if whole:
                    shape_id = whole[0][0]
                elif fits:
                    sh, pts, idx = min(fits, key=lambda f: len(f[1]))
                    shape_id = f"{first}-{last}"
                    if shape_id not in shapes:
                        shapes[shape_id] = pts[idx[0]:idx[-1] + 1]
                        report.append(f"  shape {shape_id} vyříznut z {sh}")
            if not shape_id:
                shape_id = f"{first}-{last}"
                shapes[shape_id] = brouter(cs)
                report.append(f"  shape {shape_id} z brouter.de")
            rows_ = [(s, a, d) for s, a, d in sig]
            rows_[0] = (rows_[0][0], rows_[0][2], rows_[0][2])
            rows_[-1] = (rows_[-1][0], rows_[-1][1], rows_[-1][1])
            old = [(r["stop_id"], tmin(r["arrival_time"]), tmin(r["departure_time"]))
                   for r in sorted(old_st.get(tid, []), key=lambda r: int(r["stop_sequence"]))]
            if tid not in old_trips:
                report.append(f"Nový vlak {typ} {tid} {names[first]} – {names[last]} ({len(days)} dní v okně)")
            elif old != rows_:
                report.append(f"Změna jízdního řádu vlaku {tid} {names[first]} – {names[last]}")
            trips.append({"route_id": train, "service_id": sid, "trip_id": tid, "trip_headsign": names[last],
                          "shape_id": shape_id, "bikes_allowed": "1"})
            for n, (s, a, d) in enumerate(rows_, 1):
                st_rows.append({"trip_id": tid, "arrival_time": fmt(a), "departure_time": fmt(d), "stop_id": s,
                                "stop_sequence": str(n), "timepoint": "1"})
    new_ids = {t["trip_id"] for t in trips}
    for tid in old_trips:
        if tid not in keep and tid not in new_ids:
            report.append(f"Vlak {tid} (ŽRS) web nevrací – odstraněn")

    # ---- other agencies (ŽFBH) untouched
    keep_services = set()
    for tid in sorted(keep):
        trips.append(old_trips[tid])
        keep_services.add(old_trips[tid]["service_id"])
        st_rows += sorted(old_st.get(tid, []), key=lambda r: int(r["stop_sequence"]))
    rid = {t["route_id"] for t in trips}
    routes += [r for k, r in old_routes.items() if k in rid and r["agency_id"] != AGENCY]
    cal, cd = [], []
    for sid in sorted(keep_services):
        if sid in old_cal:
            cal.append(old_cal[sid])
            cd += old_cd.get(sid, [])
    for dates_, sid in sorted(services.items(), key=lambda x: int(x[1][1:])):
        c, e = calendar_rows(sid, dates_, w0, end)
        cal.append(c)
        cd += e

    fi = read(feed, "feed_info.txt")
    for f in fi:
        f["feed_end_date"] = max(f.get("feed_end_date", ""), end.strftime("%Y%m%d"))
        f["feed_version"] = today.strftime("%Y%m%d")
    cal_fields = ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                  "sunday", "start_date", "end_date"]
    write(feed, "routes.txt", ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"], routes)
    write(feed, "trips.txt", ["route_id", "service_id", "trip_id", "trip_headsign", "shape_id", "bikes_allowed"], trips)
    write(feed, "stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
                                   "timepoint"], st_rows)
    write(feed, "calendar.txt", cal_fields, cal)
    write(feed, "calendar_dates.txt", ["service_id", "date", "exception_type"], cd)
    write(feed, "feed_info.txt", list(fi[0].keys()), fi)
    write(feed, "shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
          [{"shape_id": k, "shape_pt_lat": f"{la:.6f}".rstrip("0").rstrip("."),
            "shape_pt_lon": f"{lo:.6f}".rstrip("0").rstrip("."), "shape_pt_sequence": str(n)}
           for k in shapes for n, (la, lo) in enumerate(shapes[k], 1)])
    files = ["agency.txt", "calendar.txt", "calendar_dates.txt", "feed_info.txt", "routes.txt",
             "shapes.txt", "stop_times.txt", "stops.txt", "trips.txt"]
    with zipfile.ZipFile(os.path.join(feed, "zfbih.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for n in files:
            z.write(os.path.join(feed, n), n)
    return report, warnings


def main():
    feed = sys.argv[1]
    ws = os.environ.get("GITHUB_WORKSPACE", ".")
    cfg = json.load(open(os.path.join(feed, "zrs.json"), encoding="utf-8"))
    if "--obs" in sys.argv:
        data = json.load(open(sys.argv[sys.argv.index("--obs") + 1], encoding="utf-8"))
    else:
        data = crawl(cfg)
        with open(os.path.join(ws, "zrs_observations.json"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    report, warnings = build(feed, cfg, data)
    text = "\n".join(report) or "Beze změn v jízdním řádu ŽRS."
    print(text)
    with open(os.path.join(ws, "zrs_report.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    if warnings:
        print("\n".join(warnings))
        with open(os.path.join(ws, "zrs_alert.txt"), "w", encoding="utf-8") as f:
            f.write("⚠️ Aktualizace ŽRS (zfbih) – co je potřeba zkontrolovat:\n\n"
                    + "\n".join(f"- {w}" for w in warnings) + "\n")


if __name__ == "__main__":
    main()
