import pandas as pd
import os
import shutil
import glob
import zipfile
from datetime import datetime, timedelta

def manage_backups(current_zip):
    backup_dir = 'backup'
    os.makedirs(backup_dir, exist_ok=True)
    
    # 1. RETENCE: Smazání záloh VDV starších než 120 dní (cca 4 měsíce)
    for b in glob.glob(os.path.join(backup_dir, 'vdv_*.zip')):
        date_str = os.path.basename(b).replace('vdv_', '').replace('.zip', '')
        try:
            b_date = datetime.strptime(date_str, '%Y-%m-%d')
            if datetime.now() - b_date > timedelta(days=120):
                os.remove(b)
                print(f"Smazána stará záloha: {b}")
        except ValueError:
            continue

    # 2. ZÁLOHOVÁNÍ: Pokud uplynulo 30 dní od poslední zálohy, vytvoř novou
    backups = sorted(glob.glob(os.path.join(backup_dir, 'vdv_*.zip')))
    needs_backup = True
    
    if backups:
        last_date_str = os.path.basename(backups[-1]).replace('vdv_', '').replace('.zip', '')
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
            if datetime.now() - last_date < timedelta(days=30):
                needs_backup = False
        except ValueError:
            pass
            
    if needs_backup:
        dest = os.path.join(backup_dir, f"vdv_{datetime.now().strftime('%Y-%m-%d')}.zip")
        shutil.copy(current_zip, dest)
        print(f"Vytvořena nová záloha: {dest}")

def filter_gtfs():
    print("Spouštím filtraci dat VDV...")

    # --- FILTRACE ---
    # 1. Routes (bez vlaků)
    routes = pd.read_csv('routes.txt', dtype=str)
    train_types = ['2'] + [str(i) for i in range(100, 200)]
    routes = routes[~routes['route_type'].isin(train_types)]
    valid_routes = set(routes['route_id'])

    # 2. Stops (jen zona V)
    stops = pd.read_csv('stops.txt', dtype=str)
    stops = stops[stops['zone_id'].fillna('').str.contains('V')]
    valid_stops = set(stops['stop_id'])

    # 3. Stop_times (filtrace podle zastávek)
    if os.path.exists('stop_times.txt'):
        stop_times = pd.read_csv('stop_times.txt', dtype=str)
        stop_times = stop_times[stop_times['stop_id'].isin(valid_stops)]
        valid_trips = set(stop_times['trip_id'].value_counts()[stop_times['trip_id'].value_counts() >= 2].index)
        stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
    else:
        valid_trips = set()
        stop_times = pd.DataFrame()

    # 4. Trips (filtrace podle platných zastávek a tras)
    if os.path.exists('trips.txt'):
        trips = pd.read_csv('trips.txt', dtype=str)
        trips = trips[trips['trip_id'].isin(valid_trips) & trips['route_id'].isin(valid_routes)]
        valid_services = set(trips['service_id'].dropna())
        valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
    else:
        trips = pd.DataFrame()
        valid_services = set()
        valid_shapes = set()

    # Zápis filtrovaných základních souborů
    if not routes.empty:
        routes = routes[routes['route_id'].isin(trips['route_id'])]
        routes.to_csv('routes.txt', index=False)
    if not trips.empty:
        trips.to_csv('trips.txt', index=False)
    if not stop_times.empty:
        stop_times.to_csv('stop_times.txt', index=False)
    if not stops.empty:
        stops = stops[stops['stop_id'].isin(stop_times['stop_id'])]
        stops.to_csv('stops.txt', index=False)

    # Ostatní soubory (Agency, Calendar, Shapes, Transfers)
    if os.path.exists('agency.txt') and not routes.empty:
        agency = pd.read_csv('agency.txt', dtype=str)
        agency = agency[agency['agency_id'].isin(routes['agency_id'].dropna())]
        agency.to_csv('agency.txt', index=False)
    if os.path.exists('calendar.txt'):
        pd.read_csv('calendar.txt', dtype=str).query("service_id in @valid_services").to_csv('calendar.txt', index=False)
    if os.path.exists('calendar_dates.txt'):
        pd.read_csv('calendar_dates.txt', dtype=str).query("service_id in @valid_services").to_csv('calendar_dates.txt', index=False)
    if os.path.exists('shapes.txt'):
        pd.read_csv('shapes.txt', dtype=str).query("shape_id in @valid_shapes").to_csv('shapes.txt', index=False)
    if os.path.exists('transfers.txt'):
        pd.read_csv('transfers.txt', dtype=str).query("from_stop_id in @valid_stops and to_stop_id in @valid_stops").to_csv('transfers.txt', index=False)

    # Smazání nepotřebných
    for f in ['pathways.txt', 'levels.txt']:
        if os.path.exists(f): os.remove(f)

    # --- ZIPOVÁNÍ ---
    # Soubor nazveme vdv-gtfs.zip, aby byl jednoznačný
    zip_name = 'vdv-gtfs.zip'
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as z:
        for f in glob.glob('*.txt'):
            z.write(f)
            # Volitelně můžeme rovnou textové soubory mazat, aby nám nezabíraly místo
            os.remove(f)
    
    # --- ZÁLOHOVÁNÍ ---
    manage_backups(zip_name)
    print("Filtrace a zálohování dokončeno.")

if __name__ == "__main__":
    filter_gtfs()
