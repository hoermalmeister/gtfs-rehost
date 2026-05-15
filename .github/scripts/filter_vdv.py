import pandas as pd
import os

def filter_gtfs():
    print("Začínám filtrovat data Spojenka...")

    # 1. Zastávky (Stops) - ponechat pouze ty, které mají 'V' v zone_id
    stops = pd.read_csv('stops.txt', dtype=str)
    # Vyplníme prázdné hodnoty, aby to nespadlo, a hledáme 'V'
    stops = stops[stops['zone_id'].fillna('').str.contains('V')]
    valid_stops = set(stops['stop_id'])
    stops.to_csv('stops.txt', index=False)
    print(f"Ponecháno {len(valid_stops)} zastávek s písmenem V v zone_id.")

    # 2. Časy zastávek (Stop_times) - pouze pro platné zastávky
    stop_times = pd.read_csv('stop_times.txt', dtype=str)
    stop_times = stop_times[stop_times['stop_id'].isin(valid_stops)]
    
    # DŮLEŽITÉ: Jízda (trip) musí mít po filtraci alespoň 2 zastávky, jinak nedává smysl
    trip_counts = stop_times['trip_id'].value_counts()
    valid_trips = set(trip_counts[trip_counts >= 2].index)
    
    # Odstraníme stop_times u jízd, které už nemají dostatek zastávek
    stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
    stop_times.to_csv('stop_times.txt', index=False)

    # 3. Jízdy (Trips)
    trips = pd.read_csv('trips.txt', dtype=str)
    trips = trips[trips['trip_id'].isin(valid_trips)]
    valid_routes = set(trips['route_id'])
    valid_services = set(trips['service_id'].dropna())
    valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
    trips.to_csv('trips.txt', index=False)

    # 4. Trasy (Routes)
    routes = pd.read_csv('routes.txt', dtype=str)
    routes = routes[routes['route_id'].isin(valid_routes)]
    valid_agencies = set(routes['agency_id'].dropna()) if 'agency_id' in routes.columns else set()
    routes.to_csv('routes.txt', index=False)

    # 5. Dopravci (Agency)
    if os.path.exists('agency.txt') and valid_agencies:
        agency = pd.read_csv('agency.txt', dtype=str)
        agency = agency[agency['agency_id'].isin(valid_agencies)]
        agency.to_csv('agency.txt', index=False)

    # 6. Kalendář (Calendar & Calendar_dates)
    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        cal = cal[cal['service_id'].isin(valid_services)]
        cal.to_csv('calendar.txt', index=False)
        
    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        cal_d = cal_d[cal_d['service_id'].isin(valid_services)]
        cal_d.to_csv('calendar_dates.txt', index=False)

    # 7. Tvary tras (Shapes) - odstranění osiřelých tvarů, pokud existují
    if os.path.exists('shapes.txt'):
        shapes = pd.read_csv('shapes.txt', dtype=str)
        shapes = shapes[shapes['shape_id'].isin(valid_shapes)]
        shapes.to_csv('shapes.txt', index=False)

    # 8. Přestupy (Transfers)
    if os.path.exists('transfers.txt'):
        transfers = pd.read_csv('transfers.txt', dtype=str)
        transfers = transfers[transfers['from_stop_id'].isin(valid_stops) & transfers['to_stop_id'].isin(valid_stops)]
        transfers.to_csv('transfers.txt', index=False)

    # 9. Smazání nepotřebných souborů
    files_to_drop = ['pathways.txt', 'levels.txt']
    for file_name in files_to_drop:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Smazán soubor {file_name}")

if __name__ == "__main__":
    filter_gtfs()
