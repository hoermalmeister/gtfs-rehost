import pandas as pd
import os

def filter_gtfs():
    print("Začínám filtrovat data Spojenka")

    # 1. Odstranění vlaků z Routes
    routes = pd.read_csv('routes.txt', dtype=str)
    # Vlaky mají route_type 2 (standard) nebo 100-199 (rozšířený standard)
    train_types = ['2'] + [str(i) for i in range(100, 200)]
    # Značka ~ (tilda) znamená "NEOBSAHUJE" -> ponecháme jen to, co NENÍ vlak
    routes = routes[~routes['route_type'].isin(train_types)]
    valid_routes_by_type = set(routes['route_id'])
    print(f"Ponecháno {len(valid_routes_by_type)} nevlakových tras.")

    # 2. Filtrace zastávek podle zóny V
    stops = pd.read_csv('stops.txt', dtype=str)
    stops = stops[stops['zone_id'].fillna('').str.contains('V')]
    valid_stops_initial = set(stops['stop_id'])

    # 3. Filtrace stop_times podle platných zastávek
    if os.path.exists('stop_times.txt'):
        stop_times = pd.read_csv('stop_times.txt', dtype=str)
        stop_times = stop_times[stop_times['stop_id'].isin(valid_stops_initial)]
        
        # Jízda musí mít po filtraci alespoň 2 zastávky
        trip_counts = stop_times['trip_id'].value_counts()
        valid_trips_by_stops = set(trip_counts[trip_counts >= 2].index)
    else:
        valid_trips_by_stops = set()

    # 4. Průnik v Trips (musí mít platnou nevlakovou trasu A ZÁROVEŇ platné zastávky)
    trips = pd.read_csv('trips.txt', dtype=str)
    trips = trips[trips['trip_id'].isin(valid_trips_by_stops) & trips['route_id'].isin(valid_routes_by_type)]
    
    final_valid_trips = set(trips['trip_id'])
    final_valid_routes = set(trips['route_id'])
    final_valid_services = set(trips['service_id'].dropna())
    final_valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
    
    trips.to_csv('trips.txt', index=False)

    # 5. Zpětné dočištění stop_times, stops a routes (odstranění absolutně všech sirotků)
    
    # Dočistíme stop_times (vyhodíme časy pro vlakové jízdy)
    if os.path.exists('stop_times.txt'):
        stop_times = stop_times[stop_times['trip_id'].isin(final_valid_trips)]
        final_valid_stops = set(stop_times['stop_id'])
        stop_times.to_csv('stop_times.txt', index=False)
    else:
        final_valid_stops = set()

    # Dočistíme zastávky (vyhodíme ty s písmenem V, kde ale jezdil jen a pouze vlak)
    stops = stops[stops['stop_id'].isin(final_valid_stops)]
    stops.to_csv('stops.txt', index=False)

    # Dočistíme trasy (vyhodíme nevlakové trasy, kterým po filtraci zóny V nezbyly žádné jízdy)
    routes = routes[routes['route_id'].isin(final_valid_routes)]
    final_valid_agencies = set(routes['agency_id'].dropna()) if 'agency_id' in routes.columns else set()
    routes.to_csv('routes.txt', index=False)

    # 6. Dočištění ostatních navázaných souborů
    if os.path.exists('agency.txt') and final_valid_agencies:
        agency = pd.read_csv('agency.txt', dtype=str)
        agency = agency[agency['agency_id'].isin(final_valid_agencies)]
        agency.to_csv('agency.txt', index=False)

    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        cal = cal[cal['service_id'].isin(final_valid_services)]
        cal.to_csv('calendar.txt', index=False)
        
    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        cal_d = cal_d[cal_d['service_id'].isin(final_valid_services)]
        cal_d.to_csv('calendar_dates.txt', index=False)

    if os.path.exists('shapes.txt'):
        shapes = pd.read_csv('shapes.txt', dtype=str)
        shapes = shapes[shapes['shape_id'].isin(final_valid_shapes)]
        shapes.to_csv('shapes.txt', index=False)

    if os.path.exists('transfers.txt'):
        transfers = pd.read_csv('transfers.txt', dtype=str)
        transfers = transfers[transfers['from_stop_id'].isin(final_valid_stops) & transfers['to_stop_id'].isin(final_valid_stops)]
        transfers.to_csv('transfers.txt', index=False)

    # 7. Smazání nepotřebných souborů
    files_to_drop = ['pathways.txt', 'levels.txt']
    for file_name in files_to_drop:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Smazán soubor {file_name}")
            
    print("Filtrace úspěšně dokončena.")

if __name__ == "__main__":
    filter_gtfs()
