import pandas as pd
import os
from datetime import datetime

def filter_gtfs():
    # Get today's date in GTFS format (YYYYMMDD)
    today = datetime.now().strftime('%Y%m%d')

    # 1. Process Routes (Load, Filter by Type)
    routes = pd.read_csv('routes.txt', dtype=str)
    valid_types = ['2'] + [str(i) for i in range(100, 200)]
    routes = routes[routes['route_type'].isin(valid_types)]

    # --- UPDATED: APPLY RELATIONS MAPPING ---
    # Because you put it in the czrail folder, it's right here in the current directory!
    if os.path.exists('relations.txt'):
        relations = pd.read_csv('relations.txt', dtype=str)
        relations = relations.rename(columns={'route_short_name': 'mapped_short_name'})
        
        if 'agency_id' in routes.columns:
            routes = routes.merge(relations, on=['agency_id', 'route_long_name'], how='left')
            mask = routes['mapped_short_name'].notna()
            routes.loc[mask, 'route_short_name'] = routes.loc[mask, 'mapped_short_name']
            routes = routes.drop(columns=['mapped_short_name'])
            print("Successfully applied relations.txt mapping from local folder.")
    else:
        print("relations.txt not found in current folder, skipping name mapping.")
    # ------------------------------------

    # 2. Unify Identical Routes
    routes['route_short_name'] = routes['route_short_name'].fillna('')
    routes['route_long_name'] = routes['route_long_name'].fillna('')
    
    group_cols = ['route_short_name', 'route_long_name']
    if 'agency_id' in routes.columns:
        group_cols = ['agency_id'] + group_cols

    routes['unified_route_id'] = routes.groupby(group_cols)['route_id'].transform('first')
    route_mapping = dict(zip(routes['route_id'], routes['unified_route_id']))
    
    routes['route_id'] = routes['unified_route_id']
    routes = routes.drop(columns=['unified_route_id']).drop_duplicates(subset=['route_id'])
    
    valid_routes_initial = set(routes['route_id'])

    # 3. Determine Future Valid Services (Services that operate today or in the future)
    future_valid_services = set()
    
    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        valid_cal = cal[cal['end_date'] >= today]
        future_valid_services.update(valid_cal['service_id'])

    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        valid_cal_d = cal_d[(cal_d['date'] >= today) & (cal_d['exception_type'] == '1')]
        future_valid_services.update(valid_cal_d['service_id'])

    # 4. Filter Trips
    valid_trips = set()
    valid_services_final = set()
    valid_shapes = set()
    valid_routes_final = set()
    
    if os.path.exists('trips.txt'):
        trips = pd.read_csv('trips.txt', dtype=str)
        trips['route_id'] = trips['route_id'].map(route_mapping).fillna(trips['route_id'])
        
        trips = trips[
            trips['route_id'].isin(valid_routes_initial) & 
            trips['service_id'].isin(future_valid_services)
        ]
        
        valid_trips = set(trips['trip_id'])
        valid_services_final = set(trips['service_id'].dropna())
        valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
        valid_routes_final = set(trips['route_id'].dropna())
        
        trips.to_csv('trips.txt', index=False)

    # 5. Save Final Filtered Routes
    routes = routes[routes['route_id'].isin(valid_routes_final)]
    routes.to_csv('routes.txt', index=False)

    # 6. Filter stop_times
    valid_stops = set()
    if os.path.exists('stop_times.txt'):
        stop_times = pd.read_csv('stop_times.txt', dtype=str)
        stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
        valid_stops = set(stop_times['stop_id'])
        stop_times.to_csv('stop_times.txt', index=False)

    # 7. Filter stops (including parent stations)
    stops_to_keep = valid_stops
    if os.path.exists('stops.txt'):
        all_stops = pd.read_csv('stops.txt', dtype=str)
        direct_stops = all_stops[all_stops['stop_id'].isin(valid_stops)]
        
        parent_stations = set()
        if 'parent_station' in direct_stops.columns:
            parent_stations = set(direct_stops['parent_station'].dropna())
            
        stops_to_keep = valid_stops.union(parent_stations)
        final_stops = all_stops[all_stops['stop_id'].isin(stops_to_keep)]
        final_stops.to_csv('stops.txt', index=False)

    # 8. Filter calendar and calendar_dates
    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        cal = cal[cal['service_id'].isin(valid_services_final)]
        cal.to_csv('calendar.txt', index=False)
    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        cal_d = cal_d[cal_d['service_id'].isin(valid_services_final)]
        cal_d.to_csv('calendar_dates.txt', index=False)

    # 9. Filter shapes
    if os.path.exists('shapes.txt'):
        shapes = pd.read_csv('shapes.txt', dtype=str)
        shapes = shapes[shapes['shape_id'].isin(valid_shapes)]
        shapes.to_csv('shapes.txt', index=False)
        
    # 10. Filter agency
    if os.path.exists('agency.txt') and 'agency_id' in routes.columns:
        valid_agencies = set(routes['agency_id'].dropna())
        if valid_agencies:
            agency = pd.read_csv('agency.txt', dtype=str)
            agency = agency[agency['agency_id'].isin(valid_agencies)]
            agency.to_csv('agency.txt', index=False)

    # 11. Filter transfers (train-to-train only)
    if os.path.exists('transfers.txt'):
        transfers = pd.read_csv('transfers.txt', dtype=str)
        transfers = transfers[transfers['from_stop_id'].isin(stops_to_keep) & transfers['to_stop_id'].isin(stops_to_keep)]
        transfers.to_csv('transfers.txt', index=False)

    # 12. Drop unwanted files entirely
    files_to_drop = ['pathways.txt', 'levels.txt']
    for file_name in files_to_drop:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Dropped {file_name}")

if __name__ == "__main__":
    filter_gtfs()
