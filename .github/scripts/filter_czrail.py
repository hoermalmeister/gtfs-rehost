import pandas as pd
import os

def filter_gtfs():
    # 1. Filter routes: Keep standard rail (2) and extended rail types (100-199)
    routes = pd.read_csv('routes.txt', dtype=str)
    valid_types = ['2'] + [str(i) for i in range(100, 200)]
    routes = routes[routes['route_type'].isin(valid_types)]

    # --- NEW: MERGE IDENTICAL ROUTES ---
    # Fill empty names with blanks so Pandas can group them properly
    routes['route_short_name'] = routes['route_short_name'].fillna('')
    routes['route_long_name'] = routes['route_long_name'].fillna('')
    
    # We group by agency, short name, and long name to ensure accuracy
    group_cols = ['route_short_name', 'route_long_name']
    if 'agency_id' in routes.columns:
        group_cols = ['agency_id'] + group_cols

    # Assign the first route_id of each group to all members of that group
    routes['unified_route_id'] = routes.groupby(group_cols)['route_id'].transform('first')
    
    # Create a dictionary to map the old route_ids to our new unified ones
    route_mapping = dict(zip(routes['route_id'], routes['unified_route_id']))
    
    # Update the route_id column and remove the redundant route rows
    routes['route_id'] = routes['unified_route_id']
    routes = routes.drop(columns=['unified_route_id']).drop_duplicates(subset=['route_id'])
    
    valid_routes = set(routes['route_id'])
    routes.to_csv('routes.txt', index=False)

    # 2. Filter trips based on valid routes AND update their route_ids
    valid_trips = set()
    valid_services = set()
    valid_shapes = set()
    if os.path.exists('trips.txt'):
        trips = pd.read_csv('trips.txt', dtype=str)
        
        # --- NEW: Apply the dictionary mapping to update the trips ---
        # If a trip's route_id is in our mapping, it gets updated to the unified ID.
        trips['route_id'] = trips['route_id'].map(route_mapping).fillna(trips['route_id'])
        
        # Keep only the trips that belong to our valid rail routes
        trips = trips[trips['route_id'].isin(valid_routes)]
        
        valid_trips = set(trips['trip_id'])
        valid_services = set(trips['service_id'].dropna())
        valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
        trips.to_csv('trips.txt', index=False)

    # 3. Filter stop_times based on valid trips
    valid_stops = set()
    if os.path.exists('stop_times.txt'):
        stop_times = pd.read_csv('stop_times.txt', dtype=str)
        stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
        valid_stops = set(stop_times['stop_id'])
        stop_times.to_csv('stop_times.txt', index=False)

    # 4. Filter stops based on valid stop_times (including parent stations)
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

    # 5. Filter calendar and calendar_dates
    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        cal = cal[cal['service_id'].isin(valid_services)]
        cal.to_csv('calendar.txt', index=False)
    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        cal_d = cal_d[cal_d['service_id'].isin(valid_services)]
        cal_d.to_csv('calendar_dates.txt', index=False)

    # 6. Filter shapes
    if os.path.exists('shapes.txt'):
        shapes = pd.read_csv('shapes.txt', dtype=str)
        shapes = shapes[shapes['shape_id'].isin(valid_shapes)]
        shapes.to_csv('shapes.txt', index=False)
        
    # 7. Filter agency
    if os.path.exists('agency.txt') and 'agency_id' in routes.columns:
        valid_agencies = set(routes['agency_id'].dropna())
        if valid_agencies:
            agency = pd.read_csv('agency.txt', dtype=str)
            agency = agency[agency['agency_id'].isin(valid_agencies)]
            agency.to_csv('agency.txt', index=False)

    # 8. Filter transfers (train-to-train only)
    if os.path.exists('transfers.txt'):
        transfers = pd.read_csv('transfers.txt', dtype=str)
        transfers = transfers[transfers['from_stop_id'].isin(stops_to_keep) & transfers['to_stop_id'].isin(stops_to_keep)]
        transfers.to_csv('transfers.txt', index=False)

    # 9. Drop unwanted files entirely
    files_to_drop = ['pathways.txt', 'levels.txt']
    for file_name in files_to_drop:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Dropped {file_name}")

if __name__ == "__main__":
    filter_gtfs()
