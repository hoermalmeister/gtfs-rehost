import pandas as pd
import os

def filter_gtfs():
    # 1. Filter routes: Keep standard rail (2) and extended rail types (100-199)
    routes = pd.read_csv('routes.txt', dtype=str)
    valid_types = ['2'] + [str(i) for i in range(100, 200)]
    routes = routes[routes['route_type'].isin(valid_types)]
    valid_routes = set(routes['route_id'])
    routes.to_csv('routes.txt', index=False)

    # 2. Filter trips based on valid routes
    if os.path.exists('trips.txt'):
        trips = pd.read_csv('trips.txt', dtype=str)
        trips = trips[trips['route_id'].isin(valid_routes)]
        valid_trips = set(trips['trip_id'])
        valid_services = set(trips['service_id'].dropna())
        valid_shapes = set(trips['shape_id'].dropna()) if 'shape_id' in trips.columns else set()
        trips.to_csv('trips.txt', index=False)

    # 3. Filter stop_times based on valid trips
    if os.path.exists('stop_times.txt'):
        stop_times = pd.read_csv('stop_times.txt', dtype=str)
        stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
        valid_stops = set(stop_times['stop_id'])
        stop_times.to_csv('stop_times.txt', index=False)

    # 4. Filter stops based on valid stop_times (including parent stations)
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

if __name__ == "__main__":
    filter_gtfs()
