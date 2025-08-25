import pandas as pd
import numpy as np

import os

min_trip_duration = 60
max_trip_duration = 8*3600

stations_data = pd.DataFrame(pd.read_json('data/station_information.json')['data']['stations'])

stations_data['station_id'] = pd.to_numeric(stations_data['station_id'], downcast='unsigned')
stations_data['physical_configuration'] = stations_data['physical_configuration'].astype('category')
stations_data['lat'] = pd.to_numeric(stations_data['lat'], downcast='float')
stations_data['lon'] = pd.to_numeric(stations_data['lon'], downcast='float')
stations_data['altitude'] = pd.to_numeric(stations_data['altitude'], downcast='float')
stations_data['capacity'] = pd.to_numeric(stations_data['capacity'], downcast='unsigned')
stations_data['nearby_distance'] = pd.to_numeric(stations_data['nearby_distance'], downcast='float')

stations_data.drop(columns='is_valet_station', inplace=True)

stations_data.set_index('station_id', inplace=True)

minimal_stations_data = stations_data.drop(columns=['name', 'physical_configuration', 'address', 'is_charging_station', 'rental_methods', 'groups', 'obcn', 'short_name', 'nearby_distance', '_ride_code_support', 'rental_uris', 'post_code', 'cross_street'])


def read_usage_data(path):
    df = pd.read_csv(path, encoding='ISO-8859-1', parse_dates=['Start Time', 'End Time'], index_col=0)

    # sometimes there's an issue with the encoding so we have to make sure that the index column is labelled correctly
    df.index.name = 'Trip Id'

    # check if Model is there
    if 'Model' not in df.columns:
        df['Model'] = 'NULL'

    df.drop(columns=['Start Station Name', 'End Station Name'], inplace=True)

    # we can cut the size of the data frame in half by downcasting the types here
    df['Trip  Duration'] = pd.to_numeric(df['Trip  Duration'], downcast='unsigned')
    df['Start Station Id'] = pd.to_numeric(df['Start Station Id'], downcast='unsigned')
    df['End Station Id'] = pd.to_numeric(df['End Station Id'], downcast='unsigned')
    df['Bike Id'] = pd.to_numeric(df['Bike Id'], downcast='unsigned')
    df['User Type'] = df['User Type'].astype('category')
    df['Model'] = df['Model'].astype('category')
    
    return df

def usage_data_path(month, year):
    if type(month) is not int:
        raise TypeError('month must be an int, given {}'.format(type(month)))
    if type(year) is not int:
        raise TypeError('year must be an int, given {}'.format(type(year)))
    
    if month < 10:
        return os.path.join(f'data/bikeshare-ridership-{year}', f'Bike share ridership {year}-0{month}.csv')
    else:
        return os.path.join(f'data/bikeshare-ridership-{year}', f'Bike share ridership {year}-{month}.csv')

offset_mod = lambda a,b,n: int(a - b*np.floor_divide(a-n,b))

def read_multiple_usage_data(start_month, start_year, end_month, end_year):
    data_frames = []

    for i in range(end_month-start_month+1 + 12*(end_year-start_year)):
        month = offset_mod(start_month + i, 12, 1)
        year = start_year + (start_month + i - 1)//12

        try:
            data_frames.append(read_usage_data(usage_data_path(month, year)))
        except:
            print(f'something went wrong at {month} {year}')

    return pd.concat(data_frames)

def prepare_usage_data(raw_usage_data):
    usage_data = raw_usage_data[(raw_usage_data['Trip  Duration'] >= min_trip_duration) & (raw_usage_data['Trip  Duration'] <= max_trip_duration)]
    usage_data = usage_data[usage_data['Start Station Id'].isin(stations_data.index) & usage_data['End Station Id'].isin(stations_data.index)]

    # sometimes this doesn't copy over correctly from raw_usage_data, so I'll just redo it here
    usage_data['Trip  Duration'] = pd.to_numeric(usage_data['Trip  Duration'], downcast='unsigned')
    usage_data['Start Station Id'] = pd.to_numeric(usage_data['Start Station Id'], downcast='unsigned')
    usage_data['End Station Id'] = pd.to_numeric(usage_data['End Station Id'], downcast='unsigned')
    usage_data['Bike Id'] = pd.to_numeric(usage_data['Bike Id'], downcast='unsigned')
    usage_data['User Type'] = usage_data['User Type'].astype('category')
    usage_data['Model'] = usage_data['Model'].astype('category')

    return usage_data



def data_between_time(data, time_index, start, end):
    return data.iloc[time_index.indexer_between_time(start, end)]

def data_between_dates(data, start_date, end_date):
    return data[(data['Start Time'] >= start_date) & (data['Start Time'] <= end_date)]

def data_on_days(data, days):
    return data[data['Start Time'].apply(lambda d: d.day_of_week).isin(days)]

def data_in_months(data, months):
    return data[data['Start Time'].apply(lambda d: d.month).isin(months)]

def data_in_years(data, years):
    return data[data['Start Time'].apply(lambda d: d.year).isin(years)]

def get_sources_sinks_date_time_range(data, time_index, start_date, end_date, start_time, end_time):
    this_data = data_between_time(data, time_index, start_time, end_time)
    this_data = data_between_dates(this_data, start_date, end_date)
    
    sources = this_data.value_counts('Start Station Id')
    sinks = this_data.value_counts('End Station Id')
    
    sources.name = 'source counts'
    sinks.name = 'sink counts'

    combined_df = pd.DataFrame({'source counts': sources, 'sink counts': sinks})
    combined_df.fillna(0, inplace=True)

    return combined_df

def get_sources_sinks_days_months_years_time_range(data, time_index, start_time, end_time, days, months, years):
    this_data = data_between_time(data, time_index, start_time, end_time)
    this_data = data_in_years(this_data, years)
    this_data = data_in_months(this_data, months)
    this_data = data_on_days(this_data, days)
    
    sources = this_data.value_counts('Start Station Id')
    sinks = this_data.value_counts('End Station Id')
    
    sources.name = 'source counts'
    sinks.name = 'sink counts'

    combined_df = pd.DataFrame({'source counts': sources, 'sink counts': sinks})
    combined_df.fillna(0, inplace=True)

    return combined_df

def get_stations_data_source_sink_date_time_range(data, time_index, start_date, end_date, start_time, end_time):
    sources_sinks = get_sources_sinks_date_time_range(data, time_index, start_date, end_date, start_time, end_time)

    augmented_stations_data = sources_sinks.join(minimal_stations_data)

    return augmented_stations_data

def get_stations_data_source_sink_days_months_years_time_range(data, time_index, start_time, end_time, days, months, years):
    sources_sinks = get_sources_sinks_days_months_years_time_range(data, time_index, start_time, end_time, days, months, years)

    augmented_stations_data = sources_sinks.join(minimal_stations_data)

    return augmented_stations_data

def get_net_sources_sinks(data_with_sources_sinks):
    return (data_with_sources_sinks['source counts']-data_with_sources_sinks['sink counts'])/(data_with_sources_sinks['source counts']+data_with_sources_sinks['sink counts'])