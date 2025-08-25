from toronto_bikeshare_tools import prepare_usage_data, read_multiple_usage_data
import pandas as pd

'''
first_year = 2023
last_year = 2024

first_month = 1
last_month = 9
'''

first_year = 2024
last_year = 2024

first_month = 1
last_month = 9

available_years = [2023, 2024]

#usage_data = prepare_usage_data(read_multiple_usage_data(first_month, first_year, last_month, last_year))
usage_data = pd.read_parquet('data/usage_data.parquet')
start_time_index = pd.DatetimeIndex(usage_data['Start Time'])
end_time_index = pd.DatetimeIndex(usage_data['End Time'])