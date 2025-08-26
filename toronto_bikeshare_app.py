import streamlit as st
from toronto_bikeshare_tools import *
from toronto_bikeshare_load_data import *

import datetime
import calendar

import plotly.express as px

import matplotlib.pyplot as plt

month_list = list(calendar.month_name)

def add_times(t0: datetime.time, t1: datetime.time):
    ''' assume that t0 and t1 are'''
    minutes = (t0.minute + t1.minute) % 60
    hours = t0.hour + t1.hour + (t0.minute + t1.minute) // 60
    if hours > 23:
        return datetime.time(23,59,59)
    return datetime.time(hours, minutes)

def geographic_plots_page():
    st.title('Visualizing City Bike Usage Geographically')
    with st.container(border=True):
        st.header('Select Data Range')
        years = st.multiselect(
            'Select the years to view data for',
            options = available_years,
            default = available_years
        )
        month = st.select_slider(
            'Select the month to view data for',
            options = month_list[1:]
        )
        days_of_week = st.multiselect(
            'Select the days of the week to view data for',
            options = range(7),
            default = range(7),
            format_func = lambda d: calendar.day_name[d]
        )

        time_control_option_columns = st.columns([1,3])

        fixed_window = time_control_option_columns[0].toggle(
            'Fixed time window',
            value=False
        )
        if fixed_window:
            window_length = time_control_option_columns[1].time_input(
                'time window length',
                value = datetime.time(23, 59),
                label_visibility='collapsed'
            )
            t = st.slider(
                'Select the time range you want to view',
                value=datetime.time(0,0)
            )
            t0 = t
            t1 = add_times(t, window_length)
        else:
            t = st.slider(
                'Select the time range you want to view',
                value=(datetime.time(0,0), datetime.time(23,59))
            )
            t0 = t[0]
            t1 = t[1]

    usage_data = prepare_usage_data(read_multiple_year_single_month_usage_data(month_list.index(month), years, 'parquet'))
    start_time_index = pd.DatetimeIndex(usage_data['Start Time'])

    stations_data_with_source_sink = get_stations_data_source_sink_days_time_range(usage_data, start_time_index, str(t0), str(t1), days_of_week)
    source_sink_difference = get_net_sources_sinks(stations_data_with_source_sink)
    source_sink_total = stations_data_with_source_sink['source counts']+stations_data_with_source_sink['sink counts']


    fig_diff = px.scatter_map(stations_data_with_source_sink, lat='lat', lon='lon', color=source_sink_difference, size=np.square(source_sink_difference), zoom=10, labels={'color': 'sources-sinks'}, range_color=(-1,1), color_continuous_scale='PuOr', title='sources - sinks normalized to total trips from each station', subtitle='+1 means that all trips were taken from this station and -1 means that all trips were taken to this station')
    fig_diff.update_layout(mapbox_style='open-street-map')

    fig_counts = px.scatter_map(stations_data_with_source_sink, lat='lat', lon='lon', color=source_sink_total, size=np.square(source_sink_total), zoom=10, labels={'color': 'total trips'}, color_continuous_scale='plasma', title='total trips from each station')
    fig_counts.update_layout(mapbox_style='open-street-map')

    col_left, col_right = st.columns(2)
    col_left.plotly_chart(fig_diff)
    col_right.plotly_chart(fig_counts)

def day_of_week_histogram_page():
    st.title('Visualizing City Bike Usage by Days of the Week')

    month = st.select_slider(
        'Select the month to view data for',
        options = month_list[1:]
    )

    usage_data = read_multiple_year_single_month_usage_data(month_list.index(month), available_years, 'parquet')
    start_time_index = pd.DatetimeIndex(usage_data['Start Time'])

    hists = []
    edges = []

    fig, axes = plt.subplots(2, 4, sharex=True, sharey=True)

    for day in range(7):
        h, e = np.histogram(usage_data['Start Time'][start_time_index.day_of_week == day].apply(lambda x: x.hour), bins=24)

        col = day % 4
        row = day // 4

        if col == 0:
            axes[row, col].set_ylabel('Trips Taken')
        if row == 1:
            axes[row, col].set_xlabel('Hour of Day')

        axes[row, col].set_title(calendar.day_name[day])
        axes[row, col].stairs(h, e, fill=True)

        hists.append(h)
        edges.append(e)

    for day in range(7):
        axes[1, 3].stairs(hists[day], edges[day], label=calendar.day_abbr[day])
    axes[1, 3].set_xlabel('Hour of Day')
    axes[1, 3].legend(loc=0, fontsize='xx-small')

    st.pyplot(fig)

pg = st.navigation([st.Page(geographic_plots_page, title='Visualizing Usage Geographically'),
                    st.Page(day_of_week_histogram_page, title='Visualizing Usage by Day')])
pg.run()