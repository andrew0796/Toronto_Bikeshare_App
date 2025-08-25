import streamlit as st
from toronto_bikeshare_tools import *
from toronto_bikeshare_load_data import *

import datetime
import calendar

import plotly.express as px

import matplotlib.pyplot as plt

def geographic_plots_page():
    st.title('Visualizing City Bike Usage Geographically')
    with st.container(border=True):
        st.header('Select Data Range')
        years = st.multiselect(
            'Select the years to view data for',
            options = available_years,
            default = available_years
        )
        months = st.multiselect(
            'Select the months to view data for',
            options = range(1, 13),
            default = range(1, 13),
            format_func=lambda m: calendar.month_name[m]
        )
        days_of_week = st.multiselect(
            'Select the days of the week to view data for',
            options = range(7),
            default = range(7),
            format_func = lambda d: calendar.day_name[d]
        )
        t = st.slider(
            'Select the time range you want to view',
            value=(datetime.time(0,0), datetime.time(23,59))
        )

    stations_data_with_source_sink = get_stations_data_source_sink_days_months_years_time_range(usage_data, start_time_index, str(t[0]), str(t[1]), days_of_week, months, years)
    source_sink_difference = get_net_sources_sinks(stations_data_with_source_sink)
    source_sink_total = stations_data_with_source_sink['source counts']+stations_data_with_source_sink['sink counts']


    fig_diff = px.scatter_map(stations_data_with_source_sink, lat='lat', lon='lon', color=source_sink_difference, size=np.square(source_sink_difference), zoom=10, labels={'color': 'sources-sinks'}, range_color=(-1,1), color_continuous_scale='PuOr', title='sources - sinks normalized to total trips from each station', subtitle='+1 means that all trips were taken from this station and -1 means that all trips were taken to this station')
    fig_diff.update_layout(mapbox_style='open-street-map')

    fig_counts = px.scatter_map(stations_data_with_source_sink, lat='lat', lon='lon', color=source_sink_total, size=np.square(source_sink_total), zoom=10, labels={'color': 'total trips'}, color_continuous_scale='plasma', title='total trips from each station')
    fig_counts.update_layout(mapbox_style='open-street-map')

    with st.container():
        st.plotly_chart(fig_diff)
        st.plotly_chart(fig_counts)

def day_of_week_histogram_page():
    st.title('Visualizing City Bike Usage by Days of the Week')
    hists = []
    edges = []

    for day in range(7):
        fig = plt.figure()

        h, e = np.histogram(usage_data['Start Time'][start_time_index.day_of_week == day].apply(lambda x: x.hour), bins=24)

        #h, e = plt.hist(usage_data['Start Time'][start_time_index.day_of_week == day].apply(lambda x: x.hour), histtype='stepfilled')
        plt.stairs(h, e, fill=True)
        plt.xlabel('Hour of Day')
        plt.ylabel('Trips Taken')
        plt.title(calendar.day_name[day])

        st.pyplot(fig)

        hists.append(h)
        edges.append(e)
    
    fig = plt.figure()
    for day in range(7):
        plt.stairs(hists[day], edges[day], label=calendar.day_name[day])
    plt.xlabel('Hour of Day')
    plt.ylabel('Trips Taken')
    plt.legend(loc=0)
    st.pyplot(fig)

pg = st.navigation([st.Page(geographic_plots_page, title='Visualizing Usage Geographically'),
                    st.Page(day_of_week_histogram_page, title='Visualizing Usage by Day')])
pg.run()