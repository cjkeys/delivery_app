import pandas as pd
from datetime import datetime, timedelta
import folium
from streamlit_folium import st_folium
from simple_salesforce import Salesforce
import streamlit as st
import requests
import numpy as np

sf = Salesforce(username=st.secrets["salesforce"]["username"],
                password=st.secrets["salesforce"]["password"],
                security_token=st.secrets["salesforce"]["security_token"])

# Detrack API endpoint and your API key
api_url = "https://app.detrack.com/api/v2/dn/jobs"
api_key = st.secrets["detrack"]["api_key"] # Fetch the API key from environment variables

# Define initial query parameters
default_params = {
    "page": 1,
    "limit": 100,
    "sort": "-created_at",
    "date": pd.Timestamp.now().strftime("%Y-%m-%d"),
    "type": "Delivery",
}

# Function to get all paginated results from Detrack API
def get_all_detrack_jobs(params):
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }

    all_data = []
    next_url = api_url  # Start with the initial URL

    while next_url:
        response = requests.get(next_url, headers=headers, params=params if next_url == api_url else None)

        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get("data", []))
            next_url = data.get("links", {}).get("next")
        else:
            st.error(f"Failed to retrieve data: {response.status_code} {response.text}")
            return []

    return all_data

def dataframeFromSF(query):
    results = sf.query_all(query)
    df = pd.DataFrame(results['records']).drop(columns='attributes')
    return df

def mergeWaypointsDetrack(df_detrack, route_date):
    query = f"""
    SELECT Id, maps__Longitude__c, maps__Latitude__c, Location_Name__c
    FROM maps__Waypoint__c
    WHERE RouteDate__c = {route_date}
    """
    df_waypoints = dataframeFromSF(query)

    df_waypoints.rename(columns={'maps__Latitude__c' : 'latitude', 'maps__Longitude__c':'longitude'}, inplace=True)
    df_waypoints['Id'] = df_waypoints['Id'].str[:-3]

    df_merged = pd.merge(df_waypoints, df_detrack, left_on='Id', right_on='deliver_to_collect_from', how='inner')
    return df_merged


def plotDeliveryRoute(df, route_name):
    df = df.dropna(subset=['latitude', 'longitude'])
    df_filtered = df[df['run_number'] == route_name].copy()
    df_filtered = df_filtered.sort_values('pod_time', ignore_index=True)

    m = folium.Map(location=[51.5, -0.1], zoom_start=13, control_scale=True, tiles="Cartodb Positron")

    status_colors = {
        "completed": "green",
        "failed": "red"
    }

    for index,row in df_filtered.iterrows():
        stop_num = index + 1
        color = status_colors.get(row["primary_job_status"], "gray") 
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            tooltip=f"{row['Location_Name__c']}<br>POD Time: {row['pod_time']}<br> Reason: {row['reason']}",
            icon=folium.DivIcon(html=f'''
                <div style="font-size: 10pt; color: white; background-color: {color}; border-radius: 50%; width: 24px; height: 24px; text-align: center; line-height: 24px;">
                    {stop_num}
                </div>
            ''')
        ).add_to(m)

    return(m)

## App Layout
# def main():
#     st.title('View In Progress Delivery Routes')
#     if st.button('Get Data'):
#         with st.spinner('Loading...'):
#             df_detrack = pd.DataFrame(get_all_detrack_jobs(params=default_params))
#             df_merged = mergeWaypointsDetrack(df_detrack, pd.Timestamp.now().strftime("%Y-%m-%d"))
        
#         route_options = df_merged['run_number'].unique()
#         selected_route = st.selectbox(
#             'Select Route',
#             options=route_options
#         )

#         map = plotDeliveryRoute(df_merged, selected_route)
#         st_folium(map, width=700)

# main()

def main():
    st.title('View In Progress Delivery Routes')

    # Initialize session state
    if 'df_merged' not in st.session_state:
        st.session_state.df_merged = None
    if 'selected_route' not in st.session_state:
        st.session_state.selected_route = None
    if 'map' not in st.session_state:
        st.session_state.map = None

    # Get Data button
    if st.button('Get Data'):
        with st.spinner('Loading...'):
            df_detrack = pd.DataFrame(get_all_detrack_jobs(params=default_params))
            df_merged = mergeWaypointsDetrack(df_detrack, pd.Timestamp.now().strftime("%Y-%m-%d"))
            st.session_state.df_merged = df_merged
            
            # Reset selected route when new data is loaded
            st.session_state.selected_route = None
            st.session_state.map = None

    # Show selectbox only if we have data
    if st.session_state.df_merged is not None:
        route_options = np.sort(st.session_state.df_merged['run_number'].unique())
        
        # Create selectbox outside the button block
        selected_route = st.selectbox(
            'Select Route',
            options=route_options,
            index=0 if st.session_state.selected_route is None 
                  else route_options.tolist().index(st.session_state.selected_route)
        )

        # Update the map only if the route selection changes
        if selected_route != st.session_state.selected_route:
            st.session_state.selected_route = selected_route
            st.session_state.map = plotDeliveryRoute(st.session_state.df_merged, selected_route)

        # Display the map if it exists
        if st.session_state.map is not None:
            st.header(st.session_state.selected_route)
            st_folium(st.session_state.map, width=700, height=500)

if __name__ == "__main__":
    main()
