import requests
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from simple_salesforce import Salesforce
import folium

sf = Salesforce(username=st.secrets["salesforce"]["username"],
                password=st.secrets["salesforce"]["password"],
                security_token=st.secrets["salesforce"]["security_token"])

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

try:
    authenticator.login()
except Exception as e:
    st.error(e)

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

# Function to clean the DataFrame
def clean_dataframe(df):
    threshold = 0.6
    missing_percentage = df.isnull().mean()
    return df.drop(columns=missing_percentage[missing_percentage > threshold].index)

# Function to display metrics
def display_metrics(grouped_df):
    total_completed = grouped_df['num_completed'].sum()
    total_failed = grouped_df['num_failed'].sum()
    overall_success_rate = total_completed / (total_completed + total_failed) if total_completed + total_failed > 0 else 0
    total_num = total_completed + total_failed

    # Create three columns for metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="Total Number", value = total_num)
    with col2:
        st.metric(label="Total Completed", value=total_completed)
    with col3:
        st.metric(label="Total Failed", value=total_failed)
    with col4:
        st.metric(label="Overall Success Rate", value=f"{overall_success_rate:.0%}")
    

def groupDetrackJobs(df):
    df_new = df[[
    'id', 'primary_job_status', 'do_number', 'tracking_number',
    'job_sequence', 'assign_to', 'address', 'postal_code', 'customer',
    'detrack_number', 'reason', 'pod_time', 'run_number', 'items',
    'milestones']]

    grouped_df = df_new.groupby('run_number').agg(
        total_num = ('primary_job_status', 'size'),
        num_completed=('primary_job_status', lambda x: (x == 'completed').sum()),
        num_failed=('primary_job_status', lambda x: (x == 'failed').sum()),
        num_failed_time=('primary_job_status', lambda x: ((x == 'failed') & (df_new.loc[x.index, 'reason'] == 'Ran out of Time')).sum()),
        num_in_progress=('primary_job_status', lambda x: (x == 'dispatched').sum())
    ).reset_index()
    # Calculate success rate
    grouped_df['success_rate'] = grouped_df['num_completed'] / (
        grouped_df['num_completed'] + grouped_df['num_failed']
    )
    grouped_df['run_number'] = grouped_df['run_number'].str.replace('"', '', regex=False)
    return grouped_df

def getFailedJobs(df):
    df_failed = df[df['status']=='failed'].reset_index(drop=True).copy()
    df_failed['first_item'] = df_failed['items'].str[0].apply(lambda x: x['description'] if isinstance(x, dict) else None)
    df_failed = df_failed[['run_number', 'customer', 'reason', 'pod_time', 'postal_code', 'do_number', 'items_count', 'first_item']]
    return df_failed

def dataframeFromSF(query):
    results = sf.query_all(query)
    df = pd.DataFrame(results['records']).drop(columns='attributes')
    return df

def get_daily_dispatch_driver(date):
    query = f"""
        SELECT Id, Name, Ops_Start_Time__c, Ops_End_Time__c, DriverId__c, Dispatch_Date__c
        FROM DailyDispatch__c
        WHERE Dispatch_Date__c = {date}
    """
    df = dataframeFromSF(query)

    df[['Ops_Start_Time__c', 'Ops_End_Time__c']] = df[['Ops_Start_Time__c', 'Ops_End_Time__c']].apply(pd.to_datetime, errors='coerce')
    # Calculate duration, set to None where there's an error
    df['duration'] = df.apply(lambda row: row['Ops_End_Time__c'] - row['Ops_Start_Time__c'] if pd.notna(
        row['Ops_End_Time__c']) and pd.notna(row['Ops_Start_Time__c']) else None, axis=1)

    df['duration_hh_mm'] = df['duration'].astype(str).str.split().str[-1].str[:-3]

    df['start_time_hh_mm'] = df['Ops_Start_Time__c'].dt.strftime("%H:%M")
    df['end_time_hh_mm'] = df['Ops_End_Time__c'].dt.strftime("%H:%M")

    ## get driver data

    query = """
    SELECT Id, Name, Job_Title__c
    FROM Driver__c
    """
    df_driver = dataframeFromSF(query)
    df_driver.rename(columns={'Id': 'DriverId', 'Name': 'Driver_Name'}, inplace=True)

    df = pd.merge(df, df_driver, left_on = "DriverId__c", right_on="DriverId", how="left")
    return df

def prep_dd_display(df):
    df = df.dropna(subset='Ops_Start_Time__c')
    df = df[df['Name'].str.contains("DELIVERY")].reset_index(drop=True)
    df = df[['Name', 'Driver_Name', 'start_time_hh_mm', 'end_time_hh_mm', 'duration_hh_mm']]
    df.rename(columns = {'start_time_hh_mm' : 'start_time',
                          'end_time_hh_mm' : 'end_time'}, inplace=True)
    return df

def mergeDelivery(df_detrack, df_sf):
    df_detrack['run_number'] = df_detrack['run_number'].str.replace("EM11", "EM1.1 ")
    df_detrack['run_number'] = df_detrack['run_number'].str.replace("EM12", "EM1.2 ")
    df_detrack['Name'] = df_detrack['run_number'].str.split(" ").str[0]

    df_sf['Name'] = df_sf['Name'].str.replace("DELIVERY - ", "").str.strip()
    df_sf['Name'] = df_sf['Name'].str.split(" ").str[0]

    df_merged = pd.merge(df_sf, df_detrack, on='Name', how='right')
    df_merged = df_merged[['Name', 'Driver_Name', 'start_time', 'end_time', 'duration_hh_mm',
           'success_rate', 'run_number', 'total_num', 'num_completed', 'num_failed',
       'num_failed_time', 'num_in_progress']]
    return df_merged


# Streamlit app layout
st.title("Detrack API Data Fetcher")
def load_app():
    st.write("Click the button below to fetch and clean data from the Detrack API.")

    # Date input for the query, defaulting to today's date
    selected_date = st.date_input("Select a date", value=pd.Timestamp.now().date())

    # Initialize session state
    if "df_new" not in st.session_state:
        st.session_state.df_new = None
        st.session_state.grouped_df = None
        st.session_state.failed_df = None

    if st.button("Fetch and Process Data"):
        # Update the query parameters with the selected date
        params = {**default_params, "date": selected_date.strftime("%Y-%m-%d")}

        all_jobs = get_all_detrack_jobs(params)
        df_dispatch = prep_dd_display(get_daily_dispatch_driver(selected_date.strftime("%Y-%m-%d")))

        if all_jobs:
            df = pd.DataFrame(all_jobs)
            st.session_state.df_new = clean_dataframe(df)
            st.session_state.grouped_df = groupDetrackJobs(df)
            # Format the success_rate column as a percentage
            st.session_state.grouped_df['success_rate'] = st.session_state.grouped_df['success_rate'].map('{:.0%}'.format)

            st.session_state.failed_df = getFailedJobs(df)

            st.success("Data fetched, cleaned, and grouped successfully.")
        else:
            st.warning("No data retrieved.")

    # Display metrics and summary if grouped data is available
    if st.session_state.grouped_df is not None:
        display_metrics(st.session_state.grouped_df)
        st.write("Salesforce and Detrack Data")
        st.write(mergeDelivery(st.session_state.grouped_df, df_dispatch))
        st.write("Summary of jobs by Route Number:")
        st.write(st.session_state.grouped_df)
        st.write("Dispatch and driver info")
        st.write(df_dispatch)

    # CSV download button for the full DataFrame (df_new)
    if st.session_state.df_new is not None:
        csv = st.session_state.df_new.to_csv(index=False)
        st.download_button(label="Download Full Data as CSV", data=csv, file_name="detrack_data.csv", mime="text/csv")

    if st.session_state.failed_df is not None:
        st.write("All Failed Jobs")
        st.write(st.session_state.failed_df)

if st.session_state['authentication_status']:
    authenticator.logout('Logout', 'main')
    load_app()
elif st.session_state['authentication_status'] == False:
    st.error('Username/password is incorrect')
elif st.session_state['authentication_status'] == None:
    st.warning('Please enter your username and password')