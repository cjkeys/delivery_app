import requests
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

name, authentication_status, username = authenticator.login(location='main')

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

    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Total Completed", value=total_completed)

    with col2:
        st.metric(label="Total Failed", value=total_failed)

    with col3:
        st.metric(label="Overall Success Rate", value=f"{overall_success_rate:.0%}")

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

    if st.button("Fetch and Process Data"):
        # Update the query parameters with the selected date
        params = {**default_params, "date": selected_date.strftime("%Y-%m-%d")}

        all_jobs = get_all_detrack_jobs(params)

        if all_jobs:
            df = pd.DataFrame(all_jobs)
            st.session_state.df_new = clean_dataframe(df)

            # Select specific columns and group by 'run_number'
            st.session_state.df_new = st.session_state.df_new[[
                'id', 'primary_job_status', 'do_number', 'tracking_number',
                'job_sequence', 'assign_to', 'address', 'postal_code', 'customer',
                'detrack_number', 'reason', 'pod_time', 'run_number', 'items',
                'milestones'
            ]]

            st.session_state.grouped_df = st.session_state.df_new.groupby('run_number').agg(
                num_completed=('primary_job_status', lambda x: (x == 'completed').sum()),
                num_failed=('primary_job_status', lambda x: (x == 'failed').sum())
            ).reset_index()

            # Calculate success rate
            st.session_state.grouped_df['success_rate'] = st.session_state.grouped_df['num_completed'] / (
                st.session_state.grouped_df['num_completed'] + st.session_state.grouped_df['num_failed']
            )

            st.success("Data fetched, cleaned, and grouped successfully.")
        else:
            st.warning("No data retrieved.")

    # Display metrics and summary if grouped data is available
    if st.session_state.grouped_df is not None:
        display_metrics(st.session_state.grouped_df)
        st.write("Summary of jobs by Route Number:")
        st.write(st.session_state.grouped_df)

    # CSV download button for the full DataFrame (df_new)
    if st.session_state.df_new is not None:
        csv = st.session_state.df_new.to_csv(index=False)
        st.download_button(label="Download Full Data as CSV", data=csv, file_name="detrack_data.csv", mime="text/csv")

if authentication_status:
    authenticator.logout('Logout', 'main')
    load_app()
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')