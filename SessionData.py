import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta

# Define the GA4 dataset IDs for the communities
ga4_datasets = {
    "Astoria Senior Living - Oakdale": "425639557",
    "Astoria Senior Living - Omaha": "425639557",
    "Astoria Senior Living - Tracy": "425639557",
    "CountryHouse - Cedar Rapids": "435942576",
    "CountryHouse - Council Bluffs": "435942576",
    "CountryHouse - Cumberland": "435942576",
    "CountryHouse - Dickinson": "435942576",
    "CountryHouse - Elkhorn": "435942576",
    "CountryHouse - Folsom CA": "435942576",
    "CountryHouse - Grand Island": "435942576",
    "CountryHouse - Granite Bay": "435942576",
    "CountryHouse - Kearney": "435942576",
    "CountryHouse - Omaha": "435942576",
    "CountryHouse Lincoln - 70th and O": "435942576",
    "CountryHouse Lincoln - Old Cheney": "435942576",
    "CountryHouse Lincoln - Pine Lake": "435942576",
    "Evergreen - Dickinson": "425556002",
    "Holland Farms": "425702360",
    "Kingston Bay Senior Living": "425660587",
    "Sage Glendale": "425578596",
    "Sage Mountain": "425578596",
    "Serra Sol": "425709023",
    "Sunol Creek Memory Care": "441750995",
    "Symphony Pointe": "425732958",
    "The Kensington - Cumberland": "425556002",
    "The Kensington - Fort Madison": "425556002",
    "The Kensington - Hastings": "425556002",
    "The Terrace at Via Verde": "434302697",
    "TreVista - Concord": "425698056",
    "TreVista-Antioch Senior Living": "425698056"
}

# Define the path to your service account keys
ga_key_path = 'ga_keys.json'

# Authenticate using the service account key for Google Analytics
ga_credentials = service_account.Credentials.from_service_account_file(ga_key_path)
ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

# Authenticate using the service account key for BigQuery
bq_client = bigquery.Client(credentials=ga_credentials, project=ga_credentials.project_id)

# Ensure the 'combined' dataset exists, and create it if it does not
dataset_id = f"{ga_credentials.project_id}.combined"
try:
    bq_client.get_dataset(dataset_id)  # Make an API request.
except Exception as e:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Created dataset {dataset_id}")

# Define the new table ID for BigQuery uploads
session_data_table_id = f"{ga_credentials.project_id}.combined.SessionData"

# Define the date range variables
start_date = "2024-01-01"
end_date = datetime.now().strftime('%Y-%m-%d')  # Set end_date to today's date

# Function to get the new users and engaged sessions data
def get_session_data(property_id, community_name):
    print(f"Fetching Session Data for property ID: {property_id}")
    all_rows = []
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="newUsers"),
            Metric(name="engagedSessions")
        ],
        limit=100000
    )

    try:
        response = ga_client.run_report(request)
    except Exception as e:
        print(f"Error fetching Session Data for property ID {property_id}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

    rows = [[dimension_value.value for dimension_value in row.dimension_values] +
            [metric_value.value for metric_value in row.metric_values] for row in response.rows]
    all_rows.extend(rows)

    session_df = pd.DataFrame(all_rows, columns=['Date', 'newUsers', 'engagedSessions'])
    
    # Convert 'Date' column to datetime type
    session_df['Date'] = pd.to_datetime(session_df['Date'], format='%Y%m%d')
    
    # Add Community_ID and Community_Name columns
    session_df['Community_ID'] = property_id
    session_df['Community_Name'] = community_name
    
    # Convert metrics to numeric and handle errors
    session_df['newUsers'] = pd.to_numeric(session_df['newUsers'], errors='coerce').fillna(0).astype(int)
    session_df['engagedSessions'] = pd.to_numeric(session_df['engagedSessions'], errors='coerce').fillna(0).astype(int)
    
    return session_df

# Function to load data to BigQuery with proper handling and deletion
def load_data_to_bigquery(df, table_id, start_date, end_date):
    if df.empty:
        print("DataFrame is empty. Skipping load to BigQuery.")
        return

    # Get the unique Community_IDs in the DataFrame
    community_ids = df['Community_ID'].unique()
    for community_id in community_ids:
        # Delete existing data for this Community_ID and date range
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE Community_ID = '{community_id}' AND Date BETWEEN '{start_date}' AND '{end_date}'
        """
        query_job = bq_client.query(delete_query)
        query_job.result()  # Wait for the query to finish
        print(f"Deleted existing data for Community_ID {community_id} between {start_date} and {end_date}.")

    # Load new data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )
    
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_id}.")

# Track processed property IDs for session data
processed_property_ids = set()

# Initialize an empty DataFrame for storing all session data
all_session_data_df = pd.DataFrame()

# Iterate through the communities
for community_name, property_id in ga4_datasets.items():
    # Process each property ID only once
    if property_id not in processed_property_ids:
        print(f"Processing community: {community_name}")

        # Fetch Session Data
        session_data_df = get_session_data(property_id, community_name)
        if not session_data_df.empty:
            all_session_data_df = pd.concat([all_session_data_df, session_data_df], ignore_index=True)

        processed_property_ids.add(property_id)

# Load the session data DataFrame to BigQuery
load_data_to_bigquery(all_session_data_df, session_data_table_id, start_date, end_date)

# Print the session data DataFrame
print("\nSession Data:")
print(all_session_data_df.head())

print("Processing complete.")
