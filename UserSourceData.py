import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

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
except Exception:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Created dataset {dataset_id}")

# Define the table ID for BigQuery uploads
# Changed from 'SessionData' to 'SessionDataWithSource'
session_data_table_id = f"{ga_credentials.project_id}.combined.SessionDataWithSource"

# Define the date range variables
start_date = "2024-01-01"
end_date = datetime.now().strftime('%Y-%m-%d')  # Set end_date to today's date

# Function to get session data
def get_session_data(property_id, community_name):
    print(f"Fetching Session Data for property ID: {property_id}")
    all_rows = []
    
    # Define dimensions
    dimensions = [
        Dimension(name="date"),
        Dimension(name="sessionDefaultChannelGroup"),
        #Dimension(name="eventName")  
    ]
    
    # Define metrics
    metrics = [
        Metric(name="engagedSessions"),
        Metric(name="eventCount"),
        Metric(name="sessions")
    ]
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=dimensions,
        metrics=metrics,
        limit=100000  # Ensure this limit is sufficient for your data
    )

    try:
        response = ga_client.run_report(request)
    except Exception as e:
        print(f"Error fetching Session Data for property ID {property_id}: {e}")
        return pd.DataFrame()
    
    # Check for sampling
    if getattr(response.metadata, 'sampled_report', False):
        print(f"Warning: Data for property ID {property_id} is sampled.")
    
    # Extract rows
    for row in response.rows:
        dimension_values = [dimension.value for dimension in row.dimension_values]
        metric_values = [metric.value for metric in row.metric_values]
        all_rows.append(dimension_values + metric_values)
    
    # Define column names
    columns = [
        'Date',
        'sessionDefaultChannelGroup',
        #'eventName',
        'engagedSessions',
        'eventCount',
        'sessions'
    ]
    
    # Create DataFrame
    session_df = pd.DataFrame(all_rows, columns=columns)
    
    # Convert 'Date' column to datetime type
    session_df['Date'] = pd.to_datetime(session_df['Date'], format='%Y%m%d')
    
    # Add Community_ID and Community_Name columns
    session_df['Community_ID'] = property_id
    session_df['Community_Name'] = community_name
    
    # Convert metrics to numeric and handle errors
    session_df['engagedSessions'] = pd.to_numeric(session_df['engagedSessions'], errors='coerce').fillna(0).astype(int)
    session_df['eventCount'] = pd.to_numeric(session_df['eventCount'], errors='coerce').fillna(0).astype(int)
    session_df['sessions'] = pd.to_numeric(session_df['sessions'], errors='coerce').fillna(0).astype(int)
    
    # Handle 'sessionDefaultChannelGroup' column if needed
    session_df['sessionDefaultChannelGroup'] = session_df['sessionDefaultChannelGroup'].fillna('Unknown')
    
    # Debug: Print number of rows fetched
    print(f"Number of rows fetched for property ID {property_id}: {len(session_df)}")
    
    # Debug: Print a few rows
    print(f"Sample data:")
    print(session_df.head())
    
    # Debug: Print total sessions and events for this property
    total_engaged_sessions = session_df['engagedSessions'].sum()
    total_event_count = session_df['eventCount'].sum()
    total_sessions = session_df['sessions'].sum()
    print(f"Total engagedSessions for {community_name} (Property ID: {property_id}): {total_engaged_sessions}")
    print(f"Total eventCount for {community_name} (Property ID: {property_id}): {total_event_count}")
    print(f"Total sessions for {community_name} (Property ID: {property_id}): {total_sessions}\n")
    
    return session_df

# Function to load data to BigQuery with proper handling and deletion
def load_data_to_bigquery(df, table_id, start_date, end_date):
    if df.empty:
        print("DataFrame is empty. Skipping load to BigQuery.\n")
        return

    # Get the unique Community_IDs in the DataFrame
    community_ids = df['Community_ID'].unique()
    for community_id in community_ids:
        # Delete existing data for this Community_ID and date range
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE Community_ID = '{community_id}' AND Date BETWEEN '{start_date}' AND '{end_date}'
        """
        try:
            query_job = bq_client.query(delete_query)
            query_job.result()  # Wait for the query to finish
            print(f"Deleted existing data for Community_ID {community_id} between {start_date} and '{end_date}'.")
        except Exception as e:
            print(f"Error deleting data for Community_ID {community_id}: {e}\n")

    # Load new data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )
    
    try:
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into {table_id}.\n")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}\n")

# Track processed property IDs to avoid redundant processing
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
    else:
        print(f"Property ID {property_id} already processed.\n")

# Calculate total sessions and events for reporting
total_engaged_sessions = all_session_data_df['engagedSessions'].sum()
total_event_count = all_session_data_df['eventCount'].sum()
total_sessions = all_session_data_df['sessions'].sum()

# Print the total sessions and events
print(f"\nTotal engagedSessions across all communities: {total_engaged_sessions}")
print(f"Total eventCount across all communities: {total_event_count}")
print(f"Total sessions across all communities: {total_sessions}\n")

# Load the session data DataFrame to BigQuery
load_data_to_bigquery(all_session_data_df, session_data_table_id, start_date, end_date)

# Print the combined session data DataFrame
print("Combined Session Data:")
print(all_session_data_df.head())

print("Processing complete.")
