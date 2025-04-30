import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

# --------------------------------------------------------------------------------
# 1) Community property IDs
# --------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------
# 2) Service account credentials and GA4/BigQuery clients
# --------------------------------------------------------------------------------
ga_key_path = 'ga_keys.json'

# Authenticate for Google Analytics
ga_credentials = service_account.Credentials.from_service_account_file(ga_key_path)
ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

# Authenticate for BigQuery
bq_client = bigquery.Client(credentials=ga_credentials, project=ga_credentials.project_id)

# --------------------------------------------------------------------------------
# 3) Ensure the dataset exists
# --------------------------------------------------------------------------------
dataset_id = f"{ga_credentials.project_id}.combined"
try:
    bq_client.get_dataset(dataset_id)  # Make an API request.
except Exception:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Created dataset {dataset_id}")

# --------------------------------------------------------------------------------
# 4) Create (or update) the SessionEventDataWithSource table schema
# --------------------------------------------------------------------------------
session_event_table_id = f"{ga_credentials.project_id}.combined.SessionEventDataWithSource"
session_event_table = bigquery.Table(session_event_table_id)
session_event_table.schema = [
    bigquery.SchemaField("Community_ID", "STRING"),
    bigquery.SchemaField("Community_Name", "STRING"),
    bigquery.SchemaField("Date", "DATE"),
    bigquery.SchemaField("engagedSessions", "INTEGER"),
    bigquery.SchemaField("eventCount", "INTEGER"),
    bigquery.SchemaField("eventName", "STRING"),
    bigquery.SchemaField("sessionDefaultChannelGroup", "STRING"),
    bigquery.SchemaField("sessions", "INTEGER"),
    bigquery.SchemaField("sessionSource", "STRING"),
    bigquery.SchemaField("sessionSourceMedium", "STRING")
]

# Create the table if it does not exist, or just update schema if it does
session_event_table = bq_client.create_table(session_event_table, exists_ok=True)
print(f"Updated table {session_event_table_id} schema.")

# --------------------------------------------------------------------------------
# 5) Define the date range
# --------------------------------------------------------------------------------
start_date = "2024-01-01"
end_date = datetime.now().strftime('%Y-%m-%d')  # Today's date

# --------------------------------------------------------------------------------
# 6) Function to fetch session-based event data
# --------------------------------------------------------------------------------
def get_session_event_data(property_id):
    """
    Fetch session- and event-based metrics/dimensions from GA4.
    Dimensions: date, eventName, sessionDefaultChannelGroup, sessionSource, sessionSourceMedium
    Metrics: engagedSessions, eventCount, sessions
    """
    print(f"Fetching Session Event Data for property ID: {property_id}")

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="sessionDefaultChannelGroup"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionSourceMedium")
        ],
        metrics=[
            Metric(name="engagedSessions"),
            Metric(name="eventCount"),
            Metric(name="sessions")
        ],
        limit=100000
    )

    try:
        response = ga_client.run_report(request)
    except Exception as e:
        print(f"Error fetching Session Event Data for property ID {property_id}: {e}")
        return pd.DataFrame()

    # Parse rows -> columns
    rows = [
        [dim.value for dim in row.dimension_values] +
        [met.value for met in row.metric_values]
        for row in response.rows
    ]

    # Convert to dataframe
    columns = [d.name for d in request.dimensions] + [m.name for m in request.metrics]
    df = pd.DataFrame(rows, columns=columns)

    if df.empty:
        return df

    # Convert 'date' column to datetime type
    df["date"] = pd.to_datetime(df["date"], format='%Y%m%d')
    # Rename for BQ schema
    df.rename(columns={"date": "Date"}, inplace=True)

    # Convert numeric columns
    numeric_cols = ["engagedSessions", "eventCount", "sessions"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    return df

# --------------------------------------------------------------------------------
# 7) Function to delete and load data into BigQuery
# --------------------------------------------------------------------------------
def load_data_to_bigquery(df, table_id, community_id, community_name, start_date, end_date):
    """
    1) Delete existing rows for (Community_ID) in the date range
    2) Write (append) new data from df
    """
    # Add Community_ID and Community_Name columns
    df["Community_ID"] = community_id
    df["Community_Name"] = community_name

    # Delete existing data for this Community_ID and date range
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE Community_ID = '{community_id}' 
      AND Date BETWEEN '{start_date}' AND '{end_date}'
    """
    query_job = bq_client.query(delete_query)
    query_job.result()  # Wait for the query to finish
    print(f"Deleted existing data for Community_ID {community_id} between {start_date} and {end_date}.")

    # Load job config
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    # Write the dataframe
    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

# --------------------------------------------------------------------------------
# 8) Main loop
# --------------------------------------------------------------------------------
processed_property_ids = set()

for community_name, property_id in ga4_datasets.items():
    print(f"Processing community: {community_name} (property {property_id})")

    # Only fetch once per property_id
    if property_id in processed_property_ids:
        print("  - Already processed this property ID; skipping.")
        continue

    # Fetch data
    session_df = get_session_event_data(property_id)
    if session_df.empty:
        print("  - No rows returned from GA4.")
    else:
        # Optional: Print a small preview
        print(session_df.head(5))

        # Load to BigQuery
        load_data_to_bigquery(
            df=session_df,
            table_id=session_event_table_id,
            community_id=property_id,
            community_name=community_name,
            start_date=start_date,
            end_date=end_date
        )

    processed_property_ids.add(property_id)

print("Processing complete.")
