import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest
)
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    logging.info(f"Dataset {dataset_id} already exists.")
except Exception as e:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    logging.info(f"Created dataset {dataset_id}")

# Create a new BigQuery table with a different name (e.g., WebEventData_New)
web_event_table_id = f"{ga_credentials.project_id}.combined.WebEventData_New"
web_event_table = bigquery.Table(web_event_table_id)
web_event_table.schema = [
    bigquery.SchemaField("Community_ID", "STRING"),
    bigquery.SchemaField("eventName", "STRING"),
    bigquery.SchemaField("eventCount", "INTEGER"),
    bigquery.SchemaField("activeUsers", "INTEGER"),
    bigquery.SchemaField("newUsers", "INTEGER"),
    bigquery.SchemaField("screenPageViews", "INTEGER"),
    bigquery.SchemaField("screenPageViewsPerSession", "FLOAT"),
    bigquery.SchemaField("screenPageViewsPerUser", "FLOAT"),
    bigquery.SchemaField("averageSessionDuration", "FLOAT"),
    bigquery.SchemaField("sessionDefaultChannelGrouping", "STRING"),
    bigquery.SchemaField("firstUserDefaultChannelGrouping", "STRING"),
    bigquery.SchemaField("Date", "DATE")
]

web_event_table = bq_client.create_table(web_event_table, exists_ok=True)
logging.info(f"Updated table {web_event_table_id} schema.")

# Define the date range variables
start_date = "2024-01-01"
end_date = datetime.now().strftime('%Y-%m-%d')  # Set end_date to today's date

# Define the function to get Web Event Data with the new dimensions
def get_web_event_data(property_id):
    logging.info(f"Fetching Web Event Data for property ID: {property_id}")
    all_rows = []
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="sessionDefaultChannelGrouping"),
            Dimension(name="firstUserDefaultChannelGrouping")
        ],
        metrics=[
            Metric(name="eventCount"),
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="screenPageViewsPerSession"),
            Metric(name="screenPageViewsPerUser"),
            Metric(name="averageSessionDuration")
        ],
        limit=100000
    )

    try:
        response = ga_client.run_report(request)
    except Exception as e:
        logging.error(f"Error fetching Web Event Data for property ID {property_id}: {e}")
        return pd.DataFrame()

    rows = [[dimension_value.value for dimension_value in row.dimension_values] +
            [metric_value.value for metric_value in row.metric_values] for row in response.rows]
    all_rows.extend(rows)

    columns = [dimension.name for dimension in request.dimensions] + [
        'eventCount', 'activeUsers', 'newUsers', 'screenPageViews',
        'screenPageViewsPerSession', 'screenPageViewsPerUser', 'averageSessionDuration'
    ]

    web_event_df = pd.DataFrame(all_rows, columns=columns)

    # Add detailed logging for CountryHouse property ID
    if property_id == "435942576":  # CountryHouse property ID
        logging.info("\n=== CountryHouse Event Analysis ===")
        logging.info(f"Total number of events found: {len(web_event_df)}")
        
        # Get unique event names and their counts
        event_counts = web_event_df['eventName'].value_counts()
        logging.info("\nEvent Names and Counts:")
        for event_name, count in event_counts.items():
            logging.info(f"Event: {event_name}, Count: {count}")
        
        # Check for specific events of interest
        target_events = [
            "country_house_chat_lead",
            "country_house_omaha_phone_call_click",
            "country_house_lincoln84_roobrick_complet",
            "country_house_cumberland_roobrick_comple",
            "country_house_cumberland_phone_call_clic"
        ]
        logging.info("\nSearching for specific events:")
        for target in target_events:
            matches = web_event_df[web_event_df['eventName'].str.contains(target, case=False, na=False)]
            if not matches.empty:
                logging.info(f"Found matches for '{target}':")
                for _, row in matches.iterrows():
                    logging.info(f"  - Event: {row['eventName']}, Count: {row['eventCount']}")
            else:
                logging.info(f"No matches found for '{target}'")
        
        logging.info("=== End CountryHouse Event Analysis ===\n")

    # Convert 'date' column to datetime type
    web_event_df['date'] = pd.to_datetime(web_event_df['date'], format='%Y%m%d')

    # Rename 'date' to 'Date' to match BigQuery schema and ensure it's of type DATE
    web_event_df.rename(columns={'date': 'Date'}, inplace=True)

    # Add Community_ID column
    web_event_df['Community_ID'] = property_id

    # Convert metrics to numeric and handle errors
    numeric_columns = [
        'eventCount', 'activeUsers', 'newUsers', 'screenPageViews',
        'screenPageViewsPerSession', 'screenPageViewsPerUser', 'averageSessionDuration'
    ]
    for column in numeric_columns:
        web_event_df[column] = pd.to_numeric(web_event_df[column], errors='coerce').fillna(0)

    # Adjust data types
    web_event_df['eventCount'] = web_event_df['eventCount'].astype(int)
    web_event_df['activeUsers'] = web_event_df['activeUsers'].astype(int)
    web_event_df['newUsers'] = web_event_df['newUsers'].astype(int)
    web_event_df['screenPageViews'] = web_event_df['screenPageViews'].astype(int)

    return web_event_df

# Function to load data to BigQuery with deletion of existing data
def load_data_to_bigquery(df, table_id, start_date, end_date):
    property_id = df['Community_ID'].iloc[0]

    # Delete existing data for this Community_ID and date range
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE Community_ID = '{property_id}' AND Date BETWEEN '{start_date}' AND '{end_date}'
    """
    query_job = bq_client.query(delete_query)
    query_job.result()  # Wait for the query to finish
    logging.info(f"Deleted existing data for Community_ID {property_id} between {start_date} and {end_date}.")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    logging.info(f"Loaded {job.output_rows} rows into {table_id}.")

# Track processed property IDs for web event data
processed_property_ids = set()

# Iterate through the communities
for community_name, property_id in ga4_datasets.items():
    logging.info(f"Processing community: {community_name}")

    # Fetch and load Web Event Data only if it hasn't been processed yet
    if property_id not in processed_property_ids:
        web_event_df = get_web_event_data(property_id)
        if not web_event_df.empty:
            # Debugging: Print event names and number of events
            event_counts = web_event_df['eventName'].value_counts()
            for event_name, count in event_counts.items():
                logging.info(f"Event Name: {event_name}, Count: {count}")

            load_data_to_bigquery(web_event_df, web_event_table_id, start_date, end_date)
        processed_property_ids.add(property_id)

logging.info("Processing complete.")
