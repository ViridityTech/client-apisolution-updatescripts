import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# Define the communities and their corresponding IDs
communities = {
    "Astoria Senior Living - Oakdale": 60,
    "Astoria Senior Living - Omaha": 75,
    "Astoria Senior Living - Tracy": 50,
    "CountryHouse - Cedar Rapids": 68,
    "CountryHouse - Council Bluffs": 64,
    "CountryHouse - Cumberland": 67,
    "CountryHouse - Dickinson": 61,
    "CountryHouse - Elkhorn": 71,
    "CountryHouse - Folsom CA": 70,
    "CountryHouse - Grand Island": 56,
    "CountryHouse - Granite Bay": 63,
    "CountryHouse - Kearney": 59,
    "CountryHouse - Omaha": 55,
    "CountryHouse Lincoln - 70th and O": 62,
    "CountryHouse Lincoln - Old Cheney": 57,
    "CountryHouse Lincoln - Pine Lake": 58,
    "Evergreen - Dickinson": 51,
    "Holland Farms": 72,
    "Kingston Bay Senior Living": 76,
    "Sage Glendale": 77,
    "Sage Mountain": 78,
    "Serra Sol": 73,
    "Sunol Creek Memory Care": 80,
    "Symphony Pointe": 74,
    "The Kensington - Cumberland": 53,
    "The Kensington - Fort Madison": 52,
    "The Kensington - Hastings": 54,
    "The Terrace at Via Verde": 79,
    "TreVista - Concord": 69,
    "TreVista-Antioch Senior Living": 65
}

# Define the path to your service account keys
ga_key_path = 'ga_keys.json'

# Toggle for pushing data to BigQuery
PUSH_TO_BIGQUERY = True  # Set to True to enable pushing to BigQuery

# Authenticate using the service account key for BigQuery
try:
    ga_credentials = service_account.Credentials.from_service_account_file(ga_key_path)
    bq_client = bigquery.Client(credentials=ga_credentials, project=ga_credentials.project_id)
    print("Successfully authenticated to BigQuery.")
except Exception as e:
    print(f"Error authenticating to BigQuery: {e}")

# Ensure the 'combined' dataset exists, and create it if it does not
dataset_id = f"{ga_credentials.project_id}.combined"
if PUSH_TO_BIGQUERY:
    try:
        bq_client.get_dataset(dataset_id)  # Make an API request.
        print(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        print(f"Dataset {dataset_id} not found, creating new dataset. Error: {e}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
        print(f"Created dataset {dataset_id}")

# Create a BigQuery table to store Sherpa data if PUSH_TO_BIGQUERY is enabled
table_id = f"{ga_credentials.project_id}.combined.SherpaData"
if PUSH_TO_BIGQUERY:
    try:
        table = bigquery.Table(table_id)
        table.schema = [
            bigquery.SchemaField("Community", "STRING"),
            bigquery.SchemaField("Community_ID", "STRING"),
            bigquery.SchemaField("ID", "STRING"),
            bigquery.SchemaField("Prospect_Name", "STRING"),
            bigquery.SchemaField("Currently_Living_At", "STRING"),
            bigquery.SchemaField("Stage", "STRING"),
            bigquery.SchemaField("Status", "STRING"),
            bigquery.SchemaField("Date", "DATE")
        ]
        table = bq_client.create_table(table, exists_ok=True)
        print(f"Created table {table_id}")
    except Exception as e:
        print(f"Error creating table {table_id}: {e}")

# Define the date range
start_date = "2024-01-01"  # Modify if you want a dynamic start date
end_date = datetime.now().strftime('%Y-%m-%d')  # Set end_date to today's date
print(f"Processing data from {start_date} to {end_date}")

# Define the base URL for Sherpa API
base_url = "https://members.sherpacrm.com/v1/companies/11/communities/{}/prospects?inquiryDate={}"

# Sherpa API headers
headers = {
    'Authorization': 'Bearer 9AabFuztY0Aid446zxSldXDz48FeolCnZA1HGO0MzmgL2Hwp34ZGqpIzVqHHGyEg'
}

# Function to load data to BigQuery with proper handling and deletion
def load_data_to_bigquery(df, table_id, start_date, end_date):
    try:
        community_id = df['Community_ID'].iloc[0]
        print(f"Preparing to load data for Community_ID: {community_id}")

        # Delete existing data for this Community_ID and date range
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE Community_ID = '{community_id}' AND Date BETWEEN '{start_date}' AND '{end_date}'
        """
        print(f"Running delete query: {delete_query.strip()}")
        query_job = bq_client.query(delete_query)
        query_job.result()  # Wait for the query to finish
        print(f"Deleted existing data for Community_ID {community_id} between {start_date} and {end_date}.")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
        )
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")

# Iterate through the communities
for community_name, community_id in communities.items():
    try:
        print(f"\nProcessing community: {community_name} (ID: {community_id})")
        url = base_url.format(community_id, start_date)
        print(f"Request URL: {url}")

        # Make the API request
        response = requests.get(url, headers=headers)
        print(f"Response status code: {response.status_code}")

        if response.status_code != 200:
            print(f"Failed to fetch data for {community_name} (status code: {response.status_code}). Response text: {response.text}")
            continue

        try:
            response_data = response.json()
            print(f"Response JSON keys: {list(response_data.keys())}")
        except Exception as json_error:
            print(f"Error parsing JSON for community {community_name}: {json_error}")
            continue

        # Extract the nested data element
        data = response_data.get('data', [])
        if not data:
            print(f"No 'data' key found or data is empty for community: {community_name}. Full response: {response_data}")
            continue

        print(f"Data received for {community_name}: {len(data)} record(s)")
        # Uncomment the next line to inspect the first item in detail
        # print("First data item:", data[0])

        # Extracting data into a DataFrame
        prospects_df = pd.DataFrame({
            'ID': [str(prospect.get('id', '')) for prospect in data],
            'Prospect_Name': [
                f"{prospect.get('people', [{}])[0].get('firstName', 'Unknown')} {prospect.get('people', [{}])[0].get('lastName', 'Unknown')}" 
                if prospect.get('people') else 'Unknown'
                for prospect in data
            ],
            'Currently_Living_At': [prospect.get('currentlyLivingAt', '') for prospect in data],
            'Stage': [prospect.get('stage', '') for prospect in data],
            'Status': [prospect.get('status', '') for prospect in data],
            'Initial_Inquiry_Date': [prospect.get('initialInquiryDate', '') for prospect in data],
            'Community': [community_name for _ in data],
            'Community_ID': [str(community_id) for _ in data]
        })

        print(f"DataFrame created with {len(prospects_df)} rows.")
        # Convert Initial_Inquiry_Date to datetime with error coercion
        prospects_df['Initial_Inquiry_Date'] = pd.to_datetime(prospects_df['Initial_Inquiry_Date'], errors='coerce')
        if prospects_df['Initial_Inquiry_Date'].isnull().any():
            print("Warning: Some Initial_Inquiry_Date values could not be converted and are set to NaT.")

        # Filter data within the date range
        mask = (prospects_df['Initial_Inquiry_Date'] >= pd.to_datetime(start_date)) & (prospects_df['Initial_Inquiry_Date'] <= pd.to_datetime(end_date))
        prospects_df = prospects_df.loc[mask]
        print(f"DataFrame after filtering has {len(prospects_df)} rows.")

        # Rename Initial_Inquiry_Date to Date
        prospects_df.rename(columns={'Initial_Inquiry_Date': 'Date'}, inplace=True)

        # Check if the DataFrame is not empty after filtering
        if not prospects_df.empty:
            # Save the Sherpa data to a spreadsheet
            spreadsheet_filename = f"{community_name.replace(' ', '_')}_sherpa_data.xlsx"
            prospects_df.to_excel(spreadsheet_filename, index=False)
            print(f"Sherpa data for {community_name} saved to {spreadsheet_filename}")

            # Load the Sherpa data to BigQuery if enabled
            if PUSH_TO_BIGQUERY:
                load_data_to_bigquery(prospects_df, table_id, start_date, end_date)
        else:
            print(f"No data found for community: {community_name} in the specified date range after filtering.")

    except Exception as e:
        print(f"Error processing community {community_name}: {e}")

print("Processing complete.")
