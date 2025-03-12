import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta

# Define the communities and their corresponding GA4 dataset IDs
communities = {
    "Astoria Senior Living - Oakdale": "425639557",
    "CountryHouse - Cedar Rapids": "435942576",
    "Evergreen - Dickinson": "425556002",
    "Holland Farms": "425702360",
    "Kingston Bay Senior Living": "425660587",
    "Sage Glendale": "425578596",
    "Serra Sol": "425709023",
    "Sunol Creek Memory Care": "441750995",
    "Symphony Pointe": "425732958",
    "The Kensington - Hastings": "425556002",
    "The Terrace at Via Verde": "434302697",
    "TreVista - Concord": "425698056"
}

# Define the path to your service account keys
ga_key_path = 'ga_keys.json'
bq_key_path = 'bq_keys.json'

# Authenticate using the service account key for Google Analytics
ga_credentials = service_account.Credentials.from_service_account_file(ga_key_path)
ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

# Authenticate using the service account key for BigQuery
bq_credentials = service_account.Credentials.from_service_account_file(bq_key_path)
bq_client = bigquery.Client(credentials=bq_credentials, project=bq_credentials.project_id)

# Ensure the dataset exists
dataset_id = f"{bq_credentials.project_id}.combined"
try:
    bq_client.get_dataset(dataset_id)  # Make an API request.
except Exception as e:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=30)  # Create the dataset.
    print(f"Created dataset {dataset_id}")

# Create a BigQuery table to store GA4 data in the new table "ga4_ad_data_pull"
table_id = f"{dataset_id}.ga4_ad_data_pull"
table = bigquery.Table(table_id)
table.schema = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("firstUserGoogleAdsCampaignName", "STRING"),
    bigquery.SchemaField("firstUserGoogleAdsAccountName", "STRING"),
    bigquery.SchemaField("advertiserAdCost", "FLOAT"),
    bigquery.SchemaField("advertiserAdCostPerClick", "FLOAT"),
    bigquery.SchemaField("advertiserAdClicks", "FLOAT"),
    bigquery.SchemaField("advertiserAdImpressions", "FLOAT")
]
table = bq_client.create_table(table, exists_ok=True)
print(f"Created table {table_id}")

# Define the date range
start_date = "2024-01-01"  # Modify if you want a dynamic start date
end_date = datetime.now().strftime('%Y-%m-%d')  # Set end_date to today's date

# Function to get advertiser data for a community
def get_advertiser_data(property_id, community_name):
    print(f"Fetching advertiser data for property {property_id} - {community_name}")
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="firstUserGoogleAdsCampaignName"),
            Dimension(name="firstUserGoogleAdsAccountName")
        ],
        metrics=[
            Metric(name="advertiserAdCost"), 
            Metric(name="advertiserAdCostPerClick"),
            Metric(name="advertiserAdClicks"),
            Metric(name="advertiserAdImpressions")
        ],
        limit=100000
    )
    
    try:
        response = ga_client.run_report(request)
    except Exception as e:
        print(f"Error fetching data for property {property_id}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

    rows = []
    for row in response.rows:
        rows.append([dimension_value.value for dimension_value in row.dimension_values] +
                    [metric_value.value for metric_value in row.metric_values])
    
    df = pd.DataFrame(rows, columns=[
        "date", 
        "firstUserGoogleAdsCampaignName", 
        "firstUserGoogleAdsAccountName", 
        "advertiserAdCost", 
        "advertiserAdCostPerClick", 
        "advertiserAdClicks", 
        "advertiserAdImpressions"
    ])
    
    # Convert date to datetime and other fields to numeric for summation
    df["date"] = pd.to_datetime(df["date"], format='%Y%m%d')
    numeric_columns = ["advertiserAdCost", "advertiserAdCostPerClick", "advertiserAdClicks", "advertiserAdImpressions"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    # Calculate total ad spend for the property
    total_ad_spend = df["advertiserAdCost"].sum()
    
    print(f"Total ad spend for property {property_id}: ${total_ad_spend:,.2f}")
    
    return df

# Function to load data to BigQuery with deletion of existing data
def load_data_to_bigquery(df, table_id, start_date, end_date):
    if df.empty:
        print("DataFrame is empty. Skipping load to BigQuery.")
        return

    # Load new data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )
    
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_id}.")

# Fetch data for all properties and combine into a single DataFrame
all_data = pd.DataFrame()

for community_name, property_id in communities.items():
    df = get_advertiser_data(property_id, community_name)
    
    # Ensure all numeric columns are properly converted
    numeric_columns = ['advertiserAdCost', 'advertiserAdCostPerClick', 'advertiserAdClicks', 'advertiserAdImpressions']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    if not df.empty:
        all_data = pd.concat([all_data, df], ignore_index=True)

# After processing all data, load it into BigQuery
if not all_data.empty:
    load_data_to_bigquery(all_data, table_id, start_date, end_date)
else:
    print("No data to upload to BigQuery.")

print("Processing complete.")
