import pandas as pd
import requests
import json
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_TOKEN = os.getenv('WELCOMEHOME_API_TOKEN')
BASE_URL = os.getenv('WELCOMEHOME_BASE_URL')
HEADERS = {
    "Authorization": f"Token token={API_TOKEN}",
    "Content-Type": "application/json"
}

# Dictionary mapping community names to their IDs
COMMUNITIES = {
    "Astoria Oakdale": 34062,
    "Astoria Omaha": 34083,
    "Cedar Rapids CountryHouse": 34066,
    "Council Bluffs CountryHouse": 34065,
    "Cumberland CountryHouse": 34075,
    "Dickinson CountryHouse": 34067,
    "Elkhorn CountryHouse": 34074,
    "Folsom CountryHouse": 34063,
    "Grand Island CountryHouse": 34068,
    "Granite Bay CountryHouse": 34064,
    "Holland Farms": 34079,
    "Kearney CountryHouse": 34069,
    "Kingston Bay": 34085,
    "Lincoln CountryHouse 1 (25th & Old Cheney)": 34070,
    "Lincoln CountryHouse 2 (Pine Lake)": 34071,
    "Lincoln CountryHouse 3 (70th & O)": 34072,
    "Omaha CountryHouse": 34073,
    "Sage Glendale": 34086,
    "Sage Mountain": 34087,
    "Serra Sol": 34080,
    "Sunol Creek Memory Care": 34089,
    "Symphony Pointe": 34084,
    "The Kensington Cumberland": 34076,
    "The Kensington Fort Madison": 34077,
    "The Kensington Hastings": 34078,
    "The Terraces at Via Verde": 34088,
    "TreVista Antioch": 34082
}

# Define BigQuery settings
PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
BQ_KEY_PATH = "bq_keys.json"
LOCAL_CSV_PATH = "welcomehome_data.csv"

# Rate limiting settings
MIN_DELAY = 0.075  # Reduced to 1/4 of 0.3
MAX_DELAY = 0.25   # Reduced to 1/4 of 1.0
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1.25  # Reduced to 1/4 of 5
MAX_WORKERS = 3

# Create a requests session for connection pooling
session = requests.Session()

def setup_bigquery_client():
    """Set up and return a BigQuery client with service account credentials"""
    credentials = service_account.Credentials.from_service_account_file(
        BQ_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Ensure the 'combined' dataset exists, and create it if it does not
    bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    
    try:
        bq_client.get_dataset(dataset_id)
    except Exception as e:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        bq_client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")
    
    return bq_client

def make_api_request(url, params=None, max_retries=MAX_RETRIES, retry_delay=INITIAL_RETRY_DELAY):
    """Make an API request with retry logic and rate limiting"""
    # Add random delay to prevent hitting rate limits
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=HEADERS, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"Failed after {max_retries} attempts: {e}")
                return None

def get_prospects(community_id=None, limit=50, status=None):  
    """Get prospects from the API with optional filtering"""
    url = f"{BASE_URL}/prospects/names"
    params = {"limit": limit}
    
    if community_id:
        params["filters[community_id]"] = community_id
    
    if status:
        params["filters[status]"] = status
    
    result = make_api_request(url, params)
    return result or []

def get_prospect_details(prospect_id):
    """Get detailed information about a specific prospect"""
    url = f"{BASE_URL}/prospects/{prospect_id}"
    return make_api_request(url)

def get_prospect_activities(prospect_id):
    """Get activities for a specific prospect"""
    url = f"{BASE_URL}/prospects/{prospect_id}/activities"
    result = make_api_request(url)
    return result if result else []

def process_prospects(community_id, community_name, max_prospects=50):
    """Process prospects for a specific community with all relevant status types"""
    all_data = []
    statuses = ["open", "lost", "moved_in", "on_hold"]
    
    for status in statuses:
        print(f"Fetching {status} prospects for {community_name}")
        prospects = get_prospects(community_id=community_id, status=status, limit=max_prospects)
        
        # Reduced delay after getting the list of prospects
        time.sleep(0.125)  # Reduced to 1/4 of 0.5
        
        for i, prospect in enumerate(prospects):
            prospect_id = prospect["id"]
            
            # Get detailed prospect information
            details = get_prospect_details(prospect_id)
            if not details:
                continue
            
            # Add extra delay after every 5 prospects but reduced
            if i > 0 and i % 5 == 0:
                time.sleep(0.25)  # Reduced to 1/4 of 1
            
            # Extract expanded set of fields from the API response
            prospect_data = {
                # Basic identification
                "prospect_id": prospect_id,
                "community_id": community_id,
                "community_name": community_name,
                "status": status,
                
                # Main prospect details
                "first_name": details.get("primary_person_attributes", {}).get("first_name", ""),
                "last_name": details.get("primary_person_attributes", {}).get("last_name", ""),
                "full_name": f"{details.get('primary_person_attributes', {}).get('first_name', '')} {details.get('primary_person_attributes', {}).get('last_name', '')}".strip(),
                
                # Contact information
                "email": details.get("primary_person_attributes", {}).get("email", ""),
                "primary_phone": details.get("primary_person_attributes", {}).get("cell_phone", "") or 
                                 details.get("primary_person_attributes", {}).get("home_phone", "") or 
                                 details.get("primary_person_attributes", {}).get("work_phone", ""),
                
                # Address information
                "address_line1": details.get("primary_person_attributes", {}).get("address_attributes", {}).get("line1", ""),
                "address_line2": details.get("primary_person_attributes", {}).get("address_attributes", {}).get("line2", ""),
                "city": details.get("primary_person_attributes", {}).get("address_attributes", {}).get("city", ""),
                "state": details.get("primary_person_attributes", {}).get("address_attributes", {}).get("state", ""),
                "postal_code": details.get("primary_person_attributes", {}).get("address_attributes", {}).get("zip", ""),
                
                # Reference information
                "lead_source": details.get("lead_source", {}).get("name", "") if details.get("lead_source") else "",
                "lead_source_id": details.get("lead_source_id", ""),
                "care_type": details.get("care_type", {}).get("name", "") if details.get("care_type") else "",
                
                # Important dates
                "created_at": details.get("created_at", ""),
                "active_at": details.get("active_at", ""),
                "initial_contact_at": details.get("initial_contact_at", ""),
                "last_contact_at": details.get("last_contact_at", ""),
                "move_in_date": details.get("move_in_date", ""),
                
                # Stage information
                "stage": details.get("stage", {}).get("name", "") if details.get("stage") else "",
                
                # Financial information
                "rent": details.get("rent", ""),
                
                # Outcome information
                "lost_reason": details.get("lost_reason", ""),
                "close_reason_details": details.get("close_reason_details", ""),
                
                # Metadata for tracking
                "extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Handle potential influencers/family members
            if "influencers_attributes" in details and details["influencers_attributes"]:
                influencer = details["influencers_attributes"][0]  # Get first influencer
                if influencer and "person_attributes" in influencer:
                    person = influencer["person_attributes"]
                    prospect_data.update({
                        "influencer_name": f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                        "influencer_phone": person.get("cell_phone", "") or person.get("home_phone", "") or person.get("work_phone", ""),
                        "influencer_email": person.get("email", ""),
                        "influencer_relationship": influencer.get("relationship_id", "")
                    })
            
            all_data.append(prospect_data)
        
        # Reduced delay between different statuses
        time.sleep(0.375)  # Reduced to 1/4 of 1.5
            
    print(f"Total {len(all_data)} prospects processed for {community_name}")
    return all_data

def load_data_to_bigquery(df, bq_client):
    """Upload DataFrame to BigQuery, replacing existing data"""
    if df.empty:
        print("DataFrame is empty. Skipping load to BigQuery.")
        return
    
    # Define the table ID for BigQuery uploads
    table_id = f"{PROJECT_ID}.{DATASET_ID}.WelcomeHomeCommunityData"
    
    try:
        # Check if table exists and create it if it doesn't
        try:
            bq_client.get_table(table_id)
            print(f"Table {table_id} exists, proceeding with data deletion")
            # Delete all existing data
            query = f"DELETE FROM `{table_id}` WHERE 1=1"
            query_job = bq_client.query(query)
            query_job.result()
            print(f"Deleted all existing data from {table_id}")
        except Exception as e:
            print(f"Table does not exist yet, will be created: {e}")
        
        # Load new data into BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            autodetect=True,
        )
        
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into {table_id}")
        return True
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
        return False

def save_dataframe_to_csv(df, filepath=LOCAL_CSV_PATH):
    """Save DataFrame to local CSV file"""
    try:
        df.to_csv(filepath, index=False)
        print(f"DataFrame saved to {filepath}")
        return True
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
        return False

def main():
    """Main function to run the script"""
    print("Welcome Home Community Data Uploader")
    print("===================================")
    
    # Setup BigQuery client
    print("Setting up BigQuery client...")
    try:
        bq_client = setup_bigquery_client()
    except Exception as e:
        print(f"Failed to set up BigQuery client: {e}")
        return
    
    # Process all communities in parallel, but with fewer workers to avoid rate limits
    all_prospect_data = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        
        # Submit tasks for each community
        for community_name, community_id in COMMUNITIES.items():
            future = executor.submit(process_prospects, community_id, community_name)
            futures[future] = community_name
            # Reduced delay between submitting community tasks
            time.sleep(0.25)  # Reduced to 1/4 of 1
        
        # Process results as they complete
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Communities"):
            community_name = futures[future]
            try:
                prospect_data = future.result()
                all_prospect_data.extend(prospect_data)
                print(f"Completed processing for {community_name}")
                
                # Reduced delay after each community completes
                time.sleep(0.375)  # Reduced to 1/4 of 1.5
            except Exception as e:
                print(f"Error processing community {community_name}: {e}")
    
    # Convert all prospect data to DataFrame
    if all_prospect_data:
        df = pd.DataFrame(all_prospect_data)
        print(f"Created DataFrame with {len(df)} rows")
        
        # Save DataFrame locally first
        save_dataframe_to_csv(df)
        
        # Then try to upload to BigQuery
        upload_success = load_data_to_bigquery(df, bq_client)
        
        if not upload_success:
            print(f"BigQuery upload failed but data is safely stored in {LOCAL_CSV_PATH}")
    else:
        print("No prospect data collected")
    
    print("Processing complete.")

if __name__ == "__main__":
    main() 