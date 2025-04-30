# Client API Solution Update Scripts

This repository contains a collection of Python scripts designed to pull data from various APIs (Google Analytics 4, Welcome Home CRM) and consolidate it into Google BigQuery for reporting and analysis purposes. The scripts are designed to run periodically to keep the data in BigQuery up-to-date.

## Overview

The repository consists of seven main scripts:

1. **SessionData.py** - Extracts session data from Google Analytics 4
2. **WebEventData.py** - Collects web event data from Google Analytics 4
3. **ga4_ad_data_pull.py** - Extracts advertising data from Google Analytics 4
4. **welcomehome_api.py** - Extracts basic data from Welcome Home CRM API
5. **welcomehome_prospect_details.py** - Extracts detailed prospect data from Welcome Home CRM
6. **welcomehome_to_bigquery.py** - Uploads Welcome Home CRM data to BigQuery
7. **welcomehome_main.py** - Main script to run all Welcome Home CRM scripts in sequence

Each script connects to its respective API, fetches data for the configured communities, and loads it into a corresponding BigQuery table.

## Prerequisites

- Python 3.7+
- Google Cloud project with BigQuery enabled
- Service account with appropriate permissions for Google Analytics 4 and BigQuery
- Welcome Home CRM API key with appropriate permissions

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/client-apisolution-updatescripts.git
   cd client-apisolution-updatescripts
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Set up authentication:
   - Place your Google Analytics 4 service account key in `ga_keys.json`
   - Place your BigQuery service account key in `bq_keys.json` (if needed)
   - Configure the Welcome Home CRM API token in the relevant scripts

## Configuration

Each script contains a dictionary that maps community names to their respective IDs in the relevant system (GA4 property ID, Welcome Home community ID). Update these dictionaries as needed when adding or removing communities.

The date range for data extraction is also configurable within each script. By default, the scripts fetch data from January 1, 2024, to the current date.

## Script Details

### 1. SessionData.py

This script extracts session data from Google Analytics 4 and loads it into BigQuery.

**Key Features:**
- Tracks new users and engaged sessions over time
- Processes data for multiple GA4 properties
- Organizes data by community and date
- Handles data deduplication for properties with multiple communities

**BigQuery Table:** `combined.SessionData`

### 2. WebEventData.py

This script collects detailed web event data from Google Analytics 4 and loads it into BigQuery.

**Key Features:**
- Captures event names and counts
- Tracks active users, new users, page views, and engagement metrics
- Provides detailed insights on user behavior
- Handles data deduplication for properties with multiple communities

**BigQuery Table:** `combined.WebEventData`

### 3. ga4_ad_data_pull.py

This script extracts advertising data from Google Analytics 4 and loads it into BigQuery.

**Key Features:**
- Captures campaign names and ad accounts
- Tracks ad costs, clicks, and impressions
- Calculates cost per click and other advertising metrics
- Consolidates data across all communities

**BigQuery Table:** `combined.ga4_ad_data_pull`

### 4. welcomehome_api.py

This script extracts basic data from the Welcome Home CRM API.

**Key Features:**
- Retrieves community information
- Collects prospect names and basic details
- Extracts activity data
- Gathers floor plans, units, and housing contracts
- Exports data to JSON files for further processing

**Output:** JSON files in the `data` directory

### 5. welcomehome_prospect_details.py

This script extracts detailed prospect information from the Welcome Home CRM API.

**Key Features:**
- Retrieves comprehensive prospect details
- Collects associated resident and influencer information
- Gathers prospect activities
- Flattens complex data structures for easier analysis
- Outputs both raw JSON and flattened CSV files

**Output:** JSON and CSV files in the `data` directory

### 6. welcomehome_to_bigquery.py

This script uploads Welcome Home CRM data to Google BigQuery.

**Key Features:**
- Converts JSON data to pandas DataFrames
- Creates or appends to BigQuery tables
- Uploads multiple data types (prospects, activities, housing contracts, etc.)
- Preserves data types and schema

**BigQuery Tables:**
- `combined.WelcomeHome_Prospects`
- `combined.WelcomeHome_Activities`
- `combined.WelcomeHome_HousingContracts`
- `combined.WelcomeHome_Units`
- `combined.WelcomeHome_FloorPlans`
- `combined.WelcomeHome_Communities`
- `combined.WelcomeHome_CareTypes`
- `combined.WelcomeHome_LeadSources`
- `combined.WelcomeHome_Relationships`

### 7. welcomehome_main.py

This script runs all the Welcome Home CRM scripts in sequence.

**Key Features:**
- Orchestrates the execution of all Welcome Home scripts
- Handles errors and logs execution status
- Provides summary of the overall execution

## Running the Scripts

Each script can be run independently:

```
python SessionData.py
python WebEventData.py
python ga4_ad_data_pull.py
python welcomehome_api.py
python welcomehome_prospect_details.py
python welcomehome_to_bigquery.py
```

Alternatively, run the Welcome Home CRM scripts as a pipeline:

```
python welcomehome_main.py
```

For regular updates, consider setting up cron jobs or scheduled tasks to run these scripts periodically.

## Data Model

The scripts create the following tables in the BigQuery dataset named `combined`:

- `SessionData` - Session data by community and date
- `WebEventData` - Web event data by community, event, and date
- `ga4_ad_data_pull` - Advertising data from Google Analytics 4
- `WelcomeHome_Prospects` - Prospect data from Welcome Home CRM
- `WelcomeHome_Activities` - Activity data from Welcome Home CRM
- `WelcomeHome_HousingContracts` - Housing contract data from Welcome Home CRM
- `WelcomeHome_Units` - Unit data from Welcome Home CRM
- `WelcomeHome_FloorPlans` - Floor plan data from Welcome Home CRM
- `WelcomeHome_Communities` - Community data from Welcome Home CRM
- `WelcomeHome_CareTypes` - Care type data from Welcome Home CRM
- `WelcomeHome_LeadSources` - Lead source data from Welcome Home CRM
- `WelcomeHome_Relationships` - Relationship data from Welcome Home CRM

These tables can be joined using community IDs and dates for comprehensive reporting.

## Troubleshooting

- **Authentication Issues**: Ensure your service account has the necessary permissions and the key files are properly formatted.
- **API Rate Limits**: If you encounter rate limiting, consider adding delays between API calls or implementing exponential backoff.
- **BigQuery Errors**: Check that your service account has appropriate permissions to create/modify tables in the BigQuery dataset.
- **Welcome Home CRM API Token**: If you receive 401 errors, verify that your API token is valid and has the appropriate permissions.

## License

This project is licensed under the terms of the license included in the repository.