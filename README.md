# Client API Solution Update Scripts

This repository contains a collection of Python scripts designed to pull data from various APIs (Google Analytics 4) and consolidate it into Google BigQuery for reporting and analysis purposes. The scripts are designed to run periodically to keep the data in BigQuery up-to-date.

## Overview

The repository consists of four main scripts:

1. **SessionData.py** - Extracts session data from Google Analytics 4
2. **WebEventData.py** - Collects web event data from Google Analytics 4
3. **ga4_ad_data_pull.py** - Extracts advertising data from Google Analytics 4 

Each script connects to its respective API, fetches data for the configured communities, and loads it into a corresponding BigQuery table.

## Prerequisites

- Python 3.7+
- Google Cloud project with BigQuery enabled
- Service account with appropriate permissions for Google Analytics 4 and BigQuery

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

## Configuration

Each script contains a dictionary that maps community names to their respective IDs in the relevant system (GA4 property ID ). Update these dictionaries as needed when adding or removing communities.

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


### 4. ga4_ad_data_pull.py

This script extracts advertising data from Google Analytics 4 and loads it into BigQuery.

**Key Features:**
- Captures campaign names and ad accounts
- Tracks ad costs, clicks, and impressions
- Calculates cost per click and other advertising metrics
- Consolidates data across all communities

**BigQuery Table:** `combined.ga4_ad_data_pull`

## Running the Scripts

Each script can be run independently:

```
python SessionData.py
python WebEventData.py
python ga4_ad_data_pull.py
```

For regular updates, consider setting up cron jobs or scheduled tasks to run these scripts periodically.

## Data Model

The scripts create the following tables in the BigQuery dataset named `combined`:

- `SessionData` - Session data by community and date
- `WebEventData` - Web event data by community, event, and date
- `ga4_ad_data_pull` - Advertising data from Google Analytics 4

These tables can be joined using community IDs and dates for comprehensive reporting.

## Troubleshooting

- **Authentication Issues**: Ensure your service account has the necessary permissions and the key files are properly formatted.
- **API Rate Limits**: If you encounter rate limiting, consider adding delays between API calls or implementing exponential backoff.
- **BigQuery Errors**: Check that your service account has appropriate permissions to create/modify tables in the BigQuery dataset.

## License

This project is licensed under the terms of the license included in the repository.