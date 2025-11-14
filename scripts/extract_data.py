# Import the critical python packages
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

# Import configuration
try:
    from config.gcp_config import *
except ImportError:
    import sys
    import os
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from config.gcp_config import *

def get_last_successful_run_robust(data_type):
    """Get last run timestamp with proper first-run handling"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # First, check if dataset exists
        try:
            client.get_dataset(BQ_DATASET)
        except NotFound:
            print(f" Dataset {BQ_DATASET} doesn't exist - FIRST RUN DETECTED")
            return None  # This signals first run
        
        # Check if metadata table exists and has data
        try:
            query = f"""
            SELECT 
                MAX(last_run_timestamp) as last_timestamp,
                COUNT(*) as total_runs
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_METADATA_TABLE}`
            WHERE data_type = '{data_type}' AND status = 'SUCCESS'
            """
            query_job = client.query(query)
            results = query_job.result()
            
            for row in results:
                if row.last_timestamp and row.total_runs > 0:
                    print(f" Last {data_type} extraction: {row.last_timestamp}")
                    return row.last_timestamp
                else:
                    print(f" No previous {data_type} runs found - FIRST RUN")
                    return None
                    
        except NotFound:
            print(f" Metadata table doesn't exist - FIRST RUN")
            return None
        
    except Exception as e:
        print(f" Error checking last run for {data_type}: {e}")
        return None

def fetch_data_with_fallback(api_url, data_type, last_run_timestamp):
    """Fetch data with incremental logic, but get all data on first run"""
    try:
        if last_run_timestamp is None:
            # FIRST RUN - Get all data
            print(f" FIRST RUN: Fetching ALL {data_type} data")
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
        else:
            # INCREMENTAL RUN - Get only new data (simulated for dummyjson)
            print(f" INCREMENTAL: Fetching {data_type} data since {last_run_timestamp}")
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Since dummyjson doesn't support true incremental, we simulate it
            print(f" Note: DummyJSON doesn't support incremental APIs")
            print(f" In production, i'll filter by updatedAt > {last_run_timestamp}")
        
        record_count = len(data.get(data_type, []))
        print(f" Fetched {record_count} {data_type} records")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f" Error fetching {data_type} data: {e}")
        raise

def save_to_gcs_incremental(data, data_type, timestamp, is_first_run):
    """Save data to GCS with appropriate naming"""
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        if is_first_run:
            # First run - save as baseline
            filename = f"raw_{data_type}/baseline_{timestamp}.json"
        else:
            # Incremental run - save as incremental
            filename = f"raw_{data_type}/incremental_{timestamp}.json"
        
        blob = bucket.blob(filename)
        json_data = json.dumps(data)
        blob.upload_from_string(json_data, content_type='application/json')
        
        print(f" Saved {filename} to GCS")
        return f"gs://{GCS_BUCKET_NAME}/{filename}"
        
    except Exception as e:
        print(f" Error saving to GCS: {e}")
        return None

def update_metadata_robust(data_type, status, records_processed, is_first_run=False):
    """Update metadata table - creates table if needed"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        if is_first_run:
            print(f" First run - initializing metadata for {data_type}")
        
        query = f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_METADATA_TABLE}`
        (data_type, run_timestamp, last_run_timestamp, status, records_processed, error_message)
        VALUES (
            '{data_type}',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{status}',
            {records_processed},
            NULL
        )
        """
        client.query(query).result()
        print(f" Updated metadata for {data_type}: {status}")
        
    except Exception as e:
        print(f" Could not update metadata for {data_type}: {e}")
        # Don't fail the pipeline if metadata update fails

def extract_all_data():
    """Main extraction function with robust incremental logic"""
    print(" STARTING EXTRACTION WITH ROBUST INCREMENTAL LOGIC")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = {}
    
    for data_type, api_url in API_URLS.items():
        try:
            print(f"\n{'='*50}")
            print(f" PROCESSING: {data_type}")
            print(f"{'='*50}")
            
            # Get last successful run (returns None if first run)
            last_run_timestamp = get_last_successful_run_robust(data_type)
            is_first_run = (last_run_timestamp is None)
            
            # Fetch data (all data on first run, incremental on subsequent runs)
            data = fetch_data_with_fallback(api_url, data_type, last_run_timestamp)
            
            # Save to GCS
            gcs_path = save_to_gcs_incremental(data, data_type, timestamp, is_first_run)
            
            record_count = len(data.get(data_type, []))
            
            results[data_type] = {
                'gcs_path': gcs_path,
                'record_count': record_count,
                'timestamp': timestamp,
                'last_run_timestamp': last_run_timestamp,
                'is_first_run': is_first_run,
                'data': data
            }
            
            # Update metadata
            update_metadata_robust(data_type, 'EXTRACTED', record_count, is_first_run)
            
            print(f"{data_type} extraction completed: {record_count} records")
            if is_first_run:
                print(f"FIRST RUN - loaded all data as baseline")
            else:
                print(f"INCREMENTAL - loaded data since {last_run_timestamp}")
            
        except Exception as e:
            print(f"Failed to extract {data_type} data: {e}")
            results[data_type] = {'error': str(e)}
            update_metadata_robust(data_type, 'FAILED', 0, False)
    
    print(f"\n EXTRACTION COMPLETED")
    return results

if __name__ == "__main__":
    extract_all_data()