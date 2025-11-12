# Import the required libraries
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime

# Import configuration with error handling
try:
    from config.gcp_config import *
except ImportError:
    # Fallback for when running directly or in VSCode
    import sys
    import os
    # Add project root to path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from config.gcp_config import *

def create_metadata_table():
    """Create pipeline metadata table"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    schema = [
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("last_run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("records_processed", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]
    
    table_ref = client.dataset(BQ_DATASET).table(BQ_METADATA_TABLE)
    try:
        client.get_table(table_ref)
        print(f"Table {BQ_METADATA_TABLE} already exists")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {BQ_METADATA_TABLE}")

def create_bq_tables_if_not_exist():
    """Create BigQuery tables if they don't exist with incremental support"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    dataset_ref = client.dataset(BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {BQ_DATASET} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {BQ_DATASET}")

    create_metadata_table()

    users_schema = [
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    products_schema = [
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("brand", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    carts_schema = [
        bigquery.SchemaField("cart_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("total_cart_value", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    staging_users_schema = users_schema
    staging_products_schema = products_schema
    staging_carts_schema = carts_schema
    
    tables_config = {
        BQ_CLEAN_USERS_TABLE: users_schema,
        BQ_CLEAN_PRODUCTS_TABLE: products_schema,
        BQ_CLEAN_CARTS_TABLE: carts_schema,
        BQ_STAGING_USERS_TABLE: staging_users_schema,
        BQ_STAGING_PRODUCTS_TABLE: staging_products_schema,
        BQ_STAGING_CARTS_TABLE: staging_carts_schema,
    }
    
    for table_name, schema in tables_config.items():
        table_ref = dataset_ref.table(table_name)
        try:
            client.get_table(table_ref)
            print(f"Table {table_name} already exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_name}")

def is_table_empty(table_name):
    """Check if a table is empty (first run detection)"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"SELECT COUNT(*) as count FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            return row.count == 0
    except Exception as e:
        print(f"Could not check if {table_name} is empty: {e}")
        return True  
    
    return False

def load_to_staging(df, staging_table_name):
    """Load data to staging table"""
    if df is None or df.empty:
        print(f"No data to load to staging table {staging_table_name}")
        return 0
        
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{staging_table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows to staging table {staging_table_name}")
        return table.num_rows
        
    except Exception as e:
        print(f"Error loading data to staging table {staging_table_name}: {e}")
        raise

def load_direct_insert(df, target_table_name):
    """Direct INSERT for first run (faster than MERGE on empty tables)"""
    if df is None or df.empty:
        print(f"No data to load to {target_table_name}")
        return 0
        
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{target_table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        table = client.get_table(table_id)
        print(f"FIRST RUN: Loaded {table.num_rows} rows to {target_table_name} via direct INSERT")
        return table.num_rows
        
    except Exception as e:
        print(f"Error loading data to {target_table_name}: {e}")
        raise

def merge_from_staging(target_table, staging_table, merge_key):
    """Merge data from staging to target table for incremental runs"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        query = f"""
        MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{target_table}` T
        USING `{GCP_PROJECT_ID}.{BQ_DATASET}.{staging_table}` S
        ON T.{merge_key} = S.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET 
                {get_update_columns(target_table)}
        WHEN NOT MATCHED THEN
            INSERT ({get_insert_columns(target_table)})
            VALUES ({get_insert_columns(staging_table)})
        """
        
        query_job = client.query(query)
        query_job.result()
        
        # Get merge statistics
        updated_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else "unknown"
        
        print(f"INCREMENTAL: Merge completed for {target_table}. Rows affected: {updated_rows}")
        
    except Exception as e:
        print(f"Error merging data from staging to {target_table}: {e}")
        raise

def get_update_columns(table_name):
    """Get UPDATE column list based on table"""
    if 'users' in table_name:
        return """
            first_name = S.first_name,
            last_name = S.last_name, 
            gender = S.gender,
            age = S.age,
            street = S.street,
            city = S.city,
            postal_code = S.postal_code,
            load_timestamp = S.load_timestamp
        """
    elif 'products' in table_name:
        return """
            name = S.name,
            category = S.category,
            brand = S.brand, 
            price = S.price,
            load_timestamp = S.load_timestamp
        """
    elif 'carts' in table_name:
        return """
            user_id = S.user_id,
            product_id = S.product_id,
            quantity = S.quantity,
            price = S.price,
            total_cart_value = S.total_cart_value,
            load_timestamp = S.load_timestamp
        """
    else:
        raise ValueError(f"Unknown table: {table_name}")

def get_insert_columns(table_name):
    """Get INSERT column list based on table"""
    if 'users' in table_name:
        return "user_id, first_name, last_name, gender, age, street, city, postal_code, load_timestamp"
    elif 'products' in table_name:
        return "product_id, name, category, brand, price, load_timestamp"
    elif 'carts' in table_name:
        return "cart_id, user_id, product_id, quantity, price, total_cart_value, load_timestamp"
    else:
        raise ValueError(f"Unknown table: {table_name}")

def load_incremental_data(transformed_data):
    """Load transformed data with smart first-run detection"""
    create_bq_tables_if_not_exist()
    
    load_results = {}
    
    table_mapping = {
        'users': {
            'staging': BQ_STAGING_USERS_TABLE,
            'target': BQ_CLEAN_USERS_TABLE,
            'merge_key': 'user_id'
        },
        'products': {
            'staging': BQ_STAGING_PRODUCTS_TABLE, 
            'target': BQ_CLEAN_PRODUCTS_TABLE,
            'merge_key': 'product_id'
        },
        'carts': {
            'staging': BQ_STAGING_CARTS_TABLE,
            'target': BQ_CLEAN_CARTS_TABLE,
            'merge_key': 'cart_id'
        }
    }
    
    for data_type, df in transformed_data.items():
        if df is None or df.empty:
            print(f"No data to load for {data_type}")
            load_results[data_type] = 0
            continue
            
        try:
            config = table_mapping[data_type]
            target_table = config['target']
            
            # Check if this is first run (table is empty)
            is_first_run = is_table_empty(target_table)
            
            if is_first_run:
                print(f"FIRST RUN DETECTED for {data_type}")
                print(f"   Using DIRECT INSERT (faster for empty tables)")
                
                # First run - use direct insert (faster)
                records_loaded = load_direct_insert(df, target_table)
                
            else:
                print(f"INCREMENTAL RUN DETECTED for {data_type}")
                print(f"   Using MERGE pattern for incremental load")
                
                # Incremental run - use staging + merge
                records_loaded = load_to_staging(df, config['staging'])
                merge_from_staging(config['target'], config['staging'], config['merge_key'])
            
            load_results[data_type] = records_loaded
            
            print(f"Successfully loaded {records_loaded} records for {data_type}")
            
        except Exception as e:
            print(f"Failed to load {data_type} data: {e}")
            load_results[data_type] = 0
    
    # Print summary
    print("\n" + "="*50)
    print("LOADING SUMMARY")
    print("="*50)
    total_records = sum(load_results.values())
    print(f"Total records loaded: {total_records}")
    for data_type, count in load_results.items():
        print(f"  {data_type}: {count} records")
    print("="*50)
    
    return load_results

def load_all_data(transformed_data):
    """Main load function with incremental support"""
    return load_incremental_data(transformed_data)