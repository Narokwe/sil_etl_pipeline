# Import bigquery package
from google.cloud import bigquery

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

def execute_bq_query(query):
    """Execute BigQuery query and return results"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query_job = client.query(query)
    return query_job.result()

def create_incremental_user_summary():
    """Create incremental user summary using latest data"""
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_USER_SUMMARY_TABLE}` AS
    WITH latest_carts AS (
        SELECT 
            user_id,
            SUM(total_cart_value) as total_spent,
            SUM(quantity) as total_items
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_CARTS_TABLE}`
        GROUP BY user_id
    )
    SELECT 
        u.user_id,
        u.first_name,
        COALESCE(lc.total_spent, 0) as total_spent,
        COALESCE(lc.total_items, 0) as total_items,
        u.age,
        u.city,
        u.load_timestamp as last_updated
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_USERS_TABLE}` u
    LEFT JOIN latest_carts lc ON u.user_id = lc.user_id
    ORDER BY total_spent DESC
    """
    execute_bq_query(query)
    print("Created/Updated user_summary table")

def create_incremental_category_summary():
    """Create incremental category summary"""
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CATEGORY_SUMMARY_TABLE}` AS
    SELECT 
        p.category,
        SUM(c.total_cart_value) as total_sales,
        SUM(c.quantity) as total_items_sold,
        CURRENT_TIMESTAMP() as last_updated
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_PRODUCTS_TABLE}` p
    JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_CARTS_TABLE}` c 
        ON p.product_id = c.product_id
    GROUP BY p.category
    ORDER BY total_sales DESC
    """
    execute_bq_query(query)
    print("Created/Updated category_summary table")

def create_incremental_cart_details():
    """Create incremental cart details"""
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CART_DETAILS_TABLE}` AS
    SELECT 
        c.cart_id,
        c.user_id,
        c.product_id,
        c.quantity,
        c.price,
        c.total_cart_value
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_CARTS_TABLE}` c
    ORDER BY c.cart_id, c.product_id
    """
    execute_bq_query(query)
    print("Created/Updated cart_details table")

def run_all_analyses():
    """Run all analysis queries with incremental support"""
    print("Running analysis queries...")
    create_incremental_user_summary()
    create_incremental_category_summary()
    create_incremental_cart_details()
    print("All analysis tables updated successfully!")