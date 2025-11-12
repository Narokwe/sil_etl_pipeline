#Import the Operating system library (os)  and datetime library
import os
from datetime import datetime

# Google Cloud info
GCP_PROJECT_ID = "sil-data-engineering-2025"
GCS_BUCKET_NAME = "sil-data-bucket"
BQ_DATASET = "savannah_analytics"

# Google Cloud Storage (Data Lake) Raw data paths
GCS_RAW_USERS_PATH = "raw/users/"
GCS_RAW_PRODUCTS_PATH = "raw/products/"
GCS_RAW_CARTS_PATH = "raw/carts/"

#BigQuery Cleaned Tables Paths
BQ_CLEAN_USERS_TABLE = "users_table"
BQ_CLEAN_PRODUCTS_TABLE = "products_table"
BQ_CLEAN_CARTS_TABLE = "carts_table"

#BigQuery Final Reports
BQ_USER_SUMMARY_TABLE = "user_summary"
BQ_CATEGORY_SUMMARY_TABLE = "category_summary"
BQ_CART_DETAILS_TABLE = "cart_details"

# Incremental Loading Tables
BQ_METADATA_TABLE = "pipeline_metadata"
BQ_STAGING_USERS_TABLE = "staging_users"
BQ_STAGING_PRODUCTS_TABLE = "staging_products"
BQ_STAGING_CARTS_TABLE = "staging_carts"

# API Endpoints
API_URLS = {
    'users': 'https://dummyjson.com/users',
    'products': 'https://dummyjson.com/products', 
    'carts': 'https://dummyjson.com/carts'
}

# Incremental loading configuration
INCREMENTAL_CONFIG = {
    'users': {
        'incremental_key': 'id',
        'timestamp_field': 'updated_at',
        'lookback_days': 30
    },
    'products': {
        'incremental_key': 'id', 
        'timestamp_field': 'updated_at',
        'lookback_days': 30
    },
    'carts': {
        'incremental_key': 'id',
        'timestamp_field': 'updated_at', 
        'lookback_days': 7
    }
}