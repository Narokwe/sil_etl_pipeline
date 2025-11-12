# Import the necessary libraries
import pandas as pd
import json
from datetime import datetime
from google.cloud import storage, bigquery

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

def get_max_ids_from_target():
    """Get maximum IDs from target tables for incremental processing"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        max_ids = {}
        
        query = f"SELECT COALESCE(MAX(user_id), 0) as max_id FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_USERS_TABLE}`"
        result = client.query(query).result()
        for row in result:
            max_ids['users'] = row.max_id
        
        query = f"SELECT COALESCE(MAX(product_id), 0) as max_id FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_PRODUCTS_TABLE}`"
        result = client.query(query).result()
        for row in result:
            max_ids['products'] = row.max_id
            
        query = f"SELECT COALESCE(MAX(cart_id), 0) as max_id FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CLEAN_CARTS_TABLE}`"
        result = client.query(query).result()
        for row in result:
            max_ids['carts'] = row.max_id
            
        return max_ids
        
    except Exception as e:
        print(f"Error getting max IDs: {e}")
        return {'users': 0, 'products': 0, 'carts': 0}

def transform_users_data(raw_data, max_user_id):
    """Transform and clean users data with incremental logic"""
    try:
        users_list = raw_data.get('users', [])
        
        if not users_list:
            return pd.DataFrame()
            
        df = pd.json_normalize(users_list)
        
        df = df[df['id'] > max_user_id]
        
        if df.empty:
            print("No new user records to transform")
            return pd.DataFrame()
        
        users_clean = pd.DataFrame({
            'user_id': df['id'],
            'first_name': df['firstName'],
            'last_name': df['lastName'],
            'gender': df['gender'],
            'age': df['age'],
            'street': df['address.address'],
            'city': df['address.city'],
            'postal_code': df['address.postalCode'],
            'load_timestamp': datetime.now()
        })
        
        print(f"Transformed {len(users_clean)} new user records")
        return users_clean
        
    except Exception as e:
        print(f"Error transforming users data: {e}")
        raise

def transform_products_data(raw_data, max_product_id):
    """Transform and clean products data with incremental logic"""
    try:
        products_list = raw_data.get('products', [])
        
        if not products_list:
            return pd.DataFrame()
            
        df = pd.json_normalize(products_list)
        
        df = df[(df['id'] > max_product_id) & (df['price'] > 50)]
        
        if df.empty:
            print("No new product records to transform")
            return pd.DataFrame()
        
        products_clean = pd.DataFrame({
            'product_id': df['id'],
            'name': df['title'],
            'category': df['category'],
            'brand': df['brand'],
            'price': df['price'],
            'load_timestamp': datetime.now()
        })
        
        print(f"Transformed {len(products_clean)} new product records")
        return products_clean
        
    except Exception as e:
        print(f"Error transforming products data: {e}")
        raise

def transform_carts_data(raw_data, max_cart_id):
    """Transform and clean carts data with incremental logic"""
    try:
        carts_list = raw_data.get('carts', [])
        
        if not carts_list:
            return pd.DataFrame()
            
        exploded_data = []
        
        for cart in carts_list:
            if cart['id'] <= max_cart_id:
                continue
                
            cart_id = cart['id']
            user_id = cart['userId']
            total = cart['total']
            
            for product in cart['products']:
                exploded_data.append({
                    'cart_id': cart_id,
                    'user_id': user_id,
                    'product_id': product['id'],
                    'quantity': product['quantity'],
                    'price': product['price'],
                    'total_cart_value': total,
                    'load_timestamp': datetime.now()
                })
        
        if not exploded_data:
            print("No new cart records to transform")
            return pd.DataFrame()
            
        carts_clean = pd.DataFrame(exploded_data)
        print(f"Transformed {len(carts_clean)} new cart product records")
        return carts_clean
        
    except Exception as e:
        print(f"Error transforming carts data: {e}")
        raise

def transform_all_data(extraction_results):
    """Transform all datasets with incremental logic"""
    transformed_data = {}
    
    max_ids = get_max_ids_from_target()
    
    for data_type, result in extraction_results.items():
        if 'error' in result:
            print(f"Skipping {data_type} due to previous error")
            continue
            
        try:
            raw_data = load_json_from_gcs(result['gcs_path'])
            
            if data_type == 'users':
                transformed_data[data_type] = transform_users_data(raw_data, max_ids['users'])
            elif data_type == 'products':
                transformed_data[data_type] = transform_products_data(raw_data, max_ids['products'])
            elif data_type == 'carts':
                transformed_data[data_type] = transform_carts_data(raw_data, max_ids['carts'])
                
        except Exception as e:
            print(f"Failed to transform {data_type} data: {e}")
            transformed_data[data_type] = None
    
    return transformed_data

def load_json_from_gcs(gcs_path):
    """Load JSON data from GCS"""
    try:
        path_parts = gcs_path.replace("gs://", "").split("/", 1)
        bucket_name = path_parts[0]
        blob_path = path_parts[1]
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        json_data = json.loads(blob.download_as_string())
        return json_data
        
    except Exception as e:
        print(f"Error loading from GCS {gcs_path}: {e}")
        raise