# Savannah Informatics - Data Engineering Technical Assessment

## Project Overview
A complete ETL pipeline that processes user, product, and cart data from multiple APIs. The pipeline extracts data, loads it into the data lake (Google Cloud Storage bucket), cleans and normalizes it, loads it into the data warehouse (BigQuery), and generates business insights through automated analysis.

## Pipeline Architecture
API Sources -> Cloud Storage (Raw Data) -> BigQuery (Cleaned Data) -> BigQuery (Analysis Reports)

## Technology Stack
- **Orchestration**: Apache Airflow
- **Cloud Storage**: Google Cloud Storage (GCS)
- **Data Warehouse**: Google BigQuery
- **Processing**: Python 3.8+
- **APIs**: DummyJSON REST APIs

## 📁 Project Structure
sil_dataengineering_technical_assessment_Augustine_Narokwe/
├── dags/
│ └── savannah_etl_pipeline.py 
├── scripts/
│ ├── extract_data.py 
│ ├── transform_data.py
│ ├── load_data.py
│ └── queries.py
├── config/
│ └── gcp_config.py
├── requirements.txt 
└── README.md 

## Quick Start

### Prerequisites
- Google Cloud Platform account with BigQuery and GCS access
- Python 3.8+
- Apache Airflow 

### Installation
```bash
# Clone repository
git clone <repository-url>
cd sil_dataengineering_technical_assessment_Augustine_Narokwe

# Install dependencies
pip install -r requirements.txt

# Set up Airflow (This is optional though)
export AIRFLOW_HOME=~/airflow
airflow db init
Running the Pipeline
Option A: Using Airflow (Recommended)


# Copy DAG to Airflow directory
cp dags/savannah_etl_pipeline.py $AIRFLOW_HOME/dags/

# Start Airflow services
airflow scheduler &
airflow webserver --port 8080 &

# Access Airflow UI at http://localhost:8080 and trigger the DAG
Option B: Direct Python Execution

python run_pipeline.py


Data Processing Steps

1. Data Extraction
- Fetches data from Users, Products, and Carts APIs

- Implements incremental loading to optimize performance

- Stores raw JSON data in Google Cloud Storage

2. Data Transformation

- Users: Flattens address into street, city, postal_code

- Products: Filters out products with price <= 50

- Carts: Expands products array into individual rows, calculates total_cart_value

3. Data Loading

- Uses incremental MERGE operations for efficient data updates

- Maintains metadata for pipeline tracking

- Implements staging tables for data validation

- Loads data into the data warehouse (BigQuery)

4. Analysis & Reporting
- User Summary: Total spending and purchase counts per user

- Category Summary: Sales performance by product category

- Cart Details: Transaction-level insights

Configuration
Update config/gcp_config.py with your GCP settings:

GCP_PROJECT_ID = "your-project-id"
GCS_BUCKET_NAME = "your-bucket-name"
BQ_DATASET = "your_tables_container" 

Results & Insights
After pipeline execution, the following BigQuery tables are generated:

Cleaned Data Tables
- users_table: 30 customer records with normalized addresses

- products_table: Almost 10 products filtered to price > 50

- carts_table: 50 cart items with calculated totals

Analysis Reports
- user_summary: Customer spending behavior and demographics

- category_summary: Product category performance metrics

- cart_details: Detailed transaction records

Key Features
- Incremental Processing: Only processes new/changed data

- Error Handling: Robust error handling and retry mechanisms (Emergency notifications using 'email_on_failure': False -For now it is set False in the ready production. Also There is Error Recovery Settings using 'retries': 2  -DAG Tries two more times before giving up. And also, 'retry_delay': timedelta (minutes=3))

- Modular Design: Separated concerns for maintainability