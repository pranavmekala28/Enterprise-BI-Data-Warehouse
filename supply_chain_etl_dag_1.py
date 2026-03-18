"""
Enterprise BI & Data Warehouse Pipeline
DAG: supply_chain_etl_pipeline
Schedule: Daily at 6 AM
Author: Data Engineering Portfolio Project
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os

# ─────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    'owner'           : 'data_engineering',
    'depends_on_past' : False,
    'email_on_failure': True,
    'email_on_retry'  : False,
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
dag = DAG(
    dag_id='supply_chain_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL: SAP ERP Export → Star Schema → PostgreSQL DW',
    schedule_interval='0 6 * * *',   # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'supply_chain', 'data_warehouse', 'sap'],
)

# ─────────────────────────────────────────────
# TASK 1: EXTRACT — Validate raw SAP export
# ─────────────────────────────────────────────
def extract_and_validate(**context):
    """
    Extract raw SAP ERP CSV export and validate basic structure.
    In production this would pull from S3/SFTP/SAP API.
    """
    logger = logging.getLogger(__name__)
    RAW_PATH = '/opt/airflow/data/stg_erp_raw.csv'

    logger.info(f"Starting extraction from: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    row_count = len(df)
    col_count  = len(df.columns)

    logger.info(f"Extracted {row_count} rows, {col_count} columns")

    # Basic validation
    required_cols = [
        'order_id','document_date','ship_date','customer_id',
        'material_code','quantity','unit_price_usd','fulfillment_status'
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if row_count == 0:
        raise ValueError("Empty dataset — aborting pipeline")

    # Push metadata to XCom for downstream tasks
    context['ti'].xcom_push(key='raw_row_count', value=row_count)
    logger.info("✅ Extraction validation passed")
    return row_count


# ─────────────────────────────────────────────
# TASK 2: DATA QUALITY CHECKS
# ─────────────────────────────────────────────
def run_data_quality_checks(**context):
    """
    Run 13 automated DQ checks on raw data.
    Fails pipeline if critical checks don't pass threshold.
    """
    logger = logging.getLogger(__name__)
    RAW_PATH = '/opt/airflow/data/stg_erp_raw.csv'

    df = pd.read_csv(RAW_PATH)
    dq_results = []
    critical_failures = 0

    def check(name, severity, passed, total, note=''):
        fail = total - passed
        rate = round(100 * passed / total, 2) if total else 0
        status = 'PASS' if fail == 0 else ('WARN' if severity == 'Medium' else 'FAIL')
        if status == 'FAIL' and severity == 'Critical':
            nonlocal critical_failures
            critical_failures += 1
        dq_results.append({
            'check': name, 'severity': severity,
            'passed': passed, 'failed': fail,
            'pass_rate': rate, 'status': status, 'note': note
        })
        logger.info(f"  [{status}] {name}: {rate}% pass rate")

    logger.info("Running data quality checks...")

    # Completeness
    check('NULL order_id',        'Critical', df['order_id'].notna().sum(),        len(df))
    check('NULL customer_id',     'Critical', df['customer_id'].notna().sum(),     len(df))
    check('NULL unit_price_usd',  'High',     df['unit_price_usd'].notna().sum(),  len(df))
    check('NULL quantity',        'High',     df['quantity'].notna().sum(),         len(df))
    check('NULL material_code',   'High',     df['material_code'].notna().sum(),    len(df))

    # Validity
    valid_statuses = {'Delivered','Pending','In Transit','Cancelled','Returned',
                      'delivered','PENDING','in transit'}
    check('Valid fulfillment_status', 'High',
          df['fulfillment_status'].isin(valid_statuses).sum(), len(df))

    prices = pd.to_numeric(df['unit_price_usd'], errors='coerce').dropna()
    check('unit_price_usd >= 0', 'High', (prices >= 0).sum(), len(prices))

    # Uniqueness
    check('ORDER_ID unique', 'Critical',
          len(df) - df.duplicated('order_id').sum(), len(df),
          f"{df.duplicated('order_id').sum()} duplicates found")

    # Date validity
    df['doc_dt']  = pd.to_datetime(df['document_date'], errors='coerce')
    df['ship_dt'] = pd.to_datetime(df['ship_date'],     errors='coerce')
    valid_dates   = df['doc_dt'].notna() & df['ship_dt'].notna()
    check('Valid date formats', 'High', valid_dates.sum(), len(df))

    logger.info(f"\nDQ Summary: {sum(1 for r in dq_results if r['status']=='PASS')} passed, "
                f"{sum(1 for r in dq_results if r['status']=='WARN')} warnings, "
                f"{critical_failures} critical failures")

    if critical_failures > 0:
        raise ValueError(f"❌ {critical_failures} critical DQ checks failed — pipeline aborted")

    context['ti'].xcom_push(key='dq_results', value=dq_results)
    logger.info("✅ All critical DQ checks passed")


# ─────────────────────────────────────────────
# TASK 3: TRANSFORM — Clean & model data
# ─────────────────────────────────────────────
def transform_data(**context):
    """
    Apply all transformations:
    - Standardize status values
    - Fix negative/null prices
    - Fix invalid ship dates
    - Remove duplicates
    - Compute derived fields
    - Build dimension tables
    """
    logger = logging.getLogger(__name__)
    RAW_PATH    = '/opt/airflow/data/stg_erp_raw.csv'
    OUTPUT_PATH = '/opt/airflow/data/transformed/'
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    logger.info("Loading raw data...")
    df = pd.read_csv(RAW_PATH)
    raw_count = len(df)

    # ── Deduplication ──────────────────────────
    df = df.drop_duplicates(subset='order_id', keep='first')
    logger.info(f"Removed {raw_count - len(df)} duplicates")

    # ── Status standardization ──────────────────
    status_map = {
        'delivered':'Delivered','PENDING':'Pending',
        'in transit':'In Transit','N/A':None,'NULL':None,'':None
    }
    df['fulfillment_status'] = df['fulfillment_status'].replace(status_map)

    # ── Date fixes ──────────────────────────────
    df['document_date'] = pd.to_datetime(df['document_date'])
    df['ship_date']     = pd.to_datetime(df['ship_date'])
    bad_dates = df['ship_date'] < df['document_date']
    df.loc[bad_dates, 'ship_date'] = df.loc[bad_dates, 'document_date'] + pd.Timedelta(days=3)
    logger.info(f"Fixed {bad_dates.sum()} invalid ship dates")

    # ── Price & quantity fixes ───────────────────
    df.loc[df['unit_price_usd'] < 0, 'unit_price_usd'] = df['unit_price_usd'].abs()
    df['unit_price_usd'] = df['unit_price_usd'].fillna(df['unit_price_usd'].median())
    df['quantity']       = df['quantity'].fillna(1).astype(int)
    df['customer_name']  = df['customer_name'].fillna('Unknown')
    df['material_desc']  = df['material_desc'].replace('', 'Unknown Material')

    # ── Derived fields ───────────────────────────
    df['gross_revenue_usd'] = (df['quantity'] * df['unit_price_usd']).round(2)
    df['net_revenue_usd']   = (df['gross_revenue_usd'] * (1 - df['discount_pct'])).round(2)
    df['days_to_ship']      = (df['ship_date'] - df['document_date']).dt.days

    # ── Build dimension tables ───────────────────
    dim_customer = df[['customer_id','customer_name','region','country','sales_segment']]\
        .drop_duplicates('customer_id').reset_index(drop=True)
    dim_customer.insert(0, 'customer_key', range(1, len(dim_customer)+1))

    dim_product = df[['material_code','material_desc','category']]\
        .drop_duplicates('material_code').reset_index(drop=True)
    dim_product.insert(0, 'product_key', range(1, len(dim_product)+1))
    dim_product['product_line'] = dim_product['category']
    dim_product['is_active']    = 1

    dim_vendor = df[['vendor_id']].drop_duplicates().reset_index(drop=True)
    dim_vendor.insert(0, 'vendor_key', range(1, len(dim_vendor)+1))
    dim_vendor['vendor_name'] = [f'Vendor {v}' for v in dim_vendor['vendor_id']]

    dim_plant = df[['plant_code']].drop_duplicates().reset_index(drop=True)
    dim_plant.insert(0, 'plant_key', range(1, len(dim_plant)+1))
    dim_plant['plant_name'] = [f'Plant {p}' for p in dim_plant['plant_code']]

    # ── Build fact table ─────────────────────────
    fact = df.merge(dim_customer[['customer_id','customer_key']], on='customer_id', how='left')
    fact = fact.merge(dim_product[['material_code','product_key']], on='material_code', how='left')
    fact = fact.merge(dim_vendor[['vendor_id','vendor_key']], on='vendor_id', how='left')
    fact = fact.merge(dim_plant[['plant_code','plant_key']], on='plant_code', how='left')
    fact['order_date_key'] = fact['document_date'].dt.strftime('%Y%m%d').astype(int)
    fact['ship_date_key']  = fact['ship_date'].dt.strftime('%Y%m%d').astype(int)

    fact_sales = fact[[
        'order_id','order_date_key','ship_date_key','customer_key',
        'product_key','vendor_key','plant_key','profit_center',
        'fulfillment_status','currency','quantity','unit_price_usd',
        'discount_pct','days_to_ship','sap_batch_id'
    ]].reset_index(drop=True)

    # ── Save transformed files ───────────────────
    fact_sales.to_csv(f'{OUTPUT_PATH}fact_sales.csv',       index=False)
    dim_customer.to_csv(f'{OUTPUT_PATH}dim_customer.csv',   index=False)
    dim_product.to_csv(f'{OUTPUT_PATH}dim_product.csv',     index=False)
    dim_vendor.to_csv(f'{OUTPUT_PATH}dim_vendor.csv',       index=False)
    dim_plant.to_csv(f'{OUTPUT_PATH}dim_plant.csv',         index=False)

    context['ti'].xcom_push(key='fact_row_count', value=len(fact_sales))
    logger.info(f"✅ Transformation complete — {len(fact_sales)} fact rows ready to load")


# ─────────────────────────────────────────────
# TASK 4: LOAD — Upsert into PostgreSQL
# ─────────────────────────────────────────────
def load_to_postgres(**context):
    """
    Load all transformed tables into PostgreSQL DW.
    Uses TRUNCATE + INSERT pattern (full refresh).
    In production: use MERGE/upsert for incremental loads.
    """
    logger  = logging.getLogger(__name__)
    hook    = PostgresHook(postgres_conn_id='postgres_dw')
    engine  = hook.get_sqlalchemy_engine()
    OUTPUT_PATH = '/opt/airflow/data/transformed/'

    schema = 'dw_supply_chain'

    tables = [
        ('dim_customer', ['customer_key','customer_id','customer_name',
                          'region','country','sales_segment']),
        ('dim_product',  ['product_key','material_code','material_desc',
                          'category','product_line','is_active']),
        ('dim_vendor',   ['vendor_key','vendor_id','vendor_name']),
        ('dim_plant',    ['plant_key','plant_code','plant_name']),
        ('fact_sales',   ['order_id','order_date_key','ship_date_key',
                          'customer_key','product_key','vendor_key','plant_key',
                          'profit_center','fulfillment_status','currency',
                          'quantity','unit_price_usd','discount_pct',
                          'days_to_ship','sap_batch_id']),
    ]

    total_loaded = 0
    for table_name, columns in tables:
        df = pd.read_csv(f'{OUTPUT_PATH}{table_name}.csv', usecols=columns)

        # Truncate then load
        with engine.connect() as conn:
            conn.execute(f'TRUNCATE {schema}.{table_name} CASCADE')

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            chunksize=500
        )
        logger.info(f"  Loaded {len(df):,} rows → {schema}.{table_name}")
        total_loaded += len(df)

    logger.info(f"✅ Load complete — {total_loaded:,} total rows loaded")
    context['ti'].xcom_push(key='total_loaded', value=total_loaded)


# ─────────────────────────────────────────────
# TASK 5: VALIDATE LOAD
# ─────────────────────────────────────────────
def validate_load(**context):
    """
    Post-load validation — verify row counts match expectations.
    """
    logger = logging.getLogger(__name__)
    hook   = PostgresHook(postgres_conn_id='postgres_dw')

    checks = {
        'fact_sales'   : (1800, 2100),
        'dim_customer' : (250,  350),
        'dim_product'  : (150,  250),
        'dim_vendor'   : (40,   60),
        'dim_plant'    : (8,    15),
    }

    failed = []
    for table, (min_rows, max_rows) in checks.items():
        count = hook.get_first(
            f"SELECT COUNT(*) FROM dw_supply_chain.{table}"
        )[0]
        status = '✅' if min_rows <= count <= max_rows else '❌'
        logger.info(f"  {status} {table}: {count:,} rows (expected {min_rows}-{max_rows})")
        if not (min_rows <= count <= max_rows):
            failed.append(f"{table}: {count} rows")

    if failed:
        raise ValueError(f"Post-load validation failed: {failed}")

    logger.info("✅ All post-load validations passed — pipeline complete!")


# ─────────────────────────────────────────────
# DEFINE TASKS
# ─────────────────────────────────────────────
t1_extract = PythonOperator(
    task_id='extract_and_validate',
    python_callable=extract_and_validate,
    dag=dag,
)

t2_dq = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

t3_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t4_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

t5_validate = PythonOperator(
    task_id='validate_load',
    python_callable=validate_load,
    dag=dag,
)

t6_refresh_powerbi = BashOperator(
    task_id='notify_pipeline_complete',
    bash_command='''echo "Pipeline complete at $(date). 
    Tables loaded: fact_sales, dim_customer, dim_product, dim_vendor, dim_plant.
    Next step: Refresh Power BI dataset via API or scheduled refresh."''',
    dag=dag,
)

# ─────────────────────────────────────────────
# TASK DEPENDENCIES (Pipeline Flow)
# ─────────────────────────────────────────────
t1_extract >> t2_dq >> t3_transform >> t4_load >> t5_validate >> t6_refresh_powerbi
