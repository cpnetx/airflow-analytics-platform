"""
Airflow DAG for initializing new maintenance tenant.

This DAG:
1. Creates tenant schema
2. Downloads Excel files from Azure Blob Storage
3. Detects schema and creates tables
4. Loads data into PostgreSQL
5. Generates embeddings for semantic search
6. Configures default joins and views
7. Sends webhook notification on completion
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# Import task functions
from maintenance_tasks.file_handlers import (
    download_files_from_azure,
    prepare_local_file_paths,
    cleanup_temporary_files
)
from maintenance_tasks.schema_detector import detect_and_create_schema
from maintenance_tasks.data_loader import load_excel_data
from maintenance_tasks.embeddings import generate_notification_embeddings
from maintenance_tasks.kpi_calculator import calculate_maintenance_kpis
from maintenance_tasks.webhook_handler import send_webhook_notification

# Default args for all tasks
default_args = {
    'owner': 'maintenance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'maintenance_tenant_init',
    default_args=default_args,
    description='Initialize new maintenance tenant with CMMS data',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=2,
    tags=['maintenance', 'tenant', 'initialization']
)

# Task 1: Create tenant schema
create_schema = SQLExecuteQueryOperator(
    task_id='create_tenant_schema',
    conn_id='maintenance_db',
    sql="""
    -- Create schema for tenant
    CREATE SCHEMA IF NOT EXISTS maint_{{ dag_run.conf.org_id }};
    
    -- Set search path
    SET search_path TO maint_{{ dag_run.conf.org_id }}, public;
    
    -- Create pgvector extension if not exists
    CREATE EXTENSION IF NOT EXISTS vector;
    
    -- Create metadata tables
    CREATE TABLE IF NOT EXISTS _meta_columns (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        pg_type TEXT NOT NULL,
        inferred_from TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (table_name, column_name)
    );
    
    CREATE TABLE IF NOT EXISTS _meta_joins (
        view_name TEXT PRIMARY KEY,
        left_table TEXT NOT NULL,
        right_table TEXT NOT NULL,
        left_key TEXT NOT NULL,
        right_key TEXT NOT NULL,
        join_type TEXT NOT NULL DEFAULT 'inner',
        any_distinct BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS _meta_uploads (
        upload_id SERIAL PRIMARY KEY,
        file_name TEXT NOT NULL,
        file_type TEXT NOT NULL,
        table_name TEXT NOT NULL,
        row_count INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        uploaded_by TEXT
    );
    """,
    dag=dag
)

# Task 2: Choose file handler based on storage type
def choose_file_handler(**context):
    """Choose file handler based on storage type."""
    storage_type = context['dag_run'].conf.get('storage_type', 'azure')
    if storage_type == 'azure':
        return 'download_from_azure'
    else:
        return 'prepare_local_files'

choose_handler = BranchPythonOperator(
    task_id='choose_file_handler',
    python_callable=choose_file_handler,
    dag=dag
)

# Task 3a: Download files from Azure
download_from_azure = PythonOperator(
    task_id='download_from_azure',
    python_callable=lambda **context: context['task_instance'].xcom_push(
        key='file_paths',
        value=download_files_from_azure(**context)
    ),
    op_kwargs={},
    dag=dag
)

# Task 3b: Prepare local files (alternative path)
prepare_local_files = PythonOperator(
    task_id='prepare_local_files',
    python_callable=lambda **context: context['task_instance'].xcom_push(
        key='file_paths',
        value=prepare_local_file_paths(**context)
    ),
    op_kwargs={},
    dag=dag
)

# Task 4: Rejoin paths
files_ready = EmptyOperator(
    task_id='files_ready',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# Task 5: Detect and create schema for orders
detect_schema_orders = PythonOperator(
    task_id='detect_schema_orders',
    python_callable=lambda **context: detect_and_create_schema(
        **context, file_type='orders'
    ),
    op_kwargs={'file_type': 'orders'},
    dag=dag
)

# Task 6: Detect and create schema for notifications
detect_schema_notifs = PythonOperator(
    task_id='detect_schema_notifs',
    python_callable=lambda **context: detect_and_create_schema(
        **context, file_type='notifs'
    ),
    op_kwargs={'file_type': 'notifs'},
    dag=dag
)

# Task 7: Load orders data
load_orders = PythonOperator(
    task_id='load_orders_data',
    python_callable=lambda **context: load_excel_data(
        **context, file_type='orders'
    ),
    op_kwargs={'file_type': 'orders'},
    dag=dag
)

# Task 8: Load notifications data
load_notifs = PythonOperator(
    task_id='load_notifs_data',
    python_callable=lambda **context: load_excel_data(
        **context, file_type='notifs'
    ),
    op_kwargs={'file_type': 'notifs'},
    dag=dag
)

# Task 9: Generate embeddings
generate_embeddings = PythonOperator(
    task_id='generate_embeddings',
    python_callable=lambda **context: generate_notification_embeddings(**context),
    op_kwargs={},
    dag=dag
)

# Task 10: Create default views
create_views = SQLExecuteQueryOperator(
    task_id='create_default_views',
    conn_id='maintenance_db',
    sql="""
    SET search_path TO maint_{{ dag_run.conf.org_id }}, public;
    
    -- Insert default join configuration
    INSERT INTO _meta_joins (view_name, left_table, right_table, left_key, right_key, join_type)
    VALUES 
        ('v_order_notif', 'work_orders', 'notification', 'order_number', 'order_id', 'left'),
        ('v_equipment_orders', 'equipment', 'work_orders', 'equipment_id', 'equipment_id', 'left'),
        ('v_equipment_reliability', 'equipment', 'work_orders', 'equipment_id', 'equipment_id', 'inner')
    ON CONFLICT DO NOTHING;
    
    -- Create views
    CREATE OR REPLACE VIEW v_order_notif AS
    SELECT DISTINCT ON (w.order_number)
        w.*,
        n.notif_id,
        n.description as notif_description,
        n.notification_type,
        n.priority as notif_priority,
        n.created_date as notif_created_date
    FROM work_orders w
    LEFT JOIN notification n ON w.order_number = n.order_id
    ORDER BY w.order_number, n.created_date DESC;
    
    CREATE OR REPLACE VIEW v_equipment_orders AS
    SELECT 
        e.*,
        COUNT(DISTINCT w.order_number) as total_orders,
        SUM(w.cost) as total_cost,
        SUM(w.actual_hours) as total_hours,
        AVG(w.actual_hours) as avg_hours_per_order
    FROM equipment e
    LEFT JOIN work_orders w ON e.equipment_id = w.equipment_id
    GROUP BY e.id, e.equipment_id, e.name, e.type, e.manufacturer, 
             e.model, e.location, e.criticality, e.metadata, e.created_at, e.updated_at;
    """,
    dag=dag
)

# Task 11: Calculate initial KPIs
calculate_kpis = PythonOperator(
    task_id='calculate_initial_kpis',
    python_callable=lambda **context: calculate_maintenance_kpis(**context),
    op_kwargs={},
    dag=dag
)

# Task 12: Send webhook notification
send_webhook = PythonOperator(
    task_id='send_completion_webhook',
    python_callable=lambda **context: send_webhook_notification(**context),
    op_kwargs={},
    trigger_rule='none_failed',  # Send even if some tasks failed
    dag=dag
)

# Task 13: Cleanup temporary files
cleanup_files = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=lambda **context: cleanup_temporary_files(**context),
    op_kwargs={},
    trigger_rule='all_done',  # Always run cleanup
    dag=dag
)

# Define task dependencies
create_schema >> choose_handler
choose_handler >> [download_from_azure, prepare_local_files]
[download_from_azure, prepare_local_files] >> files_ready
files_ready >> [detect_schema_orders, detect_schema_notifs]
detect_schema_orders >> load_orders
detect_schema_notifs >> load_notifs
[load_orders, load_notifs] >> generate_embeddings
generate_embeddings >> create_views
create_views >> calculate_kpis
calculate_kpis >> send_webhook
send_webhook >> cleanup_files

