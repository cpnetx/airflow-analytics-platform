"""Data loading tasks for maintenance DAGs."""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def load_excel_data(**context) -> Dict[str, Any]:
    """Load Excel data into PostgreSQL tables."""
    ti = context['ti']
    conf = context['dag_run'].conf
    file_type = context['params']['file_type']
    
    # Get file paths
    file_paths = ti.xcom_pull(key='file_paths', task_ids=['download_from_azure', 'prepare_local_files'])
    file_paths = [fp for fp in file_paths if fp is not None][0]
    
    file_path = file_paths[file_type]
    org_id = conf['org_id']
    
    # Get schema info from previous task
    schema_task_id = f'detect_schema_{file_type}'
    schema_info = ti.xcom_pull(task_ids=schema_task_id)
    table_name = schema_info['table_name']
    schema = schema_info['schema']
    
    logger.info(f"Loading {file_type} data from {file_path} into {table_name}")
    
    # Read Excel file
    df = pd.read_excel(file_path, engine='openpyxl')
    
    # Clean column names to match schema
    df.columns = [col[0] for col in schema]
    
    # Handle special columns based on table type
    if table_name == 'work_orders':
        # Ensure order_number column exists
        if 'order_number' not in df.columns and 'order' in df.columns:
            df['order_number'] = df['order'].astype(str)
        elif 'order_number' not in df.columns:
            # Try to find a suitable column
            for col in df.columns:
                if 'order' in col.lower() and 'number' in col.lower():
                    df['order_number'] = df[col].astype(str)
                    break
            else:
                # Generate order numbers if not found
                df['order_number'] = [f"ORD-{i:06d}" for i in range(len(df))]
        
        # Ensure equipment_id exists
        if 'equipment_id' not in df.columns and 'equipment' in df.columns:
            df['equipment_id'] = df['equipment'].astype(str)
        
    else:  # notification table
        # Ensure notif_id column exists
        if 'notif_id' not in df.columns and 'notification' in df.columns:
            df['notif_id'] = df['notification'].astype(str)
        elif 'notif_id' not in df.columns:
            # Generate notification IDs if not found
            df['notif_id'] = [f"NOTIF-{i:06d}" for i in range(len(df))]
        
        # Ensure order_id exists (link to work orders)
        if 'order_id' not in df.columns and 'order' in df.columns:
            df['order_id'] = df['order'].astype(str)
        
        # Ensure equipment_id exists
        if 'equipment_id' not in df.columns and 'equipment' in df.columns:
            df['equipment_id'] = df['equipment'].astype(str)
    
    # Convert date columns
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
    
    # Replace NaN with None for proper NULL handling
    df = df.replace({np.nan: None})
    
    # Connect to database
    db_url = context['var']['value'].get('MAINTENANCE_DB_URL')
    engine = create_engine(db_url)
    
    # Load data
    rows_loaded = 0
    with engine.connect() as conn:
        # Set search path
        conn.execute(text(f"SET search_path TO maint_{org_id}, public"))
        
        # Load data using pandas to_sql
        schema_name = f"maint_{org_id}"
        rows_loaded = df.to_sql(
            table_name,
            conn,
            schema=schema_name,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Update metadata
        conn.execute(text("""
            INSERT INTO _meta_uploads (file_name, file_type, table_name, row_count, uploaded_by)
            VALUES (:file_name, :file_type, :table_name, :row_count, :uploaded_by)
        """), {
            "file_name": file_path.split('/')[-1],
            "file_type": file_type,
            "table_name": table_name,
            "row_count": len(df),
            "uploaded_by": conf.get('created_by', 'airflow')
        })
        
        conn.commit()
    
    # Also create equipment records if they don't exist
    if 'equipment_id' in df.columns:
        unique_equipment = df[df['equipment_id'].notna()]['equipment_id'].unique()
        
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO maint_{org_id}, public"))
            
            # Create equipment table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS equipment (
                    id SERIAL PRIMARY KEY,
                    equipment_id VARCHAR(50) UNIQUE NOT NULL,
                    name VARCHAR(255),
                    type VARCHAR(100),
                    manufacturer VARCHAR(255),
                    model VARCHAR(255),
                    location VARCHAR(255),
                    criticality VARCHAR(20),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Insert equipment records
            for eq_id in unique_equipment:
                if eq_id:
                    conn.execute(text("""
                        INSERT INTO equipment (equipment_id, name)
                        VALUES (:eq_id, :name)
                        ON CONFLICT (equipment_id) DO NOTHING
                    """), {"eq_id": str(eq_id), "name": f"Equipment {eq_id}"})
            
            conn.commit()
    
    logger.info(f"Loaded {len(df)} rows into {table_name}")
    
    return {
        "table_name": table_name,
        "rows_loaded": len(df),
        "columns": list(df.columns)
    }