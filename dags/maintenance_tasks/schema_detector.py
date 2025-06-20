"""Schema detection and table creation tasks."""

import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any
from sqlalchemy import create_engine, text
import logging
import re

logger = logging.getLogger(__name__)


class SchemaDetector:
    """Auto-detect schema from Excel files."""
    
    TYPE_MAPPING = {
        'int64': 'BIGINT',
        'float64': 'NUMERIC',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT',
        'string': 'TEXT'
    }
    
    def detect_schema(self, file_path: str) -> List[Tuple[str, str]]:
        """Detect PostgreSQL schema from Excel file."""
        # Read Excel file
        df = pd.read_excel(file_path, engine='openpyxl')
        schema = []
        
        for column in df.columns:
            # Clean column name for PostgreSQL
            clean_name = self._clean_column_name(column)
            
            # Infer type
            dtype = str(df[column].dtype)
            
            # Special handling for dates
            if self._is_date_column(df[column]):
                pg_type = 'TIMESTAMP'
            else:
                pg_type = self.TYPE_MAPPING.get(dtype, 'TEXT')
            
            schema.append((clean_name, pg_type))
        
        return schema
    
    def _clean_column_name(self, name: str) -> str:
        """Clean column name for PostgreSQL compatibility."""
        # Convert to lowercase and replace special chars with underscore
        clean = re.sub(r'[^\w]', '_', str(name).lower())
        # Remove multiple underscores
        clean = re.sub(r'_+', '_', clean)
        # Remove leading/trailing underscores
        clean = clean.strip('_')
        # Ensure it doesn't start with a number
        if clean and clean[0].isdigit():
            clean = f"col_{clean}"
        return clean or "column"
    
    def _is_date_column(self, series: pd.Series) -> bool:
        """Check if column contains date values."""
        if series.dtype == 'object':
            try:
                # Try to parse a sample
                sample = series.dropna().head(100)
                if len(sample) > 0:
                    pd.to_datetime(sample, errors='coerce')
                    # If more than 80% parse successfully, it's likely a date column
                    parsed = pd.to_datetime(sample, errors='coerce')
                    if parsed.notna().sum() / len(sample) > 0.8:
                        return True
            except:
                pass
        return False


def detect_and_create_schema(**context) -> Dict[str, Any]:
    """Detect schema and create table in PostgreSQL."""
    ti = context['ti']
    conf = context['dag_run'].conf
    file_type = context['params']['file_type']
    
    # Get file paths
    file_paths = ti.xcom_pull(key='file_paths', task_ids=['download_from_azure', 'prepare_local_files'])
    file_paths = [fp for fp in file_paths if fp is not None][0]
    
    file_path = file_paths[file_type]
    org_id = conf['org_id']
    
    # Determine table name
    table_name = 'work_orders' if file_type == 'orders' else 'notification'
    
    logger.info(f"Detecting schema for {file_type} from {file_path}")
    
    # Detect schema
    detector = SchemaDetector()
    schema = detector.detect_schema(file_path)
    
    # Connect to database
    db_url = context['var']['value'].get('MAINTENANCE_DB_URL')
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # Set search path
        conn.execute(text(f"SET search_path TO maint_{org_id}, public"))
        
        # Build CREATE TABLE statement
        columns = []
        for col_name, col_type in schema:
            columns.append(f"{col_name} {col_type}")
        
        # Add primary key
        if table_name == 'work_orders':
            columns.insert(0, "id SERIAL PRIMARY KEY")
            columns.append("order_number VARCHAR(50) UNIQUE")
            columns.append("equipment_id VARCHAR(50)")
            columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        else:
            columns.insert(0, "id SERIAL PRIMARY KEY")
            columns.append("notif_id VARCHAR(50) UNIQUE")
            columns.append("order_id VARCHAR(50)")
            columns.append("equipment_id VARCHAR(50)")
            columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        
        logger.info(f"Creating table with SQL: {create_sql}")
        conn.execute(text(create_sql))
        
        # Create indexes
        if table_name == 'work_orders':
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_equipment ON {table_name}(equipment_id)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dates ON {table_name}(created_date, scheduled_date)"))
        else:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_order ON {table_name}(order_id)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_equipment ON {table_name}(equipment_id)"))
        
        # Insert metadata
        for col_name, col_type in schema:
            conn.execute(text("""
                INSERT INTO _meta_columns(table_name, column_name, pg_type, inferred_from)
                VALUES (:table, :col, :type, 'xlsx')
                ON CONFLICT (table_name, column_name) DO NOTHING
            """), {"table": table_name, "col": col_name, "type": col_type})
        
        conn.commit()
    
    return {
        "table_name": table_name,
        "schema": schema,
        "column_count": len(schema)
    }