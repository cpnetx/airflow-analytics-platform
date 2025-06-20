"""KPI calculation tasks for maintenance metrics."""

from sqlalchemy import create_engine, text
from typing import Dict, Any
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def calculate_maintenance_kpis(**context) -> Dict[str, Any]:
    """Calculate initial maintenance KPIs after data load."""
    conf = context['dag_run'].conf
    org_id = conf['org_id']
    
    # Connect to database
    db_url = context['var']['value'].get('MAINTENANCE_DB_URL')
    engine = create_engine(db_url)
    
    kpis = {}
    
    with engine.connect() as conn:
        # Set search path
        conn.execute(text(f"SET search_path TO maint_{org_id}, public"))
        
        # 1. Calculate equipment counts
        result = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT equipment_id) as total_equipment,
                COUNT(DISTINCT CASE WHEN criticality = 'A' THEN equipment_id END) as critical_equipment
            FROM equipment
        """))
        row = result.fetchone()
        kpis['equipment'] = {
            'total': row['total_equipment'],
            'critical': row['critical_equipment']
        }
        
        # 2. Calculate work order statistics
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_orders,
                COUNT(CASE WHEN work_type = 'PM' THEN 1 END) as pm_orders,
                COUNT(CASE WHEN work_type = 'CM' THEN 1 END) as cm_orders,
                COUNT(CASE WHEN work_type = 'EM' THEN 1 END) as em_orders,
                SUM(cost) as total_cost,
                SUM(actual_hours) as total_hours,
                AVG(actual_hours) as avg_hours_per_order
            FROM work_orders
            WHERE created_date >= CURRENT_DATE - INTERVAL '365 days'
        """))
        row = result.fetchone()
        kpis['work_orders'] = {
            'total': row['total_orders'] or 0,
            'by_type': {
                'PM': row['pm_orders'] or 0,
                'CM': row['cm_orders'] or 0,
                'EM': row['em_orders'] or 0
            },
            'total_cost': float(row['total_cost'] or 0),
            'total_hours': float(row['total_hours'] or 0),
            'avg_hours': float(row['avg_hours_per_order'] or 0)
        }
        
        # 3. Calculate basic MTBF/MTTR for top equipment
        result = conn.execute(text("""
            WITH equipment_failures AS (
                SELECT 
                    equipment_id,
                    COUNT(*) as failure_count,
                    MIN(created_date) as first_failure,
                    MAX(created_date) as last_failure,
                    SUM(actual_hours) as total_repair_hours
                FROM work_orders
                WHERE work_type IN ('CM', 'EM')
                AND created_date >= CURRENT_DATE - INTERVAL '365 days'
                GROUP BY equipment_id
                HAVING COUNT(*) > 1
            )
            SELECT 
                equipment_id,
                failure_count,
                EXTRACT(EPOCH FROM (last_failure - first_failure)) / 3600 / NULLIF(failure_count - 1, 0) as mtbf_hours,
                total_repair_hours / failure_count as mttr_hours
            FROM equipment_failures
            ORDER BY failure_count DESC
            LIMIT 10
        """))
        
        top_equipment_reliability = []
        for row in result:
            top_equipment_reliability.append({
                'equipment_id': row['equipment_id'],
                'failure_count': row['failure_count'],
                'mtbf_hours': float(row['mtbf_hours'] or 0),
                'mttr_hours': float(row['mttr_hours'] or 0)
            })
        
        kpis['reliability'] = {
            'top_equipment': top_equipment_reliability
        }
        
        # 4. Calculate cost distribution
        result = conn.execute(text("""
            SELECT 
                equipment_id,
                SUM(cost) as total_cost,
                COUNT(*) as order_count
            FROM work_orders
            WHERE cost > 0
            GROUP BY equipment_id
            ORDER BY total_cost DESC
            LIMIT 20
        """))
        
        cost_distribution = []
        for row in result:
            cost_distribution.append({
                'equipment_id': row['equipment_id'],
                'total_cost': float(row['total_cost']),
                'order_count': row['order_count']
            })
        
        kpis['costs'] = {
            'top_cost_equipment': cost_distribution
        }
        
        # 5. Store KPIs in a summary table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kpi_summary (
                id SERIAL PRIMARY KEY,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                kpi_data JSONB,
                period_start DATE,
                period_end DATE
            )
        """))
        
        conn.execute(text("""
            INSERT INTO kpi_summary (kpi_data, period_start, period_end)
            VALUES (:kpi_data, :period_start, :period_end)
        """), {
            "kpi_data": kpis,
            "period_start": datetime.now().date() - timedelta(days=365),
            "period_end": datetime.now().date()
        })
        
        conn.commit()
    
    logger.info(f"Calculated KPIs: {kpis}")
    
    return {
        "status": "success",
        "kpis": kpis,
        "calculated_at": datetime.now().isoformat()
    }