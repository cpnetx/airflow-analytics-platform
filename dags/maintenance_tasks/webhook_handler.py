"""Webhook notification handler for maintenance DAGs."""

import requests
import json
import hmac
import hashlib
from typing import Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def send_webhook_notification(**context) -> Dict[str, Any]:
    """Send webhook notification about DAG completion."""
    conf = context['dag_run'].conf
    org_id = conf['org_id']
    
    # Check if webhooks are configured
    webhook_url = context['var']['value'].get('MAINTENANCE_WEBHOOK_URL')
    webhook_secret = context['var']['value'].get('MAINTENANCE_WEBHOOK_SECRET')
    
    if not webhook_url:
        logger.info("No webhook URL configured, skipping notification")
        return {"status": "skipped", "reason": "No webhook configured"}
    
    # Gather execution summary
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Get results from previous tasks
    kpi_result = ti.xcom_pull(task_ids='calculate_initial_kpis')
    
    # Count loaded records
    orders_result = ti.xcom_pull(task_ids='load_orders_data')
    notifs_result = ti.xcom_pull(task_ids='load_notifs_data')
    embeddings_result = ti.xcom_pull(task_ids='generate_embeddings')
    
    # Build webhook payload
    payload = {
        "event": "customer.created",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "customer_slug": conf['customer_slug'],
            "org_id": org_id,
            "job_id": conf.get('job_id'),
            "dag_run_id": dag_run.run_id,
            "status": "completed",
            "summary": {
                "work_orders_loaded": orders_result.get('rows_loaded', 0) if orders_result else 0,
                "notifications_loaded": notifs_result.get('rows_loaded', 0) if notifs_result else 0,
                "embeddings_created": embeddings_result.get('embeddings_created', 0) if embeddings_result else 0,
                "kpis": kpi_result.get('kpis', {}) if kpi_result else {}
            },
            "execution_time": {
                "start": dag_run.start_date.isoformat() if dag_run.start_date else None,
                "end": datetime.utcnow().isoformat()
            }
        }
    }
    
    # Calculate signature if secret is provided
    headers = {
        'Content-Type': 'application/json',
        'X-Webhook-Event': 'customer.created',
        'X-Webhook-Timestamp': payload['timestamp']
    }
    
    if webhook_secret:
        # Create HMAC signature
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            webhook_secret.encode('utf-8'),
            payload_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        headers['X-Webhook-Signature'] = f"sha256={signature}"
    
    # Send webhook
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        
        logger.info(f"Webhook sent successfully to {webhook_url}")
        
        return {
            "status": "success",
            "webhook_url": webhook_url,
            "response_status": response.status_code,
            "response_body": response.text[:500]  # First 500 chars
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send webhook: {str(e)}")
        
        # Don't fail the DAG for webhook errors
        return {
            "status": "failed",
            "webhook_url": webhook_url,
            "error": str(e)
        }


def send_failure_webhook(**context) -> Dict[str, Any]:
    """Send webhook notification about DAG failure."""
    conf = context['dag_run'].conf
    org_id = conf['org_id']
    
    webhook_url = context['var']['value'].get('MAINTENANCE_WEBHOOK_URL')
    webhook_secret = context['var']['value'].get('MAINTENANCE_WEBHOOK_SECRET')
    
    if not webhook_url:
        return {"status": "skipped", "reason": "No webhook configured"}
    
    # Build failure payload
    payload = {
        "event": "customer.failed",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "customer_slug": conf['customer_slug'],
            "org_id": org_id,
            "job_id": conf.get('job_id'),
            "dag_run_id": context['dag_run'].run_id,
            "status": "failed",
            "error": {
                "task_id": context.get('task_instance').task_id if context.get('task_instance') else None,
                "exception": str(context.get('exception', 'Unknown error'))
            }
        }
    }
    
    # Send webhook (similar to success webhook)
    headers = {
        'Content-Type': 'application/json',
        'X-Webhook-Event': 'customer.failed',
        'X-Webhook-Timestamp': payload['timestamp']
    }
    
    if webhook_secret:
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            webhook_secret.encode('utf-8'),
            payload_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        headers['X-Webhook-Signature'] = f"sha256={signature}"
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        return {
            "status": "sent",
            "response_status": response.status_code
        }
        
    except Exception as e:
        logger.error(f"Failed to send failure webhook: {str(e)}")
        return {"status": "failed", "error": str(e)}