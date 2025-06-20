"""File handling tasks for maintenance DAGs."""

import os
import tempfile
import shutil
import requests
from typing import Dict, Any
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)


def download_files_from_azure(**context) -> Dict[str, str]:
    """Download files from Azure Blob Storage using SAS URLs."""
    conf = context['dag_run'].conf
    
    # Create temp directory for this DAG run
    temp_dir = tempfile.mkdtemp(prefix=f"maintenance_{conf['customer_slug']}_")
    logger.info(f"Created temp directory: {temp_dir}")
    
    # Download files
    file_paths = {}
    
    try:
        # Download orders file
        orders_path = os.path.join(temp_dir, "orders.xlsx")
        logger.info(f"Downloading orders file from SAS URL...")
        response = requests.get(conf['orders_sas_url'], stream=True)
        response.raise_for_status()
        
        with open(orders_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_paths['orders'] = orders_path
        logger.info(f"Orders file downloaded to: {orders_path}")
        
        # Download notifications file
        notifs_path = os.path.join(temp_dir, "notifs.xlsx")
        logger.info(f"Downloading notifications file from SAS URL...")
        response = requests.get(conf['notifs_sas_url'], stream=True)
        response.raise_for_status()
        
        with open(notifs_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_paths['notifs'] = notifs_path
        logger.info(f"Notifications file downloaded to: {notifs_path}")
        
        # Store temp directory for cleanup
        file_paths['temp_dir'] = temp_dir
        
        return file_paths
        
    except Exception as e:
        logger.error(f"Error downloading files: {str(e)}")
        # Clean up on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def prepare_local_file_paths(**context) -> Dict[str, str]:
    """Prepare file paths for local file system storage."""
    conf = context['dag_run'].conf
    
    # For local files, paths are provided directly
    file_paths = {
        'orders': conf['orders_path'],
        'notifs': conf['notifs_path'],
        'temp_dir': None  # No temp dir for local files
    }
    
    # Verify files exist
    for file_type, path in file_paths.items():
        if path and not os.path.exists(path):
            raise FileNotFoundError(f"{file_type} file not found at: {path}")
    
    logger.info(f"Using local files: {file_paths}")
    return file_paths


def cleanup_temporary_files(**context) -> None:
    """Clean up temporary files and optionally blob storage."""
    ti = context['ti']
    
    # Get file paths from previous tasks
    file_paths = ti.xcom_pull(task_ids=['download_from_azure', 'prepare_local_files'])
    file_paths = [fp for fp in file_paths if fp is not None]
    
    if not file_paths:
        logger.warning("No file paths found for cleanup")
        return
    
    file_info = file_paths[0]
    
    # Clean up local temp directory
    if file_info and file_info.get('temp_dir'):
        temp_dir = file_info['temp_dir']
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info(f"Cleaned up temp directory: {temp_dir}")
    
    # Optionally clean up blob storage
    conf = context['dag_run'].conf
    if conf.get('delete_after_processing', True) and conf.get('storage_type') == 'azure':
        try:
            connection_string = Variable.get("AZURE_STORAGE_CONNECTION_STRING")
            blob_service = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service.get_container_client("maintenance-uploads")
            
            # Delete blobs
            for blob_name in [conf.get('orders_blob'), conf.get('notifs_blob')]:
                if blob_name:
                    try:
                        container_client.delete_blob(blob_name)
                        logger.info(f"Deleted blob: {blob_name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete blob {blob_name}: {e}")
                        
        except Exception as e:
            logger.error(f"Error cleaning up blob storage: {str(e)}")
            # Don't fail the task for cleanup errors
    
    logger.info("Cleanup completed")