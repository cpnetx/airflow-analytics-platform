"""
Process Change Report DAG for Airflow

This DAG generates process change reports for manufacturing sites.
It fetches data from InfluxDB, analyzes process changes, and generates HTML reports.
"""

from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from collections import defaultdict
import base64
import requests
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Default arguments for the DAG
default_args = {
    'owner': 'process_report_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'process_change_report',
    default_args=default_args,
    description='Generate process change reports for sites',
    schedule=None,  # Manual trigger only for now
    catchup=False,
    tags=['process_report', 'reporting'],
)


def fetch_configurations(**context):
    """Fetch report configurations from the API."""
    
    # Get parameters from dag_run.conf
    dag_conf = context['dag_run'].conf or {}
    site = dag_conf.get('site')
    lines = dag_conf.get('lines', [])
    triggered_by = dag_conf.get('triggered_by', 'unknown')
    
    logging.info(f"Fetching configuration for site: {site}, lines: {lines}")
    
    # Validate required parameters
    if not site or not lines:
        logging.error("Missing required parameters: site and lines")
        logging.info("dag_conf received: %s", dag_conf)
        raise ValueError("DAG requires 'site' and 'lines' parameters in dag_run.conf")
    
    # Get API configuration from Variables
    api_base_url = Variable.get("PROCESS_REPORT_API_URL", "http://backend:8000")
    service_token = Variable.get("AIRFLOW_SERVICE_TOKEN", "")
    
    if not service_token:
        logging.warning("No service token configured, using mock configurations")
        # Fall back to mock configurations
        mock_configs = {
        "sch-l8": {
            "site": "sch",
            "line": "l8",
            "heat_threshold": 10.0,
            "controls": {
                "heats": {
                    "num_heats": 47,
                    "threshold": 10.0,
                    "name": "ui_heat_pid_setpoints_ft_{x}"
                },
                "nonheats": {
                    "controls": ["ui_plug_extend_pos_f"]
                }
            },
            "recipients": ["bicheng@cpnet.io"],
            "schedule_cron": "2 6,18 * * *",
            "enabled": True,
            "time_start_dayshift": "07:00:00",
            "alert_lookback_days": 60,
            "org": "WPP-SCH",
            "bucket": "v-wpp-l8-1/ch_opcua"
        },
        "sch-l22": {
            "site": "sch",
            "line": "l22",
            "heat_threshold": 10.0,
            "controls": {
                "heats": {
                    "num_heats": 107,
                    "threshold": 10.0,
                    "name": "ui_heat_pid_setpoints_ft_{x}"
                },
                "nonheats": {
                    "controls": ["ui_plug_extend_pos_f"]
                }
            },
            "recipients": ["bicheng@cpnet.io"],
            "schedule_cron": "2 6,18 * * *",
            "enabled": True,
            "time_start_dayshift": "07:00:00",
            "alert_lookback_days": 60,
            "org": "WPP-SCH",
            "bucket": "v-wpp-l22-3/ch_opcua"
        }
    }
    
        configs = []  # Initialize configs list
        # Get configurations for requested lines
        for line in lines:
            if line in mock_configs:
                configs.append(mock_configs[line])
                logging.info(f"Using mock configuration for {line}")
            else:
                logging.warning(f"No mock configuration found for {line}")
    else:
        # Use service token to fetch real configurations
        configs = []
        headers = {"X-Service-Token": service_token}
        
        for line in lines:
            try:
                # Extract just the line part if it includes site prefix
                if '-' in line:
                    _, line_part = line.split('-', 1)
                else:
                    line_part = line
                    
                response = requests.get(
                    f"{api_base_url}/api/process-report/configs/{site}/{line_part}",
                    headers=headers
                )
                
                if response.status_code == 200:
                    config = response.json()
                    configs.append(config)
                    logging.info(f"Fetched configuration for {site}-{line_part}")
                else:
                    logging.error(f"Failed to fetch config for {site}-{line_part}: {response.status_code}")
                    logging.error(f"Response: {response.text}")
                    
            except Exception as e:
                logging.error(f"Error fetching config for {site}-{line}: {str(e)}")
    
    # Store configurations for next tasks
    context['task_instance'].xcom_push(key='report_configs', value=configs)
    context['task_instance'].xcom_push(key='site', value=site)
    context['task_instance'].xcom_push(key='triggered_by', value=triggered_by)
    
    return len(configs)


def get_controls(config: Dict) -> Tuple[List[str], List[str]]:
    """Extract heat and non-heat controls from configuration."""
    controls = config.get('controls', {})
    heats_config = controls.get('heats', {})
    nonheats_config = controls.get('nonheats', {})
    
    # Generate heat control names
    num_heats = heats_config.get('num_heats', 0)
    heat_name_template = heats_config.get('name', '')
    liststr_heats = [
        heat_name_template.replace('{x}', str(i))
        for i in range(1, num_heats + 1)
    ]
    
    # Get non-heat controls
    liststr_nonheats = nonheats_config.get('controls', [])
    
    return liststr_heats, liststr_nonheats


def fetch_influx_data(config: Dict, liststr_columns: List[str]) -> Optional[pd.DataFrame]:
    """Fetch data from InfluxDB for the specified columns."""
    
    # TODO: Replace with actual InfluxDB call once tokens are configured
    # For now, generate mock data for testing
    
    logging.info(f"Generating mock data for {len(liststr_columns)} columns")
    
    # Generate time series for last 24 hours
    end_time = pd.Timestamp.now()
    start_time = end_time - pd.Timedelta('24h')
    time_index = pd.date_range(start=start_time, end=end_time, freq='1min')
    
    # Create base dataframe
    df = pd.DataFrame(index=time_index)
    df['minute_level'] = df.index
    
    # Generate mock data for each column
    for col in liststr_columns:
        if 'heat' in col.lower():
            # Heat columns: base temperature with some variation
            base_temp = 350 + np.random.randint(-20, 20)
            variation = np.random.normal(0, 2, len(time_index))
            # Add a trend change in the middle
            trend = np.zeros(len(time_index))
            midpoint = len(time_index)//2
            trend[midpoint:] = np.linspace(0, 15, len(time_index) - midpoint)
            df[col] = base_temp + variation + trend
        else:
            # Non-heat controls: smaller variations
            base_value = 100 + np.random.randint(-10, 10)
            variation = np.random.normal(0, 0.5, len(time_index))
            # Add occasional step change
            if np.random.rand() > 0.7:
                step_point = np.random.randint(len(time_index)//3, 2*len(time_index)//3)
                variation[step_point:] += 5
            df[col] = base_value + variation
    
    logging.info(f"Generated mock data with shape: {df.shape}")
    
    return df


def get_cutoff_timestamps_shift(ts_runtime, time_start_dayshift="07:00:00"):
    """Calculate shift timestamps based on runtime."""
    date_runtime = ts_runtime.date()
    int_hour_runtime = ts_runtime.hour
    
    if int_hour_runtime < 12:
        ts_left = pd.Timestamp(f"{date_runtime} {time_start_dayshift}") - pd.Timedelta("12h")
        str_shift = ts_left.strftime("%Y_%m_%d N")
    else:
        ts_left = pd.Timestamp(f"{date_runtime} {time_start_dayshift}")
        str_shift = ts_left.strftime("%Y_%m_%d D")
        
    ts_right = ts_left + pd.Timedelta("12h")
    return ts_left, ts_right, str_shift


def calculate_changes(df, liststr_controls_heat, liststr_controls_nonheat):
    """Calculate process changes for heat and non-heat controls."""
    listdfs_change = []
    
    for liststr_controls in [liststr_controls_heat, liststr_controls_nonheat]:
        defdict_change = defaultdict(list)
        
        for str_control in liststr_controls:
            defdict_change["Control"].append(str_control)
            
            if str_control not in df.columns or len(df[str_control].dropna()) == 0:
                defdict_change["Shift start"].append(np.nan)
                defdict_change["Shift end"].append(np.nan)
                defdict_change["Shift end-start"].append(0)
                defdict_change["Shift max-min"].append(0)
            else:
                float_shift_start = df[str_control].dropna().values[0]
                float_shift_end = df[str_control].dropna().values[-1]
                defdict_change["Shift start"].append(float_shift_start)
                defdict_change["Shift end"].append(float_shift_end)
                defdict_change["Shift end-start"].append(float_shift_end - float_shift_start)
                
                float_shift_max = df[str_control].max()
                float_shift_min = df[str_control].min()
                defdict_change["Shift max-min"].append(float_shift_max - float_shift_min)
                
        listdfs_change.append(
            pd.DataFrame(defdict_change).sort_values(
                by=["Shift max-min"], ascending=False
            )
        )
    
    return listdfs_change


def color_table_to_html(df, liststr_columns, str_positive="e62525aa", str_negative="199bd6aa"):
    """Convert dataframe to colored HTML table."""
    str_html = (
        df.style.hide_index()
        .format(precision=2)
        .set_table_styles([
            {
                "selector": "th,td",
                "props": [
                    ("font-size", "12pt"),
                    ("border-style", "solid"),
                    ("border-width", "1px"),
                    ("text-align", "right"),
                ],
            },
            {"selector": "td.col0", "props": "text-align:left;"},
        ])
        .apply(
            lambda series: np.where(
                (series > 0),
                f"background-color:#{str_positive}; border: 2px solid #000066;",
                "",
            ),
            axis=0,
            subset=liststr_columns,
        )
        .apply(
            lambda series: np.where(
                (series < 0),
                f"background-color:#{str_negative}; border: 2px solid #000066;",
                "",
            ),
            axis=0,
            subset=liststr_columns,
        )
        .to_html()
    )
    return str_html


def generate_report_for_line(config: Dict) -> Tuple[str, str, str]:
    """Generate report for a single line."""
    
    site = config.get('site', '')
    line = config.get('line', '')
    # Check if line already includes site prefix
    if line.startswith(f"{site}-"):
        line_id = line
    else:
        line_id = f"{site}-{line}"
    
    # Get controls
    liststr_heats, liststr_nonheats = get_controls(config)
    heat_threshold = config.get('heat_threshold', 10.0)
    time_start_dayshift = config.get('time_start_dayshift', '07:00:00')
    
    # Fetch data from InfluxDB
    df = fetch_influx_data(config, liststr_heats + liststr_nonheats)
    
    # Calculate shift times
    ts_now = pd.Timestamp.now()
    ts_left, ts_right, str_shift = get_cutoff_timestamps_shift(ts_now, time_start_dayshift)
    
    str_msg = ""
    
    if df is None or len(df) == 0:
        str_subject = f"{line_id} process report : {str_shift}"
        str_msg += f"<h1>{line_id}</h1>\n\n"
        str_msg += f"<h2>Shift : {str_shift}</h2>\n\n"
        str_msg += f"No machine data available between {ts_left} and {ts_right} for {line_id}\n"
        return str_subject, str_msg, str_shift
    
    # Filter data for shift
    df = df.set_index("minute_level").copy()
    df["local_time"] = pd.to_datetime(df.index).tz_localize(None)
    df_shift = df[(df["local_time"] >= ts_left) & (df["local_time"] <= ts_right)]
    
    if len(df_shift) == 0:
        str_subject = f"{line_id} process report : {str_shift}"
        str_msg += f"<h1>{line_id}</h1>\n\n"
        str_msg += f"<h2>Shift : {str_shift}</h2>\n\n"
        str_msg += f"No machine data available between {ts_left} and {ts_right} for {line_id}\n"
        return str_subject, str_msg, str_shift
    
    # Calculate changes
    df_change_heat, df_change_nonheat = calculate_changes(df_shift, liststr_heats, liststr_nonheats)
    
    # Filter heat changes by threshold
    df_change_heat = df_change_heat[
        (df_change_heat["Shift end-start"] > heat_threshold) |
        (df_change_heat["Shift end-start"] < -heat_threshold)
    ]
    
    # Generate HTML
    if len(df_change_heat) > 0:
        df_change_heat = df_change_heat.astype({
            "Shift end-start": int,
            "Shift start": int,
            "Shift end": int,
            "Shift max-min": int
        })
        str_change_heat_html = color_table_to_html(df_change_heat, ["Shift end-start"])
    else:
        str_change_heat_html = f"No heat exceeded {heat_threshold} degrees."
    
    str_change_nonheat_html = color_table_to_html(df_change_nonheat, ["Shift end-start"])
    
    # Check if any changes occurred
    bool_changes = (
        (len(df_change_heat) > 0) or 
        (df_change_nonheat["Shift end-start"].abs() > 0).any()
    )
    
    if bool_changes:
        str_green = "80bfb7aa"
        str_colored_line = f'<h1 style="background-color:#{str_green};">{line_id}</h1>\n\n'
    else:
        str_colored_line = f"<h1>{line_id}</h1>\n\n"
    
    # Build report message
    str_subject = f"{line_id} process report : {str_shift}"
    str_msg += str_colored_line
    str_msg += f"<h2>Shift : {str_shift}</h2>\n\n"
    str_msg += f"<b>Earliest timepoint</b>: {df_shift['local_time'].min()}\n\n"
    str_msg += f"<b>Latest timepoint</b>: {df_shift['local_time'].max()}\n\n"
    str_msg += f"<h2>Non heats</h2> \n\n {str_change_nonheat_html} \n\n"
    str_msg += f"<h2>Heats</h2> \n\n {str_change_heat_html}" + "\n"
    
    return str_subject, str_msg, str_shift


def generate_reports(**context):
    """Generate reports for all configured lines."""
    
    # Pull from specific task
    configs = context['task_instance'].xcom_pull(task_ids='fetch_configurations', key='report_configs')
    site = context['task_instance'].xcom_pull(task_ids='fetch_configurations', key='site')
    
    # If configs is still None, try without specifying task_ids
    if configs is None:
        configs = context['task_instance'].xcom_pull(key='report_configs')
        site = context['task_instance'].xcom_pull(key='site')
    
    # Debug logging
    logging.info(f"Retrieved configs from XCom: {configs}")
    logging.info(f"Type of configs: {type(configs)}")
    
    if configs is None:
        logging.error("No configurations received from fetch_configurations task")
        raise ValueError("No configurations available for report generation")
    
    logging.info(f"Generating reports for {site} with {len(configs)} line configurations")
    
    reports = []
    full_html = ""
    shift_name = ""
    
    for config in configs:
        try:
            subject, html_content, shift = generate_report_for_line(config)
            
            reports.append({
                'site': config['site'],
                'line': config['line'],
                'subject': subject,
                'html_content': html_content,
                'shift': shift,
                'status': 'success'
            })
            
            full_html += html_content + "\n\n" + "-" * 40 + "<br>\n\n"
            shift_name = shift  # Use last shift name
            
        except Exception as e:
            logging.error(f"Error generating report for {config['site']}-{config['line']}: {str(e)}")
            reports.append({
                'site': config['site'],
                'line': config['line'],
                'status': 'failed',
                'error': str(e)
            })
    
    # Prepare final report data
    report_data = {
        'site': site,
        'shift': shift_name,
        'generated_at': datetime.now().isoformat(),
        'reports': reports,
        'full_html': full_html,
        'subject': f"{site.upper()} process report: {shift_name}"
    }
    
    context['task_instance'].xcom_push(key='report_data', value=report_data)
    
    return f"Generated {len(reports)} reports"


def upload_to_azure(**context):
    """Upload the generated report to Azure Blob Storage."""
    
    report_data = context['task_instance'].xcom_pull(task_ids='generate_reports', key='report_data')
    
    if not report_data:
        logging.error("No report data received from generate_reports task")
        raise ValueError("No report data available for upload")
    
    site = report_data['site']
    shift = report_data['shift']
    full_html = report_data['full_html']
    
    # Generate blob path
    date_str = datetime.now().strftime('%Y-%m-%d')
    shift_letter = 'D' if 'D' in shift else 'N'
    blob_name = f"{site}/{date_str}/process_change_{shift_letter}.html"
    
    logging.info(f"Uploading report to Azure: {blob_name}")
    
    # Get Azure configuration from Variables
    account_name = Variable.get("AZURE_STORAGE_ACCOUNT_NAME")
    container_name = Variable.get("PROCESS_REPORT_CONTAINER_NAME")
    sas_token = Variable.get("AZURE_STORAGE_SAS_TOKEN")
    
    if not all([account_name, container_name, sas_token]):
        raise AirflowException("Azure Storage configuration missing")
    
    try:
        from azure.storage.blob import BlobServiceClient
        
        # Clean up SAS token
        if sas_token.startswith('?'):
            sas_token = sas_token[1:]
        
        # Create blob service client
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=sas_token
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Upload HTML content
        blob_client.upload_blob(
            full_html,
            content_type='text/html',
            overwrite=True
        )
        
        logging.info(f"Successfully uploaded report to: {blob_name}")
        
        # Update report data with blob path
        report_data['blob_path'] = blob_name
        context['task_instance'].xcom_push(key='blob_path', value=blob_name)
        context['task_instance'].xcom_push(key='report_data', value=report_data)
        
        return blob_name
        
    except Exception as e:
        logging.error(f"Failed to upload to Azure: {str(e)}")
        raise


def update_report_history(**context):
    """Update the report history via API callback."""
    
    report_data = context['task_instance'].xcom_pull(task_ids='generate_reports', key='report_data')
    blob_path = context['task_instance'].xcom_pull(task_ids='upload_to_azure', key='blob_path')
    dag_run_id = context['dag_run'].run_id
    
    if not report_data:
        logging.error("No report data received")
        return "No report data to update"
    
    # Get API configuration
    api_base_url = Variable.get("PROCESS_REPORT_API_URL", "http://backend:8000")
    api_token = Variable.get("PROCESS_REPORT_API_TOKEN", "")
    
    # Update history for each line
    for report in report_data['reports']:
        if report.get('status') != 'success':
            continue
            
        history_data = {
            'site': report['site'],
            'line': report['line'],
            'shift': report['shift'],
            'blob_path': blob_path,
            'status': 'success',
            'airflow_run_id': dag_run_id,
            'generated_at': datetime.now().isoformat()
        }
        
        try:
            # Note: This endpoint needs to be implemented in the API
            response = requests.post(
                f"{api_base_url}/api/process-report/history/update",
                json=history_data,
                headers={"Authorization": f"Bearer {api_token}"}
            )
            
            if response.status_code in [200, 201]:
                logging.info(f"Updated history for {report['site']}-{report['line']}")
            else:
                logging.error(f"Failed to update history: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Error updating history: {str(e)}")
    
    return "History updated"


def send_email_notification(**context):
    """Send email notification with report link."""
    
    report_data = context['task_instance'].xcom_pull(task_ids='generate_reports', key='report_data')
    configs = context['task_instance'].xcom_pull(task_ids='fetch_configurations', key='report_configs')
    
    if not report_data:
        logging.error("No report data received")
        return "No report data for email"
    
    # Get unique recipients from all configs
    recipients = set()
    for config in configs:
        recipients.update(config.get('recipients', []))
    
    # Override with test recipient for now
    recipients = ['bicheng@cpnet.io']
    
    subject = report_data['subject']
    html_content = report_data['full_html']
    
    logging.info(f"Sending email to {len(recipients)} recipients")
    
    # Get email configuration
    email_api_url = Variable.get("EMAIL_API_URL", "https://jarvis.api.cpnet.ai/api/emails")
    email_from = Variable.get("EMAIL_FROM", "noreply@cpnet.io")
    
    # Get authentication token (simplified version)
    try:
        # In production, implement proper email sending via your email service
        # For now, just log the action
        logging.info(f"Would send email with subject: {subject}")
        logging.info(f"Recipients: {recipients}")
        
        # Save report locally for testing
        with open('/tmp/process_report.html', 'w') as f:
            f.write(html_content)
        
        return f"Email notification prepared for {len(recipients)} recipients"
        
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        raise


# Define tasks
fetch_config_task = PythonOperator(
    task_id='fetch_configurations',
    python_callable=fetch_configurations,
    dag=dag,
)

generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

upload_to_azure_task = PythonOperator(
    task_id='upload_to_azure',
    python_callable=upload_to_azure,
    dag=dag,
)

update_history_task = PythonOperator(
    task_id='update_report_history',
    python_callable=update_report_history,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    dag=dag,
)

# Define task dependencies
fetch_config_task >> generate_reports_task >> upload_to_azure_task >> [update_history_task, send_email_task]