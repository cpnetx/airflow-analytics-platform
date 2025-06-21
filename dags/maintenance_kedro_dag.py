from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


class KedroOperator(BaseOperator):
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str | list[str],
        project_path: str | Path,
        env: str,
        conf_source: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env
        self.conf_source = conf_source

    def execute(self, context):
        configure_project(self.package_name)
        with KedroSession.create(self.project_path, env=self.env, conf_source=self.conf_source) as session:
            if isinstance(self.node_name, str):
                self.node_name = [self.node_name]
            session.run(self.pipeline_name, node_names=self.node_name)

# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path("/opt/kedro_project")
package_name = "maintenance_kedro"
conf_source = project_path / "conf"


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="maintenance-kedro",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule="@once",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
) as dag:
    tasks = {
        "create-tenant-schema": KedroOperator(
            task_id="create-tenant-schema",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="create_tenant_schema",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "download-files-from-azure": KedroOperator(
            task_id="download-files-from-azure",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="download_files_from_azure",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "detect-notifications-schema": KedroOperator(
            task_id="detect-notifications-schema",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="detect_notifications_schema",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "detect-orders-schema": KedroOperator(
            task_id="detect-orders-schema",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="detect_orders_schema",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "process-notifications-data": KedroOperator(
            task_id="process-notifications-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="process_notifications_data",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "process-orders-data": KedroOperator(
            task_id="process-orders-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="process_orders_data",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "calculate-maintenance-kpis": KedroOperator(
            task_id="calculate-maintenance-kpis",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="calculate_maintenance_kpis",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "save-notifications-to-db": KedroOperator(
            task_id="save-notifications-to-db",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="save_notifications_to_db",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "save-orders-to-db": KedroOperator(
            task_id="save-orders-to-db",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="save_orders_to_db",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "send-completion-webhook": KedroOperator(
            task_id="send-completion-webhook",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="send_completion_webhook",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        )
    }
    tasks["download-files-from-azure"] >> tasks["detect-notifications-schema"]
    tasks["download-files-from-azure"] >> tasks["detect-orders-schema"]
    tasks["download-files-from-azure"] >> tasks["process-notifications-data"]
    tasks["detect-notifications-schema"] >> tasks["process-notifications-data"]
    tasks["download-files-from-azure"] >> tasks["process-orders-data"]
    tasks["detect-orders-schema"] >> tasks["process-orders-data"]
    tasks["process-notifications-data"] >> tasks["calculate-maintenance-kpis"]
    tasks["process-orders-data"] >> tasks["calculate-maintenance-kpis"]
    tasks["process-notifications-data"] >> tasks["save-notifications-to-db"]
    tasks["process-orders-data"] >> tasks["save-orders-to-db"]
    tasks["calculate-maintenance-kpis"] >> tasks["send-completion-webhook"]