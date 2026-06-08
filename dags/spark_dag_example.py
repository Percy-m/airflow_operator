"""Example DAG for AiDatalake Spark jar jobs.

The default SparkOperator job type is spark_jar_job.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG

from custom_operator.spark.operators.spark import SparkOperator
from custom_operator.token.operators.token import TokenOperator


with DAG(
    dag_id="example_aidatalake_spark_jar",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_token = TokenOperator(
        task_id="test_token",
        auth_url="https://auth-api.example.com/v3/auth/tokens",
        auth_body={
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": "demo-user",
                            "password": "demo-password",
                        }
                    },
                }
            }
        },
    )

    test_spark = SparkOperator(
        task_id="test_spark",
        spark_base_url="https://spark-api.example.com",
        token="{{ ti.xcom_pull(task_ids='test_token') }}",
        workspace_id="12345678-1234-1234-1234-123456789012",
        name="spark-jar-demo",
        endpoint_name="endpoint1",
        spark_version="3.3.2",
        spark_jar_parameter={
            "main_class": "com.example.Main",
            "main_jar": "/mnt/OBS/demo/jars/main.jar",
            "main_args": ["--input", "/mnt/OBS/demo/input", "--output", "/mnt/OBS/demo/output"],
            "dependency_jars": ["/mnt/OBS/demo/jars/dependency.jar"],
            "dependency_files": [],
            "dependency_archives": [],
            "dependency_py_files": [],
        },
        resource_config={
            "executor_number": 4,
            "driver_resource_spec": {"cpu": 2, "memory": "4GB", "disk": 100},
            "executor_resource_spec": {"cpu": 2, "memory": "4GB", "disk": 100},
        },
        restore_strategy={
            "max_retry": 0,
            "queued_timeout": 10800,
            "running_timeout": -1,
        },
        deferrable=True,
        poll_interval=30,
    )

    test_token >> test_spark
