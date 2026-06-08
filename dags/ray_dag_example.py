"""Example DAG for AiDatalake Ray jobs."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG

from custom_operator.ray.operators.ray import RayOperator
from custom_operator.token.operators.token import TokenOperator


with DAG(
    dag_id="example_aidatalake_ray",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_token = TokenOperator(
        task_id="test_token",
        token_conn_id="aidatalake_token",
    )

    test_ray = RayOperator(
        task_id="test_ray",
        ray_base_url="https://ray-api.example.com",
        token="{{ ti.xcom_pull(task_ids='test_token') }}",
        workspace_id="12345678-1234-1234-1234-123456789012",
        name="ray-demo",
        endpoint_name="ray",
        entrypoint="python train.py --epochs 10",
        runtime_env={
            "working_dir": "/mnt/OBS/demo/ray/train",
            "py_modules": ["/mnt/OBS/demo/ray/libs/common.zip"],
            "pip": ["numpy==1.26.0", "pandas==2.2.0"],
            "env_vars": {
                "ENV": "test",
            },
        },
        entrypoint_num_cpus=4,
        entrypoint_num_gpus=1,
        entrypoint_memory=8589934592,
        deferrable=True,
        poll_interval=30,
    )

    test_token >> test_ray
