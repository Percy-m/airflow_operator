"""Example DAG for AiDatalake Ray jobs."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG

from airflow_provider_aidatalake_ray.operators.ray import RayOperator


with DAG(
    dag_id="example_aidatalake_ray",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ray_connection_task = RayOperator(
        task_id="ray_connection_task",
        ray_conn_id="aidatalake_ray",
        workspace_id="12345678-1234-1234-1234-123456789012",
        name="ray-connection-demo",
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

    ray_direct_token_task = RayOperator(
        task_id="ray_direct_token_task",
        ray_base_url="https://ray-api.example.com",
        token="{{ var.value.test_ray_token }}",
        workspace_id="12345678-1234-1234-1234-123456789012",
        name="ray-direct-token-demo",
        endpoint_name="ray",
        entrypoint="python hello.py",
        runtime_env={
            "working_dir": "/mnt/OBS/demo/ray/hello",
            "pip": ["requests==2.32.3"],
        },
        entrypoint_num_cpus=1,
        entrypoint_num_gpus=0,
        entrypoint_memory=1073741824,
        deferrable=True,
        poll_interval=30,
    )
