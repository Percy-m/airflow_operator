# Airflow Provider AiDatalake

Airflow 3.0 custom provider for submitting, polling, and cancelling AiDatalake Spark and Ray jobs.

Connection mode reads the API endpoint from the HTTP Connection host and reads
`X-Auth-Token` from either `Connection.extra.token` or `Connection.password`.
Testing environments can configure endpoints and tokens directly on the operators.

```python
from custom_operator.spark.operators.spark import SparkOperator

SparkOperator(
    task_id="spark_jar_task",
    spark_base_url="https://spark-api.example.com",
    token="{{ var.value.test_spark_token }}",
    workspace_id="12345678-1234-1234-1234-123456789012",
    name="spark-jar-demo",
    endpoint_name="endpoint1",
    spark_version="3.3.2",
    spark_jar_parameter={
        "main_class": "com.example.Main",
        "main_jar": "/mnt/OBS/demo/jars/main.jar",
    },
)
```

If direct endpoint config is omitted, the operator falls back to Airflow Connection mode.

See `docs/spark_operator_design.md` for the full design.

## Ray Operator

Ray code is isolated from Spark under `src/custom_operator/ray`.

Connection mode uses the same static token rules:

```python
from custom_operator.ray.operators.ray import RayOperator

RayOperator(
    task_id="ray_train",
    ray_conn_id="aidatalake_ray",
    workspace_id="12345678-1234-1234-1234-123456789012",
    name="ray-train-demo",
    endpoint_name="ray",
    entrypoint="python train.py --epochs 10",
    runtime_env={
        "working_dir": "/mnt/OBS/demo/ray/train",
        "pip": ["numpy==1.26.0"],
        "env_vars": {"ENV": "test"},
    },
)
```

For test-only DAGs, a token can be configured directly:

```python
RayOperator(
    task_id="ray_train_test",
    ray_base_url="https://ray-api.example.com",
    token="{{ var.value.test_ray_token }}",
    workspace_id="12345678-1234-1234-1234-123456789012",
    name="ray-train-test",
    endpoint_name="ray",
    entrypoint="python train.py",
)
```
