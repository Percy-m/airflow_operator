# Airflow Provider AiDatalake

Airflow 3.0 custom provider for submitting, polling, and cancelling AiDatalake Spark and Ray jobs.

Spark and Ray connection mode keeps only static gateway access config in the
Airflow Connection: put the API base URL in `host` and the static
`X-Auth-Token` in `password`. `extra.token`, `extra.timeout`, and
`extra.verify` are not read; request timeout and TLS verification come from the
operator or hook arguments.

Dynamic token acquisition lives in `custom_operator.token`. Business DAGs should
fetch the token in a separate task and pass that XCom value into Spark or Ray.

```python
from custom_operator.spark.operators.spark import SparkOperator
from custom_operator.token.operators.token import TokenOperator

test_token = TokenOperator(
    task_id="test_token",
    auth_url="https://auth-api.example.com/v3/auth/tokens",
    auth_body={"auth": {"identity": {"methods": ["password"]}}},
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
    },
)

test_token >> test_spark
```

`TokenOperator` calls `auth_url` with `auth_body` and reads `x-subject-token` from the response header.

Spark log download URL generation can be enabled by pointing the operator at
WorkspaceCoreService. During polling, every Spark detail response containing
`log_url` is sent to `POST /internal/log/v1/create`; the latest download URL is
written to XCom as `spark_log_download_url`.

```python
test_spark = SparkOperator(
    task_id="test_spark",
    spark_base_url="https://spark-api.example.com",
    token="{{ ti.xcom_pull(task_ids='test_token') }}",
    workspace_core_base_url="https://workspace-core.example.com",
    workspace_core_internal_token="{{ var.value.workspace_core_internal_token }}",
    workspace_id="12345678-1234-1234-1234-123456789012",
    name="spark-jar-demo",
    endpoint_name="endpoint1",
    spark_version="3.3.2",
    spark_jar_parameter={"main_jar": "/mnt/OBS/demo/jars/main.jar"},
)
```

For production, prefer `workspace_core_conn_id`; put the WorkspaceCoreService
base URL in Connection `host` and the internal token in `password`.

See `docs/spark_operator_design.md` for the full design.

## Ray Operator

Ray code is isolated from Spark under `src/custom_operator/ray`.

Ray uses the same task-to-task token flow:

```python
from custom_operator.ray.operators.ray import RayOperator
from custom_operator.token.operators.token import TokenOperator

test_token = TokenOperator(
    task_id="test_token",
    auth_url="https://auth-api.example.com/v3/auth/tokens",
    auth_body={"auth": {"identity": {"methods": ["password"]}}},
)

test_ray = RayOperator(
    task_id="test_ray",
    ray_base_url="https://ray-api.example.com",
    token="{{ ti.xcom_pull(task_ids='test_token') }}",
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

test_token >> test_ray
```
