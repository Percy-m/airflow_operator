# Airflow Provider AiDatalake

Airflow 3.0 custom provider for submitting, polling, and cancelling AiDatalake Spark and Ray jobs.

Dynamic token acquisition lives in `custom_operator.token`. Business DAGs should
fetch the token in a separate task and pass that XCom value into Spark or Ray.

```python
from custom_operator.spark.operators.spark import SparkOperator
from custom_operator.token.operators.token import TokenOperator

task_token = TokenOperator(
    task_id="task_token",
    token_conn_id="aidatalake_token",
)

task_spark = SparkOperator(
    task_id="spark_jar_task",
    spark_base_url="https://spark-api.example.com",
    token="{{ ti.xcom_pull(task_ids='task_token') }}",
    workspace_id="12345678-1234-1234-1234-123456789012",
    name="spark-jar-demo",
    endpoint_name="endpoint1",
    spark_version="3.3.2",
    spark_jar_parameter={
        "main_class": "com.example.Main",
        "main_jar": "/mnt/OBS/demo/jars/main.jar",
    },
)

task_token >> task_spark
```

`TokenOperator` calls the configured auth API and reads `x-subject-token` from the response header.

See `docs/spark_operator_design.md` for the full design.

## Ray Operator

Ray code is isolated from Spark under `src/custom_operator/ray`.

Ray uses the same task-to-task token flow:

```python
from custom_operator.ray.operators.ray import RayOperator
from custom_operator.token.operators.token import TokenOperator

task_token = TokenOperator(
    task_id="task_token",
    token_conn_id="aidatalake_token",
)

task_ray = RayOperator(
    task_id="ray_train",
    ray_base_url="https://ray-api.example.com",
    token="{{ ti.xcom_pull(task_ids='task_token') }}",
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

task_token >> task_ray
```
