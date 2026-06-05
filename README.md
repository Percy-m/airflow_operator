# Airflow Provider AiDatalake

Airflow 3.0 custom provider for submitting, polling, and cancelling AiDatalake Spark jobs.

Testing environments can configure endpoints directly on `SparkOperator`:

```python
SparkOperator(
    task_id="spark_jar_task",
    spark_base_url="https://spark-api.example.com",
    auth_url="https://auth.example.com/v3/auth/tokens",
    auth_body={"TODO": "token request body"},
    auth_headers={},
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
