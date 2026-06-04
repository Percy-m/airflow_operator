from airflow_provider_aidatalake.operators.spark import SparkOperator


def test_spark_operator_defaults_to_jar_job_and_builds_payload():
    operator = SparkOperator(
        task_id="spark_jar",
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="endpoint1",
        spark_version="3.3.2",
        spark_jar_parameter={
            "main_class": "com.example.Main",
            "main_jar": "/mnt/OBS/bucket/main.jar",
            "main_args": ["--input", "/mnt/OBS/bucket/input"],
        },
    )

    operator._validate()
    payload = operator._build_payload()

    assert payload["job_config"]["job_type"] == "spark_jar_job"
    assert payload["job_config"]["spark_jar_parameter"]["main_jar"] == "obs://bucket/main.jar"
    assert payload["job_config"]["spark_jar_parameter"]["main_args"] == [
        "--input",
        "obs://bucket/input",
    ]

