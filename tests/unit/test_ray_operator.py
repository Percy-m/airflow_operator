from airflow_provider_aidatalake_ray.operators.ray import RayOperator


def test_ray_operator_builds_api_payload_and_converts_runtime_env_paths_only():
    operator = RayOperator(
        task_id="ray_task",
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="ray-endpoint",
        entrypoint="python /mnt/OBS/bucket/train.py",
        runtime_env={
            "working_dir": "/mnt/OBS/bucket/project",
            "py_modules": ["/mnt/OBS/bucket/libs/lib.zip", "obs://bucket/other.zip"],
            "pip": ["numpy==1.26.0"],
            "env_vars": {"ENV": "test"},
        },
        entrypoint_num_cpus=2,
        entrypoint_num_gpus=1,
        entrypoint_memory=1024,
    )

    operator._validate()
    payload = operator._build_payload({})

    assert payload == {
        "name": "demo",
        "endpoint_name": "ray-endpoint",
        "config": {
            "entrypoint": "python /mnt/OBS/bucket/train.py",
            "runtime_env": {
                "working_dir": "obs://bucket/project",
                "py_modules": ["obs://bucket/libs/lib.zip", "obs://bucket/other.zip"],
                "pip": ["numpy==1.26.0"],
                "env_vars": {"ENV": "test"},
            },
            "entrypoint_num_cpus": 2,
            "entrypoint_num_gpus": 1,
            "entrypoint_memory": 1024,
        },
    }


def test_ray_operator_accepts_direct_test_token_config():
    operator = RayOperator(
        task_id="ray_task",
        ray_base_url="https://ray-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="ray-endpoint",
        entrypoint="python train.py",
    )

    hook = operator._hook()

    assert hook.ray_base_url == "https://ray-api.example.com"
    assert hook.token == "test-token"
    assert hook.request_timeout == 12
    assert hook.verify is False
