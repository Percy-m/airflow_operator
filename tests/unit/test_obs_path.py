from airflow_provider_aidatalake.utils.obs_path import convert_obs_paths


def test_convert_local_obs_path_string():
    assert convert_obs_paths("/mnt/OBS/bucket/path/file.py") == "obs://bucket/path/file.py"


def test_convert_nested_obs_paths():
    value = {
        "main_args": ["--input", "/mnt/OBS/bucket/input", "--plain", "value"],
        "nested": {"path": "/mnt/OBS/bucket/output"},
    }

    assert convert_obs_paths(value) == {
        "main_args": ["--input", "obs://bucket/input", "--plain", "value"],
        "nested": {"path": "obs://bucket/output"},
    }


def test_keep_existing_obs_uri():
    assert convert_obs_paths("obs://bucket/path") == "obs://bucket/path"

