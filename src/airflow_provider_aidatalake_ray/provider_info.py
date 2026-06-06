"""Airflow provider metadata for AiDatalake Ray."""

from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aidatalake",
        "name": "AiDatalake Ray",
        "description": "AiDatalake Ray operator for Airflow 3",
        "versions": ["0.1.0"],
        "hooks": [
            {
                "integration-name": "AiDatalake Ray",
                "python-modules": ["airflow_provider_aidatalake_ray.hooks.ray"],
            }
        ],
    }
