"""Airflow provider metadata."""

from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aidatalake",
        "name": "AiDatalake",
        "description": "AiDatalake Spark operator for Airflow 3",
        "versions": ["0.1.0"],
        "hooks": [
            {
                "integration-name": "AiDatalake Spark",
                "python-modules": ["airflow_provider_aidatalake.hooks.spark"],
            }
        ],
    }

