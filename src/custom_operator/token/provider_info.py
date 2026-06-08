"""Airflow provider metadata for token acquisition."""

from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-aidatalake",
        "name": "AiDatalake Token",
        "description": "Dynamic token acquisition helpers for AiDatalake operators",
        "versions": ["0.1.0"],
        "hooks": [
            {
                "integration-name": "AiDatalake Token",
                "python-modules": ["custom_operator.token.hooks.token"],
            }
        ],
    }
