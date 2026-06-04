from unittest.mock import Mock

import pytest

from airflow_provider_aidatalake.clients.token import TokenProvider
from airflow_provider_aidatalake.exceptions import AiDatalakeAuthError


def test_token_provider_reads_x_subject_token_header():
    response = Mock()
    response.headers = {"x-subject-token": "token-value"}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(
        http_client=http_client,
        auth_url="https://auth.example.com/token",
        auth_body={"auth": "body"},
    )

    assert provider.get_token() == "token-value"
    http_client.request.assert_called_once_with(
        "POST",
        "https://auth.example.com/token",
        headers={"Content-Type": "application/json"},
        json={"auth": "body"},
        expected_statuses={200, 201, 204},
        retry=2,
        retry_backoff=(1.0, 2.0),
    )


def test_token_provider_requires_header():
    response = Mock()
    response.headers = {}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(http_client=http_client, auth_url="https://auth.example.com/token")

    with pytest.raises(AiDatalakeAuthError):
        provider.get_token()

