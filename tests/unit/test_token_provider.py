from unittest.mock import Mock

import pytest

from airflow_provider_aidatalake.clients.token import TokenProvider, mask_token
from airflow_provider_aidatalake.exceptions import AiDatalakeAuthError


def test_mask_token_shows_first_five_characters_only():
    assert mask_token("abcdefghij") == "abcde***"
    assert mask_token("abcde") == "abcde***"
    assert mask_token("abcd") == "***"
    assert mask_token("") == "<empty>"


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
        retry=0,
    )


def test_token_provider_logs_masked_token_only(caplog):
    response = Mock()
    response.headers = {"x-subject-token": "abcdefghij"}
    http_client = Mock()
    http_client.request.return_value = response
    provider = TokenProvider(http_client=http_client, auth_url="https://auth.example.com/token")

    with caplog.at_level("INFO", logger="airflow_provider_aidatalake.clients.token"):
        assert provider.refresh_token() == "abcdefghij"

    log_output = "\n".join(record.getMessage() for record in caplog.records)
    assert "abcde***" in log_output
    assert "abcdefghij" not in log_output


def test_token_provider_requires_header():
    response = Mock()
    response.headers = {}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(http_client=http_client, auth_url="https://auth.example.com/token")

    with pytest.raises(AiDatalakeAuthError):
        provider.get_token()
