from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from custom_operator.token.connection import resolve_auth_connection_config, resolve_connection_config
from custom_operator.token.exceptions import CustomOperatorAuthError
from custom_operator.token.token import StaticTokenProvider, TokenProvider, mask_token


def test_mask_token_shows_first_five_characters_only():
    assert mask_token("abcdefghij") == "abcde***"
    assert mask_token("abcde") == "abcde***"
    assert mask_token("abcd") == "***"
    assert mask_token("") == "<empty>"


def test_static_token_provider_returns_fixed_token():
    provider = StaticTokenProvider("token-value")

    assert provider.get_token() == "token-value"
    assert provider.refresh_token() == "token-value"


def test_token_provider_reads_x_subject_token_header():
    response = Mock()
    response.headers = {"x-subject-token": "dynamic-token"}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(
        http_client=http_client,
        auth_url="/v3/auth/tokens",
        auth_body={"auth": "body"},
        auth_headers={"X-Test": "1"},
    )

    assert provider.get_token() == "dynamic-token"
    http_client.request.assert_called_once_with(
        "POST",
        "/v3/auth/tokens",
        headers={"Content-Type": "application/json", "X-Test": "1"},
        json={"auth": "body"},
        expected_statuses={200, 201, 204},
        retry=0,
    )


def test_token_provider_requires_x_subject_token_header():
    response = Mock()
    response.headers = {}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(http_client=http_client, auth_url="/v3/auth/tokens")

    with pytest.raises(CustomOperatorAuthError):
        provider.get_token()


def test_resolve_connection_config_prefers_dag_token():
    class Hook:
        def get_connection(self, conn_id):
            raise AssertionError("get_connection should not be called")

    config = resolve_connection_config(
        Hook(),
        conn_id="conn",
        base_url="https://api.example.com",
        token="dag-token",
        request_timeout=12,
        verify=False,
        service_name="Spark",
    )

    assert config.base_url == "https://api.example.com"
    assert config.token == "dag-token"
    assert config.timeout == 12
    assert config.verify is False


def test_resolve_connection_config_reads_extra_token():
    conn = SimpleNamespace(
        host="https://api.example.com",
        schema=None,
        port=None,
        password="password-token",
        extra_dejson={"token": "extra-token", "timeout": 15, "verify": "false"},
    )

    class Hook:
        def get_connection(self, conn_id):
            return conn

    config = resolve_connection_config(
        Hook(),
        conn_id="conn",
        base_url=None,
        token=None,
        request_timeout=30,
        verify=True,
        service_name="Ray",
    )

    assert config.base_url == "https://api.example.com"
    assert config.token == "extra-token"
    assert config.timeout == 15
    assert config.verify is False


def test_resolve_connection_config_reads_password_token():
    conn = SimpleNamespace(
        host="api.example.com",
        schema="https",
        port=8443,
        password="password-token",
        extra_dejson={},
    )

    class Hook:
        def get_connection(self, conn_id):
            return conn

    config = resolve_connection_config(
        Hook(),
        conn_id="conn",
        base_url=None,
        token=None,
        request_timeout=30,
        verify=True,
        service_name="Ray",
    )

    assert config.base_url == "https://api.example.com:8443"
    assert config.token == "password-token"


def test_resolve_connection_config_requires_token():
    conn = SimpleNamespace(
        host="api.example.com",
        schema="https",
        port=None,
        password=None,
        extra_dejson={},
    )

    class Hook:
        def get_connection(self, conn_id):
            return conn

    with pytest.raises(CustomOperatorAuthError):
        resolve_connection_config(
            Hook(),
            conn_id="conn",
            base_url=None,
            token=None,
            request_timeout=30,
            verify=True,
            service_name="Ray",
        )


def test_resolve_auth_connection_config_reads_auth_extra():
    conn = SimpleNamespace(
        host="https://auth.example.com",
        schema=None,
        port=None,
        password=None,
        extra_dejson={
            "auth_url": "/v3/auth/tokens",
            "auth_body": {"auth": "body"},
            "auth_headers": {"X-Test": "1"},
            "timeout": 15,
            "verify": "false",
        },
    )

    class Hook:
        def get_connection(self, conn_id):
            return conn

    config = resolve_auth_connection_config(
        Hook(),
        conn_id="auth_conn",
    )

    assert config.base_url == "https://auth.example.com"
    assert config.auth_url == "/v3/auth/tokens"
    assert config.auth_body == {"auth": "body"}
    assert config.auth_headers == {"X-Test": "1"}
    assert config.timeout == 15
    assert config.verify is False
