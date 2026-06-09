from types import SimpleNamespace

import pytest

from custom_operator.common.connection import connection_base_url, resolve_connection_config
from custom_operator.common.exceptions import AiDatalakeAuthError


class Reader:
    def __init__(self, conn):
        self.conn = conn
        self.calls = 0

    def get_connection(self, conn_id: str):
        self.calls += 1
        return self.conn


def test_resolve_connection_config_direct_config_does_not_read_connection():
    reader = Reader(conn=None)

    config = resolve_connection_config(
        reader,
        conn_id="compute_conn",
        base_url="https://api.example.com",
        token="direct-token",
        request_timeout=12,
        verify=False,
    )

    assert reader.calls == 0
    assert config.base_url == "https://api.example.com"
    assert config.token == "direct-token"
    assert config.timeout == 12
    assert config.verify is False


def test_resolve_connection_config_fills_missing_token_from_password():
    conn = SimpleNamespace(host="https://connection.example.com", schema=None, port=None, password="conn-token")
    reader = Reader(conn)

    config = resolve_connection_config(
        reader,
        conn_id="compute_conn",
        base_url="https://direct.example.com",
        token=None,
        request_timeout=30,
        verify=True,
    )

    assert config.base_url == "https://direct.example.com"
    assert config.token == "conn-token"


def test_resolve_connection_config_fills_missing_base_url_from_host():
    conn = SimpleNamespace(host="connection.example.com", schema="https", port=None, password="conn-token")
    reader = Reader(conn)

    config = resolve_connection_config(
        reader,
        conn_id="compute_conn",
        base_url=None,
        token="direct-token",
        request_timeout=30,
        verify=True,
    )

    assert config.base_url == "https://connection.example.com"
    assert config.token == "direct-token"


def test_resolve_connection_config_reads_base_url_and_token_from_connection():
    conn = SimpleNamespace(host="connection.example.com", schema="https", port=8443, password="conn-token")
    reader = Reader(conn)

    config = resolve_connection_config(
        reader,
        conn_id="compute_conn",
        base_url=None,
        token=None,
        request_timeout=30,
        verify=True,
    )

    assert config.base_url == "https://connection.example.com:8443"
    assert config.token == "conn-token"


def test_resolve_connection_config_requires_password_for_missing_token():
    conn = SimpleNamespace(host="connection.example.com", schema="https", port=None, password=None)
    reader = Reader(conn)

    with pytest.raises(AiDatalakeAuthError, match="Compute token must be configured"):
        resolve_connection_config(
            reader,
            conn_id="compute_conn",
            base_url="https://direct.example.com",
            token=None,
            request_timeout=30,
            verify=True,
            service_name="Compute",
        )


def test_connection_base_url_defaults_to_https_and_strips_trailing_slash():
    conn = SimpleNamespace(host="connection.example.com/", schema=None, port=None)

    assert connection_base_url(conn) == "https://connection.example.com"
