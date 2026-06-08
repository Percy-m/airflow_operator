from unittest.mock import Mock

import pytest

from custom_operator.token.hooks.token import AuthTokenHook
from custom_operator.token.operators.token import TokenOperator
from custom_operator.token.exceptions import CustomOperatorAuthError
from custom_operator.token.token import TokenProvider, mask_token


def test_mask_token_shows_first_five_characters_only():
    assert mask_token("abcdefghij") == "abcde***"
    assert mask_token("abcde") == "abcde***"
    assert mask_token("abcd") == "***"
    assert mask_token("") == "<empty>"


def test_token_provider_reads_x_subject_token_header():
    response = Mock()
    response.headers = {"x-subject-token": "dynamic-token"}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(
        http_client=http_client,
        auth_url="/v3/auth/tokens",
        auth_body={"auth": "body"},
    )

    assert provider.get_token() == "dynamic-token"
    http_client.request.assert_called_once_with(
        "POST",
        "/v3/auth/tokens",
        headers={"Content-Type": "application/json"},
        json={"auth": "body"},
        expected_statuses={200, 201, 204},
        retry=0,
    )


def test_token_provider_requires_x_subject_token_header():
    response = Mock()
    response.headers = {}
    http_client = Mock()
    http_client.request.return_value = response

    provider = TokenProvider(http_client=http_client, auth_url="/v3/auth/tokens", auth_body={})

    with pytest.raises(CustomOperatorAuthError):
        provider.get_token()


def test_auth_token_hook_uses_auth_url_auth_body_and_runtime_options(monkeypatch):
    captured = {}
    response = Mock()
    response.headers = {"x-subject-token": "dynamic-token"}

    class FakeHttpClient:
        def __init__(self, *, timeout, verify):
            captured["timeout"] = timeout
            captured["verify"] = verify

        def request(self, *args, **kwargs):
            captured["request"] = (args, kwargs)
            return response

    monkeypatch.setattr("custom_operator.token.hooks.token.HttpClient", FakeHttpClient)

    hook = AuthTokenHook(
        auth_url="https://auth.example.com/v3/auth/tokens",
        auth_body={"auth": "body"},
        request_timeout=12,
        verify=False,
    )

    assert hook.get_token() == "dynamic-token"
    assert captured["timeout"] == 12
    assert captured["verify"] is False
    assert captured["request"] == (
        ("POST", "https://auth.example.com/v3/auth/tokens"),
        {
            "headers": {"Content-Type": "application/json"},
            "json": {"auth": "body"},
            "expected_statuses": {200, 201, 204},
            "retry": 0,
        },
    )


def test_token_operator_passes_only_auth_config_and_runtime_options(monkeypatch):
    captured = {}

    class FakeAuthTokenHook:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def get_token(self):
            return "operator-token"

    monkeypatch.setattr("custom_operator.token.operators.token.AuthTokenHook", FakeAuthTokenHook)

    operator = TokenOperator(
        task_id="test_token",
        auth_url="https://auth.example.com/v3/auth/tokens",
        auth_body={"auth": "body"},
        request_timeout=12,
        verify=False,
    )

    assert operator.execute({}) == "operator-token"
    assert captured == {
        "auth_url": "https://auth.example.com/v3/auth/tokens",
        "auth_body": {"auth": "body"},
        "request_timeout": 12,
        "verify": False,
    }
