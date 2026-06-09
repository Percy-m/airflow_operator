from unittest.mock import Mock

import pytest
import requests

from custom_operator.common.exceptions import AiDatalakeApiError
from custom_operator.common.http_client import HttpClient


def response(status_code: int, *, text: str = "", body=None):
    item = Mock()
    item.status_code = status_code
    item.text = text
    if body is None:
        item.json.side_effect = ValueError("not json")
    else:
        item.json.return_value = body
    return item


def test_http_client_builds_url_and_returns_success_response():
    session = Mock()
    session.request.return_value = response(200, text="ok")
    client = HttpClient(base_url="https://api.example.com/", timeout=12, verify=False, session=session)

    result = client.request("GET", "/jobs", headers={"X": "1"})

    assert result.status_code == 200
    session.request.assert_called_once_with(
        method="GET",
        url="https://api.example.com/jobs",
        headers={"X": "1"},
        json=None,
        timeout=12,
        verify=False,
    )


def test_http_client_raises_api_error_from_json_body():
    session = Mock()
    session.request.return_value = response(
        400,
        text="bad request",
        body={"error_msg": "invalid payload", "error_code": "E001", "request_id": "req-1"},
    )
    client = HttpClient(base_url="https://api.example.com", session=session)

    with pytest.raises(AiDatalakeApiError) as exc_info:
        client.request("POST", "/jobs")

    assert str(exc_info.value) == "invalid payload"
    assert exc_info.value.status_code == 400
    assert exc_info.value.error_code == "E001"
    assert exc_info.value.request_id == "req-1"
    assert exc_info.value.retryable is False


def test_http_client_uses_injected_api_error_class():
    class CustomApiError(AiDatalakeApiError):
        pass

    session = Mock()
    session.request.side_effect = requests.Timeout("timed out")
    client = HttpClient(base_url="https://api.example.com", session=session, api_error_cls=CustomApiError)

    with pytest.raises(CustomApiError) as exc_info:
        client.request("GET", "/jobs")

    assert "timed out" in str(exc_info.value)
    assert exc_info.value.retryable is True
