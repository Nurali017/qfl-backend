from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from app.services.sota_client import SotaClient


@pytest.mark.asyncio
class TestSotaClientRequest:
    async def test_get_request_does_not_send_json_body(self):
        response = Mock()
        response.raise_for_status = Mock()

        with patch(
            "app.services.sota_client.httpx.AsyncClient.request",
            new=AsyncMock(return_value=response),
        ) as request_mock:
            client = SotaClient()
            await client._make_request("get", "https://example.com/test", json={"x": 1})

        _, kwargs = request_mock.call_args
        assert "json" not in kwargs

    async def test_post_request_sends_json_body(self):
        response = Mock()
        response.raise_for_status = Mock()

        with patch(
            "app.services.sota_client.httpx.AsyncClient.request",
            new=AsyncMock(return_value=response),
        ) as request_mock:
            client = SotaClient()
            await client._make_request("post", "https://example.com/test", json={"x": 1})

        _, kwargs = request_mock.call_args
        assert kwargs["json"] == {"x": 1}

    async def test_retry_on_transient_network_error(self):
        response = Mock()
        response.raise_for_status = Mock()

        request_mock = AsyncMock(
            side_effect=[httpx.ConnectTimeout("timeout"), response]
        )

        with patch("app.services.sota_client.httpx.AsyncClient.request", new=request_mock):
            client = SotaClient()
            result = await client._make_request("get", "https://example.com/test")

        assert result is response
        assert request_mock.await_count == 2
