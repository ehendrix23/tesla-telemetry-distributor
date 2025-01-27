import asyncio
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from teslemetry_teslamate.Handlers.WebSocket import WebSocket

TOKEN: str = "1234"
VIN: str = "5Y1234FG"
VIN2: str = "7Y1234G"


async def setup(mock_TeslemetryHandler):
    server = WebSocket(mock_TeslemetryHandler(access_TOKEN=TOKEN))
    await server.start()
    return server, aiohttp.ClientSession()


async def stop(server, session):
    if server is not None:
        await server.stop()
    if session is not None:
        await session.close()


async def authenticate(
    ws: aiohttp.ClientWebSocketResponse, tag: str = VIN, value: str | None = None
):
    auth = {
        "msg_type": "data:subscribe_oauth",
        "token": TOKEN,
        "tag": tag,
    }
    if value is not None:
        auth["value"] = value
    await ws.send_json(auth)

    assert await ws.receive_json(timeout=2) == {
        "msg_type": f"control:hello:{tag}",
        "connection_timeout": 30000,
    }


class TestWebSocketHandler:
    server: WebSocket | None = None
    session: aiohttp.ClientSession | None = None

    @classmethod
    async def start_server(cls, mock_TeslemetryHandler):
        cls.server, cls.session = await setup(mock_TeslemetryHandler)

    @classmethod
    async def stop_server(cls):
        await stop(cls.server, cls.session)

    async def test_invalid_json(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_str("INVALID JSON")

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": "",
                "error_type": "client_error",
                "value": "Expecting valid JSON",
            }
        await self.stop_server()

    async def test_no_msg_type(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "token": TOKEN,
                    "tag": VIN,
                }
            )

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": "",
                "error_type": "client_error",
                "value": "Expecting msg_type in JSON",
            }
        await self.stop_server()

    async def test_invalid_request(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "msg_type": "Invalid",
                    "token": TOKEN,
                    "tag": VIN,
                }
            )

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": "",
                "error_type": "client_error",
                "value": "Invalid data request received",
            }
        await self.stop_server()


class TestOauth:
    server: WebSocket | None = None
    session: aiohttp.ClientSession | None = None

    @classmethod
    async def start_server(cls, mock_TeslemetryHandler):
        cls.server, cls.session = await setup(mock_TeslemetryHandler)

    @classmethod
    async def stop_server(cls):
        await stop(cls.server, cls.session)

    async def test_valid_connection(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "msg_type": "data:subscribe_oauth",
                    "token": TOKEN,
                    "tag": VIN,
                }
            )
            assert await ws.receive_json(timeout=2) == {
                "msg_type": f"control:hello:{VIN}",
                "connection_timeout": 30000,
            }
        await self.stop_server()

    async def test_no_tag_provided(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "msg_type": "data:subscribe_oauth",
                    "token": TOKEN,
                }
            )
            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": None,
                "error_type": "client_error",
                "value": "owner_api error:tag not provided",
            }
        await self.stop_server()

    async def test_no_token_provided(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "msg_type": "data:subscribe_oauth",
                    "tag": VIN,
                }
            )
            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": VIN,
                "error_type": "client_error",
                "value": "owner_api error:token not provided",
            }
        await self.stop_server()

    async def test_invalid_token_provided(self, mock_TeslemetryHandler):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        async with self.session.ws_connect("http://localhost:8081") as ws:
            await ws.send_json(
                {
                    "msg_type": "data:subscribe_oauth",
                    "token": "invalid_TOKEN",
                    "tag": VIN,
                }
            )
            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": VIN,
                "error_type": "client_error",
                "value": "Can't validate token",
            }
        await self.stop_server()


class TestHello:
    server: WebSocket | None = None
    session: aiohttp.ClientSession | None = None

    @classmethod
    async def start_server(cls, mock_TeslemetryHandler):
        cls.server, cls.session = await setup(mock_TeslemetryHandler)

    @classmethod
    async def stop_server(cls):
        await stop(cls.server, cls.session)

    async def test_hello(self, mock_TeslemetryHandler, mocker):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        mocker.patch("asyncio.sleep", Mock(side_effect=AsyncMock()))

        async with self.session.ws_connect("http://localhost:8081") as ws:
            await authenticate(ws=ws)

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "control:hello",
                "connection_timeout": 30000,
            }

        await self.stop_server()


class TestUpdate:
    server: WebSocket | None = None
    session: aiohttp.ClientSession | None = None

    @classmethod
    async def start_server(cls, mock_TeslemetryHandler):
        cls.server, cls.session = await setup(mock_TeslemetryHandler)

    @classmethod
    async def stop_server(cls):
        await stop(cls.server, cls.session)

    @pytest.mark.freeze_time("2025-01-01 00:00:00")
    async def test_update_no_values_provided(self, mock_TeslemetryHandler, mocker):
        mocker.patch.object(WebSocket, attribute="_hello", return_value=AsyncMock())
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        mocker.patch(
            "asyncio.Queue.get",
            AsyncMock(
                side_effect=[
                    {VIN: {"key1": "value1", "key2": "value2"}},
                ]
            ),
        )

        async with self.session.ws_connect("http://localhost:8081") as ws:
            await authenticate(ws=ws)

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:update",
                "data": [
                    {"key": "key1", "value": {"stringValue": "value1"}},
                    {"key": "key2", "value": {"stringValue": "value2"}},
                ],
                "vin": VIN,
                "created_at": "2025-01-01T00:00:00.000000",
            }
        await self.stop_server()

    @pytest.mark.freeze_time("2025-01-01 00:00:00")
    async def test_update_values_provided(self, mock_TeslemetryHandler, mocker):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        mocker.patch(
            "asyncio.Queue.get",
            AsyncMock(
                side_effect=[
                    {VIN: {"key1": "value1", "key2": "value2", "key3": "value3"}},
                ]
            ),
        )
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await authenticate(ws=ws, value="key1, key2")

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:update",
                "data": [
                    {"key": "key1", "value": {"stringValue": "value1"}},
                    {"key": "key2", "value": {"stringValue": "value2"}},
                ],
                "vin": VIN,
                "created_at": "2025-01-01T00:00:00.000000",
            }
        await self.stop_server()

    @pytest.mark.freeze_time("2025-01-01 00:00:00")
    async def test_update_multiple_vins(self, mock_TeslemetryHandler, mocker):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        mocker.patch(
            "asyncio.Queue.get",
            AsyncMock(
                side_effect=[
                    {VIN: {"key1": "value1", "key2": "value2"}},
                    {VIN2: {"key1": "valueA", "key2": "valueB"}},
                ]
            ),
        )

        async with self.session.ws_connect("http://localhost:8081") as ws:
            await authenticate(ws=ws, tag=f"{VIN},{VIN2}")

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:update",
                "data": [
                    {"key": "key1", "value": {"stringValue": "value1"}},
                    {"key": "key2", "value": {"stringValue": "value2"}},
                ],
                "vin": VIN,
                "created_at": "2025-01-01T00:00:00.000000",
            }

            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:update",
                "data": [
                    {"key": "key1", "value": {"stringValue": "valueA"}},
                    {"key": "key2", "value": {"stringValue": "valueB"}},
                ],
                "vin": VIN2,
                "created_at": "2025-01-01T00:00:00.000000",
            }
        await self.stop_server()

    async def test_update_server_shutdown(self, mock_TeslemetryHandler, mocker):
        await self.start_server(mock_TeslemetryHandler)
        assert self.session is not None

        mocker.patch(
            "asyncio.Queue.get",
            AsyncMock(
                side_effect=[
                    asyncio.QueueShutDown,
                ]
            ),
        )
        async with self.session.ws_connect("http://localhost:8081") as ws:
            await authenticate(ws=ws)
            assert await ws.receive_json(timeout=2) == {
                "msg_type": "data:error",
                "tag": VIN,
                "error_type": "server_shutdown",
                "value": "Server is being shutdown",
            }

        await self.stop_server()
