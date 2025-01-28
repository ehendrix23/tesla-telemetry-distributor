import aiohttp
import pytest

from tesla_telemetry_distributor.Handlers.WebServer import (
    INVALID_TOKEN,
    TOKEN_NOT_PROVIDED,
    VIN_NOT_FOUND,
    WebServer,
)
from tests.data.tesla_data import METADATA, PRODUCTS, TESLA_VEHICLE_DATA


async def test_oauth(mock_TeslemetryHandler):
    server = WebServer(mock_TeslemetryHandler(access_token="1234"))
    await server.start()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=1234"
        ) as resp:
            assert resp.status == 200
            text = await resp.json()
            assert "access_token" in text
            assert "expires_in" in text
            assert text["token_type"] == "Bearer"

        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=invalid"
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN

        async with session.post("http://localhost:8080/api/oauth2/v3/token") as resp:
            assert resp.status == 400
            assert await resp.text() == TOKEN_NOT_PROVIDED

    await server.stop()


@pytest.mark.freeze_time("2025-01-01 00:00:00")
async def test_check_invalid_tokens(mock_TeslemetryHandler, freezer):
    teslemetry = mock_TeslemetryHandler(access_token="1234")
    server = WebServer(teslemetry=teslemetry)
    await server.start()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=1234"
        ) as resp:
            resp = await resp.json()
            token = resp["access_token"]

        async with session.get(
            "http://localhost:8080/api/metadata?token=invalid"
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN

        async with session.get("http://localhost:8080/api/metadata") as resp:
            assert resp.status == 400
            assert await resp.text() == TOKEN_NOT_PROVIDED

        freezer.move_to("2025-01-01 08:00:01")
        # Dummy wait to allow the refresh token to expire
        await teslemetry.teslemetry_stream.connect(0)
        async with session.get(
            f"http://localhost:8080/api/metadata?token={token}",
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN
    await server.stop()


async def test_metadata(mock_TeslemetryHandler):
    server = WebServer(mock_TeslemetryHandler(access_token="1234"))
    await server.start()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=1234"
        ) as resp:
            resp = await resp.json()
            token = resp["access_token"]

        async with session.get(
            f"http://localhost:8080/api/metadata?token={token}"
        ) as resp:
            assert resp.status == 200
            assert await resp.json() == METADATA[0]

        async with session.get(
            "http://localhost:8080/api/metadata?token=invalid"
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN

    await server.stop()


async def test_product(mock_TeslemetryHandler):
    server = WebServer(mock_TeslemetryHandler(access_token="1234"))
    await server.start()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=1234"
        ) as resp:
            resp = await resp.json()
            token = resp["access_token"]

        async with session.get(
            f"http://localhost:8080/api/1/products?token={token}"
        ) as resp:
            assert resp.status == 200
            assert await resp.json() == PRODUCTS[0]

        async with session.get(
            "http://localhost:8080/api/1/products?token=invalid"
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN

    await server.stop()


async def test_vehicle_data(mock_TeslemetryHandler):
    server = WebServer(mock_TeslemetryHandler(access_token="1234"))
    await server.start()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8080/api/oauth2/v3/token?token=1234"
        ) as resp:
            resp = await resp.json()
            token = resp["access_token"]

        async with session.get(
            f"http://localhost:8080/api/1/vehicles/vin123/vehicle_data?token={token}"
        ) as resp:
            assert resp.status == 200
            assert await resp.json() == TESLA_VEHICLE_DATA

        async with session.get(
            f"http://localhost:8080/api/1/vehicles/vin123/vehicle_data?token={token}"
        ) as resp:
            assert resp.status == 404
            assert await resp.text() == VIN_NOT_FOUND

        async with session.get(
            "http://localhost:8080/api/1/vehicles/vin123/vehicle_data?token=invalid"
        ) as resp:
            assert resp.status == 401
            assert await resp.text() == INVALID_TOKEN

    await server.stop()
