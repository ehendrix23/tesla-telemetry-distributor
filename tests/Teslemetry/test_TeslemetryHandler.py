import asyncio
from unittest.mock import AsyncMock

import pytest
from tesla_fleet_api.exceptions import InvalidToken, TeslaFleetError

from tesla_telemetry_distributor.Teslemetry.TeslemetryHandler import TeslemetryHandler
from tests.conftest import log_entry_exist
from tests.data.tesla_data import METADATA, PRODUCT_VIN1, STREAM_DATA


async def test_close(mock_teslemetry, mock_teslemetry_stream) -> None:
    teslemetry = TeslemetryHandler(access_token="1234")
    queue: asyncio.Queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    await teslemetry.close()
    assert await queue.get() == {"CLOSED": "CLOSED"}


async def test_remove_queue(mock_teslemetry, mock_teslemetry_stream) -> None:
    teslemetry = TeslemetryHandler(access_token="1234")
    queue: asyncio.Queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    assert await teslemetry.remove_queue("test") is True
    assert await teslemetry.remove_queue("test") is False
    await teslemetry.close()


async def test_properties(mock_teslemetry, mock_teslemetry_stream) -> None:
    teslemetry = TeslemetryHandler(access_token="1234")
    await teslemetry.run()

    assert teslemetry.token == "1234"
    assert teslemetry.metadata == METADATA[0]
    assert teslemetry.get_vehicle("5YJ12345EBG999999") is not None

    await teslemetry.close()


async def test_check_metadata(mock_teslemetry, mock_teslemetry_stream) -> None:
    teslemetry = TeslemetryHandler(access_token="1234")
    await teslemetry.run()

    assert len(teslemetry.scopes) == 9
    assert teslemetry.scopes == [
        "openid",
        "offline_access",
        "user_data",
        "vehicle_device_data",
        "vehicle_location",
        "vehicle_cmds",
        "vehicle_charging_cmds",
        "energy_device_data",
        "energy_cmds",
    ]

    assert len(teslemetry.products["response"]) == 2

    assert teslemetry.products["response"][0]["vin"] == "5YJ12345EBG999999"
    assert teslemetry.products["response"][1]["vin"] == "7SA12345EBG999999"
    await teslemetry.close()

    await teslemetry.get_metadata()


async def test_getmetadata_exception(mock_teslemetry, mock_teslemetry_stream) -> None:
    mock_teslemetry.return_value.metadata = AsyncMock(
        side_effect=[
            METADATA[0],
            InvalidToken(),
            TeslaFleetError(),
            TypeError(),
        ]
    )
    teslemetry = TeslemetryHandler(access_token="1234")
    await teslemetry.run()

    try:
        await teslemetry.get_metadata()
    except InvalidToken:
        pass
    else:
        pytest.fail("InvalidToken not raised")

    try:
        await teslemetry.get_metadata()
    except TeslaFleetError:
        pass
    else:
        pytest.fail("TeslaFleetError not raised")

    try:
        await teslemetry.get_metadata()
    except TypeError:
        pass
    else:
        pytest.fail("TypeError not raised")

    await teslemetry.close()


async def test_vins_changed(mock_teslemetry, mock_teslemetry_stream):
    teslemetry = TeslemetryHandler(access_token="1234", refresh=1)
    queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    await teslemetry.run()
    async with asyncio.timeout(1):
        resp1 = await queue.get()
        resp2 = await queue.get()

    assert [resp1, resp2] == [
        {"5YJ12345EBG999999": "ADDED"},
        {"7SA12345EBG999999": "ADDED"},
    ]
    vins = teslemetry.vins
    assert len(vins) == 2
    assert "5YJ12345EBG999999" in vins
    assert "7SA12345EBG999999" in vins

    await teslemetry.refresh_vehicle_data()
    async with asyncio.timeout(1):
        resp1 = await queue.get()
    assert resp1 == {"7SA12345EBG999999": "REMOVED"}
    vins = teslemetry.vins
    assert len(vins) == 1
    assert "5YJ12345EBG999999" in vins

    await teslemetry.refresh_vehicle_data()
    async with asyncio.timeout(1):
        resp1 = await queue.get()
    assert resp1 == {"7SA12345EBG999999": "ADDED"}
    vins = teslemetry.vins
    assert len(vins) == 2
    assert "5YJ12345EBG999999" in vins
    assert "7SA12345EBG999999" in vins

    await teslemetry.close()


async def test_queue_shutdown(mock_teslemetry, mock_teslemetry_stream):
    teslemetry = TeslemetryHandler(access_token="1234", refresh=1)
    queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    queue.shutdown()
    try:
        await teslemetry.sent_to_queues({"test": {"test": "test"}})
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


async def test_queue_full(mock_teslemetry, mock_teslemetry_stream):
    teslemetry = TeslemetryHandler(access_token="1234", refresh=1)
    queue = asyncio.Queue(maxsize=1)
    await teslemetry.add_queue("test", queue)

    await teslemetry.sent_to_queues({"test": {"test": "test"}})
    try:
        await teslemetry.sent_to_queues({"test": {"test": "test"}})
        await teslemetry.sent_to_queues({"test": {"test": "test"}})
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


async def test_update(mock_teslemetry, mock_teslemetry_stream, caplog):
    teslemetry = TeslemetryHandler(access_token="1234")
    queue = asyncio.Queue(maxsize=1)
    await teslemetry.add_queue("test", queue)

    await teslemetry.update({"vin": PRODUCT_VIN1["vin"]} | STREAM_DATA)

    assert PRODUCT_VIN1["vin"] in teslemetry.registered_vehicles
    assert queue.qsize() == 1
    assert queue.get_nowait() == {PRODUCT_VIN1["vin"]: "ADDED"}

    await teslemetry.update(STREAM_DATA)
    assert (
        log_entry_exist(
            caplog,
            "DEBUG",
            f"Received value without vin: {STREAM_DATA}",
        )
        is True
    )
