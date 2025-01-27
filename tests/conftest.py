"""Common methods used across tests for Tesla."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import pytest
from teslemetry_stream import TeslemetryStream

from teslemetry_teslamate.Teslemetry import TeslemetryHandler
from teslemetry_teslamate.Teslemetry.TeslemetryHandler import Teslemetry
from teslemetry_teslamate.Teslemetry.Vehicle import Vehicle
from tests.data.tesla_data import METADATA, PRODUCTS, TESLA_VEHICLE_DATA


def log_entry_exist(caplog, levelname: str, message: str) -> bool:
    """Check if a log entry exists in the caplog records."""
    log_record_exists = False
    for record in caplog.records:
        if record.levelname == levelname and record.message == message:
            log_record_exists = True
            break
    return log_record_exists


@pytest.fixture()
def mock_teslemetry():
    with patch(
        "teslemetry_teslamate.Teslemetry.TeslemetryHandler.Teslemetry", spec=Teslemetry
    ) as mocked_teslemetry:
        mocked_teslemetry.return_value.metadata = AsyncMock(side_effect=METADATA)
        mocked_teslemetry.return_value.products = AsyncMock(side_effect=PRODUCTS)
        mocked_teslemetry.return_value.vehicle_data = AsyncMock(
            return_value=TESLA_VEHICLE_DATA
        )

        yield mocked_teslemetry


@pytest.fixture
def mock_teslemetry_stream():
    with patch(
        "teslemetry_teslamate.Teslemetry.TeslemetryHandler.TeslemetryStream",
        spec=TeslemetryStream,
    ) as mocked_stream:
        mocked_stream.return_value.connect = AsyncMock()
        mocked_stream.return_value.close = Mock()

        def remove():
            pass

        mocked_stream.return_value.async_add_listener.return_value = remove

        yield mocked_stream


@pytest.fixture()
def mock_Vehicle():
    mocked_vehicle = Mock(spec=Vehicle)
    mocked_vehicle.return_value.vehicle_data = AsyncMock(
        return_value=TESLA_VEHICLE_DATA
    )

    return mocked_vehicle


@pytest.fixture()
def mock_TeslemetryHandler(mock_Vehicle, mock_teslemetry):
    mocked_handler = Mock(spec=TeslemetryHandler)

    mocked_handler.return_value.token = "1234"
    mocked_handler.return_value.metadata = METADATA[0]
    mocked_handler.return_value.products = PRODUCTS[0]

    mocked_vehicle = mock_Vehicle()
    mocked_handler.return_value.registered_vehicles.get = PropertyMock(
        side_effect=[mocked_vehicle, None]
    )

    mocked_handler.return_value.teslemetry.return_value.metadata = AsyncMock(
        side_effect=METADATA
    )
    mocked_handler.return_value.teslemetry.return_value.products = AsyncMock(
        side_effect=PRODUCTS
    )
    mocked_handler.return_value.vehicle_data = AsyncMock(
        return_value=TESLA_VEHICLE_DATA
    )
    mocked_handler.return_value.add_queue = AsyncMock()
    mocked_handler.return_value.remove_queue = AsyncMock()
    mocked_handler.return_value.sent_to_queues = AsyncMock()

    mocked_handler.return_value.teslemetry_stream.connect = AsyncMock()
    mocked_handler.return_value.teslemetry_stream.close = Mock()

    def remove():
        pass

    mocked_handler.return_value.teslemetry_stream.async_add_listener.return_value = (
        remove
    )

    yield mocked_handler
