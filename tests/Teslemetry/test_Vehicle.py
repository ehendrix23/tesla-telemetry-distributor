import asyncio
from copy import deepcopy

from tesla_telemetry_distributor.Teslemetry.TeslemetryHandler import TeslemetryHandler
from tesla_telemetry_distributor.Teslemetry.Vehicle import Vehicle
from tests.conftest import log_entry_exist
from tests.data.tesla_data import STREAM_DATA, TESLA_VEHICLE_DATA

VEHICLE_DATA = {"vehicle_data": TESLA_VEHICLE_DATA["response"]}


async def test_no_streaming(mock_TeslemetryHandler, caplog):
    mock_TeslemetryHandler.return_value.teslemetry_stream = None
    mock_TeslemetryHandler.return_value.teslemetry = None

    vehicle = Vehicle(
        teslemetry_handler=mock_TeslemetryHandler(access_token="1234"), vin="1234"
    )

    await vehicle.vehicle_data()
    assert (
        log_entry_exist(
            caplog,
            "ERROR",
            "1234: Teslemetry is not available, unable to update vehicle data",
        )
        is True
    )


async def test_updated_value_vehicle_data():
    teslemetry = TeslemetryHandler(access_token="1234")
    vehicle = Vehicle(teslemetry_handler=teslemetry, vin="1234")

    queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    await vehicle.updated_value(value=VEHICLE_DATA)
    assert queue.qsize() == 1
    assert queue.get_nowait() == {
        "1234": {
            "state": VEHICLE_DATA["vehicle_data"]["state"],
            "BatteryHeaterOn": VEHICLE_DATA["vehicle_data"]["charge_state"][
                "battery_heater_on"
            ],
            "InsideTemp": VEHICLE_DATA["vehicle_data"]["climate_state"]["inside_temp"],
            "supercharging_enabled": VEHICLE_DATA["vehicle_data"][
                "supercharging_enabled"
            ],
            "est_lat": VEHICLE_DATA["vehicle_data"]["drive_state"]["latitude"],
            "est_lng": VEHICLE_DATA["vehicle_data"]["drive_state"]["longitude"],
        }
    }  # state
    queue.task_done()

    await vehicle.updated_value(value=VEHICLE_DATA)
    assert queue.qsize() == 0

    await vehicle.updated_value(value={"vehicle_data": {"state": "offline"}})
    assert queue.qsize() == 1

    assert queue.get_nowait() == {
        "1234": {
            "state": "offline",
        }
    }  # state
    queue.task_done()

    await vehicle.updated_value(
        value={
            "vehicle_data": {
                "drive_state": {
                    "latitude": 30.222798114771205,
                    "longitude": -97.61875183086214,
                }
            }
        }
    )
    assert queue.qsize() == 1
    assert queue.get_nowait() == {
        "1234": {
            "est_lat": 30.222798114771205,
            "est_lng": -97.61875183086214,
        }
    }  # drive_state.latitude
    queue.task_done()


async def test_updated_value_streaming():
    teslemetry = TeslemetryHandler(access_token="1234")
    vehicle = Vehicle(teslemetry_handler=teslemetry, vin="1234")

    queue = asyncio.Queue()
    await teslemetry.add_queue("test", queue)

    await vehicle.updated_value(value=STREAM_DATA)
    assert queue.qsize() == 1
    assert queue.get_nowait() == {
        "1234": {
            "state": STREAM_DATA["data"]["state"],
            "BatteryHeaterOn": STREAM_DATA["data"]["BatteryHeaterOn"],
            "InsideTemp": STREAM_DATA["data"]["InsideTemp"],
            "supercharging_enabled": STREAM_DATA["data"]["supercharging_enabled"],
            "est_lat": STREAM_DATA["data"]["Location"]["latitude"],
            "est_lng": STREAM_DATA["data"]["Location"]["longitude"],
            "Location": {
                "latitude": 37.49342052074031,
                "longitude": -121.94498020015668,
            },
        }
    }  # state
    queue.task_done()

    await vehicle.updated_value(value=STREAM_DATA)
    assert queue.qsize() == 0

    await vehicle.updated_value(value={"data": {"state": "offline"}})
    assert queue.qsize() == 1

    assert queue.get_nowait() == {
        "1234": {
            "state": "offline",
        }
    }  # state
    queue.task_done()

    await vehicle.updated_value(
        value={
            "data": {
                "Location": {
                    "latitude": 30.222798114771205,
                    "longitude": -97.61875183086214,
                }
            }
        }
    )
    assert queue.qsize() == 1
    assert queue.get_nowait() == {
        "1234": {
            "est_lat": 30.222798114771205,
            "est_lng": -97.61875183086214,
            "Location": {
                "latitude": 30.222798114771205,
                "longitude": -97.61875183086214,
            },
        }
    }  # drive_state.latitude
    queue.task_done()


async def test_get_vehicle_data(mock_TeslemetryHandler, mocker):
    vehicle = Vehicle(
        teslemetry_handler=mock_TeslemetryHandler(access_token="1234"), vin="1234"
    )
    tesla_data = deepcopy(TESLA_VEHICLE_DATA)

    data = await vehicle.vehicle_data()
    assert data["state"] == tesla_data["response"]["state"]
    assert (
        data["charge_state"]["battery_heater_on"]
        == tesla_data["response"]["charge_state"]["battery_heater_on"]
    )
    assert (
        data["climate_state"]["inside_temp"]
        == tesla_data["response"]["climate_state"]["inside_temp"]
    )
    assert (
        data["supercharging_enabled"] == tesla_data["response"]["supercharging_enabled"]
    )
    assert (
        data["drive_state"]["latitude"]
        == tesla_data["response"]["drive_state"]["latitude"]
    )
    assert (
        data["drive_state"]["longitude"]
        == tesla_data["response"]["drive_state"]["longitude"]
    )
    assert isinstance(data["charge_schedules"], list)
    assert len(data["charge_schedules"]) == 2
    assert isinstance(data["charge_schedules"][0], dict)
    assert data["charge_schedules"][0] == {
        "id": 1,
        "name": "Test Schedule 1",
        "days_of_week": 12345,
    }
    assert isinstance(data["charge_schedules"][1], dict)
    assert data["charge_schedules"][1] == {
        "id": 2,
        "name": "Test Schedule 2",
        "days_of_week": 12,
    }

    tesla_data["response"]["state"] = "offline"
    await vehicle.updated_value(value={"vehicle_data": {"state": "offline"}})
    data = await vehicle.vehicle_data()
    assert data["state"] == tesla_data["response"]["state"]
    assert (
        data["charge_state"]["battery_heater_on"]
        == tesla_data["response"]["charge_state"]["battery_heater_on"]
    )
    assert (
        data["climate_state"]["inside_temp"]
        == tesla_data["response"]["climate_state"]["inside_temp"]
    )
    assert (
        data["supercharging_enabled"] == tesla_data["response"]["supercharging_enabled"]
    )
    assert (
        data["drive_state"]["latitude"]
        == tesla_data["response"]["drive_state"]["latitude"]
    )
    assert (
        data["drive_state"]["longitude"]
        == tesla_data["response"]["drive_state"]["longitude"]
    )

    mock_get_vehicle_data = mocker.patch.object(
        Vehicle, "_get_tesla_vehicle_data", return_value={}
    )
    await vehicle.updated_value(value={"vehicle_data": {"state": "offline"}})

    data = await vehicle.vehicle_data()
    mock_get_vehicle_data.assert_not_called()
