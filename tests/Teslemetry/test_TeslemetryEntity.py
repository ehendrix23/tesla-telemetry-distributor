import datetime

import pytest

from teslemetry_teslamate.Teslemetry.TeslemetryEntity import TeslemetryEntity


def test_values():
    entity = TeslemetryEntity(streaming_key="key", vehicledata_casting=int)

    assert entity.streaming_key == "key"
    assert entity.queue_key == "key"
    assert entity.vehicledata_value is None
    assert entity.streaming_value is None
    assert entity.last_update is None

    entity = TeslemetryEntity(
        streaming_key="key", queue_key="key2", vehicledata_casting=int
    )
    assert entity.streaming_key == "key"
    assert entity.queue_key == "key2"


@pytest.mark.freeze_time("2025-01-01 12:00:00")
def test_update_vehicledata():
    entity = TeslemetryEntity(streaming_key="key", vehicledata_casting=int)

    value = 10
    assert entity.update_vehicledata(value) is True
    assert entity.vehicledata_value == value
    assert entity.streaming_value == value
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    assert entity.update_vehicledata(value) is False


@pytest.mark.freeze_time("2025-01-01 12:00:00")
def test_update_stream_no_casting():
    entity = TeslemetryEntity(streaming_key="key", vehicledata_casting=None)

    value = 10.5
    assert entity.update_stream(value) == (True, True)
    assert entity.vehicledata_value == value
    assert entity.streaming_value == value
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    assert entity.update_stream(value) == (False, False)


@pytest.mark.freeze_time("2025-01-01 12:00:00")
def test_update_stream_vehiclecasting_int(freezer):
    entity = TeslemetryEntity(streaming_key="key", vehicledata_casting=int)

    value = 10.5
    assert entity.update_stream(value) == (True, True)
    assert entity.vehicledata_value == 10
    assert entity.streaming_value == value
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    freezer.move_to("2025-01-01 12:00:01")
    assert entity.update_stream(value) == (False, False)
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    value = 10.9
    freezer.move_to("2025-01-01 12:00:02")
    assert entity.update_stream(value) == (True, False)
    assert entity.vehicledata_value == 10
    assert entity.streaming_value == value
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 2)

    value = 11.5
    freezer.move_to("2025-01-01 12:00:03")
    assert entity.update_stream(value) == (True, True)
    assert entity.vehicledata_value == 11
    assert entity.streaming_value == value
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 3)


@pytest.mark.freeze_time("2025-01-01 12:00:00")
def test_update_stream_streamingcasting_int(freezer):
    entity = TeslemetryEntity(streaming_key="key", streaming_casting=int)

    value = 10.5
    assert entity.update_stream(value) == (True, True)
    assert entity.vehicledata_value == 10
    assert entity.streaming_value == 10
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    freezer.move_to("2025-01-01 12:00:01")
    assert entity.update_stream(value) == (False, False)
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    value = 10.9
    freezer.move_to("2025-01-01 12:00:02")
    assert entity.update_stream(value) == (False, False)
    assert entity.vehicledata_value == 10
    assert entity.streaming_value == 10
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 0)

    value = 11.5
    freezer.move_to("2025-01-01 12:00:03")
    assert entity.update_stream(value) == (True, True)
    assert entity.vehicledata_value == 11
    assert entity.streaming_value == 11
    assert entity.last_update == datetime.datetime(2025, 1, 1, 12, 0, 3)
