from teslemetry_teslamate.helpers import (
    cast_bool,
    cast_latitude,
    cast_longitude,
    cast_round_2,
)


def test_cast_bool():
    assert cast_bool("true") is True
    assert cast_bool("True") is True
    assert cast_bool("false") is False
    assert cast_bool("False") is False


def test_cast_round_2():
    assert cast_round_2(1.234567) == 1.23
    assert cast_round_2(1.235567) == 1.24


def test_cast_latitude():
    assert cast_latitude({"latitude": 1.234567, "longitude": -104.345}) == 1.234567
    assert cast_latitude({"longitude": -104.345}) == 0.0


def test_cast_longitude():
    assert cast_longitude({"latitude": 1.234567, "longitude": -104.345}) == -104.345
    assert cast_longitude({"latitude": -104.345}) == 0.0
