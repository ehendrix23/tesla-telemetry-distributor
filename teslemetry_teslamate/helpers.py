def cast_bool(val: str) -> bool:
    """Convert bool string to actual bool."""
    return val.lower() in ["true", "True"]


def cast_round_2(val: float) -> float:
    """Round to 2 decimal places."""
    return round(val, 2)


def cast_latitude(val: dict) -> float:
    """Return the latitude."""
    return val.get("latitude", 0.0)


def cast_longitude(val: dict) -> float:
    """Return the longitude."""
    return val.get("longitude", 0.0)
