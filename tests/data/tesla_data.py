"""Test data for Tesla API."""

ACCESS_DATA = {
    "proxy": True,
    "access": True,
    "polling": True,
    "firmware": "2024.45.25",
}


SCOPES = [
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


METADATA = [
    {
        "uid": "970afb60-7bf9-486d-91c0-ace3fa5b07ea",
        "region": "NA",
        "scopes": SCOPES,
        "vins": ["7SA12345EBG999999", "5YJ12345EBG999999"],
        "vehicles": {
            "7SA12345EBG999999": ACCESS_DATA,
            "5YJ12345EBG999999": ACCESS_DATA,
        },
    },
    {
        "uid": "970afb60-7bf9-486d-91c0-ace3fa5b07ea",
        "region": "NA",
        "scopes": SCOPES,
        "vins": ["5YJ12345EBG999999"],
        "vehicles": {
            "5YJ12345EBG999999": ACCESS_DATA,
        },
    },
    {
        "uid": "970afb60-7bf9-486d-91c0-ace3fa5b07ea",
        "region": "NA",
        "scopes": SCOPES,
        "vins": ["5YJ12345EBG999999"],
        "vehicles": {
            "7SA12345EBG999999": ACCESS_DATA,
            "5YJ12345EBG999999": ACCESS_DATA,
        },
    },
]
PRODUCT_VIN1 = {
    "id": 3912395738573902,
    "user_id": 999999,
    "vehicle_id": 3758493957372949,
    "vin": "5YJ12345EBG999999",
    "color": None,
    "access_type": "OWNER",
    "display_name": "Teslemetry",
    "option_codes": None,
    "cached_data": "cached",
    "mobile_access_disabled": False,
    "granular_access": {"hide_private": False},
    "tokens": None,
    "state": "offline",
    "in_service": False,
    "id_s": "3912395738573902",
    "calendar_enabled": True,
    "api_version": 84,
    "backseat_token": None,
    "backseat_token_updated_at": None,
    "ble_autopair_enrolled": False,
    "device_type": "vehicle",
    "command_signing": "required",
    "release_notes_supported": True,
}
PRODUCT_VIN2 = {
    "id": 2847675894029385,
    "user_id": 999999,
    "vehicle_id": 4753932928475849,
    "vin": "7SA12345EBG999999",
    "color": None,
    "access_type": "OWNER",
    "display_name": "Distributor",
    "option_codes": None,
    "cached_data": "cached",
    "mobile_access_disabled": False,
    "granular_access": {"hide_private": False},
    "tokens": None,
    "state": "online",
    "in_service": False,
    "id_s": "2847675894029385",
    "calendar_enabled": True,
    "api_version": 84,
    "backseat_token": None,
    "backseat_token_updated_at": None,
    "ble_autopair_enrolled": False,
    "device_type": "vehicle",
    "command_signing": "required",
    "release_notes_supported": True,
}

PRODUCTS = [
    {
        "response": [PRODUCT_VIN1, PRODUCT_VIN2],
        "count": 2,
    },
    {
        "response": [PRODUCT_VIN1],
        "count": 1,
    },
    {
        "response": [PRODUCT_VIN1, PRODUCT_VIN2],
        "count": 2,
    },
]

TESLA_VEHICLE_DATA = {
    "response": {
        "state": "online",
        "charge_state": {"battery_heater_on": "true"},
        "climate_state": {"inside_temp": 70},
        "supercharging_enabled": "true",
        "drive_state": {
            "latitude": 37.49342052074031,
            "longitude": -121.94498020015668,
        },
        "charge_schedules": [
            {
                "id": 1,
                "name": "Test Schedule 1",
                "days_of_week": 12345,
            },
            {
                "id": 2,
                "name": "Test Schedule 2",
                "days_of_week": 12,
            },
        ],
    }
}

STREAM_DATA = {
    "data": {
        "state": "online",
        "BatteryHeaterOn": "true",
        "InsideTemp": 70.005,
        "supercharging_enabled": "true",
        "Location": {"latitude": 37.49342052074031, "longitude": -121.94498020015668},
    }
}
