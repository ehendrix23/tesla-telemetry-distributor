from datetime import datetime
from typing import Union

from teslemetry_stream.const import Signal

from .helpers import cast_bool, cast_latitude, cast_longitude, cast_round_2
from .Teslemetry.TeslemetryEntity import TeslemetryEntity, TeslemetryEntityList

type StateType = str | int | float | datetime | None | bool
TeslaVehicleDataType = dict[
    str, Union[StateType, "TeslaVehicleDataType", list["TeslaVehicleDataType"]]
]
VehicleEntityType = dict[str, TeslemetryEntity]
VehicleDataType = dict[
    str, Union[TeslemetryEntity, TeslemetryEntityList, "VehicleDataType"]
]


STREAMING_VEHICLE_DATA: VehicleDataType = {
    "id": TeslemetryEntity(queue_key="id", vehicledata_casting=int),
    "user_id": TeslemetryEntity(queue_key="user_id", vehicledata_casting=int),
    "vehicle_id": TeslemetryEntity(queue_key="vehicle_id", vehicledata_casting=int),
    "vin": TeslemetryEntity(queue_key="vin", vehicledata_casting=str),
    "color": TeslemetryEntity(queue_key="color", vehicledata_casting=str),
    "access_type": TeslemetryEntity(queue_key="access_type", vehicledata_casting=str),
    "granular_access": {
        "hide_private": TeslemetryEntity(
            queue_key="hide_private", vehicledata_casting=bool
        )
    },
    "tokens": TeslemetryEntity(queue_key="tokens", vehicledata_casting=str),
    "state": TeslemetryEntity(streaming_key="state", vehicledata_casting=str),
    "in_service": TeslemetryEntity(queue_key="in_service", vehicledata_casting=bool),
    "id_s": TeslemetryEntity(queue_key="id_s", vehicledata_casting=str),
    "calendar_enabled": TeslemetryEntity(
        queue_key="calendar_enabled", vehicledata_casting=bool
    ),
    "api_version": TeslemetryEntity(queue_key="api_version", vehicledata_casting=int),
    "backseat_token": TeslemetryEntity(
        queue_key="backseat_token", vehicledata_casting=str
    ),
    "backseat_token_updated_at": TeslemetryEntity(
        queue_key="backseat_token_updated_at", vehicledata_casting=str
    ),
    "ble_autopair_enrolled": TeslemetryEntity(
        queue_key="ble_autopair_enrolled", vehicledata_casting=bool
    ),
    "supercharger_payment_needed": TeslemetryEntity(
        queue_key="supercharger_payment_needed", vehicledata_casting=cast_bool
    ),
    "supercharging_enabled": TeslemetryEntity(
        queue_key="supercharging_enabled", vehicledata_casting=bool
    ),
    "charge_state": {
        "battery_heater_on": TeslemetryEntity(
            streaming_key=Signal.BATTERY_HEATER_ON,
            vehicledata_casting=cast_bool,
        ),
        "battery_level": TeslemetryEntity(
            streaming_key=Signal.BATTERY_LEVEL, vehicledata_casting=int
        ),
        "battery_range": TeslemetryEntity(
            streaming_key=Signal.RATED_RANGE,
            queue_key="range",
            vehicledata_casting=cast_round_2,
        ),
        "charge_amps": TeslemetryEntity(
            streaming_key=Signal.CHARGE_AMPS,
            queue_key="charge_amps",
            vehicledata_casting=int,
        ),
        "charge_current_request": TeslemetryEntity(
            streaming_key=Signal.CHARGE_CURRENT_REQUEST,
            vehicledata_casting=int,
        ),
        "charge_current_request_max": TeslemetryEntity(
            streaming_key=Signal.CHARGE_CURRENT_REQUEST_MAX,
            vehicledata_casting=int,
        ),
        "charge_enable_request": TeslemetryEntity(
            streaming_key=Signal.CHARGE_ENABLE_REQUEST,
            vehicledata_casting=bool,
        ),
        "charge_energy_added": TeslemetryEntity(
            queue_key="charge_energy_added", vehicledata_casting=cast_round_2
        ),
        "charge_limit_soc": TeslemetryEntity(
            streaming_key=Signal.CHARGE_LIMIT_SOC,
            vehicledata_casting=int,
        ),
        "charge_limit_soc_max": TeslemetryEntity(
            queue_key="charge_limit_soc_max", vehicledata_casting=int
        ),
        "charge_limit_soc_min": TeslemetryEntity(
            queue_key="charge_limit_soc_min", vehicledata_casting=int
        ),
        "charge_limit_soc_std": TeslemetryEntity(
            queue_key="charge_limit_soc_std", vehicledata_casting=int
        ),
        "charge_miles_added_ideal": TeslemetryEntity(
            queue_key="charge_miles_added_ideal", vehicledata_casting=int
        ),
        "charge_miles_added_rated": TeslemetryEntity(
            queue_key="charge_miles_added_rated", vehicledata_casting=int
        ),
        "charge_port_cold_weather_mode": TeslemetryEntity(
            streaming_key=Signal.CHARGE_PORT_COLD_WEATHER_MODE,
            vehicledata_casting=bool,
        ),
        "charge_port_color": TeslemetryEntity(
            queue_key="charge_port_color", vehicledata_casting=str
        ),
        "charge_port_door_open": TeslemetryEntity(
            streaming_key=Signal.CHARGE_PORT_DOOR_OPEN,
            vehicledata_casting=bool,
        ),
        "charge_port_latch": TeslemetryEntity(
            streaming_key=Signal.CHARGE_PORT_LATCH,
            vehicledata_casting=str,
        ),
        "charge_rate": TeslemetryEntity(
            queue_key="charge_rate", vehicledata_casting=int
        ),
        "charger_actual_current": TeslemetryEntity(
            queue_key="charger_actual_current", vehicledata_casting=int
        ),
        "charger_phases": TeslemetryEntity(
            streaming_key=Signal.CHARGER_PHASES,
            vehicledata_casting=str,
        ),
        "charger_pilot_current": TeslemetryEntity(
            queue_key="charger_pilot_current", vehicledata_casting=int
        ),
        "charger_power": TeslemetryEntity(
            queue_key="charger_power", vehicledata_casting=int
        ),
        "charger_voltage": TeslemetryEntity(
            queue_key="charger_voltage", vehicledata_casting=int
        ),
        "charging_state": TeslemetryEntity(
            streaming_key=Signal.CHARGE_STATE,
            vehicledata_casting=str,
        ),
        "conn_charge_cable": TeslemetryEntity(
            streaming_key=Signal.CHARGING_CABLE_TYPE,
            vehicledata_casting=str,
        ),
        "est_battery_range": TeslemetryEntity(
            streaming_key=Signal.EST_BATTERY_RANGE,
            queue_key="est_range",
            vehicledata_casting=cast_round_2,
        ),
        "fast_charger_brand": TeslemetryEntity(
            queue_key="fast_charger_brand", vehicledata_casting=str
        ),
        "fast_charger_present": TeslemetryEntity(
            streaming_key=Signal.FAST_CHARGER_PRESENT,
            vehicledata_casting=bool,
        ),
        "fast_charger_type": TeslemetryEntity(
            streaming_key=Signal.FAST_CHARGER_TYPE,
            vehicledata_casting=str,
        ),
        "ideal_battery_range": TeslemetryEntity(
            streaming_key=Signal.IDEAL_BATTERY_RANGE,
            vehicledata_casting=cast_round_2,
        ),
        "max_range_charge_counter": TeslemetryEntity(
            queue_key="max_range_charge_counter", vehicledata_casting=int
        ),
        "minutes_to_full_charge": TeslemetryEntity(
            streaming_key=Signal.TIME_TO_FULL_CHARGE,
            vehicledata_casting=int,
        ),
        "not_enough_power_to_heat": TeslemetryEntity(
            queue_key="not_enough_power_to_heat", vehicledata_casting=str
        ),
        "off_peak_charging_enabled": TeslemetryEntity(
            queue_key="off_peak_charging_enabled", vehicledata_casting=bool
        ),
        "off_peak_charging_times": TeslemetryEntity(
            queue_key="off_peak_charging_times", vehicledata_casting=str
        ),
        "preconditioning_enabled": TeslemetryEntity(
            queue_key="preconditioning_enabled", vehicledata_casting=bool
        ),
        "preconditioning_times": TeslemetryEntity(
            queue_key="preconditioning_times", vehicledata_casting=str
        ),
        "scheduled_charging_mode": TeslemetryEntity(
            queue_key="scheduled_charging_mode", vehicledata_casting=str
        ),
        "scheduled_charging_pending": TeslemetryEntity(
            queue_key="scheduled_charging_pending", vehicledata_casting=bool
        ),
        "scheduled_charging_start_time": TeslemetryEntity(
            queue_key="scheduled_charging_start_time", vehicledata_casting=int
        ),
        "scheduled_charging_start_time_app": TeslemetryEntity(
            queue_key="scheduled_charging_start_time_app", vehicledata_casting=int
        ),
        "scheduled_charging_start_time_minutes": TeslemetryEntity(
            queue_key="scheduled_charging_start_time_minutes",
            vehicledata_casting=int,
        ),
        "scheduled_departure_time": TeslemetryEntity(
            queue_key="scheduled_departure_time", vehicledata_casting=int
        ),
        "supercharger_session_trip_planner": TeslemetryEntity(
            queue_key="supercharger_session_trip_planner", vehicledata_casting=bool
        ),
        "time_to_full_charge": TeslemetryEntity(
            queue_key="time_to_full_charge", vehicledata_casting=int
        ),
        "timestamp": TeslemetryEntity(
            queue_key="charge_state_timestamp", vehicledata_casting=int
        ),
        "trip_charging": TeslemetryEntity(
            queue_key="trip_charging", vehicledata_casting=bool
        ),
        "usable_battery_level": TeslemetryEntity(
            streaming_key="soc",
            vehicledata_casting=int,
        ),
        "user_charge_enable_request": TeslemetryEntity(
            queue_key="user_charge_enable_request", vehicledata_casting=str
        ),
    },
    "climate_state": {
        "allow_cabin_overheat_protection": TeslemetryEntity(
            queue_key="allow_cabin_overheat_protection", vehicledata_casting=bool
        ),
        "auto_seat_climate_left": TeslemetryEntity(
            streaming_key=Signal.AUTO_SEAT_CLIMATE_LEFT,
            vehicledata_casting=bool,
        ),
        "auto_seat_climate_right": TeslemetryEntity(
            streaming_key=Signal.AUTO_SEAT_CLIMATE_RIGHT,
            vehicledata_casting=bool,
        ),
        "auto_steering_wheel_heat": TeslemetryEntity(
            streaming_key=Signal.HVAC_STEERING_WHEEL_HEAT_AUTO,
            vehicledata_casting=bool,
        ),
        "battery_heater": TeslemetryEntity(
            queue_key="battery_heater", vehicledata_casting=bool
        ),
        "battery_heater_no_power": TeslemetryEntity(
            queue_key="battery_heater_no_power", vehicledata_casting=str
        ),
        "bioweapon_mode": TeslemetryEntity(
            queue_key="bioweapon_mode", vehicledata_casting=bool
        ),
        "cabin_overheat_protection": TeslemetryEntity(
            queue_key="cabin_overheat_protection", vehicledata_casting=str
        ),
        "cabin_overheat_protection_actively_cooling": TeslemetryEntity(
            queue_key="cabin_overheat_protection_actively_cooling",
            vehicledata_casting=bool,
        ),
        "climate_keeper_mode": TeslemetryEntity(
            streaming_key=Signal.CLIMATE_KEEPER_MODE,
            vehicledata_casting=str,
        ),
        "cop_activation_temperature": TeslemetryEntity(
            queue_key="cop_activation_temperature", vehicledata_casting=str
        ),
        "defrost_mode": TeslemetryEntity(
            streaming_key=Signal.DEFROST_MODE,
            vehicledata_casting=int,
        ),
        "driver_temp_setting": TeslemetryEntity(
            streaming_key=Signal.HVAC_LEFT_TEMPERATURE_REQUEST,
            vehicledata_casting=cast_round_2,
        ),
        "fan_status": TeslemetryEntity(queue_key="fan_status", vehicledata_casting=int),
        "hvac_auto_request": TeslemetryEntity(
            queue_key="hvac_auto_request", vehicledata_casting=str
        ),
        "inside_temp": TeslemetryEntity(
            streaming_key=Signal.INSIDE_TEMP,
            vehicledata_casting=cast_round_2,
        ),
        "is_auto_conditioning_on": TeslemetryEntity(
            queue_key="is_auto_conditioning_on", vehicledata_casting=bool
        ),
        "is_climate_on": TeslemetryEntity(
            queue_key="is_climate_on", vehicledata_casting=bool
        ),
        "is_front_defroster_on": TeslemetryEntity(
            queue_key="is_front_defroster_on", vehicledata_casting=bool
        ),
        "is_preconditioning": TeslemetryEntity(
            queue_key="is_preconditioning", vehicledata_casting=bool
        ),
        "is_rear_defroster_on": TeslemetryEntity(
            queue_key="is_rear_defroster_on", vehicledata_casting=bool
        ),
        "left_temp_direction": TeslemetryEntity(
            queue_key="left_temp_direction", vehicledata_casting=int
        ),
        "max_avail_temp": TeslemetryEntity(
            queue_key="max_avail_temp", vehicledata_casting=int
        ),
        "min_avail_temp": TeslemetryEntity(
            queue_key="min_avail_temp", vehicledata_casting=int
        ),
        "outside_temp": TeslemetryEntity(
            streaming_key=Signal.OUTSIDE_TEMP,
            vehicledata_casting=int,
        ),
        "passenger_temp_setting": TeslemetryEntity(
            streaming_key=Signal.HVAC_RIGHT_TEMPERATURE_REQUEST,
            vehicledata_casting=cast_round_2,
        ),
        "remote_heater_control_enabled": TeslemetryEntity(
            queue_key="remote_heater_control_enabled", vehicledata_casting=bool
        ),
        "right_temp_direction": TeslemetryEntity(
            queue_key="right_temp_direction", vehicledata_casting=int
        ),
        "seat_heater_left": TeslemetryEntity(
            streaming_key=Signal.SEAT_HEATER_LEFT,
            vehicledata_casting=int,
        ),
        "seat_heater_rear_center": TeslemetryEntity(
            streaming_key=Signal.SEAT_HEATER_REAR_CENTER,
            vehicledata_casting=int,
        ),
        "seat_heater_rear_left": TeslemetryEntity(
            streaming_key=Signal.SEAT_HEATER_REAR_LEFT,
            vehicledata_casting=int,
        ),
        "seat_heater_rear_right": TeslemetryEntity(
            streaming_key=Signal.SEAT_HEATER_REAR_RIGHT,
            vehicledata_casting=int,
        ),
        "seat_heater_right": TeslemetryEntity(
            streaming_key=Signal.SEAT_HEATER_RIGHT,
            vehicledata_casting=int,
        ),
        "side_mirror_heaters": TeslemetryEntity(
            queue_key="side_mirror_heaters", vehicledata_casting=bool
        ),
        "steering_wheel_heat_level": TeslemetryEntity(
            streaming_key=Signal.HVAC_STEERING_WHEEL_HEAT_LEVEL,
            vehicledata_casting=int,
        ),
        "steering_wheel_heater": TeslemetryEntity(
            queue_key="steering_wheel_heater", vehicledata_casting=bool
        ),
        "supports_fan_only_cabin_overheat_protection": TeslemetryEntity(
            queue_key="supports_fan_only_cabin_overheat_protection",
            vehicledata_casting=bool,
        ),
        "timestamp": TeslemetryEntity(
            queue_key="climate_state_timestamp", vehicledata_casting=int
        ),
        "wiper_blade_heater": TeslemetryEntity(
            streaming_key=Signal.WIPER_HEAT_ENABLED,
            vehicledata_casting=bool,
        ),
    },
    "drive_state": {
        "gps_as_of": TeslemetryEntity(queue_key="gps_as_of", vehicledata_casting=int),
        "heading": TeslemetryEntity(
            streaming_key=Signal.GPS_HEADING,
            queue_key="est_heading",
            vehicledata_casting=int,
        ),
        "latitude": TeslemetryEntity(
            streaming_key=Signal.LOCATION,
            queue_key="est_lat",
            streaming_casting=cast_latitude,
            vehicledata_casting=float,
        ),
        "longitude": TeslemetryEntity(
            streaming_key=Signal.LOCATION,
            queue_key="est_lng",
            streaming_casting=cast_longitude,
            vehicledata_casting=float,
        ),
        "native_latitude": TeslemetryEntity(
            queue_key="native_latitude",
            streaming_casting=cast_latitude,
            vehicledata_casting=float,
        ),
        "native_location_supported": TeslemetryEntity(
            queue_key="native_location_supported", vehicledata_casting=int
        ),
        "native_longitude": TeslemetryEntity(
            queue_key="native_longitude",
            streaming_casting=cast_longitude,
            vehicledata_casting=float,
        ),
        "native_type": TeslemetryEntity(
            queue_key="native_type", vehicledata_casting=str
        ),
        "power": TeslemetryEntity(queue_key="power", vehicledata_casting=int),
        "shift_state": TeslemetryEntity(
            streaming_key=Signal.GEAR, queue_key="shift_state", vehicledata_casting=str
        ),
        "speed": TeslemetryEntity(
            streaming_key=Signal.VEHICLE_SPEED,
            queue_key="speed",
            vehicledata_casting=int,
        ),
        "timestamp": TeslemetryEntity(
            queue_key="drive_state_timestamp", vehicledata_casting=int
        ),
    },
    "gui_settings": {
        "gui_24_hour_time": TeslemetryEntity(
            streaming_key=Signal.SETTING_24_HOUR_TIME,
            vehicledata_casting=bool,
        ),
        "gui_charge_rate_units": TeslemetryEntity(
            queue_key="gui_charge_rate_units", vehicledata_casting=str
        ),
        "gui_distance_units": TeslemetryEntity(
            streaming_key=Signal.SETTING_DISTANCE_UNIT,
            vehicledata_casting=str,
        ),
        "gui_range_display": TeslemetryEntity(
            queue_key="gui_range_display", vehicledata_casting=str
        ),
        "gui_temperature_units": TeslemetryEntity(
            streaming_key=Signal.SETTING_TEMPERATURE_UNIT,
            vehicledata_casting=str,
        ),
        "gui_tirepressure_units": TeslemetryEntity(
            streaming_key=Signal.SETTING_TIRE_PRESSURE_UNIT,
            vehicledata_casting=str,
        ),
        "show_range_units": TeslemetryEntity(
            streaming_key=Signal.SETTING_CHARGE_UNIT,
            vehicledata_casting=bool,
        ),
        "timestamp": TeslemetryEntity(
            queue_key="gui_settings_timestamp", vehicledata_casting=int
        ),
    },
    "vehicle_config": {
        "aux_park_lamps": TeslemetryEntity(
            queue_key="aux_park_lamps", vehicledata_casting=str
        ),
        "badge_version": TeslemetryEntity(
            queue_key="badge_version", vehicledata_casting=int
        ),
        "can_accept_navigation_requests": TeslemetryEntity(
            queue_key="can_accept_navigation_requests", vehicledata_casting=bool
        ),
        "can_actuate_trunks": TeslemetryEntity(
            queue_key="can_actuate_trunks", vehicledata_casting=bool
        ),
        "car_special_type": TeslemetryEntity(
            queue_key="car_special_type", vehicledata_casting=str
        ),
        "car_type": TeslemetryEntity(
            streaming_key=Signal.CAR_TYPE, vehicledata_casting=str
        ),
        "charge_port_type": TeslemetryEntity(
            queue_key="charge_port_type", vehicledata_casting=str
        ),
        "cop_user_set_temp_supported": TeslemetryEntity(
            queue_key="cop_user_set_temp_supported", vehicledata_casting=bool
        ),
        "dashcam_clip_save_supported": TeslemetryEntity(
            queue_key="dashcam_clip_save_supported", vehicledata_casting=bool
        ),
        "default_charge_to_max": TeslemetryEntity(
            queue_key="default_charge_to_max", vehicledata_casting=bool
        ),
        "driver_assist": TeslemetryEntity(
            queue_key="driver_assist", vehicledata_casting=str
        ),
        "ece_restrictions": TeslemetryEntity(
            queue_key="ece_restrictions", vehicledata_casting=bool
        ),
        "efficiency_package": TeslemetryEntity(
            queue_key="efficiency_package", vehicledata_casting=str
        ),
        "eu_vehicle": TeslemetryEntity(
            queue_key="eu_vehicle", vehicledata_casting=bool
        ),
        "exterior_color": TeslemetryEntity(
            streaming_key=Signal.EXTERIOR_COLOR,
            vehicledata_casting=str,
        ),
        "exterior_trim": TeslemetryEntity(
            streaming_key=Signal.TRIM,
            vehicledata_casting=str,
        ),
        "exterior_trim_override": TeslemetryEntity(
            queue_key="exterior_trim_override", vehicledata_casting=str
        ),
        "has_air_suspension": TeslemetryEntity(
            queue_key="has_air_suspension", vehicledata_casting=bool
        ),
        "has_ludicrous_mode": TeslemetryEntity(
            queue_key="has_ludicrous_mode", vehicledata_casting=bool
        ),
        "has_seat_cooling": TeslemetryEntity(
            queue_key="has_seat_cooling", vehicledata_casting=bool
        ),
        "headlamp_type": TeslemetryEntity(
            queue_key="headlamp_type", vehicledata_casting="str"
        ),
        "interior_trim_type": TeslemetryEntity(
            queue_key="interior_trim_type", vehicledata_casting=str
        ),
        "key_version": TeslemetryEntity(
            queue_key="key_version", vehicledata_casting=int
        ),
        "motorized_charge_port": TeslemetryEntity(
            queue_key="motorized_charge_port", vehicledata_casting=bool
        ),
        "paint_color_override": TeslemetryEntity(
            queue_key="paint_color_override", vehicledata_casting=str
        ),
        "performance_package": TeslemetryEntity(
            queue_key="performance_package", vehicledata_casting=str
        ),
        "plg": TeslemetryEntity(queue_key="plg", vehicledata_casting=bool),
        "pws": TeslemetryEntity(queue_key="pws", vehicledata_casting=bool),
        "rear_drive_unit": TeslemetryEntity(
            queue_key="rear_drive_unit", vehicledata_casting=str
        ),
        "rear_seat_heaters": TeslemetryEntity(
            queue_key="rear_seat_heaters", vehicledata_casting=int
        ),
        "rear_seat_type": TeslemetryEntity(
            queue_key="rear_seat_type", vehicledata_casting=int
        ),
        "rhd": TeslemetryEntity(queue_key="rhd", vehicledata_casting=bool),
        "roof_color": TeslemetryEntity(
            streaming_key=Signal.ROOF_COLOR,
            vehicledata_casting=str,
        ),
        "seat_type": TeslemetryEntity(queue_key="seat_type", vehicledata_casting=str),
        "sentry_preview_supported": TeslemetryEntity(
            queue_key="sentry_preview_supported", vehicledata_casting=bool
        ),
        "spoiler_type": TeslemetryEntity(
            queue_key="spoiler_type", vehicledata_casting=str
        ),
        "sun_roof_installed": TeslemetryEntity(
            queue_key="sun_roof_installed", vehicledata_casting=str
        ),
        "supports_qr_pairing": TeslemetryEntity(
            queue_key="supports_qr_pairing", vehicledata_casting=bool
        ),
        "third_row_seats": TeslemetryEntity(
            queue_key="third_row_seats", vehicledata_casting=str
        ),
        "timestamp": TeslemetryEntity(
            queue_key="vehicle_config_timestamp", vehicledata_casting=int
        ),
        "trim_badging": TeslemetryEntity(
            queue_key="trim_badging", vehicledata_casting=str
        ),
        "use_range_badging": TeslemetryEntity(
            queue_key="use_range_badging", vehicledata_casting=bool
        ),
        "utc_offset": TeslemetryEntity(queue_key="utc_offset", vehicledata_casting=int),
        "webcam_selfie_supported": TeslemetryEntity(
            queue_key="webcam_selfie_supported", vehicledata_casting=bool
        ),
        "webcam_supported": TeslemetryEntity(
            queue_key="webcam_supported", vehicledata_casting=bool
        ),
        "wheel_type": TeslemetryEntity(
            streaming_key=Signal.WHEEL_TYPE,
            vehicledata_casting=str,
        ),
    },
    "vehicle_state": {
        "api_version": TeslemetryEntity(
            queue_key="api_version", vehicledata_casting=int
        ),
        "autopark_state_v3": TeslemetryEntity(
            queue_key="autopark_state_v3", vehicledata_casting=str
        ),
        "autopark_style": TeslemetryEntity(
            queue_key="autopark_style", vehicledata_casting=str
        ),
        "calendar_supported": TeslemetryEntity(
            queue_key="calendar_supported", vehicledata_casting=bool
        ),
        "car_version": TeslemetryEntity(
            streaming_key=Signal.VERSION,
            vehicledata_casting=str,
        ),
        "center_display_state": TeslemetryEntity(
            streaming_key=Signal.CENTER_DISPLAY,
            vehicledata_casting=int,
        ),
        "dashcam_clip_save_available": TeslemetryEntity(
            queue_key="dashcam_clip_save_available", vehicledata_casting=bool
        ),
        "dashcam_state": TeslemetryEntity(
            queue_key="dashcam_state", vehicledata_casting=str
        ),
        "df": TeslemetryEntity(queue_key="df", vehicledata_casting=int),
        "dr": TeslemetryEntity(queue_key="dr", vehicledata_casting=int),
        "fd_window": TeslemetryEntity(
            streaming_key=Signal.FD_WINDOW,
            vehicledata_casting=int,
        ),
        "feature_bitmask": TeslemetryEntity(
            queue_key="feature_bitmask", vehicledata_casting=str
        ),
        "fp_window": TeslemetryEntity(
            streaming_key=Signal.FP_WINDOW,
            vehicledata_casting=int,
        ),
        "ft": TeslemetryEntity(queue_key="ft", vehicledata_casting=int),
        "is_user_present": TeslemetryEntity(
            queue_key="is_user_present", vehicledata_casting=bool
        ),
        "last_autopark_error": TeslemetryEntity(
            queue_key="last_autopark_error", vehicledata_casting=str
        ),
        "locked": TeslemetryEntity(
            streaming_key=Signal.LOCKED, vehicledata_casting=bool
        ),
        "media_info": {
            "a2dp_source_name": TeslemetryEntity(
                queue_key="a2dp_source_name", vehicledata_casting=str
            ),
            "audio_volume": TeslemetryEntity(
                queue_key="audio_volume", vehicledata_casting=float
            ),
            "audio_volume_increment": TeslemetryEntity(
                queue_key="audio_volume_increment", vehicledata_casting=float
            ),
            "audio_volume_max": TeslemetryEntity(
                queue_key="audio_volume_max", vehicledata_casting=float
            ),
            "media_playback_status": TeslemetryEntity(
                queue_key="media_playback_status", vehicledata_casting=str
            ),
            "now_playing_album": TeslemetryEntity(
                queue_key="now_playing_album", vehicledata_casting=str
            ),
            "now_playing_artist": TeslemetryEntity(
                queue_key="now_playing_artist", vehicledata_casting=str
            ),
            "now_playing_duration": TeslemetryEntity(
                queue_key="now_playing_duration", vehicledata_casting=int
            ),
            "now_playing_elapsed": TeslemetryEntity(
                queue_key="now_playing_elapsed", vehicledata_casting=int
            ),
            "now_playing_source": TeslemetryEntity(
                queue_key="now_playing_source", vehicledata_casting=int
            ),
            "now_playing_station": TeslemetryEntity(
                queue_key="now_playing_station", vehicledata_casting=str
            ),
            "now_playing_title": TeslemetryEntity(
                queue_key="now_playing_title", vehicledata_casting=str
            ),
        },
        "media_state": {
            "remote_control_enabled": TeslemetryEntity(
                queue_key="remote_control_enabled", vehicledata_casting=bool
            ),
        },
        "notifications_supported": TeslemetryEntity(
            queue_key="notifications_supported", vehicledata_casting=bool
        ),
        "odometer": TeslemetryEntity(
            streaming_key=Signal.ODOMETER,
            vehicledata_casting=float,
        ),
        "parsed_calendar_supported": TeslemetryEntity(
            queue_key="parsed_calendar_supported", vehicledata_casting=bool
        ),
        "pf": TeslemetryEntity(queue_key="pf", vehicledata_casting=int),
        "pr": TeslemetryEntity(queue_key="pr", vehicledata_casting=int),
        "rd_window": TeslemetryEntity(
            streaming_key=Signal.RD_WINDOW,
            vehicledata_casting=int,
        ),
        "remote_start": TeslemetryEntity(
            queue_key="remote_start", vehicledata_casting=bool
        ),
        "remote_start_enabled": TeslemetryEntity(
            streaming_key=Signal.REMOTE_START_ENABLED,
            vehicledata_casting=bool,
        ),
        "remote_start_supported": TeslemetryEntity(
            queue_key="remote_start_supported", vehicledata_casting=bool
        ),
        "rp_window": TeslemetryEntity(
            streaming_key=Signal.RP_WINDOW,
            vehicledata_casting=int,
        ),
        "rt": TeslemetryEntity(queue_key="rt", vehicledata_casting=int),
        "santa_mode": TeslemetryEntity(queue_key="santa_mode", vehicledata_casting=int),
        "sentry_mode": TeslemetryEntity(
            streaming_key=Signal.SENTRY_MODE,
            vehicledata_casting=bool,
        ),
        "sentry_mode_available": TeslemetryEntity(
            queue_key="sentry_mode_available", vehicledata_casting=bool
        ),
        "service_mode": TeslemetryEntity(
            streaming_key=Signal.SERVICE_MODE,
            vehicledata_casting=bool,
        ),
        "service_mode_plus": TeslemetryEntity(
            queue_key="service_mode_plus", vehicledata_casting=bool
        ),
        "smart_summon_available": TeslemetryEntity(
            queue_key="smart_summon_available", vehicledata_casting=bool
        ),
        "software_update": {
            "download_perc": TeslemetryEntity(
                streaming_key=Signal.SOFTWARE_UPDATE_DOWNLOAD_PERCENT_COMPLETE,
                vehicledata_casting=int,
            ),
            "expected_duration_sec": TeslemetryEntity(
                streaming_key=Signal.SOFTWARE_UPDATE_EXPECTED_DURATION_MINUTES,
                vehicledata_casting=int,
            ),
            "install_perc": TeslemetryEntity(
                streaming_key=Signal.SOFTWARE_UPDATE_INSTALLATION_PERCENT_COMPLETE,
                vehicledata_casting=int,
            ),
            "status": TeslemetryEntity(
                queue_key="software_update_status", vehicledata_casting=str
            ),
            "version": TeslemetryEntity(
                streaming_key=Signal.SOFTWARE_UPDATE_VERSION,
                vehicledata_casting=str,
            ),
        },
        "speed_limit_mode": {
            "active": TeslemetryEntity(
                streaming_key=Signal.SPEED_LIMIT_MODE,
                vehicledata_casting=bool,
            ),
            "current_limit_mph": TeslemetryEntity(
                streaming_key=Signal.CURRENT_LIMIT_MPH,
                vehicledata_casting=int,
            ),
            "max_limit_mph": TeslemetryEntity(
                queue_key="max_limit_mph", vehicledata_casting=int
            ),
            "min_limit_mph": TeslemetryEntity(
                queue_key="min_limit_mph", vehicledata_casting=int
            ),
            "pin_code_set": TeslemetryEntity(
                queue_key="pin_code_set", vehicledata_casting=bool
            ),
        },
        "summon_standby_mode_enabled": TeslemetryEntity(
            queue_key="summon_standby_mode_enabled", vehicledata_casting=bool
        ),
        "timestamp": TeslemetryEntity(
            queue_key="vehicle_state_timestamp", vehicledata_casting=int
        ),
        "tpms_hard_warning_fl": TeslemetryEntity(
            queue_key="tpms_hard_warning_fl", vehicledata_casting=bool
        ),
        "tpms_hard_warning_fr": TeslemetryEntity(
            queue_key="tpms_hard_warning_fr", vehicledata_casting=bool
        ),
        "tpms_hard_warning_rl": TeslemetryEntity(
            queue_key="tpms_hard_warning_rl", vehicledata_casting=bool
        ),
        "tpms_hard_warning_rr": TeslemetryEntity(
            queue_key="tpms_hard_warning_rr", vehicledata_casting=bool
        ),
        "tpms_last_seen_pressure_time_fl": TeslemetryEntity(
            streaming_key=Signal.TPMS_LAST_SEEN_PRESSURE_TIME_FL,
            vehicledata_casting=int,
        ),
        "tpms_last_seen_pressure_time_fr": TeslemetryEntity(
            streaming_key=Signal.TPMS_LAST_SEEN_PRESSURE_TIME_FR,
            vehicledata_casting=int,
        ),
        "tpms_last_seen_pressure_time_rl": TeslemetryEntity(
            streaming_key=Signal.TPMS_LAST_SEEN_PRESSURE_TIME_RL,
            vehicledata_casting=int,
        ),
        "tpms_last_seen_pressure_time_rr": TeslemetryEntity(
            streaming_key=Signal.TPMS_LAST_SEEN_PRESSURE_TIME_RR,
            vehicledata_casting=int,
        ),
        "tpms_pressure_fl": TeslemetryEntity(
            streaming_key=Signal.TPMS_PRESSURE_FL,
            vehicledata_casting=float,
        ),
        "tpms_pressure_fr": TeslemetryEntity(
            streaming_key=Signal.TPMS_PRESSURE_FR,
            vehicledata_casting=float,
        ),
        "tpms_pressure_rl": TeslemetryEntity(
            streaming_key=Signal.TPMS_PRESSURE_RL,
            vehicledata_casting=float,
        ),
        "tpms_pressure_rr": TeslemetryEntity(
            streaming_key=Signal.TPMS_PRESSURE_RR,
            vehicledata_casting=float,
        ),
        "tpms_rcp_front_value": TeslemetryEntity(
            queue_key="tpms_rcp_front_value", vehicledata_casting=float
        ),
        "tpms_rcp_rear_value": TeslemetryEntity(
            queue_key="tpms_rcp_rear_value", vehicledata_casting=float
        ),
        "tpms_soft_warning_fl": TeslemetryEntity(
            queue_key="tpms_soft_warning_fl", vehicledata_casting=bool
        ),
        "tpms_soft_warning_fr": TeslemetryEntity(
            queue_key="tpms_soft_warning_fr", vehicledata_casting=bool
        ),
        "tpms_soft_warning_rl": TeslemetryEntity(
            queue_key="tpms_soft_warning_rl", vehicledata_casting=bool
        ),
        "tpms_soft_warning_rr": TeslemetryEntity(
            queue_key="tpms_soft_warning_rr", vehicledata_casting=bool
        ),
        "valet_mode": TeslemetryEntity(
            streaming_key=Signal.VALET_MODE_ENABLED,
            vehicledata_casting=bool,
        ),
        "valet_pin_needed": TeslemetryEntity(
            queue_key="valet_pin_needed", vehicledata_casting=bool
        ),
        "vehicle_name": TeslemetryEntity(
            streaming_key=Signal.VEHICLE_NAME,
            vehicledata_casting=str,
        ),
        "vehicle_self_test_progress": TeslemetryEntity(
            queue_key="vehicle_self_test_progress", vehicledata_casting=int
        ),
        "vehicle_self_test_requested": TeslemetryEntity(
            queue_key="vehicle_self_test_requested", vehicledata_casting=bool
        ),
        "webcam_available": TeslemetryEntity(
            queue_key="webcam_available", vehicledata_casting=bool
        ),
    },
    "charge_schedule_data": {
        "charge_schedules": TeslemetryEntityList(
            queue_key="charge_schedules",
            entities={
                "id": TeslemetryEntity(
                    queue_key="charge_schedules_id", vehicledata_casting=int
                ),
                "name": TeslemetryEntity(queue_key="name", vehicledata_casting=str),
                "days_of_week": TeslemetryEntity(
                    queue_key="days_of_week", vehicledata_casting=int
                ),
                "start_enabled": TeslemetryEntity(
                    queue_key="start_enabled", vehicledata_casting=bool
                ),
                "start_time": TeslemetryEntity(
                    queue_key="start_time", vehicledata_casting=int
                ),
                "end_enabled": TeslemetryEntity(
                    queue_key="end_enabled", vehicledata_casting=bool
                ),
                "end_time": TeslemetryEntity(
                    queue_key="end_time", vehicledata_casting=int
                ),
                "one_time": TeslemetryEntity(
                    queue_key="one_time", vehicledata_casting=bool
                ),
                "enabled": TeslemetryEntity(
                    queue_key="enabled", vehicledata_casting=bool
                ),
                "latitude": TeslemetryEntity(
                    queue_key="charge_schedule_latitude", vehicledata_casting=float
                ),
                "longitude": TeslemetryEntity(
                    queue_key="charge_schedule_longitude", vehicledata_casting=float
                ),
            },
        ),
        "timestamp": {
            "seconds": TeslemetryEntity(
                queue_key="charge_schedule_data_seconds", vehicledata_casting=int
            ),
            "nanos": TeslemetryEntity(
                queue_key="charge_schedule_data_nanos", vehicledata_casting=int
            ),
        },
        "charge_schedule_window": {
            "id": TeslemetryEntity(
                queue_key="charge_schedule_window_id", vehicledata_casting=int
            ),
            "name": TeslemetryEntity(
                queue_key="charge_schedule_window_name", vehicledata_casting=str
            ),
            "days_of_week": TeslemetryEntity(
                queue_key="charge_schedule_windowdays_of_week",
                vehicledata_casting=int,
            ),
            "start_enabled": TeslemetryEntity(
                queue_key="charge_schedule_window_start_enabled",
                vehicledata_casting=bool,
            ),
            "start_time": TeslemetryEntity(
                streaming_key=Signal.SCHEDULED_CHARGING_START_TIME,
                vehicledata_casting=int,
            ),
            "end_enabled": TeslemetryEntity(
                queue_key="charge_schedule_windowend_enabled",
                vehicledata_casting=bool,
            ),
            "end_time": TeslemetryEntity(
                Signal.SCHEDULED_DEPARTURE_TIME,
                queue_key="charge_schedule_window_end_time",
                vehicledata_casting=int,
            ),
            "one_time": TeslemetryEntity(
                queue_key="charge_schedule_window_one_time",
                vehicledata_casting=bool,
            ),
            "enabled": TeslemetryEntity(
                queue_key="charge_schedule_window_enabled", vehicledata_casting=bool
            ),
            "latitude": TeslemetryEntity(
                queue_key="charge_schedule_window_latitude",
                vehicledata_casting=float,
            ),
            "longitude": TeslemetryEntity(
                queue_key="charge_schedule_window_longitude",
                vehicledata_casting=float,
            ),
        },
        "charge_buffer": TeslemetryEntity(
            queue_key="charge_buffer", vehicledata_casting=int
        ),
        "max_num_charge_schedules": TeslemetryEntity(
            queue_key="max_num_charge_schedules", vehicledata_casting=int
        ),
        "next_schedule": TeslemetryEntity(
            queue_key="next_schedule", vehicledata_casting=bool
        ),
        "show_schedule_complete_state": TeslemetryEntity(
            queue_key="show_schedule_complete_state", vehicledata_casting=bool
        ),
    },
    "preconditioning_schedule_data": {
        "precondition_schedules": TeslemetryEntityList(
            queue_key="precondition_schedules",
            entities={
                "id": TeslemetryEntity(
                    queue_key="precondition_schedules_id", vehicledata_casting=int
                ),
                "name": TeslemetryEntity(
                    queue_key="precondition_schedules_name", vehicledata_casting=str
                ),
                "days_of_week": TeslemetryEntity(
                    queue_key="precondition_schedules_days_of_week",
                    vehicledata_casting=int,
                ),
                "precondition_time": TeslemetryEntity(
                    queue_key="precondition_schedules_precondition_time",
                    vehicledata_casting=int,
                ),
                "one_time": TeslemetryEntity(
                    queue_key="precondition_time_one_time", vehicledata_casting=bool
                ),
                "enabled": TeslemetryEntity(
                    queue_key="precondition_time_enabled", vehicledata_casting=bool
                ),
                "latitude": TeslemetryEntity(
                    queue_key="precondition_time_latitude",
                    vehicledata_casting=float,
                ),
                "longitude": TeslemetryEntity(
                    queue_key="precondition_time_longitude",
                    vehicledata_casting=float,
                ),
            },
        ),
        "timestamp": {
            "seconds": TeslemetryEntity(
                queue_key="precondition_schedule_data_seconds",
                vehicledata_casting=int,
            ),
            "nanos": TeslemetryEntity(
                queue_key="precondition_schedule_data_nanos",
                vehicledata_casting=int,
            ),
        },
        "preconditioning_schedule_window": TeslemetryEntity(
            queue_key="preconditioning_schedule_window", vehicledata_casting=str
        ),
        "max_num_precondition_schedules": TeslemetryEntity(
            queue_key="max_num_precondition_schedules", vehicledata_casting=int
        ),
        "next_schedule": TeslemetryEntity(
            queue_key="precondition_schedule_data_next_schedule",
            vehicledata_casting=bool,
        ),
    },
    "createdAt": TeslemetryEntity(queue_key="createdAt", vehicledata_casting=str),
    "stream_only": {
        "gps_heading": TeslemetryEntity(
            streaming_key=Signal.GPS_HEADING,
        ),
        "location": TeslemetryEntity(
            streaming_key=Signal.LOCATION,
        ),
        "shift_state": TeslemetryEntity(streaming_key=Signal.GEAR),
        "speed": TeslemetryEntity(streaming_key=Signal.VEHICLE_SPEED),
        "est_battery_range": TeslemetryEntity(
            streaming_key=Signal.EST_BATTERY_RANGE,
        ),
        "battery_range": TeslemetryEntity(
            streaming_key=Signal.RATED_RANGE,
        ),
    },
}
