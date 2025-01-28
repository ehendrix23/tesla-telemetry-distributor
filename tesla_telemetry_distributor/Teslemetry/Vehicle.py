from __future__ import annotations

import logging
from asyncio import Queue
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from tesla_telemetry_distributor.const import (
    STREAMING_VEHICLE_DATA,
    Signal,
    TeslaVehicleDataType,
    VehicleDataType,
)
from tesla_telemetry_distributor.Teslemetry.TeslemetryEntity import (
    TeslemetryEntity,
    TeslemetryEntityList,
)

if TYPE_CHECKING:
    from tesla_telemetry_distributor.Teslemetry.TeslemetryHandler import (
        TeslemetryHandler,
    )

_LOGGER = logging.getLogger(__name__)


class Vehicle:
    """
    A class to represent a vehicle and manage its data.
    Attributes:
    -----------
    _vin : str
        The Vehicle Identification Number (VIN) of the vehicle.
    _vehicle_data : dict
        A dictionary containing the vehicle's data.
    Methods:
    --------
    iterate_vehicle_data():
        Yields key-value pairs from the vehicle data, including nested dictionaries.
    get_entity_for_key(streaming_key: Signal):
        Retrieves the entity associated with the given streaming key.
    update_value_for_key(streaming_key: Signal, value):
        Updates the value of the entity associated with the given streaming key.
    update_vehicle_data(returned_vehicle_data: dict):
        Updates the vehicle data with the returned vehicle data.
    _iterate_vehicle_data(vehicle_data: dict):
        A static method that recursively yields key-value pairs from the vehicle data.
    _update_vehicle_data(vehicle_data: dict, returned_vehicle_data: dict):
        A static method that recursively updates the vehicle data with the returned vehicle data.
    """

    def __init__(self, teslemetry_handler: TeslemetryHandler, vin: str):
        self._teslemetry_handler = teslemetry_handler
        self._vin = vin
        self._queues: dict[str, Queue] = {}
        self._vehicle_data: VehicleDataType = deepcopy(STREAMING_VEHICLE_DATA)
        self._vehicle_data_initialized: bool = False
        self._tesla_vehicle_data: dict[str, Any] | None = None

        self._streamkeys_in_vehicle_data: dict[str, list[TeslemetryEntity]] = {}
        self._cache_stream_to_vehicle_data(self._vehicle_data)

    def _cache_stream_to_vehicle_data(
        self,
        stream_vehicle_data: VehicleDataType,
    ) -> None:
        """
        Recursively caches streaming vehicle data into the vehicle's data cache.
        This method processes a dictionary of streaming vehicle data, which can contain
        nested dictionaries or TeslemetryEntity objects. It updates the vehicle's data
        cache with the streaming data.
        Args:
            stream_vehicle_data (dict[str, TeslemetryEntity | dict]): A dictionary
                containing streaming vehicle data. The values can be either TeslemetryEntity
                objects or nested dictionaries with similar structure.
        Returns:
            None
        """
        for value in stream_vehicle_data.values():
            if isinstance(value, dict):
                self._cache_stream_to_vehicle_data(value)
            elif isinstance(value, TeslemetryEntity):
                if value.streaming_key in self._streamkeys_in_vehicle_data:
                    self._streamkeys_in_vehicle_data[value.streaming_key].append(value)
                elif value.streaming_key is not None:
                    self._streamkeys_in_vehicle_data[value.streaming_key] = [value]

    async def updated_value(self, value: dict[str, Any]) -> None:
        """Update the vehicle data with the returned vehicle data."""
        entities_updated: list[TeslemetryEntity] = []
        if "vehicle_data" in value:
            entities_updated = Vehicle._update_vehicle_data(
                vin=self._vin,
                vehicle_data=self._vehicle_data,
                returned_vehicle_data=value["vehicle_data"],
            )
            self._vehicle_data_initialized = True
            if len(entities_updated) > 0:
                self._tesla_vehicle_data = None

        elif "data" in value:
            # This is streaming data, need to update self._vehicle_data.
            entities_updated = self._update_vehicle_data_from_stream(
                streaming_data=value["data"]
            )

        if len(entities_updated) > 0:
            await self._send_to_queues(entities_updated)

    async def _send_to_queues(self, entities_updated: list[TeslemetryEntity]) -> None:
        """Send the updated entities to the listeners."""
        queue_dict: dict[str, Any] = {}
        for entity in entities_updated:
            if entity.queue_key is not None:
                queue_dict[entity.queue_key] = entity.streaming_value

        if len(queue_dict) != 0:
            await self._teslemetry_handler.sent_to_queues({self._vin: queue_dict})

    async def vehicle_data(self) -> dict[str, Any]:
        """Retrieve the vehicle data."""
        if not self._vehicle_data_initialized:
            if self._teslemetry_handler.teslemetry is None:
                _LOGGER.error(
                    "%s: Teslemetry is not available, unable to update vehicle data",
                    self._vin,
                )
            else:
                _LOGGER.debug("%s: Retrieving vehicle data from Teslemetry", self._vin)
                teslemetry_data: dict = await self._teslemetry_handler.vehicle_data(
                    self._vin
                )
                if not isinstance(teslemetry_data.get("response"), dict):
                    _LOGGER.error(
                        "%s: Teslemetry is not available, unable to update vehicle data",
                        self._vin,
                    )
                else:
                    vehicle_data: TeslaVehicleDataType = teslemetry_data["response"]
                    entities_updated: list[TeslemetryEntity] = (
                        Vehicle._update_vehicle_data(
                            vin=self._vin,
                            vehicle_data=self._vehicle_data,
                            returned_vehicle_data=vehicle_data,
                        )
                    )
                    _LOGGER.debug(
                        "%s: Vehicle data retrieved from Teslemetry", self._vin
                    )

                    await self._send_to_queues(entities_updated)
                    self._vehicle_data_initialized = True

        if self._tesla_vehicle_data is None:
            self._tesla_vehicle_data = Vehicle._get_tesla_vehicle_data(
                self._vehicle_data, {}
            )
            _LOGGER.debug("%s: Vehicle json data refreshed", self._vin)

        return self._tesla_vehicle_data

    @staticmethod
    def _update_vehicle_data(
        vin: str,
        vehicle_data: VehicleDataType,
        returned_vehicle_data: TeslaVehicleDataType,
    ) -> list[TeslemetryEntity]:
        """Update the vehicle data with the returned vehicle data."""
        list_of_entities: list[TeslemetryEntity] = []
        for key, value in returned_vehicle_data.items():
            # If the key is not in the vehicle data, add it.
            if isinstance(value, dict):
                if key not in vehicle_data:
                    vehicle_data[key] = {}
                    _LOGGER.debug(
                        "%s: Added new vehicle_data dictionary key %s for new dictionary",
                        vin,
                        key,
                    )
                vehicle_data_dict = vehicle_data[key]
                if isinstance(vehicle_data_dict, dict):
                    list_of_entities.extend(
                        Vehicle._update_vehicle_data(vin, vehicle_data_dict, value)
                    )
            elif isinstance(value, list):
                if key not in vehicle_data:
                    vehicle_data[key] = TeslemetryEntityList()
                    _LOGGER.debug(
                        "%s: Added new vehicle_data dictionary key %s for new list",
                        vin,
                        key,
                    )

                vehicle_data_dict = vehicle_data[key]
                if isinstance(vehicle_data_dict, TeslemetryEntityList):
                    vehicle_data_dict.update_vehicledata(value)

            else:
                if key not in vehicle_data:
                    vehicle_data[key] = TeslemetryEntity()
                    _LOGGER.debug("%s: Added new vehicle_data entity key %s", vin, key)

                vehicle_data_dict = vehicle_data[key]
                if isinstance(vehicle_data_dict, TeslemetryEntity):
                    # _LOGGER.debug("%s: Key: %s, value: %s", vin, key, value)
                    try:
                        if vehicle_data_dict.update_vehicledata(value):
                            _LOGGER.debug(
                                "%s: Updated vehicle_data entity key %s to value %s",
                                vin,
                                key,
                                value,
                            )
                            if vehicle_data_dict.queue_key is not None:
                                list_of_entities.append(vehicle_data_dict)
                    except AttributeError:
                        _LOGGER.error(
                            "%s: Item %s is not a TeslemetryEntity. Value provided is: %s",
                            vin,
                            key,
                            value,
                        )

        return list_of_entities

    def _update_vehicle_data_from_stream(
        self, streaming_data: dict[str, Any]
    ) -> list[TeslemetryEntity]:
        """Update the value of the entity associated with the given streaming key."""

        list_of_entities: list[TeslemetryEntity] = []
        # Loop through all the streaming keys with values received.
        for streaming_key, streaming_value in streaming_data.items():
            if streaming_key not in self._streamkeys_in_vehicle_data:
                # New streaming key we do not have yet.
                entity = TeslemetryEntity(streaming_key=streaming_key)
                entity.update_stream(streaming_value)
                self._streamkeys_in_vehicle_data[streaming_key] = [entity]
                if self._vehicle_data.get("stream_only") is None:
                    self._vehicle_data["stream_only"] = {}

                try:
                    signal_value = Signal(streaming_key)
                except ValueError:
                    vehicle_data_key = streaming_key
                else:
                    vehicle_data_key = signal_value.name.lower()

                stream_dict = self._vehicle_data["stream_only"]
                assert isinstance(stream_dict, dict)
                if stream_dict.get(vehicle_data_key) is not None:
                    _LOGGER.warning(
                        "%s: Got duplicate streaming key %s in streaming_only vehicle data for %s. Unable to add.",
                        self._vin,
                        streaming_key,
                        vehicle_data_key,
                    )
                else:
                    stream_dict[vehicle_data_key] = entity

                list_of_entities.append(entity)
                _LOGGER.debug(
                    "%s: Added new streaming key %s with value %s",
                    self._vin,
                    streaming_key,
                    streaming_value,
                )
                continue

            # Loop through all the entities for which the streaming key matches the key.
            for entity in self._streamkeys_in_vehicle_data[streaming_key]:
                stream_updated, vehicle_updated = entity.update_stream(streaming_value)
                if stream_updated:
                    # _LOGGER.debug(
                    #     "%s: Updated streaming key %s to %s",
                    #     self._vin,
                    #     entity.streaming_key,
                    #     entity.streaming_value,
                    # )
                    list_of_entities.append(entity)
                if vehicle_updated:
                    self._tesla_vehicle_data = None

        return list_of_entities

    @staticmethod
    def _get_tesla_vehicle_data(
        vehicle_data: VehicleDataType, tesla_vehicle_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Get the Tesla vehicle data."""
        for key, value in vehicle_data.items():
            if isinstance(value, dict):
                tesla_vehicle_data[key] = Vehicle._get_tesla_vehicle_data(
                    value, tesla_vehicle_data.get(key, {})
                )
            else:
                try:
                    tesla_vehicle_data[key] = value.vehicledata_value
                except AttributeError:
                    _LOGGER.error(
                        "Item %s is not a TeslemetryEntity.",
                        key,
                    )

        return tesla_vehicle_data
