from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any

from teslemetry_stream import Signal

if TYPE_CHECKING:
    from tesla_telemetry_distributor.const import (
        StateType,
        TeslaVehicleDataType,
        VehicleDataType,
    )

_LOGGER = logging.getLogger(__name__)


class TeslemetryEntity:
    """
    Represents a telemetry entity for a Tesla vehicle.
    Attributes:
        streaming_key (Optional[Signal]): The key used for streaming data.
        streaming_enabled (bool): Indicates if streaming is enabled.
        value (dict | bool | str | int | float | None): The value associated with the telemetry entity.
        last_update (Optional[datetime]): The timestamp of the last update.
    """

    def __init__(
        self,
        streaming_key: Signal | str | None = None,
        queue_key: Signal | str | None = None,
        streaming_casting: Any = None,
        vehicledata_casting: Any = None,
    ) -> None:
        # self._streaming_key = (
        #    streaming_key.value if isinstance(streaming_key, Signal) else streaming_key
        # )

        if streaming_key is None:
            self._streaming_key = None
        else:
            try:
                signal_value = Signal(streaming_key)
            except ValueError:
                self._streaming_key = streaming_key
            else:
                self._streaming_key = signal_value.value

        self._queue_key = self._streaming_key if queue_key is None else queue_key

        self._streaming_casting = streaming_casting
        self._vehicledatacasting = vehicledata_casting
        self._streaming_value: StateType = None
        self._vehicledata_value: StateType = None
        self._last_update: datetime | None = None

    @property
    def streaming_key(self) -> Signal | str | None:
        """The key used for streaming data."""
        return self._streaming_key

    @property
    def queue_key(self) -> Signal | str | None:
        """The key used for streaming data."""
        return self._queue_key

    @property
    def vehicledata_value(self) -> StateType:
        """The value associated with the telemetry entity."""
        return self._vehicledata_value

    @property
    def streaming_value(self) -> StateType:
        """The value associated with the telemetry entity from the stream."""
        return self._streaming_value

    @property
    def last_update(self) -> datetime | None:
        """The timestamp of the last update."""
        return self._last_update

    def update_vehicledata(
        self, value: StateType, update_timestamp: datetime | None = None
    ) -> bool:
        """Update the value of the entity"""
        if self._vehicledata_value == value:
            return False

        if (
            update_timestamp is not None
            and self._last_update is not None
            and update_timestamp < self._last_update
        ):
            # Update we have is from after update received, ignoring it.
            return False

        self._vehicledata_value = value
        self._streaming_value = value
        self._last_update = update_timestamp or datetime.now()

        return True

    def update_stream(self, value: StateType) -> tuple[bool, bool]:
        """Update the value of the entity"""

        casted_value = None
        if value is not None:
            try:
                casted_value = (
                    self._streaming_casting(value)
                    if self._streaming_casting is not None
                    else value
                )
            except (TypeError, ValueError) as e:
                _LOGGER.warning(
                    "Value %s for queue key %s is not compatible with streaming casting %s: %s",
                    value,
                    self._queue_key,
                    self._streaming_casting,
                    e,
                )

        streaming_value = (
            self._streaming_casting(value)
            if self._streaming_casting is not None
            else value
        )

        if self._streaming_value == streaming_value:
            return (False, False)

        self._streaming_value = streaming_value
        self._last_update = datetime.now()

        casted_value = None
        if streaming_value is not None:
            try:
                casted_value = (
                    self._vehicledatacasting(streaming_value)
                    if self._vehicledatacasting is not None
                    else streaming_value
                )
            except (TypeError, ValueError) as e:
                _LOGGER.warning(
                    "Value %s for queue key %s is not compatible with vehicle data casting %s: %s",
                    streaming_value,
                    self._queue_key,
                    self._vehicledatacasting,
                    e,
                )

        if self._vehicledata_value != casted_value:
            self._vehicledata_value = casted_value
            return (True, True)

        return (True, False)


class TeslemetryEntityList:
    def __init__(
        self,
        entities: VehicleDataType | None = None,
        queue_key: Signal | str | None = None,
    ) -> None:
        self._entities: VehicleDataType = entities or {}
        self._queue_key = queue_key
        self._teslemetry_entities: list[VehicleDataType] = []
        self._last_update: datetime | None = None

    @property
    def streaming_key(self) -> Signal | str | None:
        """The key used for streaming data."""
        return None

    @property
    def queue_key(self) -> Signal | str | None:
        """The key used for streaming data."""
        return self._queue_key

    @property
    def vehicledata_value(self) -> list[TeslaVehicleDataType]:
        """The value associated with the telemetry entity."""
        list_values: list = []
        for item in self._teslemetry_entities:
            list_values.append(self._vehicledata_value(item))
        return list_values

    @staticmethod
    def _vehicledata_value(
        entities: VehicleDataType | TeslemetryEntityList,
    ) -> StateType | list[TeslaVehicleDataType] | TeslaVehicleDataType:
        if isinstance(entities, TeslemetryEntityList) or isinstance(
            entities, TeslemetryEntity
        ):
            return entities.vehicledata_value

        return_dict: TeslaVehicleDataType = {}
        for key, value in entities.items():
            if isinstance(value, dict):
                return_dict[key] = TeslemetryEntityList._vehicledata_value(value)
            else:
                return_dict[key] = value.vehicledata_value
        return return_dict

    @property
    def streaming_value(self) -> StateType:
        """The value associated with the telemetry entity from the stream."""
        return None

    @property
    def last_update(self) -> datetime | None:
        """The timestamp of the last update."""
        return self._last_update

    def update_vehicledata(self, value: list[TeslaVehicleDataType]) -> bool:
        self._teslemetry_entities = []
        # Loop through the list
        for list_value in value:
            new_dict: VehicleDataType = self._retrieve_dict(
                values=list_value, entities=self._entities
            )
            self._teslemetry_entities.append(new_dict)
        self._last_update = datetime.now()
        return True

    @staticmethod
    def _retrieve_dict(
        values: TeslaVehicleDataType, entities: VehicleDataType
    ) -> VehicleDataType:
        # Loop through each element within the dictionary
        new_dict: VehicleDataType = {}
        for key, value in values.items():
            if isinstance(value, list):
                if key in entities:
                    new_list_entity = deepcopy(entities[key])
                    if not isinstance(new_list_entity, TeslemetryEntityList):
                        _LOGGER.error(
                            "Received a list from Tesla but Vehicle Data has it defined as %s for key %s",
                            type(new_list_entity),
                            key,
                        )
                        continue
                else:
                    new_list_entity = TeslemetryEntityList()

                new_list_entity.update_vehicledata(value=value)
                entities[key] = new_list_entity

            elif isinstance(value, dict):
                if key in entities:
                    new_dict_entity = deepcopy(entities[key])
                    if not isinstance(new_dict_entity, dict):
                        _LOGGER.error(
                            "Received a dictionary from Tesla but Vehicle Data it defined as a %s for key %s",
                            type(new_dict_entity),
                            key,
                        )
                        continue
                else:
                    new_dict_entity = {}

                new_dict_entity = TeslemetryEntityList._retrieve_dict(
                    values=value, entities=new_dict_entity
                )
                entities[key] = new_dict_entity
            else:
                new_entity = TeslemetryEntity()
                new_entity.update_vehicledata(value=value)
                entities[key] = new_entity

            new_dict[key] = entities[key]

        return new_dict

    def update_stream(self, *args, **kwargs) -> tuple[bool, bool]:
        return (False, False)
