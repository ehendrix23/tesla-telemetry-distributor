import asyncio
import logging
from typing import Any, Callable

import aiohttp
from tesla_fleet_api.const import Scope, VehicleDataEndpoint
from tesla_fleet_api.exceptions import InvalidToken, TeslaFleetError
from tesla_fleet_api.teslemetry import Teslemetry
from teslemetry_stream import TeslemetryStream

from teslemetry_teslamate.Teslemetry.Vehicle import Vehicle

_LOGGER = logging.getLogger(__name__)


class TeslemetryHandler:
    """Class to sync Tes"""

    def __init__(self, access_token: str, refresh: int = 3600) -> None:
        self._access_token = access_token
        self._refresh = refresh

        self._metadata: dict[str, Any] = {}
        self._products: dict[str, Any] = {}
        self._queues: dict[str, asyncio.Queue] = {}
        self._teslemetry: Teslemetry | None = None
        self._stream: TeslemetryStream | None = None
        self._session: aiohttp.ClientSession | None = None
        self._refresh_task: asyncio.Task | None = None
        self._remove: Callable[[], None] | None = None

        self.scopes: list[Scope] = []
        self.registered_vehicles: dict[str, Vehicle] = {}
        self._queue_lock = asyncio.Lock()

    async def run(self) -> asyncio.Task:
        self._session = aiohttp.ClientSession()
        self._teslemetry = Teslemetry(
            session=self._session,
            access_token=self._access_token,
        )

        _LOGGER.debug("Retrieving initial metadata from Teslemetry")
        await self.get_metadata()

        region = self._metadata["region"]
        server = f"{region.lower()}.teslemetry.com"

        # Create the stream
        self._stream = TeslemetryStream(
            access_token=self._access_token,
            session=self._session,
            server=server,
        )

        # Connect to the stream
        _LOGGER.debug("Connecting to Teslemetry %s", server)
        await self._stream.connect()

        _LOGGER.debug("Setting up stream %s", server)
        self._remove = self._stream.async_add_listener(self.listener)
        _LOGGER.debug("%s: Stream listener added", server)

        _LOGGER.debug("Updating vehicle data")
        await self._update_vehicles()

        self._refresh_task = asyncio.create_task(self._refresh_vehicle_data())
        _LOGGER.debug("Refresh task scheduled")

        return self._refresh_task

    async def close(self) -> None:
        _LOGGER.debug("Closing everything")
        await self.sent_to_queues({"CLOSED": "CLOSED"})

        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await self._refresh_task

        for vin in list(self.registered_vehicles):
            await self._unregister_vehicle(vin)

        if self._stream is not None:
            self._stream.close()
            self._stream = None

        self._teslemetry = None

        if self._session is not None:
            await self._session.close()

        self._session = None
        self.scopes = []
        self._metadata = {}
        self._products = {}

    def listener(self, value: dict[str, Any]) -> None:
        asyncio.ensure_future(self.update(value=value))

    async def update(self, value: dict[str, Any]) -> None:
        vin = value.get("vin")
        if vin is not None:
            if vin not in self.registered_vehicles:
                _LOGGER.info("New vin %s received.", vin)
                self.registered_vehicles[vin] = Vehicle(
                    teslemetry_handler=self,
                    vin=vin,
                )
                await self.sent_to_queues({vin: "ADDED"})
                _LOGGER.info("Registered vehicle %s", vin)

            await self.registered_vehicles[vin].updated_value(value=value)
        else:
            _LOGGER.debug("Received value without vin: %s", value)

    async def refresh_vehicle_data(self) -> None:
        await self.get_metadata()
        await self._update_vehicles()
        return None

    async def _refresh_vehicle_data(self) -> None:
        try:
            # Loop continously until we're cancelled
            while True:
                await self._update_vehicles()

                # Wait for the refresh time
                _LOGGER.debug("Waiting for %s seconds for refresh", self._refresh)
                await asyncio.sleep(self._refresh)

                await self.get_metadata()
        except asyncio.CancelledError:
            _LOGGER.info("Stopping refresh task")
            self._refresh_task = None
            await self.close()

    async def _update_vehicles(self) -> None:
        vehicles = self._metadata["vehicles"]
        products: list[dict[str, Any]] = self._products["response"]

        if self._teslemetry is None or self._stream is None:
            return None

        for product in products:
            if (
                "vin" in product
                and vehicles.get(product["vin"], {}).get("access")
                and Scope.VEHICLE_DEVICE_DATA in self.scopes
            ):
                # Remove the protobuff 'cached_data' that we do not use to save memory
                product.pop("cached_data", None)
                vin = product["vin"]

                if vin not in self.registered_vehicles:
                    self.registered_vehicles[vin] = Vehicle(
                        teslemetry_handler=self,
                        vin=vin,
                    )
                    await self.sent_to_queues({vin: "ADDED"})
                    _LOGGER.info("Registered vehicle %s", vin)

        # Remove any vehicles that are no longer in the products
        for vin in list(self.registered_vehicles):
            vin_in_product = False
            for product in products:
                product_vin = product.get("vin")
                if (
                    product_vin == vin
                    and vehicles.get(product_vin, {}).get("access")
                    and Scope.VEHICLE_DEVICE_DATA in self.scopes
                ):
                    vin_in_product = True
                    break

            if not vin_in_product:
                # Cancel the vehicle task
                await self._unregister_vehicle(vin)

        return None

    async def _unregister_vehicle(self, vin: str) -> None:
        if vin in self.registered_vehicles:
            self.registered_vehicles.pop(vin)
            await self.sent_to_queues({vin: "REMOVED"})
            _LOGGER.info("Unregistered vehicle %s", vin)

    async def sent_to_queues(self, data: dict[str, Any]) -> None:
        queues_shut_down: list[str] = []
        async with self._queue_lock:
            for queue_name, queue in self._queues.items():
                _LOGGER.debug("Sending data to queue %s: %s", queue_name, data)
                try:
                    queue.put_nowait(data)
                except asyncio.QueueFull:
                    _LOGGER.error(
                        "Queue for %s is full, dropping data %s", queue_name, data
                    )
                except asyncio.QueueShutDown:
                    _LOGGER.error("Queue for %s is shut down, removing.", queue_name)
                    queues_shut_down.append(queue_name)
                    break

            for queue_name in queues_shut_down:
                self._queues.pop(queue_name)

    async def get_metadata(self) -> None:
        if self._teslemetry is None:
            return None

        try:
            calls = await asyncio.gather(
                self._teslemetry.metadata(),
                self._teslemetry.products(),
            )
        except InvalidToken as e:
            _LOGGER.error("Invalid token")
            raise InvalidToken from e
        except TeslaFleetError as e:
            _LOGGER.error("Fleet Error")
            raise TeslaFleetError from e
        except TypeError as e:
            _LOGGER.error("Invalid response from Teslemetry")
            raise TypeError from e

        self._metadata = calls[0]
        self._products = calls[1]
        self.scopes = self._metadata["scopes"]

        _LOGGER.debug("Metadata retrieved from Teslemetry")

        return None

    @property
    def teslemetry(self) -> Teslemetry | None:
        return self._teslemetry

    @property
    def teslemetry_stream(self) -> TeslemetryStream | None:
        return self._stream

    @property
    def vins(self) -> list[str]:
        return list(self.registered_vehicles)

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def products(self) -> dict[str, Any]:
        return self._products

    @property
    def token(self) -> str:
        return self._access_token

    def get_vehicle(self, vin: str) -> Vehicle | None:
        return self.registered_vehicles.get(vin)

    async def add_queue(self, name: str, queue: asyncio.Queue) -> bool:
        _LOGGER.info("Adding queue %s", name)
        async with self._queue_lock:
            self._queues[name] = queue
        _LOGGER.info("Added queue %s", name)
        return True

    async def remove_queue(self, name: str) -> bool:
        if name in self._queues:
            async with self._queue_lock:
                self._queues.pop(name)
            _LOGGER.info("Removed queue %s", name)
            return True
        return False

    async def vehicle_data(
        self,
        vin: str | int,
        endpoints: list[VehicleDataEndpoint | str] | None = None,
    ) -> dict[str, Any]:
        return (
            await self._teslemetry.vehicle.vehicle_data(
                vehicle_tag=vin, endpoints=endpoints
            )
            if self._teslemetry is not None
            else {}
        )
