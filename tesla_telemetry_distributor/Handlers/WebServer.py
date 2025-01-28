import asyncio
import hashlib
import logging
import os
from datetime import datetime, timedelta

from aiohttp import web
from aiohttp_remotes import ForwardedRelaxed, XForwardedRelaxed, setup

from tesla_telemetry_distributor.Teslemetry.TeslemetryHandler import TeslemetryHandler

_LOGGER = logging.getLogger(__name__)

TOKEN_NOT_PROVIDED = "Token not provided"
INVALID_TOKEN = "Invalid token"
VIN_NOT_FOUND = "Vehicle not found"
TOKEN_TIMEOUT = 28800


class WebServer:
    def __init__(
        self,
        teslemetry: TeslemetryHandler,
        hostname: str | None = None,
        port: int = 8080,
    ) -> None:
        self._hostname = hostname
        self._port = port
        self._server = web.Application()
        self._teslemetry = teslemetry

        self._server.add_routes(
            [
                web.post("/api/oauth2/v3/token", self.oauth),
                web.get("/api/metadata", self.metadata),
                web.get("/api/1/products", self.products),
                web.get("/api/1/vehicles/{vin}/vehicle_data", self.vehicle_data),
            ]
        )

        self._runner: web.AppRunner = web.AppRunner(self._server)
        self._refresh_tokens: dict[str, datetime] = {}
        self._refresh_token_tasks: dict[str, asyncio.Task] = {}

    async def oauth(self, request: web.Request) -> web.Response:
        token = request.rel_url.query.get("token")
        if not token:
            _LOGGER.error("%s OAuth: Token not provided", request.host)
            return web.Response(text=TOKEN_NOT_PROVIDED, status=400)
        if token != self._teslemetry.token:
            _LOGGER.error(
                "%s: OAuth: Invalid token provided",
                request.host,
            )
            return web.Response(text=INVALID_TOKEN, status=401)

        random_data = os.urandom(100)

        hash_gen = hashlib.new("sha512")
        hash_gen.update(random_data)
        refresh_token = hash_gen.hexdigest()[:2100]

        self._refresh_tokens[refresh_token] = datetime.now() + timedelta(
            seconds=TOKEN_TIMEOUT
        )
        self._refresh_token_tasks[refresh_token] = asyncio.create_task(
            self._clear_tokens(refresh_token, TOKEN_TIMEOUT)
        )

        _LOGGER.debug("%s Refresh token returned", request.host)

        return web.json_response(
            {
                "access_token": refresh_token,
                "expires_in": TOKEN_TIMEOUT,
                "token_type": "Bearer",
            },
            status=200,
        )

    async def metadata(self, request: web.Request) -> web.Response:
        token = request.rel_url.query.get("token")
        response = self._check_refresh_tokens(
            token=token, method="Metadata", client=request.host
        )
        if response is not None:
            return response

        _LOGGER.debug("%s: Metadata returned", request.host)
        return web.json_response(self._teslemetry.metadata)

    async def products(self, request: web.Request) -> web.Response:
        token = request.rel_url.query.get("token")
        response = self._check_refresh_tokens(
            token=token, method="Products", client=request.host
        )
        if response is not None:
            return response

        _LOGGER.debug("%s: Products returned", request.host)
        return web.json_response(self._teslemetry.products)

    async def vehicle_data(self, request: web.Request) -> web.Response:
        token = request.rel_url.query.get("token")
        response = self._check_refresh_tokens(
            token=token, method="VehicleData", client=request.host
        )
        if response is not None:
            return response

        vin = request.match_info.get("vin", "")
        vehicle = self._teslemetry.registered_vehicles.get(vin)
        if not vehicle:
            _LOGGER.error("%s:%s Vehicle not found", request.host, vin)
            return web.Response(text=VIN_NOT_FOUND, status=404)

        vehicle_data = await vehicle.vehicle_data()
        _LOGGER.debug("%s:%s: Vehicle data returned", request.host, vin)
        return web.json_response(vehicle_data)

    async def start(self) -> None:
        await setup(self._server, ForwardedRelaxed(), XForwardedRelaxed())
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._hostname, self._port)
        await site.start()
        _LOGGER.info("Server started on %s:%s", self._hostname, self._port)

    async def stop(self) -> None:
        await self._runner.cleanup()
        for task in self._refresh_token_tasks.values():
            task.cancel()
        await asyncio.gather(
            *self._refresh_token_tasks.values(), return_exceptions=True
        )

        _LOGGER.info("Server on %s:%s stopped", self._hostname, self._port)

    async def _clear_tokens(self, token: str, timeout: int) -> None:
        await asyncio.sleep(timeout)
        if token in self._refresh_tokens:
            del self._refresh_tokens[token]
            _LOGGER.debug("%s:%s Token %s cleared", self._hostname, self._port, token)
        return None

    def _check_refresh_tokens(
        self, token: str | None, method: str, client: str
    ) -> web.Response | None:
        if not token:
            _LOGGER.error("%s: %s: Token not found", client, method)
            return web.Response(text=TOKEN_NOT_PROVIDED, status=400)

        if token == self._teslemetry.token:
            _LOGGER.debug(
                "%s: %s: Main Token was provided.",
                client,
                method,
            )
            return None

        if token not in self._refresh_tokens:
            if token != self._teslemetry.token:
                _LOGGER.debug(
                    "%s: %s: Token '%s' not found",
                    client,
                    method,
                    token,
                )
                return web.Response(text=INVALID_TOKEN, status=401)

        _LOGGER.debug(
            "%s: %s: Token %s is still valid",
            client,
            method,
            token,
        )
        return None
