import asyncio
import json
import logging
from datetime import datetime

from aiohttp import WSMsgType, web
from aiohttp_remotes import ForwardedRelaxed, XForwardedRelaxed, setup

from teslemetry_teslamate.Teslemetry.TeslemetryHandler import TeslemetryHandler

_LOGGER = logging.getLogger(__name__)


class WebSocket:
    def __init__(
        self,
        teslemetry: TeslemetryHandler,
        hostname: str = "localhost",
        port: int = 8081,
    ) -> None:
        self._hostname = hostname
        self._port = port
        self._server = web.Application()
        self._teslemetry = teslemetry

        self._server.add_routes(
            [
                web.get("/", self.websocket_handler),
            ]
        )

        self._runner: web.AppRunner = web.AppRunner(self._server)

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        vins: str = ""
        values: list[str] = []

        update_task: asyncio.Task | None = None
        hello_task: asyncio.Task | None = None

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        ws_request = json.loads(msg.data)
                    except json.JSONDecodeError:
                        _LOGGER.warning(
                            "%s:%s Did not receive valid JSON: %s",
                            request.host,
                            vins,
                            msg.data,
                        )
                        await ws.send_json(
                            {
                                "msg_type": "data:error",
                                "tag": vins,
                                "error_type": "client_error",
                                "value": "Expecting valid JSON",
                            }
                        )
                        continue

                    if "msg_type" not in ws_request:
                        _LOGGER.warning(
                            "%s:%s Received invalid request, expecting msg_type: %s",
                            request.host,
                            vins,
                            ws_request,
                        )
                        await ws.send_json(
                            {
                                "msg_type": "data:error",
                                "tag": vins,
                                "error_type": "client_error",
                                "value": "Expecting msg_type in JSON",
                            }
                        )
                        continue

                    msg_type = ws_request["msg_type"]
                    match msg_type:
                        case "data:subscribe_oauth":
                            authenticated, response = self._oauth(
                                ws_request=ws_request, client=request.host
                            )

                            if not authenticated:
                                await ws.send_json(response)
                                break

                            vins = ws_request["tag"]
                            values = []
                            if (
                                value_list := ws_request.get("value")
                            ) is not None and value_list.strip() != "":
                                values = list(map(str.strip, value_list.split(",")))

                            # Start ping task
                            hello_task = asyncio.create_task(
                                self._hello(ws=ws, client=request.host, vins=vins)
                            )
                            # Start update task
                            update_task = asyncio.create_task(
                                self._update(
                                    ws=ws,
                                    vins=vins,
                                    values=values,
                                    client=request.host,
                                )
                            )
                            await ws.send_json(response)

                        case _:
                            _LOGGER.warning(
                                "%s:%s Received invalid data request: %s",
                                request.host,
                                vins,
                                ws_request,
                            )
                            await ws.send_json(
                                {
                                    "msg_type": "data:error",
                                    "tag": vins,
                                    "error_type": "client_error",
                                    "value": "Invalid data request received",
                                }
                            )

                elif msg.type == WSMsgType.ERROR:
                    _LOGGER.error(
                        "%s:%s Websocket connection closed with exception %s",
                        request.host,
                        vins,
                        ws.exception(),
                    )
                    break
                elif msg.type == WSMsgType.CLOSE:
                    _LOGGER.info(
                        "%s:%s Websocket connection is closed", request.host, vins
                    )
                    break
        except asyncio.CancelledError:
            pass

        _LOGGER.info("%s:%s Websocket has been closed", request.host, vins)
        wait_for: list[asyncio.Task] = []
        if hello_task is not None:
            hello_task.cancel()
            wait_for.append(hello_task)
        if update_task is not None:
            update_task.cancel()
            wait_for.append(update_task)

        if len(wait_for) != 0:
            asyncio.gather(*wait_for)

        return ws

    async def start(self) -> None:
        await setup(self._server, ForwardedRelaxed(), XForwardedRelaxed())
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._hostname, self._port)
        await site.start()
        _LOGGER.info("WebSocket Server started on %s:%s", self._hostname, self._port)

    async def stop(self) -> None:
        await self._runner.cleanup()
        _LOGGER.info("%s:%s Server stopped", self._hostname, self._port)

    def _oauth(self, ws_request: dict, client: str) -> tuple[bool, dict]:
        if "tag" not in ws_request:
            _LOGGER.error("%s: VIN is not provided.", client)

            return False, {
                "msg_type": "data:error",
                "tag": None,
                "error_type": "client_error",
                "value": "owner_api error:tag not provided",
            }

        vins = ws_request["tag"]
        if "token" not in ws_request:
            _LOGGER.error("%s:%s OAuth: Token not provided", client, vins)

            return False, {
                "msg_type": "data:error",
                "tag": ws_request.get("tag"),
                "error_type": "client_error",
                "value": "owner_api error:token not provided",
            }

        if ws_request["token"] != self._teslemetry.token:
            _LOGGER.error("%s:%s OAuth: Invalid token provided", client, vins)
            return False, {
                "msg_type": "data:error",
                "tag": ws_request.get("tag"),
                "error_type": "client_error",
                "value": "Can't validate token",
            }

        _LOGGER.info(
            "%s:%s WebSocket stream connection received",
            client,
            vins,
        )
        return True, {
            "msg_type": f"control:hello:{ws_request.get('tag')}",
            "connection_timeout": 30000,
        }

    @staticmethod
    async def _hello(ws: web.WebSocketResponse, client: str, vins: str) -> None:
        _LOGGER.debug("Control ping has been started.")
        try:
            while True:
                await asyncio.sleep(10)
                _LOGGER.debug("%s:%s Sending control ping", client, vins)
                await ws.send_json(
                    {"msg_type": "control:hello", "connection_timeout": 30000}
                )
        except asyncio.CancelledError:
            pass

    async def _update(
        self,
        ws: web.WebSocketResponse,
        vins: str,
        values: list[str],
        client: str,
    ) -> None:
        # Open queue to get data
        my_queue: asyncio.Queue = asyncio.Queue()
        await self._teslemetry.add_queue(
            name=f"Web Socket {client}:{vins}", queue=my_queue
        )

        _LOGGER.debug("%s:%s Update task has been started.", client, vins)
        send_object: dict[str, str | list[str] | list[dict[str, str | dict[str, str]]]]
        vin_list: list[str] = list(map(str.strip, vins.split(",")))

        # Start listening to queue
        try:
            while True:
                try:
                    stream_data = await my_queue.get()
                except asyncio.QueueShutDown:
                    send_object = {
                        "msg_type": "data:error",
                        "tag": vins,
                        "error_type": "server_shutdown",
                        "value": "Server is being shutdown",
                    }
                    _LOGGER.debug("%s:%s Sending %s", client, vins, send_object)
                    await ws.send_json(send_object)
                    break

                for vin in vin_list:
                    data: list[dict[str, str | dict[str, str]]] = []
                    queue_key: str
                    streaming_value: str
                    for queue_key, streaming_value in stream_data.get(vin, {}).items():
                        if len(values) == 0 or queue_key in values:
                            data.append(
                                {
                                    "key": queue_key,
                                    "value": {"stringValue": streaming_value},
                                }
                            )

                    if len(data) > 0:
                        now = datetime.now()
                        send_object = {
                            "msg_type": "data:update",
                            "data": data,
                            "vin": vin,
                            "created_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                        }
                        _LOGGER.debug("%s:%s Sending %s", client, vin, send_object)
                        await ws.send_json(send_object)
        except asyncio.CancelledError:
            _LOGGER.debug("%s:%s Cancel requested", client, vins)
            await self._teslemetry.remove_queue(name=f"Web Socket {client}:{vins}")
            my_queue.shutdown(immediate=True)

        except Exception as e:
            _LOGGER.error("%s:%s Exception occured!", client, vins)
            await self._teslemetry.remove_queue(name=f"Web Socket {client}:{vins}")
            my_queue.shutdown(immediate=True)
            raise e
