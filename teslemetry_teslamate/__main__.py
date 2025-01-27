import asyncio
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, ArgumentTypeError

from .__version__ import __version__ as VERSION
from .custom_logging import LOGLEVELS, init_logger
from .Handlers.WebServer import WebServer
from .Handlers.WebSocket import WebSocket
from .Teslemetry.TeslemetryHandler import TeslemetryHandler

_LOGGER = logging.getLogger(__name__)

NAME = "teslemetry-teslamate"


async def run(arguments) -> None:
    """Main function to run the program"""

    scheduled_tasks: dict[str, asyncio.Task[None]] = {}
    # Start Teslemetry
    teslemetry = TeslemetryHandler(access_token=arguments.token)
    teslemetry_task = await teslemetry.run()

    # Start Web Server
    web_server = WebServer(
        teslemetry=teslemetry,
        hostname=arguments.webserver_hostname,
        port=arguments.webserver_port,
    )
    await web_server.start()

    # Start Web Socket Server
    web_socket_server = WebSocket(
        teslemetry=teslemetry,
        hostname=arguments.websocket_hostname,
        port=arguments.websocket_port,
    )
    await web_socket_server.start()

    _LOGGER.info("Everything has been started")
    await teslemetry_task


def cancel_tasks(loop):
    """Cancel any tasks currently in the async loop

    Args:
        loop (_type_): event loop
    """
    _LOGGER.debug("Cancelling any tasks still running.")
    loop.run_until_complete(asyncio.sleep(1))
    for task in asyncio.all_tasks(loop):
        task.cancel()

    # Allow cancellations to be processed
    for _ in range(20):
        loop.run_until_complete(asyncio.sleep(1))
        if len(asyncio.all_tasks(loop)) == 0:
            break


def port_range(min_val, max_val):
    def validator(arg):
        try:
            value = int(arg)
        except ValueError:
            raise ArgumentTypeError(
                f"Port must be an integer between {min_val} and {max_val}"
            )
        if not min_val <= value <= max_val:
            raise ArgumentTypeError(
                f"Port must be an integer between {min_val} and {max_val}"
            )
        return value

    return validator


def parameters():
    arg_parser = ArgumentParser(
        description="Teslemetry to TeslaMate",
        epilog="This program caches vehicle_data for TeslaMate with information from"
        "Teslemetry.",
        fromfile_prefix_chars="@",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )

    arg_parser.add_argument(
        "--version", action="version", version=f" {NAME} " + VERSION
    )

    arg_parser.add_argument(
        "--token", required=True, type=str, help="Teslemetry API token"
    )

    logging_params = arg_parser.add_argument_group(
        title="Web Server", description="Web Server options"
    )
    logging_params.add_argument(
        "--webserver_hostname",
        default="localhost",
        type=str,
        help="Hostname or IP address web server should listen on for requests.",
    )
    logging_params.add_argument(
        "--webserver_port",
        default=8080,
        type=port_range(1024, 65535),
        help="Port web server should listen on for requests, has to be different from web socket server if same hostname is used. Valid values: 1024-65535",
    )

    logging_params = arg_parser.add_argument_group(
        title="Web Socket", description="Web Socket Server options"
    )
    logging_params.add_argument(
        "--websocket_hostname",
        default="localhost",
        type=str,
        help="Hostname or IP address web socket server should listen on",
    )
    logging_params.add_argument(
        "--websocket_port",
        default=8081,
        type=port_range(1024, 65535),
        help="Port web socket server should listen on for requests, has to be different from web server if same hostname is used. Valid values: 1024-65535",
    )

    logging_params = arg_parser.add_argument_group(
        title="Logging", description="Logging arguments"
    )
    logging_params.add_argument(
        "--loglevel",
        default="WARNING",
        choices=list(LOGLEVELS.keys()),
        type=str.upper,
        help="Logging level.",
    )
    logging_params.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="Logging filename.",
    )

    logging_params.add_argument(
        "--loglevel_entries",
        default=None,
        type=str,
        nargs="*",
        help="Set specific loglevel for certain class/methods. Each entry should be in format: class.method:loglevel",
    )

    parsed_args = arg_parser.parse_args()

    if (
        parsed_args.webserver_hostname == parsed_args.websocket_hostname
        and parsed_args.webserver_port == parsed_args.websocket_port
    ):
        print(
            f"error: Web Server port {parsed_args.webserver_port} is same as Web Socket Port {parsed_args.websocket_port}. These have to be different when same hostname ({parsed_args.websocket_hostname})"
        )
        exit(1)

    return arg_parser.parse_args()


def main() -> int:
    arguments = parameters()

    init_logger(
        loglevel=arguments.loglevel,
        loglevel_entries=arguments.loglevel_entries,
        logfile=arguments.logfile,
    )

    _LOGGER.debug("Arguments provided: %s", arguments)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run(arguments))
        cancel_tasks(loop)
        loop.close()

    except KeyboardInterrupt:
        print("Exit requested.")
        cancel_tasks(loop)
        loop.close()
        print("Closed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
