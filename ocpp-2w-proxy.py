# 2 way OCPP proxy (can also be used as a 1-way simple proxy)

import asyncio
import logging
import time
from typing import Tuple
import json

import websockets
import websockets.asyncio
import websockets.asyncio.server

from enum import IntEnum
import ssl
import argparse
import configparser
import re

__version__ = "0.1.0"

config = configparser.ConfigParser()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("proxy")

class OCPP2WProxy: # Forward declaration
    pass

class OCPPMessageType(IntEnum):
    Call = 2
    CallResult = 3
    CallError = 4

# Characters accepted in a charger ID (last segment of the websocket path)
CHARGER_ID_RE = re.compile(r"[A-Za-z0-9\-_.:@~]+")

# OCPP subprotocols offered to chargers. The one negotiated with the charger is
# used for the upstream connections as well.
SUBPROTOCOLS = ["ocpp1.6", "ocpp2.0.1", "ocpp2.1"]
DEFAULT_SUBPROTOCOL = "ocpp1.6"

# Reconnect backoff (seconds) for a lost secondary connection. Overridable in the
# ini file via secondary_reconnect_min / secondary_reconnect_max in [ext-server].
SECONDARY_RECONNECT_MIN = 5
SECONDARY_RECONNECT_MAX = 60
# A secondary connection that lasted at least this long resets the backoff.
SECONDARY_STABLE_SECONDS = 60

# main class
class OCPP2WProxy:
    # Static dict of OCPP2WProxy instances. key is charger_id
    proxy_list: dict[str, OCPP2WProxy] = {}

    # Utility functions
    @staticmethod
    def decode_ocpp_message(message: str) -> Tuple[OCPPMessageType, str]:
        """Decode an OCPP message from a string"""
        j = json.loads(message)
        return [j[0], j[1]]

    def __init__(self, websocket: websockets.asyncio.server.ServerConnection, charger_id: str):
        # Store the websocket for later     
        logger.debug(websocket.request)
        self.ws = websocket
        self.charger_id = charger_id

        # Check that charger id looks reasonable. Many chargers use IDs such as
        # "GaroCS-4133AAAA46980" or "wallbox_2", so allow the usual URL-safe
        # characters, not just alphanumerics.
        if not charger_id or not CHARGER_ID_RE.fullmatch(charger_id):
            logger.error(f"Charger ID '{charger_id}' contains unsupported characters (allowed: letters, digits, - _ . : @ ~)")
            raise Exception("Charger ID contains unsupported characters")

        # Initialize table of CSMS call ids sent to the charger in order to respond back
        self.primary_call_ids = set()
        self.secondary_call_ids = set()

        # Upstream connections. The secondary one may be None at any time: it is
        # optional, and it is re-established in the background when lost.
        self.primary_connection = None
        self.secondary_connection = None
        self.secondary_url = None
        self.secondary_task = None
        self._connect_kwargs = {}

        # Insert new OCPP2WProxy instance in the (static) dict of instances.
        self.proxy_list[charger_id] = self

    async def close(self):
        """Close all connections to the charger and primary, secondary server"""
        if self.secondary_task is not None:
            self.secondary_task.cancel()
        for connection in (self.ws, self.primary_connection, self.secondary_connection):
            if connection is None:
                continue
            try:
                await connection.close()
            except Exception:
                pass # Ignore exceptions

    async def connect_secondary(self):
        """Connect to the secondary server. Returns None (and logs) if that fails."""
        try:
            connection = await websockets.connect(uri=self.secondary_url, **self._connect_kwargs)
            logger.info(f"{self.charger_id} Connected to secondary server @ {self.secondary_url}")
            return connection
        except Exception as e:
            logger.warning(f"{self.charger_id} Secondary server @ {self.secondary_url} not reachable ({e}). Continuing with primary only")
            return None

    async def secondary_loop(self):
        """Keep the secondary connection alive.

        Receives messages while connected and reconnects with backoff when the
        connection is lost. Losing the secondary (e.g. Home Assistant restarting)
        must never take down the charger's connection to the primary server."""
        delay_min = config.getint("ext-server", "secondary_reconnect_min", fallback=SECONDARY_RECONNECT_MIN)
        delay_max = config.getint("ext-server", "secondary_reconnect_max", fallback=SECONDARY_RECONNECT_MAX)
        delay = delay_min
        while True:
            if self.secondary_connection is None:
                logger.info(f"{self.charger_id} Retrying secondary server in {delay} seconds")
                await asyncio.sleep(delay)
                delay = min(delay * 2, delay_max)
                self.secondary_connection = await self.connect_secondary()
                continue
            connected_at = time.time()
            await self.receive_secondary_messages()
            # Connection lost. Pending calls from the secondary can no longer be answered.
            self.secondary_connection = None
            self.secondary_call_ids.clear()
            # Only go back to the short delay after a connection that actually lasted.
            # The Home Assistant OCPP integration accepts and immediately closes the
            # connection of a charger that is not configured yet; without this check
            # we would knock on its door every delay_min seconds.
            if time.time() - connected_at >= SECONDARY_STABLE_SECONDS:
                delay = delay_min

    @staticmethod
    async def check_delete_old(charger_id: str):
        """Check if there are any old instances of this charger in the proxy list"""
        if charger_id in OCPP2WProxy.proxy_list:
            logger.info(f"Charger ID {charger_id} already exists. Closing and deleting")
            proxy: OCPP2WProxy = OCPP2WProxy.proxy_list[charger_id]
            await proxy.close()
            del OCPP2WProxy.proxy_list[charger_id]

    async def run(self):
        """Main loop for this proxy. This is where all the magic happens."""

        # Create connections to the two CSMSes.
        # Forward any available Authorization and User-Agent headers
        headers = {}
        if "Authorization" in self.ws.request.headers:
            headers["Authorization"] = self.ws.request.headers["Authorization"]
            logger.debug(f'Authorization header set to {headers["Authorization"]}')
        user_agent = self.ws.request.headers.get("User-Agent", None)
        # Use the subprotocol negotiated with the charger (chargers may offer several,
        # e.g. "ocpp1.6, ocpp2.0.1", or none at all - then we default to ocpp1.6)
        subprotocol = self.ws.subprotocol or DEFAULT_SUBPROTOCOL
        logger.debug(f"{self.charger_id} using subprotocol {subprotocol}")
        primary_url = config.get("ext-server", "server") + "/" + self.charger_id
        if config.get("ext-server", "secondary_server", fallback=""):
            self.secondary_url = config.get("ext-server", "secondary_server") + "/" + self.charger_id
        self._connect_kwargs = dict(
            user_agent_header=user_agent,
            additional_headers=headers,
            subprotocols=[subprotocol],
        )

        try:
            self.primary_connection = await websockets.connect(uri=primary_url, **self._connect_kwargs)
            logger.info(f"{self.charger_id} Connected to primary server @ {primary_url}")

            # Connect to secondary server if it is enabled. Failure is not fatal: the
            # secondary loop keeps retrying in the background.
            if self.secondary_url:
                self.secondary_connection = await self.connect_secondary()
            else:
                logger.info(f"{self.charger_id} Secondary server not enabled")

            # Create tasks to handle the charger. One task each to handle receiving messages
            # from the charger and from the primary CSMS. These two decide the lifetime of
            # this proxy session. The secondary CSMS runs in its own loop that may come and go.
            self._last_charger_update = time.time()
            self.tasks = []
            self.tasks.append(asyncio.create_task(self.receive_charger_messages()))
            self.tasks.append(asyncio.create_task(self.receive_primary_messages()))
            if self.secondary_url:
                self.secondary_task = asyncio.create_task(self.secondary_loop())
            #self.tasks.append(asyncio.create_task(self.watchdog()))

            # Wait for tasks to complete
            done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
            logger.debug(f"{self.charger_id} Task(s) completed: {done}, {pending}")

            for task in done:
                e = task.exception()
                if e:
                    logger.warning(f"{self.charger_id} (Not serious - likely connection loss) Task {task} raised exception {e} related to charger ")

            # Cancel any remaining tasks
            for task in pending:
                task.cancel()
            if self.secondary_task is not None:
                self.secondary_task.cancel()

        except websockets.exceptions.InvalidURI:
            logger.error(f"{self.charger_id} Invalid URI")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"{self.charger_id} Connection closed unexpectedly: {e}")
        except websockets.exceptions.InvalidHandshake:
            logger.error(f"{self.charger_id} Handshake with the external server failed")
        except Exception as e:
            logger.error(f"{self.charger_id} Unexpected error: {e}")
        finally:
            # Always close stuff. close is well tempered, so can close even if not stablished
            await self.close()
            # Remove ourselves from the list, unless a newer instance already replaced us
            if OCPP2WProxy.proxy_list.get(self.charger_id) is self:
                del OCPP2WProxy.proxy_list[self.charger_id]

    async def receive_charger_messages(self):
        try:
            while True:
                # Wait for a message from the charger
                message = await self.ws.recv()
                # Process the received message
                logger.info(f"{self.charger_id} ^ : {message}")

                # Now, if this is an OCPP CallResult (3) or CallError (4), we need to send it back to the 
                # CSMS (primary or secondary) that issued the command
                [message_type, message_id] = OCPP2WProxy.decode_ocpp_message(message)

                # If it is a Call (2), we will send it to both primary and secondary (if connected)
                if message_type == OCPPMessageType.Call:
                    await self.primary_connection.send(message)
                    await self.send_to_secondary(message)
                elif message_type == OCPPMessageType.CallResult or message_type == OCPPMessageType.CallError:
                    if message_id in self.primary_call_ids:
                        logger.info(f"{self.charger_id} ^ : Result/Error forwarded to primary")
                        self.primary_call_ids.remove(message_id)
                        await self.primary_connection.send(message)
                    elif message_id in self.secondary_call_ids:
                        logger.info(f"{self.charger_id} ^ : Result/Error forwarded to secondary")
                        self.secondary_call_ids.remove(message_id)
                        await self.send_to_secondary(message)
                    else:
                        logger.error(f"{self.charger_id} ^: Received CallResult/CallError against unknown message id {message_id}")
                else:
                    logger.error(f"{self.charger_id} ^: Unknown message type {message_type}")
        except Exception as e:
            logger.error(f"{self.charger_id} Error in receive_charger_messages: {e}")

    async def send_to_secondary(self, message: str):
        """Send a message to the secondary server, if connected. Never raises:
        the secondary is best effort and must not disturb the primary path."""
        connection = self.secondary_connection
        if connection is None:
            logger.debug(f"{self.charger_id} Secondary not connected, message not forwarded")
            return
        try:
            await connection.send(message)
        except Exception as e:
            logger.warning(f"{self.charger_id} Could not forward message to secondary ({e})")

    async def receive_primary_messages(self):
        try:
            while True:
                # Wait for a message from the primary server
                message = await self.primary_connection.recv()
                logger.info(f"{self.charger_id} v (prim) : {message}")

                [message_type, message_id] = OCPP2WProxy.decode_ocpp_message(message)
                if message_type == OCPPMessageType.Call:
                    # Record the message_id 
                    self.primary_call_ids.add(message_id)

                # Send message to the charger
                await self.ws.send(message)
        except Exception as e:
            logger.error(f"{self.charger_id} Error in receive_primary_messages: {e}")

    async def receive_secondary_messages(self):
        connection = self.secondary_connection
        try:
            while True:
                # Wait for a message from the secondary server
                message = await connection.recv()
                logger.info(f"{self.charger_id} v (sec) : {message}")

                [message_type, message_id] = OCPP2WProxy.decode_ocpp_message(message)
                if message_type == OCPPMessageType.Call:
                    # Record the message_id 
                    self.secondary_call_ids.add(message_id)
                    # Send it to the charger
                    await self.ws.send(message) 
                # Note! We do not forward CallResults or CallErrors from the secondary server
                # These are silently ignored.
        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"{self.charger_id} Secondary connection closed ({e}). Will reconnect")
        except Exception as e:
            logger.error(f"{self.charger_id} Error in receive_secondary_messages: {e}")

    async def watchdog(self):
        """Watch time vs. timestamp updated by receiving messages from charger."""
        while True:
            # And ... sleep
            await asyncio.sleep(config.getint("host", "watchdog_interval", 30))

            elapsed = time.time() - self._last_charger_update
            if elapsed > config.getint("host", "watchdog_stale", 300):
                logger.error(f"{self.charger_id} Watch dog no for {elapsed} seconds. Closing connections")
                return

# Connection handler (charger connects)
async def on_connect(websocket: websockets.asyncio.server.ServerConnection):
    logger.debug(f'Connection request {websocket.request}')
    # Determine charger_id (final part of path). Chargers may be configured with a
    # path prefix such as ws://host:8321/ocpp/<id>, so only the last segment counts.
    path = websocket.request.path.split("?")[0].strip("/")
    charger_id = path[path.rfind("/") + 1:]
    logger.info(f'{charger_id} connection request (path {websocket.request.path})')

    try:
        # Delete any existing charger setup
        if charger_id in OCPP2WProxy.proxy_list:
            await OCPP2WProxy.proxy_list[charger_id].close()
            del OCPP2WProxy.proxy_list[charger_id]

        # Setup
        proxy = OCPP2WProxy(websocket=websocket, charger_id=charger_id)

        # Connect and run proxy operations
        await proxy.run()

    except Exception as e:
        logger.error(f'{charger_id} Error creating OCPP2WProxy: {e}')
    finally:
        logger.info(f"{charger_id} closed/done")


def select_subprotocol(connection, subprotocols):
    """Pick the OCPP subprotocol for a charger connection.

    Chargers that do not send a Sec-WebSocket-Protocol header would otherwise be
    rejected by websockets with "missing subprotocol". Accept them without a
    subprotocol (the proxy then talks ocpp1.6 upstream), as the Home Assistant OCPP
    integration does. Chargers offering several versions get the first one we support."""
    if not subprotocols:
        return None
    for candidate in SUBPROTOCOLS:
        if candidate in subprotocols:
            return candidate
    raise websockets.exceptions.NegotiationError(
        f"charger offered unsupported subprotocol(s) {list(subprotocols)}; expected one of {SUBPROTOCOLS}"
    )


# Main. Decode arguments, setup handler
async def main():
    parser = argparse.ArgumentParser(
        description='ocpp-2w-proxy: A two way OCPP proxy')
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s {__version__}')
    parser.add_argument(
        "--config",
        type=str,
        default="ocpp-2w-proxy.ini",
        help="Configuration file (INI format). Default ocpp-2w-proxy.ini",
    )
    args = parser.parse_args()

    # Read config. config object is then available (via config import) to all.
    logger.warning(f"Reading config from {args.config}")
    config.read(args.config)

    # Adjust log levels
    for logger_name in config["logging"]:
        logger.warning(f'Setting log level for {logger_name} to {config.get("logging", logger_name)}')
        logging.getLogger(logger_name).setLevel(level=config.get("logging", logger_name))

    # Get host config
    host = config.get("host", "addr")
    port = config.get("host", "port")
    cert_chain = config.get("host", "cert_chain", fallback=None)
    cert_key = config.get("host", "cert_key", fallback=None)
    logger.debug(f"host: {host}, port: {port}, cert_chain: {cert_chain}, cert_key: {cert_key}")

    # Start server, either ws:// or wss://
    if cert_chain and cert_key:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile=cert_chain, keyfile=cert_key)
        server = await websockets.serve(
            on_connect,
            host,
            port,
            subprotocols=SUBPROTOCOLS,
            select_subprotocol=select_subprotocol,
            ssl=ssl_context,
            ping_timeout=config.getint("host", "ping_timeout"),
        )
    else:
        server = await websockets.serve(
            on_connect,
            host,
            port,
            subprotocols=SUBPROTOCOLS,
            select_subprotocol=select_subprotocol,
            ping_timeout=config.getint("host", "ping_timeout"),
        )

    logger.info("Proxy ready. Waiting for new connections...")
    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        exit(0)
