import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.uzx import uzx_constants as CONSTANTS, uzx_web_utils as web_utils
from hummingbot.connector.exchange.uzx.uzx_auth import UzxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant, WSResponse
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.uzx.uzx_exchange import UzxExchange


class UzxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    HEARTBEAT_TIME_INTERVAL = 30.0
    MAX_RETRIES = 3

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: UzxAuth,
                 trading_pairs: List[str],
                 connector: 'UzxExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: UzxAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._current_listen_key = None
        self._last_listen_key_ping_ts = None
        self._manage_listen_key_task = None
        self._listen_key_initialized_event = asyncio.Event()
        self._trading_pairs = trading_pairs

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WSAssistant instance.
        """
        # Always create a new assistant to avoid connection issues
        return await self._api_factory.get_ws_assistant()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.
        """
       # Get a websocket assistant and connect it
        ws = await self._get_ws_assistant()
        url = f"{CONSTANTS.WSS_PRIVATE_URL}"
        self.logger().info(f"Authenticating to user stream....")
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        payload = self._auth.get_ws_authenticate_payload(url)
        login_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(login_request)
        response : WSResponse = await ws.receive()
        if response.data.get("status") == "success":
            self.logger().info("Successfully connected to user stream")
            return ws
        else:
            self.logger().error(f"Failed to connect to user stream: {response.data.get('error')}")
            raise Exception(f"Failed to connect to user stream: {response.data.get('error')}")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        Uzx does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            ws_request = WSJSONRequest(
                payload={
                    "event": "sub",
                    "params": {
                        "type": "orderV2.spot",
                        "symbol": symbol,
                        "interval": "1min"
                    },
                    "zip": False
                }
            )
            await websocket_assistant.send(ws_request)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        """
        Handles websocket disconnection by cleaning up resources.

        :param websocket_assistant: The websocket assistant that was disconnected
        """
        self.logger().info("User stream interrupted. Cleaning up...")

        # Disconnect the websocket if it exists
        websocket_assistant and await websocket_assistant.disconnect()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if data is not None:  # data will be None when the websocket is disconnected
                await self._process_event_message(
                    event_message=data, queue=queue, websocket_assistant=websocket_assistant
                )

    async def _process_event_message(
        self, event_message: Dict[str, Any], queue: asyncio.Queue, websocket_assistant: WSAssistant
    ):
        if len(event_message) > 0:
            if "ping" in event_message:  # Send pong response to ping
                timestamp = event_message.get("ping")
                pong_payloads = {"pong": timestamp}
                pong_request = WSJSONRequest(payload=pong_payloads)
                await websocket_assistant.send(request=pong_request)
            elif event_message.get("type") == CONSTANTS.ORDER_CHANGE_EVENT_TYPE:
                queue.put_nowait(event_message)
