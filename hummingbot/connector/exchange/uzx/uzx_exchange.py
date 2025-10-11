import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.uzx import uzx_constants as CONSTANTS, uzx_utils, uzx_web_utils as web_utils
from hummingbot.connector.exchange.uzx.uzx_api_order_book_data_source import UzxAPIOrderBookDataSource
from hummingbot.connector.exchange.uzx.uzx_api_user_stream_data_source import UzxAPIUserStreamDataSource
from hummingbot.connector.exchange.uzx.uzx_auth import UzxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class UzxExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 uzx_api_key: str,
                 uzx_api_secret: str,
                 uzx_api_passphrase: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = uzx_api_key
        self.secret_key = uzx_api_secret
        self.api_passphrase = uzx_api_passphrase
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_uzx_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def to_hb_order_type(uzx_type: str) -> OrderType:
        return OrderType[uzx_type]

    @property
    def authenticator(self):
        return UzxAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            api_passphrase=self.api_passphrase,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == "com":
            return "uzx"
        else:
            return f"uzx_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.PRODUCTS_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.PRODUCTS_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return UzxAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return UzxAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_int = CONSTANTS.ORDER_TYPE[order_type]
        side_int = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {"product_name": symbol,
                      "order_buy_or_sell": side_int,
                      "amount": amount_str,
                      "order_type": type_int}
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            price_str = f"{price:f}"
            api_params["price"] = price_str
        if order_type is OrderType.MARKET:
            api_params["trade_ccy"] = 1
        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.PLACE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            o_id = str(order_result["data"]["order_id"])
            transact_time = self._time_synchronizer.time()
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        api_params = {
            "inst_type": 1,
            "order_id": tracked_order.exchange_order_id,
            "cancel_ord_type": 1
        }
        cancel_result = await self._api_put(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)
        if cancel_result.get("code") == 200:
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
          {
          "ins_type": "SPOT",
          "product_name": "BTC-USDT",
          "base_coin_name": "BTC",
          "quote_coin_name": "USDT",
          "price_precision": 2,
          "num_precision": 5,
          "max_once_vol": "10",
          "max_once_amount": "1000000",
          "min_once_vol": "0.00001",
          "min_once_amount": "5",
          "swap_value": "0",
          "price_unit": "0",
          "max_leverage": 0,
          "max_once_limit_num": 0,
          "max_once_market_num": 0,
          "max_hold_num": 0,
          "maintenance_margin_rate": "0",
          "market_max_deeps": 20,
          "max_book_num": 200
          },
        """

        trading_pair_rules = [item for item in exchange_info_dict["data"] if item.get("ins_type") == "SPOT"]
        retval = []
        for rule in filter(uzx_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("product_name"))

                min_order_size = Decimal(rule.get("min_once_vol"))
                tick_size = Decimal("1") / (Decimal(10) ** int(rule.get("price_precision")))
                step_size = Decimal("1") / (Decimal(10) ** int(rule.get("num_precision")))
                min_notional = Decimal(rule.get("min_once_amount"))

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=Decimal(tick_size),
                                min_base_amount_increment=Decimal(step_size),
                                min_notional_size=Decimal(min_notional)))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():

            try:
                event_type = event_message.get("type")
                if event_type == "orderV2.spot" :
                 order_data = event_message.get("data")
                 if order_data and order_data.get("state") is not None:
                  tracked_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(str(order_data["order_id"]))
                  if tracked_order is not None:
                    order_update = OrderUpdate(
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=order_data["updated_at"] if order_data["updated_at"] else order_data["created_at"],
                        new_state=CONSTANTS.ORDER_STATE[order_data["state"]],
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(order_data["order_id"]),
                    )
                    self._order_tracker.process_order_update(order_update=order_update)

                    if "filled_amount" in order_data and Decimal(order_data.get("filled_amount", 0)) > 0:
                        # Extract fee information from the message
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=tracked_order.quote_asset,
                            flat_fees=[TokenAmount(
                                amount=Decimal(order_data.get("maker_fee", 0)),
                                token=tracked_order.quote_asset
                            )]
                        )
                        self.logger().info(f"DEBUGGING: Trade update: {order_data.get('filled_amount')}")
                        if order_data.get("price") is not None:
                            trade_update = TradeUpdate(
                                trade_id=str(order_data.get("order_id")),
                                client_order_id=tracked_order.client_order_id,
                                exchange_order_id=tracked_order.exchange_order_id,
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(order_data.get("filled_amount")),
                                fill_quote_amount=Decimal(order_data.get("filled_quote_amount")),
                                fill_price=Decimal(order_data.get("price")),
                                fill_timestamp=order_data.get("updated_at", order_data.get("created_at")),
                            )
                            self._order_tracker.process_trade_update(trade_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case Uzx's user stream events are not working.
        NOTE: It is not required to copy this functionality in other connectors.
        This is separated from _update_order_status which only updates the order status without producing filled
        The minimum poll interval for order status is 10 seconds.
        """
        pass

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = str(order.exchange_order_id)
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            response = await self._api_get(
                path_url=CONSTANTS.ORDER_DETAILS_PATH_URL,
                params={
                    "order_id": exchange_order_id
                },
                is_auth_required=True)

            if response.get("data", None):

             order_data = response["data"]
             all_fills = order_data["bills"]

             if all_fills :
              for trade in all_fills:
                 exchange_order_id = str(order_data["order_id"])
                 fee = TradeFeeBase.new_spot_fee(
                     fee_schema=self.trade_fee_schema(),
                     trade_type=order.trade_type,
                     percent_token=order_data["coin"],
                     flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=order_data["coin"])]
                 )
                 trade_update = TradeUpdate(
                     trade_id=str(trade["bill_id"]),
                     client_order_id=order.client_order_id,
                     exchange_order_id=exchange_order_id,
                     trading_pair=trading_pair,
                     fee=fee,
                     fill_base_amount=Decimal(trade["filled_amount"]),
                     fill_quote_amount=Decimal(trade["filled_amount"]) * Decimal(trade["price"]),
                     fill_price=Decimal(trade["price"]),
                     fill_timestamp=trade["created_at"],
                 )
                 trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        resp = await self._api_get(
            path_url=CONSTANTS.ORDER_DETAILS_PATH_URL,
            params={
                "order_id":  exchange_order_id},
            is_auth_required=True)

        if "not found order" in resp :
          resp = await self._api_get(path_url=CONSTANTS.ORDERS_HISTORY_PATH_URL, is_auth_required=True)
          orders_list = resp["data"]
          updated_order_data = next((order for order in orders_list if order["order_id"] == exchange_order_id), None)
          new_state = CONSTANTS.ORDER_STATE[updated_order_data["status"]]
          order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["order_id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["finish_at"] if updated_order_data["finish_at"] else  updated_order_data["created_at"],
            new_state=new_state,
           )

          return order_update

        updated_order_data = resp["data"]

        new_state = CONSTANTS.ORDER_STATE[updated_order_data["status"]]

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["order_id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["created_at"],
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        resp = await self._api_get(
            path_url=CONSTANTS.BALANCES_PATH_URL,
            is_auth_required=True)

        balances = resp["data"]
        for balance_entry in balances:
            asset_name = balance_entry["coin"]
            free_balance = Decimal(balance_entry["available_balance"])
            total_balance = Decimal(balance_entry["available_balance"]) + Decimal(balance_entry["frozen_balance"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        spot_pairs = [item for item in exchange_info["data"] if item.get("ins_type") == "SPOT"]

        for symbol_data in filter(uzx_utils.is_exchange_information_valid, spot_pairs):
            mapping[symbol_data["product_name"]] = combine_to_hb_trading_pair(base=symbol_data["base_coin_name"],
                                                                              quote=symbol_data["quote_coin_name"])
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:

        symbol =  await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PATH_URL.format(symbol)
        )

        return float(resp_json["data"]["close"])

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all currently active orders. The cancellations are performed in parallel tasks.

        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run

        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        return await super().cancel_all(CONSTANTS.CANCEL_ALL_TIMEOUT)
