import asyncio
import os
import uuid
import json
import time
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
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
        
        # IOC taker-limit feature toggles
        self._enable_ioc_cap = (os.getenv("UZX_ENABLE_IOC_CAP") or "0") not in ("0", "", "false", "False")
        try:
            self._ioc_slippage_bps = int(os.getenv("UZX_IOC_SLIPPAGE_BPS") or os.getenv("UZX_MAX_SLIPPAGE_BPS") or "0")
        except ValueError:
            self._ioc_slippage_bps = 0
        self._ioc_use_tif = (os.getenv("UZX_IOC_USE_TIF") or "1") not in ("0", "false", "False")
        try:
            self._ioc_cancel_delay_ms = int(os.getenv("UZX_IOC_CANCEL_DELAY_MS") or "120")
        except ValueError:
            self._ioc_cancel_delay_ms = 120
        
        # Market amount semantics
        self._market_trade_ccy = (os.getenv("UZX_MARKET_TRADE_CCY") or "quote").lower()  # "quote" or "base"
        self._market_ref_price = (os.getenv("UZX_MARKET_REF_PRICE") or "bbo").lower()    # "bbo" or "last"
        
        # Rounding toggles
        self._enable_rounding = (os.getenv("UZX_ENABLE_ROUNDING") or "1") not in ("0", "", "false", "False")
        self._round_amount_mode = (os.getenv("UZX_ROUND_AMOUNT") or "floor").lower()    # floor | nearest
        self._round_price_mode  = (os.getenv("UZX_ROUND_PRICE")  or "floor").lower()    # floor | nearest
        
        # --- Structured logging ---
        self._log_json = (os.getenv("UZX_LOG_JSON") or "0") not in ("0", "", "false", "False")
        self._log_level = (os.getenv("UZX_LOG_LEVEL") or "INFO").upper()
        self._run_id = os.getenv("UZX_RUN_ID") or uuid.uuid4().hex[:8]
        self._order_traces = {}  # client_order_id -> trace_id

        # apply log level if supported
        try:
            import logging
            self.logger().setLevel(getattr(logging, self._log_level, logging.INFO))
        except Exception:
            pass
        
        # --- Kill switch & circuit breakers ---
        self._enable_kill_switch = (os.getenv("UZX_ENABLE_KILL_SWITCH") or "1") not in ("0", "", "false", "False")
        self._kill_switch_file = os.getenv("UZX_KILL_SIGNAL_FILE") or "/app/.kill"

        # consecutive error breaker
        try:
            self._max_consec_errors = int(os.getenv("UZX_MAX_CONSECUTIVE_ERRORS") or "5")
        except ValueError:
            self._max_consec_errors = 5
        self._consec_errors = 0

        # open orders breaker
        try:
            self._max_open_orders = int(os.getenv("UZX_MAX_OPEN_ORDERS") or "50")
        except ValueError:
            self._max_open_orders = 50

        # session notional breaker (quote currency, for this process lifetime)
        try:
            self._max_session_notional = Decimal(os.getenv("UZX_MAX_SESSION_NOTIONAL") or "0")
        except Exception:
            self._max_session_notional = Decimal("0")
        self._session_notional = Decimal("0")

        # run-time pause flag
        self._trading_paused = False
        
        # Policy knobs (private; expose via config later if desired)
        self._price_band_min: Decimal = Decimal("1.90")
        self._price_band_max: Decimal = Decimal("2.00")
        self._halt_below_min: bool = True

        # Jitter knobs (apply to timing/size; not to "set price")
        self._jitter_bps: int = 100  # ±1% (100 bps)
        self._jitter_on_size: bool = True
        self._jitter_on_delay_ms: int = 0  # e.g., up to ±250ms if you later allow timing jitter
            
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

    def _best_bbo(self, trading_pair):
        try:
            tracker = getattr(self, "_order_book_tracker", None)
            if tracker and getattr(tracker, "order_books", None):
                ob = tracker.order_books.get(trading_pair)
                if ob is not None:
                    bid = getattr(ob, "best_bid_price", None)
                    ask = getattr(ob, "best_ask_price", None)
                    if bid is not None and ask is not None:
                        return Decimal(str(bid)), Decimal(str(ask))
        except Exception:
            pass
        return None, None

    async def _reference_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        """
        bbo: BUY uses best_ask; SELL uses best_bid
        last: fallback to last trade price
        """
        if self._market_ref_price == "bbo":
            bid, ask = self._best_bbo(trading_pair)
            if bid is not None and ask is not None:
                return ask if is_buy else bid
        # fallback or explicit "last"
        return Decimal(str(await self._get_last_traded_price(trading_pair)))

    def _get_trading_rule(self, trading_pair: str):
        """
        Best-effort fetch of the TradingRule for this pair.
        """
        rule = None
        try:
            rule = self._trading_rules.get(trading_pair)
        except Exception:
            pass
        return rule

    def _round_units(self, val: Decimal, step: Decimal, mode: str) -> Decimal:
        """
        Round val to a multiple of step using either floor or nearest.
        """
        if step is None or step == 0:
            return val
        units = val / step
        if mode == "floor":
            units = units.to_integral_value(rounding=ROUND_FLOOR)
        else:  # nearest
            units = units.to_integral_value(rounding=ROUND_HALF_UP)
        return units * step

    def _apply_exchange_rounding(self,
                                 trading_pair: str,
                                 amount: Decimal,
                                 price: Decimal,
                                 order_type: OrderType,
                                 is_quote_amount: bool) -> Tuple[Decimal, Decimal]:
        """
        Snap amount/price to UZX increments from TradingRule.
        - For MARKET+quote mode: round the quote notional using quote increment if available.
        - For LIMIT: round price to price increment and amount to base increment.
        """
        if not self._enable_rounding:
            return amount, price

        rule = self._get_trading_rule(trading_pair)
        if rule is None:
            return amount, price

        # Try common attribute names used in Hummingbot TradingRule
        price_step = getattr(rule, "min_price_increment", None) or getattr(rule, "price_step", None)
        base_step  = getattr(rule, "min_base_amount_increment", None) or getattr(rule, "min_order_size", None)
        quote_step = getattr(rule, "min_quote_amount_increment", None)

        amt = amount
        px  = price

        if order_type is OrderType.LIMIT:
            if price_step:
                px = self._round_units(px, Decimal(str(price_step)), self._round_price_mode)
            if base_step:
                amt = self._round_units(amt, Decimal(str(base_step)), self._round_amount_mode)
        else:  # MARKET
            if is_quote_amount:
                if quote_step:
                    amt = self._round_units(amt, Decimal(str(quote_step)), self._round_amount_mode)
                # price not submitted in MARKET
            else:
                if base_step:
                    amt = self._round_units(amt, Decimal(str(base_step)), self._round_amount_mode)

        return amt, px

    def _is_manual_killed(self) -> bool:
        try:
            return os.path.exists(self._kill_switch_file)
        except Exception:
            return False

    async def _estimate_quote_notional(self, trading_pair: str, trade_type: TradeType,
                                       order_type: OrderType, amount: Decimal, price: Decimal) -> Decimal:
        # Estimate notional in quote terms for breaker accounting
        is_buy = (trade_type is TradeType.BUY)
        if order_type is OrderType.LIMIT and not price.is_nan():
            ref = price
        else:
            # prefer BBO; fallback to last price
            try:
                bid, ask = self._best_bbo(trading_pair)  # you added this earlier
            except Exception:
                bid = ask = None
            if bid is None or ask is None:
                ref = Decimal(str(await self._get_last_traded_price(trading_pair)))
            else:
                ref = ask if is_buy else bid
        return (amount.copy_abs() * ref)

    async def _within_policy_band(self, trading_pair: str) -> tuple[bool, Decimal]:
        """
        Returns (allowed, ref_price). Allowed is True iff price_band_min <= ref_price <= price_band_max.
        """
        # For BUY/SELL symmetry, use mid of BBO if available; else last.
        bid, ask = self._best_bbo(trading_pair)
        if bid is not None and ask is not None:
            mid = (bid + ask) / Decimal("2")
            ref_px = mid
        else:
            ref_px = Decimal(str(await self._get_last_traded_price(trading_pair)))
        allowed = (self._price_band_min <= ref_px <= self._price_band_max)
        return allowed, ref_px

    def _apply_symmetric_jitter(self, value: Decimal, bps: int) -> Decimal:
        if bps <= 0:
            return value
        # uniform in [-bps, +bps]
        import random
        j = Decimal(random.uniform(-bps, bps)) / Decimal(10_000)
        return (value * (Decimal("1") + j)).quantize(Decimal("1.00000000"))  # adjust precision as needed

    async def _check_kill_switch(self, trading_pair: str, trade_type: TradeType,
                                 order_type: OrderType, amount: Decimal, price: Decimal):
        """
        Raise IOError if any breaker trips. Called at the very start of _place_order.
        """
        if not self._enable_kill_switch:
            return

        # manual kill file or forced pause
        if self._trading_paused or self._is_manual_killed():
            self._trading_paused = True
            self._log_event("WARNING", "kill.paused", reason="manual_or_paused", tp=trading_pair)
            raise IOError("Kill switch engaged (manual/paused).")

        # open orders breaker
        try:
            open_count = len(self._order_tracker.active_orders)
        except Exception:
            open_count = 0
        if self._max_open_orders and open_count >= self._max_open_orders:
            self._trading_paused = True
            self._log_event("ERROR", "kill.paused", reason="too_many_open_orders", open_orders=open_count, limit=self._max_open_orders)
            raise IOError(f"Kill switch: open orders {open_count} >= {self._max_open_orders}.")

        # session notional breaker
        if self._max_session_notional and self._max_session_notional > 0:
            est_notional = await self._estimate_quote_notional(trading_pair, trade_type, order_type, amount, price)
            if (self._session_notional + est_notional) > self._max_session_notional:
                self._trading_paused = True
                self._log_event("ERROR", "kill.paused", reason="session_notional_limit",
                                next_notional=str(est_notional),
                                session=str(self._session_notional),
                                limit=str(self._max_session_notional))
                raise IOError("Kill switch: session notional limit reached.")

        # policy band breaker
        allowed, ref_price = await self._within_policy_band(trading_pair)
        if not allowed:
            self._trading_paused = True
            self._log_event("ERROR", "kill.paused", reason="price_band_violation",
                            ref_price=str(ref_price),
                            min_price=str(self._price_band_min),
                            max_price=str(self._price_band_max))
            raise IOError(f"Kill switch: price {ref_price} outside policy band [{self._price_band_min}, {self._price_band_max}].")

    def _note_submit_success(self):
        # reset consecutive error counter on success
        self._consec_errors = 0

    def _note_submit_error(self, where: str, err: Exception):
        # bump error counter; trip if at limit
        try:
            self._consec_errors += 1
            if self._max_consec_errors and self._consec_errors >= self._max_consec_errors:
                self._trading_paused = True
                self._log_event("ERROR", "kill.paused", reason="consecutive_errors",
                                where=where, count=self._consec_errors, limit=self._max_consec_errors,
                                err_type=type(err).__name__, err=str(err))
        except Exception:
            pass

    def _note_session_notional(self, quote_notional: Decimal):
        try:
            self._session_notional += quote_notional.copy_abs()
        except Exception:
            pass

    def _trace_id_for(self, client_order_id: str) -> str:
        """
        Stable short id for a client order across lifecycle logs.
        """
        tid = self._order_traces.get(client_order_id)
        if tid is None:
            tid = uuid.uuid4().hex[:8]
            self._order_traces[client_order_id] = tid
        return tid

    def _redact(self, obj):
        """
        Remove/shorten any sensitive fields before logging.
        """
        if not isinstance(obj, dict):
            return obj
        red = {}
        for k, v in obj.items():
            lk = str(k).lower()
            if any(s in lk for s in ("key", "secret", "passphrase", "auth", "token")):
                red[k] = "***"
            else:
                red[k] = v
        return red

    def _log_event(self, level: str, event: str, **fields):
        """
        Emit a structured log line. When JSON is enabled, this is a single JSON object.
        Otherwise prints key=value pairs. Includes run_id and ts by default.
        """
        lvl = level.upper()
        payload = {
            "ts": round(time.time(), 3),
            "run": self._run_id,
            "evt": event,
            **self._redact(fields),
        }
        try:
            lg = self.logger()
            if self._log_json:
                msg = json.dumps(payload, default=str, separators=(",", ":"))
            else:
                # key=value compact
                kv = " ".join(f"{k}={repr(v)}" for k, v in payload.items())
                msg = kv
            if   lvl == "DEBUG": lg.debug(msg)
            elif lvl == "INFO":  lg.info(msg)
            elif lvl == "WARN" or lvl == "WARNING": lg.warning(msg)
            else: lg.error(msg)
        except Exception:
            # never let logging break the flow
            pass

    async def _ioc_cancel_remainder(self, exchange_order_id: str):
        """
        Fallback IOC simulation: cancel whatever is left right after placement.
        Safe if fully filled (exchange will just say 'not found' or 'already filled').
        """
        api_params = {"inst_type": 1, "order_id": exchange_order_id, "cancel_ord_type": 1}
        try:
            await self._api_put(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL, data=api_params, is_auth_required=True)
        except Exception:
            # ignore any errors from cancel in IOC simulation path
            return

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

        await self._check_kill_switch(trading_pair=trading_pair, trade_type=trade_type,
                                      order_type=order_type, amount=amount, price=price)

        trace_id = self._trace_id_for(order_id)
        self._log_event(
            "INFO", "order.submit.intent",
            coid=order_id,  # client order id
            tp=trading_pair,
            side=trade_type.name,
            otype=order_type.name,
            amount=str(amount),
            price=str(price)
        )

        simulate_ioc = False  # track whether we need to cancel remainder

        # --- IOC taker-limit transform ---
        price_set_by_ioc = False
        if self._enable_ioc_cap and order_type is OrderType.MARKET and self._ioc_slippage_bps > 0:
            # compute cap price from best-of-book
            best_bid, best_ask = self._best_bbo(trading_pair)
            if best_bid is None or best_ask is None:
                # fallback to last traded price if book not ready
                last = Decimal(str(await self._get_last_traded_price(trading_pair)))
                best_bid = best_bid or last
                best_ask = best_ask or last

            bps = Decimal(self._ioc_slippage_bps) / Decimal(10000)
            if trade_type is TradeType.BUY:
                cap_price = best_ask * (Decimal(1) + bps)
            else:
                cap_price = best_bid * (Decimal(1) - bps)

            # switch to LIMIT taker at capped price
            order_type = OrderType.LIMIT
            type_int = CONSTANTS.ORDER_TYPE[order_type]
            price_str = f"{cap_price:f}"
            api_params["order_type"] = type_int
            api_params["price"] = price_str
            price_set_by_ioc = True
            # market-specific param must be removed
            api_params.pop("trade_ccy", None)

            # try native IOC if enabled; otherwise simulate
            if self._ioc_use_tif and hasattr(CONSTANTS, "TIME_IN_FORCE_IOC"):
                api_params["time_in_force"] = CONSTANTS.TIME_IN_FORCE_IOC  # if the API accepts this field
            else:
                simulate_ioc = True
        # --- end IOC transform ---

        # ensure original MARKET path applies configured semantics
        # Only set price for LIMIT if we didn't already set it (e.g., IOC-cap transform)
        if (order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER) and "price" not in api_params:
            price_str = f"{price:f}"
            api_params["price"] = price_str
        if order_type is OrderType.MARKET:
            is_buy = (trade_type is TradeType.BUY)
            if self._market_trade_ccy == "quote":
                ref_px = await self._reference_price(trading_pair, is_buy=is_buy)
                quote_notional = (amount * ref_px)
                if self._jitter_on_size and self._jitter_bps > 0:
                    quote_notional = self._apply_symmetric_jitter(quote_notional, self._jitter_bps)
                api_params["trade_ccy"] = 1
                amount = quote_notional                     # <— promote to primary amount
                api_params["amount"] = f"{amount:f}"
            else:
                base_qty = amount
                if self._jitter_on_size and self._jitter_bps > 0:
                    base_qty = self._apply_symmetric_jitter(base_qty, self._jitter_bps)
                api_params["trade_ccy"] = 0
                amount = base_qty
                api_params["amount"] = f"{amount:f}"

        # Determine whether our MARKET path is sending quote notional
        is_quote_amount = (order_type is OrderType.MARKET and api_params.get("trade_ccy") == 1)

        # IMPORTANT: round to exchange steps
        amount, price = self._apply_exchange_rounding(
            trading_pair=trading_pair,
            amount=Decimal(str(amount)),
            price=Decimal(str(price)) if not price.is_nan() else price,
            order_type=order_type,
            is_quote_amount=is_quote_amount
        )

        # reflect the rounded values back into the payload
        api_params["amount"] = f"{amount:f}"
        # price reflection only if price exists/was set
        if "price" in api_params and not price.is_nan():
            api_params["price"] = f"{price:f}"

        # right before the POST:
        self._log_event(
            "DEBUG", "order.submit.params",
            coid=order_id, tp=trading_pair, side=trade_type.name, otype=order_type.name,
            params=self._redact(dict(api_params))
        )

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.PLACE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            o_id = str(order_result["data"]["order_id"])
            transact_time = self._time_synchronizer.time()

            self._log_event(
                "INFO", "order.submit.acked",
                coid=order_id, xo=o_id, tp=trading_pair, side=trade_type.name, otype=order_type.name
            )

            self._note_submit_success()

            # account for session notional using the final semantics being submitted
            try:
                # determine whether the final request is MARKET (quote/base) or LIMIT
                final_order_type = order_type
                # if you transformed MARKET -> LIMIT (IOC cap), final_order_type is already LIMIT here
                if final_order_type is OrderType.LIMIT and "price" in api_params:
                    ref_price = Decimal(str(api_params["price"]))
                    qn = (amount.copy_abs() * ref_price)
                else:
                    # MARKET path -> inspect trade_ccy (1=quote)
                    if api_params.get("trade_ccy") == 1:
                        qn = Decimal(str(api_params.get("amount")))
                    else:
                        # base qty * reference price
                        qn = await self._estimate_quote_notional(trading_pair, trade_type, final_order_type, amount, price)
                self._note_session_notional(qn)
                self._log_event("DEBUG", "kill.session_notional.update", session=str(self._session_notional))
            except Exception:
                pass

            # Simulated IOC: immediately cancel any remainder
            if simulate_ioc:
                # Apply delay jitter if enabled
                delay_ms = self._ioc_cancel_delay_ms
                if self._jitter_on_delay_ms > 0:
                    import random
                    jitter = random.uniform(-self._jitter_on_delay_ms, self._jitter_on_delay_ms)
                    delay_ms = max(0, delay_ms + jitter)  # ensure non-negative
                
                self._log_event("DEBUG", "order.ioc.cancel.scheduled", coid=order_id, xo=o_id, delay_ms=delay_ms)
                try:
                    await asyncio.sleep(delay_ms / 1000)
                    await self._ioc_cancel_remainder(o_id)
                    self._log_event("INFO", "order.ioc.cancel.attempted", coid=order_id, xo=o_id)
                except Exception:
                    pass
        except Exception as e:
            self._note_submit_error("place_order", e)
            self._log_event(
                "ERROR", "order.submit.error",
                coid=order_id, tp=trading_pair, side=trade_type.name, otype=order_type.name,
                err_type=type(e).__name__, err=str(e)
            )
            if isinstance(e, IOError):
                error_description = str(e)
                is_server_overloaded = ("status is 503" in error_description
                                        and "Unknown error, please check your request or try again later." in error_description)
                if is_server_overloaded:
                    o_id = "UNKNOWN"
                    transact_time = self._time_synchronizer.time()
                else:
                    raise
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
                 order_data = event_message["data"]
                 if order_data.get("state") is not None:
                  tracked_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(str(order_data["order_id"]))
                  if tracked_order is not None:
                    order_data = event_message["data"]
                    order_update = OrderUpdate(
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=order_data["updated_at"] if order_data["updated_at"] else order_data["created_at"],
                        new_state=CONSTANTS.ORDER_STATE[order_data["state"]],
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(order_data["order_id"]),
                    )
                    self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "tradingV2.assets" and event_message["account_type"] == 1:
                    asset_name = event_message["name"]
                    balance_entry = event_message["data"]
                    free_balance = Decimal(balance_entry["balance"])
                    self._account_available_balances[asset_name] = free_balance

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

             if all_fills:
                 self._log_event("DEBUG", "order.backfill.details", xo=order.exchange_order_id, tp=order.trading_pair, fills_len=len(all_fills))
                 for trade in all_fills:
                     exchange_order_id = str(order_data["order_id"])
                     trade_id = str(trade["bill_id"])
                     price = Decimal(trade["price"])
                     fill_base = Decimal(trade["filled_amount"])
                     fill_quote = fill_base * price
                     fee_amt = Decimal(trade["fee"])
                     fee_token = order_data["coin"]
                     
                     self._log_event(
                         "INFO", "order.fill",
                         coid=order.client_order_id,
                         xo=order.exchange_order_id,
                         tp=order.trading_pair,
                         trade_id=trade_id,
                         px=str(price),
                         base=str(fill_base),
                         quote=str(fill_quote),
                         fee_token=fee_token,
                         fee=str(fee_amt)
                     )
                     
                     fee = TradeFeeBase.new_spot_fee(
                         fee_schema=self.trade_fee_schema(),
                         trade_type=order.trade_type,
                         percent_token=order_data["coin"],
                         flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=order_data["coin"])]
                     )
                     trade_update = TradeUpdate(
                         trade_id=trade_id,
                         client_order_id=order.client_order_id,
                         exchange_order_id=exchange_order_id,
                         trading_pair=trading_pair,
                         fee=fee,
                         fill_base_amount=fill_base,
                         fill_quote_amount=fill_quote,
                         fill_price=price,
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
