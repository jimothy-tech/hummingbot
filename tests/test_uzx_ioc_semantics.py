import asyncio
from decimal import Decimal
from types import SimpleNamespace, MethodType
import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.connector.exchange.uzx.uzx_exchange import UzxExchange
from hummingbot.connector.exchange.uzx import uzx_constants as CONSTANTS


class FakeRule:
    min_price_increment = Decimal("0.10")
    min_base_amount_increment = Decimal("0.001")
    min_quote_amount_increment = Decimal("0.01")


class FakeOB:
    def __init__(self, bid, ask):
        self.best_bid_price = Decimal(str(bid))
        self.best_ask_price = Decimal(str(ask))


class _NoopAsyncLock:
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): return False


@pytest.fixture
def ex_base():
    """Bare connector instance with just enough state for _place_order() path."""
    e = UzxExchange.__new__(UzxExchange)

    # minimal market data + rules
    e._order_book_tracker = SimpleNamespace(order_books={"BTC-USDT": FakeOB(100, 101)})
    e._trading_rules = {"BTC-USDT": FakeRule()}
    e._time_synchronizer = SimpleNamespace(time=lambda: 0.0)

    # async lock + prefilled map expected by ExchangeBase.cpython
    e._mapping_initialization_lock = _NoopAsyncLock()
    e._trading_pair_symbol_map = {"BTC-USDT": "BTC-USDT"}

    # optional: bypass symbol mapping entirely (if base method still runs, our map+lock handles it)
    async def _sym(self, trading_pair: str, **kwargs) -> str:
        return "BTC-USDT"
    e.exchange_symbol_associated_to_pair = MethodType(_sym, e)

    # logging no-ops
    e.logger = lambda: SimpleNamespace(
        info=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        setLevel=lambda *a, **k: None,
    )

    # defaults that tests override when needed
    e._enable_rounding = False          # deterministic math
    e._market_trade_ccy = "quote"
    e._market_ref_price = "bbo"

    # structured log fields
    e._log_json = False
    e._run_id = "testrun"
    e._order_traces = {}

    # safety/guard knob defaults so __init__ isn't required
    e._enable_price_band = False
    e._price_band_min = Decimal("0")
    e._price_band_max = Decimal("1e10")
    e._jitter_on_size = False
    e._jitter_bps = 0
    e._jitter_on_delay_ms = 0
    
    # IOC transform defaults
    e._enable_ioc_cap = False
    e._ioc_slippage_bps = 0
    e._ioc_use_tif = False
    e._ioc_cancel_delay_ms = 120

    # disable kill-switch for this test file (covered by safety tests)
    e._enable_kill_switch = False
    e._trading_paused = False
    e._consec_errors = 0
    e._max_consec_errors = 5
    e._order_tracker = SimpleNamespace(active_orders={})
    e._max_open_orders = 100

    async def _last(_): return Decimal("99")
    e._get_last_traded_price = _last
    return e


# ------------------------- IOC transform tests -------------------------

@pytest.mark.asyncio
async def test_ioc_transform_native_tif(ex_base):
    e = ex_base

    # Enable IOC-cap with 25 bps; ask=101 => cap = 101 * 1.0025 = 101.2525
    e._enable_ioc_cap = True
    e._ioc_slippage_bps = 25
    e._ioc_use_tif = True
    e._ioc_cancel_delay_ms = 5

    # Ensure native IOC TIF string exists
    setattr(CONSTANTS, "TIME_IN_FORCE_IOC", "IOC")

    captured = {}

    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {})
        return {"data": {"order_id": "123"}}

    e._api_post = _fake_post

    await e._place_order(
        order_id="coid1",
        trading_pair="BTC-USDT",
        amount=Decimal("1.0"),
        trade_type=TradeType.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("NaN"),
    )

    from decimal import Decimal as D
    assert "price" in captured and D(captured["price"]) == D("101.2525")
    assert captured.get("time_in_force") == CONSTANTS.TIME_IN_FORCE_IOC
    assert captured["order_type"] == CONSTANTS.ORDER_TYPE[OrderType.LIMIT]


@pytest.mark.asyncio
async def test_ioc_transform_simulated_cancel_called(ex_base):
    e = ex_base

    # Enable IOC-cap but force simulated IOC (no TIF)
    e._enable_ioc_cap = True
    e._ioc_slippage_bps = 25
    e._ioc_use_tif = False
    e._ioc_cancel_delay_ms = 5

    called = {"cancel": False}

    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        return {"data": {"order_id": "456"}}

    async def _fake_put(*args, **kwargs):
        called["cancel"] = True
        return {"code": 0}

    e._api_post = _fake_post
    e._api_put = _fake_put

    await e._place_order(
        order_id="coid2",
        trading_pair="BTC-USDT",
        amount=Decimal("1.0"),
        trade_type=TradeType.SELL,
        order_type=OrderType.MARKET,
        price=Decimal("NaN"),
    )

    # give the small cancel delay time to fire
    await asyncio.sleep(0.03)
    assert called["cancel"] is True


# ----------------------- MARKET amount semantics -----------------------

@pytest.mark.asyncio
async def test_market_semantics_quote_notional(ex_base):
    e = ex_base
    e._enable_ioc_cap = False  # keep as true MARKET
    e._market_trade_ccy = "quote"
    e._market_ref_price = "bbo"  # BUY uses ask=101

    captured = {}

    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {})
        return {"data": {"order_id": "789"}}

    e._api_post = _fake_post

    await e._place_order(
        order_id="coid3",
        trading_pair="BTC-USDT",
        amount=Decimal("0.50"),              # base qty
        trade_type=TradeType.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("NaN"),
    )

    from decimal import Decimal as D
    # Expect quote notional = 0.50 * 101 = 50.5 and trade_ccy=1
    assert captured.get("trade_ccy") == 1
    assert D(captured.get("amount")) == D("50.5")


@pytest.mark.asyncio
async def test_market_semantics_base_qty(ex_base):
    e = ex_base
    e._enable_ioc_cap = False  # keep as true MARKET
    e._market_trade_ccy = "base"

    captured = {}

    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {})
        return {"data": {"order_id": "101"}}

    e._api_post = _fake_post

    await e._place_order(
        order_id="coid4",
        trading_pair="BTC-USDT",
        amount=Decimal("0.50"),
        trade_type=TradeType.SELL,
        order_type=OrderType.MARKET,
        price=Decimal("NaN"),
    )

    from decimal import Decimal as D
    # Expect base amount to be passed through and trade_ccy=0
    assert captured.get("trade_ccy") == 0
    assert D(captured.get("amount")) == D("0.50")
