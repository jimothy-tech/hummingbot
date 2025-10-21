import asyncio
from types import SimpleNamespace
from decimal import Decimal
import os
import pytest

# Core types used by the connector
from hummingbot.core.data_type.common import OrderType, TradeType
# The connector under test
from hummingbot.connector.exchange.uzx.uzx_exchange import UzxExchange

# ---------- Test stubs ----------

class FakeRule:
    # mimic a Hummingbot TradingRule object
    min_price_increment = Decimal("0.10")
    min_base_amount_increment = Decimal("0.001")
    # not all venues have quote step; include to exercise code path
    min_quote_amount_increment = Decimal("0.01")

class FakeOB:
    # minimal order book view for _best_bbo tests
    def __init__(self, bid, ask):
        self.best_bid_price = Decimal(str(bid))
        self.best_ask_price = Decimal(str(ask))

# ---------- Fixtures ----------

@pytest.fixture
def ex():
    from types import SimpleNamespace
    from decimal import Decimal

    e = UzxExchange.__new__(UzxExchange)

    # minimal attrs used by helpers
    e._trading_rules = {"BTC-USDT": FakeRule()}
    e._order_book_tracker = SimpleNamespace(order_books={"BTC-USDT": FakeOB(100, 101)})

    # --- rounding / ref-price defaults ---
    e._enable_rounding = True
    e._round_amount_mode = "floor"
    e._round_price_mode = "floor"
    e._market_ref_price = "bbo"
    e._market_trade_ccy = "quote"

    # --- kill switch & breakers (stub values mirroring __init__) ---
    e._enable_kill_switch = True
    e._kill_switch_file = "/nonexistent"
    e._trading_paused = False
    e._max_consec_errors = 5
    e._consec_errors = 0
    e._max_open_orders = 50
    e._max_session_notional = Decimal("0")
    e._session_notional = Decimal("0")
    e._order_tracker = SimpleNamespace(active_orders={})  # used for open order count

    # fallback last price
    async def _fake_last(tp): return Decimal("99")
    e._get_last_traded_price = _fake_last

    # no-op logger
    e.logger = lambda: SimpleNamespace(
        info=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None
    )

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
    
    return e

# ---------- Tests ----------

def test_round_units_floor_and_nearest(ex):
    # expose helper
    ru = ex._round_units
    assert ru(Decimal("1.2345"), Decimal("0.01"), "floor") == Decimal("1.23")
    assert ru(Decimal("1.2345"), Decimal("0.01"), "nearest") == Decimal("1.23")  # HALF_UP rounds 1.2345 -> 1.23
    assert ru(Decimal("1.2351"), Decimal("0.01"), "nearest") == Decimal("1.24")

def test_apply_exchange_rounding_limit(ex):
    amt, px = ex._apply_exchange_rounding(
        trading_pair="BTC-USDT",
        amount=Decimal("0.123456"),
        price=Decimal("100.1234"),
        order_type=OrderType.LIMIT,
        is_quote_amount=False,
    )
    # price floors to 100.1, amount floors to 0.123
    assert amt == Decimal("0.123")
    assert px  == Decimal("100.1")

@pytest.mark.asyncio
async def test_reference_price_bbo_and_last(ex):
    # bbo mode: BUY uses ask, SELL uses bid
    ex._market_ref_price = "bbo"
    px_buy = await ex._reference_price("BTC-USDT", is_buy=True)
    px_sell = await ex._reference_price("BTC-USDT", is_buy=False)
    assert px_buy == Decimal("101")
    assert px_sell == Decimal("100")

    # last mode ignores bbo and uses last price
    ex._market_ref_price = "last"
    px_any = await ex._reference_price("BTC-USDT", is_buy=True)
    assert px_any == Decimal("99")

def test_trace_id_stable(ex):
    t1 = ex._trace_id_for("coid-123")
    t2 = ex._trace_id_for("coid-123")
    t3 = ex._trace_id_for("coid-456")
    assert t1 == t2
    assert t1 != t3
    assert len(t1) == 8  # short hex

@pytest.mark.asyncio
async def test_kill_switch_manual_file(tmp_path, ex):
    # enable kill switch and point to a temp file
    ex._enable_kill_switch = True
    kill_file = tmp_path / ".kill"
    ex._kill_switch_file = str(kill_file)

    # no file yet -> no error
    await ex._check_kill_switch("BTC-USDT", TradeType.BUY, OrderType.MARKET, Decimal("0.01"), Decimal("NaN"))

    # create kill file -> next call should raise
    kill_file.write_text("pause")
    with pytest.raises(IOError):
        await ex._check_kill_switch("BTC-USDT", TradeType.BUY, OrderType.MARKET, Decimal("0.01"), Decimal("NaN"))
