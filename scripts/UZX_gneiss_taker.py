import logging
import random
from decimal import Decimal

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy.strategy_py_base import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)


class SimpleOrder(ScriptStrategyBase):
    """
    This script places orders on the P2PB2B exchange for the GNEISS token.
    """

    # Key Parameters
    exchange = "uzx"
    base = "GNEISS"
    quote = "USDT"

    # Other Parameters
    markets = {
        exchange: {f"{base}-{quote}"}
    }

    trade_side = True  # True for buy, False for sell

    ONE_HOUR = 3600  # 1 hour in seconds
    TRADE_AMOUNT_USD_MINIMUM = 1250  # 1250 USDT worth of trades per hour
    TRADE_AMOUNT_USD_MAXIMUM = 1500  # 1500 USDT worth of trades per hour
    TRADE_FREQUENCY_MINIMUM = 60  # 60 trades per hour
    BUY_GUARDRAIL = 0.095  # Don't buy if the price is is more than 2 dollars
    SELL_GUARDRAIL = 0.07  # Don't sell if the price is is less than 0.70 dollars
    time_tracker = 0  # Tracks how much time has passed in seconds - limited to one hour at a time
    trade_times = []  # Determination of when within the hour to trade

    bullish_value = 0.62  # 62% bullish sentiment

    # Add tick control
    TICK_INTERVAL = 5  # Check every 5 seconds instead of every 1 second
    last_tick_time = 0

    def __init__(self, connectors: dict[str, ConnectorBase]):
        super().__init__(connectors)

        self.init_future_trades()

    def init_future_trades(self):
        # create a bucket range to increment by
        bucket_range = self.ONE_HOUR // self.TRADE_FREQUENCY_MINIMUM
        for i in range(self.TRADE_FREQUENCY_MINIMUM):
            # append a random trade time within the bucket range
            self.trade_times.append(
                int(random.randint(i * bucket_range, (i + 1) * bucket_range))
            )
        self.trade_times = sorted(self.trade_times)
        self.logger().info(f"Trade times: {self.trade_times}")

    def pick_trade_side(self):
        """
        Picks a trade side based on the bullish value
        """
        return random.random() < self.bullish_value

    def should_trade(self):
        """
        Returns True if the current time tracker is within 1 second of any of the trade times
        """
        return any(abs(self.time_tracker - t) <= self.TICK_INTERVAL for t in self.trade_times)

    def place_order(self, amount, price):
        # places order
        if self.trade_side:
            if price < self.BUY_GUARDRAIL:
                self.buy(
                    connector_name=self.exchange,
                    trading_pair=f"{self.base}-{self.quote}",
                    amount=amount,
                    order_type=OrderType.MARKET,
                    price=price
                )
            else:
                self.logger().info(f"Buy guardrail not met for {self.base} at price {price}. Skipping trade.")

        else:
            # SELL
            if price > self.SELL_GUARDRAIL:
                self.sell(
                    connector_name=self.exchange,
                    trading_pair=f"{self.base}-{self.quote}",
                    amount=amount,
                    order_type=OrderType.MARKET,
                    price=price
                )
            else:
                self.logger().info(f"Sell guardrail not met for {self.base} at price {price}. Skipping trade.")
        # remove the trade time that was just used
        self.trade_times.pop(0)

    def on_tick(self):
        # Rate limit the ticks
        current_time = self.current_timestamp
        if current_time - self.last_tick_time < self.TICK_INTERVAL:
            return
        self.last_tick_time = current_time

        if self.time_tracker >= self.ONE_HOUR:
            self.time_tracker = 0
            self.init_future_trades()

        if self.should_trade():
            order_amount_usd = Decimal(
                round(
                    random.uniform(
                        self.TRADE_AMOUNT_USD_MINIMUM / self.TRADE_FREQUENCY_MINIMUM,
                        self.TRADE_AMOUNT_USD_MAXIMUM / self.TRADE_FREQUENCY_MINIMUM
                    ), 2)
            )
            # # Try RateOracle first, fallback to connector price
            # conversion_rate = RateOracle.get_instance().get_pair_rate(f"{self.base}-USDT")
            # if conversion_rate is None:
            # Fallback to current market price
            conversion_rate = self.connectors[self.exchange].get_mid_price(f"{self.base}-{self.quote}")
            self.logger().warning(f"RateOracle returned None for {self.base}-USDT, using connector price: {conversion_rate}")

            amount = order_amount_usd / conversion_rate

            self.logger().info(f"Placing order: {amount} {self.base} at rate {conversion_rate} {self.quote} for {order_amount_usd} {self.quote}")
            self.place_order(amount, conversion_rate)
            self.trade_side = self.pick_trade_side()
            self.logger().info(f"Trade side flipped to: {'buy' if self.trade_side else 'sell'}")
        self.time_tracker += self.TICK_INTERVAL  # Increment by tick interval

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"{event.trade_type.name} {event.amount} of {event.trading_pair} {self.exchange} at {event.price}")
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        msg = (f"Order {event.order_id} to buy {event.base_asset_amount} of {event.base_asset} is completed.")
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        msg = (f"Order {event.order_id} to sell {event.base_asset_amount} of {event.base_asset} is completed.")
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        msg = (f"Created BUY order {event.order_id}")
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        msg = (f"Created SELL order {event.order_id}")
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)
