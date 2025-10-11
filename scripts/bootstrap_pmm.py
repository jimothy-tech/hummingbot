import asyncio
import logging
import os
import random
from decimal import Decimal
from typing import Dict, List

import yaml
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCompletedEvent, OrderFilledEvent, SellOrderCompletedEvent
from hummingbot.logger.email_warning import send_email_critical_issue
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class BootstrapPMMConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    exchange: str = Field("binance_paper_trade")
    trading_pair: str = Field("ETH-USDT")
    order_amount: Decimal = Field(0.01)
    bid_spread: Decimal = Field(0.001)
    ask_spread: Decimal = Field(0.001)
    order_evaluation_time: float = Field(15.0)
    force_evaluation_cycle: bool = Field(False)
    price_type: str = Field("mid")
    order_spread_tolerance: Decimal = Field(0.001)
    levels: int = Field(3)
    randomize_order_amount: bool = Field(False)
    replace_all_every_increment: bool = Field(False)  # Turn on replace all orders ever n seconds
    replacement_increment : float = Field(10.0)  # Amount in seconds to have all orders be replaced.
    replacement_delay : float = Field(1.5)  # Amount of time to wait between replacing each order
    random_order_floor: Decimal = Field(1)  # in quote currency (USDT)
    random_order_ceiling: Decimal = Field(2)  # in quote currency (USDT)
    price_ceiling: Decimal = Field(2.0)
    price_floor: Decimal = Field(0.5)


class BootstrapPMM(ScriptStrategyBase):
    """
    Created by Timothy Newton for Gneiss
    Description:
    The bot will place n amount of buy and sell orders based on the config.levels attribute.
    The bot will only place new orders if an order is filled OR the order evaluation shows that the mid price
    has moved by more than the config.order_spread_tolerance attribute. An evaluation occurs every order_evaluation_time seconds.
    The bot will never cancel all orders at once, but will cancel orders one by one, replacing them with new orders.
    This ensures that there is always liquidity in the market.
    """

    price_source = PriceType.MidPrice
    _order_lvl_tracker = {}

    @classmethod
    def init_markets(cls, config: BootstrapPMMConfig):
        cls.markets = {config.exchange: {config.trading_pair}}
        cls.price_source = PriceType.LastTrade if config.price_type == "last" else PriceType.MidPrice

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BootstrapPMMConfig):
        super().__init__(connectors)
        self.config = config
        self.logger().info(f"Loaded config: {self.config}")

        self.first_order_placed = False

    def is_order_out_of_desired_price(self, order: LimitOrder) -> bool:
        return order.price < self.config.price_floor or order.price > self.config.price_ceiling

    def place_initial_orders(self):
        proposal = self.create_initial_proposal()
        self.logger().info(f"Created initial proposal: {proposal}")
        orders_to_place = self.adjust_proposal_to_budget(proposal)
        self.logger().info(f"Adjusted proposal to budget: {orders_to_place}")
        self.place_orders(orders_to_place)
        self.logger().info(f"Placed initial orders!")
        self.first_order_placed = True

    def on_tick(self):
        # Create the initial proposal and place the orders
        if not self.first_order_placed:
            self.eval_target_timestamp = self.current_timestamp + self.config.order_evaluation_time
            self.repl_target_timestamp = self.current_timestamp + self.config.replacement_increment
            self.place_initial_orders()

        # On each tick, we should evaluate the orders and replace the orders if necessary. Do not run if we are replacing all orders every increment unless force_evaluation_cycle is True.
        if self.eval_target_timestamp <= self.current_timestamp and (self.config.force_evaluation_cycle or not self.config.replace_all_every_increment):
            orders_to_replace : List[LimitOrder] = self.evaluate_orders()
            if orders_to_replace:
                self.logger().info(f"Replacing {len(orders_to_replace)} orders")
                self.logger().info(f"Orders to replace: {orders_to_replace}")
                self._replace_orders_with_delay(orders_to_replace)
            self.eval_target_timestamp =  self.current_timestamp + self.config.order_evaluation_time

        # On each tick, we should check if replacement_increment has passed
        if self.repl_target_timestamp <= self.current_timestamp and self.config.replace_all_every_increment:
            # Replace all orders if so
            self.logger().info("Replacing all orders!")
            self._replace_orders_with_delay(
                sorted(self.get_active_orders(  # Sort so buys are ascending and sells are descending by price
                    connector_name=self.config.exchange
                ), key=lambda o: o.price if o.is_buy else -o.price)
            )
            self.repl_target_timestamp = self.current_timestamp + self.config.replacement_increment

    def get_order_amount(self) -> Decimal:
        """
        Get the order amount. Uses the configured amount unless the randomize_order_amount attribute is True.
        If it is True, then a random amount between the random_order_floor and random_order_ceiling attributes is returned.
        """
        if self.config.randomize_order_amount:
            ref_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
            return Decimal(
                random.uniform(float(self.config.random_order_floor), float(self.config.random_order_ceiling)) / float(ref_price)
            )
        return Decimal(self.config.order_amount)

    def calculate_order_price(self, order_side: TradeType, level: int) -> Decimal:
        """
        Calculate the order price.
        """
        ref_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
        if order_side == TradeType.BUY:
            return ref_price * Decimal(
                1 - self.config.bid_spread * Decimal(level / self.config.levels)
            )
        else:  # SELL
            return ref_price * Decimal(
                1 + self.config.ask_spread * Decimal(level / self.config.levels)
            )

    def create_initial_proposal(self) -> List[OrderCandidate]:
        """
        Create a proposal for the bot to place orders. Here, n amount of buy and sell orders are placed
        based on the config.levels attribute. For example, if levels is 3, then 3 buy and 3 sell orders are placed.
        The price of the orders is calculated based on the config.bid_spread and config.ask_spread attributes.
        Each proceeding level of order is placed at an evenly spaced amounts from the ref_price to the spread price.
        """
        ref_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
        proposal = []
        for level in range(1, self.config.levels + 1):
            buy_order = self.create_new_candidate(TradeType.BUY, level)
            sell_order = self.create_new_candidate(TradeType.SELL, level)
            proposal.extend([buy_order, sell_order])

        return proposal

    def create_new_candidate(self, order_side: TradeType, level: int) -> OrderCandidate:
        """
        Create a new order candidate.
        """
        amount = self.get_order_amount()

        if order_side == TradeType.BUY:
            price = self.calculate_order_price(TradeType.BUY, level)
        else:  # SELL
            price = self.calculate_order_price(TradeType.SELL, level)

        candidate = OrderCandidate(trading_pair=self.config.trading_pair, is_maker=True, order_type=OrderType.LIMIT,
                                   order_side=order_side, amount=amount, price=price)
        candidate.level = level
        return candidate

    def adjust_proposal_to_budget(self, proposal: List[OrderCandidate]) -> List[OrderCandidate]:
        """
        Adjust the proposal to the budget. This is done by calling the budget checker on the connector.
        This adjusts the order amounts to fit the budget if the funds are insufficient.

        Args:
            proposal: List[OrderCandidate]: A list of orders to adjust.

        Returns:
            List[OrderCandidate]: An adjusted list of orders.
        """
        proposal_adjusted = self.connectors[self.config.exchange].budget_checker.adjust_candidates(proposal, all_or_none=False)
        return proposal_adjusted

    def adjust_candidate_to_budget(self, order: OrderCandidate) -> OrderCandidate:
        """
        Adjust the order amount to fit the budget.

        Args:
            order: OrderCandidate: The order to adjust.

        Returns:
            OrderCandidate: An adjusted order.
        """
        return self.connectors[self.config.exchange].budget_checker.adjust_candidate(order, all_or_none=False)

    def evaluate_orders(self) -> List[LimitOrder]:
        """
        Evaluate the current orders to see if the mid price has moved by more than the config.order_spread_tolerance attribute
        for each order.

        Returns:
            List[LimitOrder]: A list of orders that are beyond the config.order_spread_tolerance attribute.
        """
        orders = self.get_active_orders(connector_name=self.config.exchange)
        orders_to_replace = []
        price_ref = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
        for order in orders:
            if order.client_order_id not in self._order_lvl_tracker:
                self.logger().warning(f"Order {order.client_order_id} not found in level tracker. Skipping evaluation.")
                continue
            if order.is_buy:
                if order.price < price_ref * Decimal(1 - self.config.order_spread_tolerance):
                    orders_to_replace.append(order)
            else:  # SELL
                if order.price > price_ref * Decimal(1 + self.config.order_spread_tolerance):
                    orders_to_replace.append(order)
        return orders_to_replace

    def _replace_orders_with_delay(self, proposal: List[LimitOrder]) -> None:
        """
        Replace the orders in the proposal with new orders.
        """
        asyncio.create_task(self.replace_orders(proposal))

    async def replace_orders(self, proposal: List[LimitOrder]) -> None:
        """
        Replace the orders in the proposal with new orders.

        Args:
            proposal: List[LimitOrder]: A list of orders to replace.
        """
        for order in proposal:
            self.replace_order(order)
            # Delay between orders
            await asyncio.sleep(self.config.replacement_delay)

    def replace_order(self, order: LimitOrder | BuyOrderCompletedEvent | SellOrderCompletedEvent) -> None:
        """
        Replace the order with a new order.

        Args:
            order: LimitOrder | BuyOrderCompletedEvent | SellOrderCompletedEvent: The order to replace.
        """
        if isinstance(order, LimitOrder):
            order_side = TradeType.BUY if order.is_buy else TradeType.SELL
            order_id = order.client_order_id
            amount = order.quantity
            price = order.price
        else:  # BuyOrderCompletedEvent or SellOrderCompletedEvent
            order_side = TradeType.BUY if isinstance(order, BuyOrderCompletedEvent) else TradeType.SELL
            order_id = order.order_id
            amount = order.base_asset_amount
            price = order.quote_asset_amount / order.base_asset_amount

        if order_id not in self._order_lvl_tracker:
            self.logger().warning(f"Order {order_id} not found in level tracker. Skipping replacement.")
            return

        if order_side not in [TradeType.BUY, TradeType.SELL]:
            self.logger().warning(f"Order {order_id} has invalid side. Skipping replacement.")
            return

        if amount is None or amount == 0:
            self.logger().warning(f"Order {order_id} has invalid amount. Skipping replacement.")
            return

        if price is None or price == 0:
            self.logger().warning(f"Order {order_id} has invalid price. Skipping replacement.")
            return

        level = self._order_lvl_tracker[order_id]

        self.logger().info(f"Replacing LimitOrder(id={order_id}, side={order_side}, amount={amount}, price={price})")
        if isinstance(order, LimitOrder):  # Cancel only if the order hasn't been filled
            self.cancel(self.config.exchange, self.config.trading_pair, order_id)
        # Remove old order from level tracker
        del self._order_lvl_tracker[order_id]

        candidate = self.create_new_candidate(order_side, level)
        adj_candidate = self.adjust_candidate_to_budget(candidate)
        self.place_order(connector_name=self.config.exchange, order=adj_candidate)

    def place_orders(self, proposal: List[OrderCandidate]) -> None:
        """
        Place the orders in the proposal. This does not replace the orders, but simply places them.

        Args:
            proposal: List[OrderCandidate]: A list of orders to place.
        """
        if any(order.is_zero_order for order in proposal):
            self.logger().warning(f"Some orders are zero orders. Check balances. Insufficient funds likely.")
        elif any(order.resized for order in proposal):
            self.logger().warning(f"Some orders have been resized. Check balances. Insufficient funds likely.")

        for order in proposal:
            if not self.place_order(connector_name=self.config.exchange, order=order):
                break

    def place_order(self, connector_name: str, order: OrderCandidate) -> bool:
        """
        Place the order. This does not replace the order, but simply places it.

        Args:
            connector_name: str: The name of the connector to place the order on.
            order: OrderCandidate: The order to place.
        """
        self.logger().info(f"Placing order: OrderCandidate(side={order.order_side}, amount={order.amount}, price={order.price})")
        if self.is_order_out_of_desired_price(order):
            self.logger().warning(f"Order is out of desired price range, stopping market making.")
            # TODO: Uncomment this when our smtp server is set up
            # send_email_critical_issue(
            #     f"Bootstrap PMM - {self.config.exchange} - {self.config.trading_pair} - Out of desired price range",
            #     f"Order is out of desired price range, stopping market making.")
            HummingbotApplication.main_application().stop()
            return False
        if order.is_zero_order:
            self.logger().warning(f"Order is a zero order. Check balances. Insufficient funds likely.")
            return False
        elif order.resized:
            self.logger().warning(f"Order has been resized to fit budget. Check balances. Insufficient funds likely.")

        if order.order_side == TradeType.SELL:
            order_id = self.sell(connector_name=connector_name, trading_pair=order.trading_pair,
                                 amount=order.amount, order_type=order.order_type, price=order.price)
            self._order_lvl_tracker[order_id] = order.level
        elif order.order_side == TradeType.BUY:
            order_id = self.buy(connector_name=connector_name, trading_pair=order.trading_pair,
                                amount=order.amount, order_type=order.order_type, price=order.price)
            self._order_lvl_tracker[order_id] = order.level

        return True

    def cancel_all_orders(self):
        """
        Cancel all orders. This does not replace the orders, but simply cancels them.
        """
        for order in self.get_active_orders(connector_name=self.config.exchange):
            self.cancel(self.config.exchange, order.trading_pair, order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        """
        Handle the order filled event.
        """
        msg = (f"{event.trade_type.name} {round(event.amount, 2)} {event.trading_pair} {self.config.exchange} at {round(event.price, 2)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """
        Called ONLY when a BUY order is COMPLETELY filled.
        """
        msg = f"BUY order COMPLETELY filled: {event.base_asset_amount} {event.base_asset}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        # Replace order here - safe now!
        self.replace_order(event)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """
        Called ONLY when a SELL order is COMPLETELY filled.
        """
        msg = f"SELL order COMPLETELY filled: {event.base_asset_amount} {event.base_asset}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        # Replace order here - safe now!
        self.replace_order(event)
