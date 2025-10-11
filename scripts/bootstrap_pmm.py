import asyncio
import json
import logging
import os
import random
from decimal import Decimal
from typing import Dict, List, Literal

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
    exchange: str = Field("ascend_ex")
    trading_pair: str = Field("GNEISS-USDT")
    order_amount: Decimal = Field(5)
    bid_spread: Decimal = Field(0.005)
    ask_spread: Decimal = Field(0.005)
    order_evaluation_time: float = Field(15.0)
    force_evaluation_cycle: bool = Field(False)
    price_type: str = Field("mid")
    order_spread_tolerance: Decimal = Field(0.001)
    levels: int = Field(3)  # Number of levels to place orders on each side of the market
    randomize_order_amount: bool = Field(False)
    replace_all_every_interval: bool = Field(False)  # Turn on replace all orders ever n seconds
    replace_on_fill: bool = Field(False)  # Turn on replace orders on fill - this will immediately replace the order on complete fill
    replacement_interval : float = Field(10.0)  # Amount in seconds to have all orders be replaced - only matters if replace_all_every_interval is True
    replacement_delay : float = Field(0.1)  # Amount of time to wait between replacing each order
    order_placement_delay : float = Field(0.1)  # Amount of time to wait between placing each order
    random_order_floor: Decimal = Field(5)  # in quote currency (USDT)
    random_order_ceiling: Decimal = Field(10)  # in quote currency (USDT)
    price_ceiling: Decimal = Field(2.0)
    price_floor: Decimal = Field(0.5)
    min_order_levels: int = Field(1)  # Minimum number of orders allowed on one side of the market before replacing all orders


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

    @classmethod
    def init_markets(cls, config: BootstrapPMMConfig):
        cls.markets = {config.exchange: {config.trading_pair}}
        cls.price_source = PriceType.LastTrade if config.price_type == "last" else PriceType.MidPrice

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BootstrapPMMConfig):
        super().__init__(connectors)
        self.config = config
        self.logger().info(f"Loaded config: {self.config}")

        self.first_order_placed = False
        self._order_lvl_tracker = self.OrderLevelTracker(self)
        self.last_ref_price : None | Decimal = self.load_last_ref_price()  # Used to track the last reference price

        # Lock used to prevent multiple order replacements from happening at the same time
        self.order_replacement_lock = asyncio.Lock()

    def load_last_ref_price(self):
        """
        Load the last reference price from the file.
        """
        try:
            self.logger().info(f"Loading last reference price from file")
            with open("last_ref_price.json", "r") as f:
                last_ref_price = json.load(f)[self.config.exchange]
            self.logger().info(f"Loaded last reference price: {last_ref_price}")
            return Decimal(last_ref_price)
        except Exception as e:
            self.logger().warning(f"Error loading last reference price: {e}")
            return None

    def is_order_out_of_desired_price(self, order: LimitOrder) -> bool:
        return order.price < self.config.price_floor or order.price > self.config.price_ceiling

    def place_initial_orders(self):
        proposal = self.create_initial_proposal()
        self.logger().info(f"Created initial proposal: {proposal}")
        orders_to_place = self.adjust_proposal_to_budget(proposal)
        self.logger().info(f"Adjusted proposal to budget: {orders_to_place}")
        self._place_orders_with_delay(orders_to_place)
        self.logger().info(f"Placed initial orders!")
        self.first_order_placed = True

    def on_tick(self):
        # Create the initial proposal and place the orders
        if not self.first_order_placed:
            self.eval_target_timestamp = self.current_timestamp + self.config.order_evaluation_time
            self.repl_target_timestamp = self.current_timestamp + self.config.replacement_interval
            self.place_initial_orders()

        # On each tick, we should evaluate the orders and replace the orders if necessary. Do not run if we are replacing all orders every increment unless force_evaluation_cycle is True.
        if self.eval_target_timestamp <= self.current_timestamp and (self.config.force_evaluation_cycle or not self.config.replace_all_every_interval):
            orders_to_replace : List[LimitOrder] = self.evaluate_orders()
            if orders_to_replace:
                self.logger().info(f"Replacing {len(orders_to_replace)} orders")
                self.logger().info(f"Orders to replace: {orders_to_replace}")
                self._replace_orders_with_delay(orders_to_replace)
            self.eval_target_timestamp =  self.current_timestamp + self.config.order_evaluation_time

        # On each tick, we should check if replacement_interval has passed
        if self.repl_target_timestamp <= self.current_timestamp and self.config.replace_all_every_interval:
            # Replace all orders if so
            self.logger().info("Replacing all orders!")
            self.replace_all_orders()
            self.repl_target_timestamp = self.current_timestamp + self.config.replacement_interval

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
        if ref_price is None or ref_price.is_nan():  # If the price is not available, use the fallback
            ref_price = self.calculate_mid_price_fallback()

        self.last_ref_price = ref_price

        if order_side == TradeType.BUY:
            return ref_price * Decimal(
                1 + self.config.bid_spread * Decimal(level / self.config.levels)  # Level will be negative for buy orders, so we need to add the spread
            )
        else:  # SELL
            return ref_price * Decimal(
                1 + self.config.ask_spread * Decimal(level / self.config.levels)
            )

    def calculate_mid_price_fallback(self) -> Decimal:
        """
        Calculate the mid price fallback. This is done by getting the average of the price floor and price ceiling
        or returning the last reference price if it is available.
        """
        if self.last_ref_price is not None and not self.last_ref_price.is_nan():
            return self.last_ref_price

        return Decimal((self.config.price_floor + self.config.price_ceiling) / 2)

    def create_initial_proposal(self) -> List[OrderCandidate]:
        """
        Create a proposal for the bot to place orders. Here, n amount of buy and sell orders are placed
        based on the config.levels attribute. For example, if levels is 3, then 3 buy and 3 sell orders are placed.
        The price of the orders is calculated based on the config.bid_spread and config.ask_spread attributes.
        Each proceeding level of order is placed at an evenly spaced amounts from the ref_price to the spread price.
        """
        proposal = []
        for level in range(1, self.config.levels + 1):
            buy_order = self.create_new_candidate(TradeType.BUY, -level)
            sell_order = self.create_new_candidate(TradeType.SELL, level)
            proposal.extend([buy_order, sell_order])

        return proposal

    def create_new_candidate(self, order_side: Literal[TradeType.BUY, TradeType.SELL], level: int) -> OrderCandidate:
        """
        Create a new order candidate. The candidates level will be added as an attribute to the candidate
        and propagate to order placement.
        Args:
            order_side: Literal[TradeType.BUY, TradeType.SELL]: The side of the order.
            level: int: The level of the order. A negative level will indicate a buy order and a positive level will indicate a sell order.

        Returns:
            OrderCandidate: A new order candidate.
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

    def _replace_missing_order_levels(self):
        """
        Replace missing order levels.
        """
        asyncio.create_task(self.replace_missing_orders())

    async def replace_missing_order_levels(self, order_levels: List[int]) -> None:
        """
        Replace missing orders. We check if we have all orders in the level tracker. If we do, we replace all orders.
        Any orders that are missing will be placed to ensure equal number of buy and sell orders.
        """
        async with self.order_replacement_lock:
            try:
                for level in order_levels:
                    candidate = self.create_new_candidate(TradeType.BUY if level < 0 else TradeType.SELL, level)
                    adj_candidate = self.adjust_candidate_to_budget(candidate)
                    self.place_order(connector_name=self.config.exchange, order=adj_candidate)
            except Exception as e:
                self.logger().error(f"Error replacing missing orders: {e}")
                raise

    def replace_all_orders(self) -> None:
        """
        Replace all orders. We check if we have all orders in the level tracker. If we do, we replace all orders.
        Any orders that are missing will be placed ti ensure equal number of buy and sell orders.
        """
        active_orders = self.get_active_orders(connector_name=self.config.exchange)
        missing_order_levels = self._order_lvl_tracker.get_missing_order_levels()

        # Replace all active orders
        self._replace_orders_with_delay(active_orders)

        # Place missing orders
        self._replace_missing_order_levels(missing_order_levels)

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
        async with self.order_replacement_lock:
            try:
                for order in proposal:
                    self.replace_order(order)
                    await asyncio.sleep(self.config.replacement_delay)
            except Exception as e:
                self.logger().error(f"Error replacing orders: {e}")
                raise

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
        self._order_lvl_tracker.remove_order(order_id)

        candidate = self.create_new_candidate(order_side, level)
        adj_candidate = self.adjust_candidate_to_budget(candidate)
        self.place_order(connector_name=self.config.exchange, order=adj_candidate)

    def _place_orders_with_delay(self, proposal: List[OrderCandidate]) -> None:
        """
        Place the orders in the proposal with a delay.
        """
        asyncio.create_task(self.place_orders(proposal))

    async def place_orders(self, proposal: List[OrderCandidate]) -> None:
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
                # If there is an issue placing the order, stop placing orders
                break
            # Delay between orders
            await asyncio.sleep(self.config.order_placement_delay)

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
            # HummingbotApplication.main_application().stop()

            # Set the price to the price floor or price ceiling
            order.price = self.config.price_floor if order.order_side == TradeType.BUY else self.config.price_ceiling
            self.logger().info(f"Set order price to {order.price}")

        if order.is_zero_order:
            self.logger().warning(f"Order is a zero order. Check balances. Insufficient funds likely.")
        elif order.resized:
            self.logger().warning(f"Order has been resized to fit budget. Check balances. Insufficient funds likely.")

        if order.order_side == TradeType.SELL:
            order_id = self.sell(connector_name=connector_name, trading_pair=order.trading_pair,
                                 amount=order.amount, order_type=order.order_type, price=order.price)
            self._order_lvl_tracker.add_order(order_id, order.level)
        elif order.order_side == TradeType.BUY:
            order_id = self.buy(connector_name=connector_name, trading_pair=order.trading_pair,
                                amount=order.amount, order_type=order.order_type, price=order.price)
            self._order_lvl_tracker.add_order(order_id, order.level)

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
        if self.config.replace_on_fill:
            self.replace_order(event)
        else:
            self._order_lvl_tracker.remove_order(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """
        Called ONLY when a SELL order is COMPLETELY filled.
        """
        msg = f"SELL order COMPLETELY filled: {event.base_asset_amount} {event.base_asset}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        if self.config.replace_on_fill:
            self.replace_order(event)
        else:
            self._order_lvl_tracker.remove_order(event.order_id)

    async def on_stop(self):
        """
        Called when the strategy is stopped.
        """
        await super().on_stop()

        # Record last reference price
        with open("last_ref_price.json", "w") as f:
            json.dump({self.config.exchange: float(self.last_ref_price)}, f)

    class OrderLevelTracker(dict):
        """
        Track the level of each order. Extends the dict class to add observer pattern functionality to notify
        the parent class when the number of orders on one side of the market is less than the min_order_levels attribute.
        """

        def __init__(self, parent):
            self.parent = parent
            super().__init__()

        def add_order(self, order_id: str, level: int):
            """
            Add an order to the level tracker.
            """
            self[order_id] = level

        def remove_order(self, order_id: str):
            """
            Remove an order from the level tracker.
            """
            del self[order_id]
            if len(self) < self.parent.config.min_order_levels:
                self.parent._replace_missing_order_levels(self.get_missing_order_levels())

        def get_order_level(self, order_id: str) -> int:
            """
            Get the level of an order.
            """
            return self[order_id]

        def get_missing_order_levels(self) -> List[int]:
            """
            Get the missing order levels.
            """
            levels = self.parent.config.levels
            missing_orders = []
            for level in range(1, levels + 1):
                if level not in self.values():
                    missing_orders.append(level)
            return missing_orders
