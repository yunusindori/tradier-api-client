from typing import Union

from tradier_api_client.rest import RestClient
from tradier_api_client.rest.models.orders_fixed import Order, OrderLeg


EQUITY_SIDES = frozenset({"buy", "sell", "sell_short", "buy_to_cover"})
OPTION_SIDES = frozenset({"buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close"})
ORDER_TYPES = frozenset({"market", "limit", "stop", "stop_limit"})
MULTILEG_ORDER_TYPES = frozenset({"market", "debit", "credit", "even"})
STANDARD_DURATIONS = frozenset({"day", "gtc"})
EXTENDED_DURATIONS = frozenset({"pre", "post"})


def _require_non_empty_str(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _require_positive_number(name: str, value: Union[int, float]) -> Union[int, float]:
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _validate_choice(name: str, value: str, choices: frozenset[str]) -> str:
    normalized = _require_non_empty_str(name, value)
    if normalized not in choices:
        allowed = "|".join(sorted(choices))
        raise ValueError(f"{name} must be one of: {allowed}")
    return normalized


def _validate_equity_side(side: str) -> str:
    return _validate_choice("side", side, EQUITY_SIDES)


def _validate_option_side(side: str) -> str:
    return _validate_choice("side", side, OPTION_SIDES)


def _validate_order_type(order_type: str) -> str:
    return _validate_choice("order_type", order_type, ORDER_TYPES)


def _validate_multileg_order_type(order_type: str) -> str:
    return _validate_choice("order_type", order_type, MULTILEG_ORDER_TYPES)


def _validate_duration(duration: str, allow_extended: bool = False) -> str:
    choices = STANDARD_DURATIONS | (EXTENDED_DURATIONS if allow_extended else frozenset())
    return _validate_choice("duration", duration, choices)


def _validate_equity_order_inputs(
        symbol: str, side: str, quantity: Union[int, float], duration: str, allow_extended: bool = False):
    _require_non_empty_str("symbol", symbol)
    _validate_equity_side(side)
    _require_positive_number("quantity", quantity)
    _validate_duration(duration, allow_extended=allow_extended)


def _validate_leg(
        leg: OrderLeg, allow_extended: bool = False, option_only: bool = False, require_symbol: bool = True):
    if option_only:
        _require_non_empty_str("option_symbol", leg.option_symbol)
        _validate_option_side(leg.side)
    else:
        if leg.option_symbol:
            _validate_option_side(leg.side)
        else:
            _validate_equity_side(leg.side)
            if require_symbol:
                _require_non_empty_str("symbol", leg.symbol)

    _require_positive_number("quantity", leg.quantity)
    _validate_duration(leg.duration, allow_extended=allow_extended)

    if leg.type is not None:
        _validate_order_type(leg.type)
        if leg.type in {"limit", "stop_limit"}:
            _require_positive_number("price", leg.price)
        if leg.type in {"stop", "stop_limit"}:
            _require_positive_number("stop", leg.stop)


class OrderWrapper:
    """
    Nothing much here yet, but provides order creation and posting related utilities.
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client: RestClient = rest_client

    def place_close_bracket_order(
            self, symbol: str, quantity: float, tp_price: float, sl_price: float, duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):

        """
        Place a close bracket order (OCO): take-profit + stop order.
        :param symbol: Symbol for the order (equity).
        :param quantity: Quantity of the instrument.
        :param tp_price: Take-profit price.
        :param sl_price: Stop-loss price.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _require_non_empty_str("symbol", symbol)
        _require_positive_number("quantity", quantity)
        _require_positive_number("tp_price", tp_price)
        _require_positive_number("sl_price", sl_price)
        _validate_duration(duration)
        # Create OCO order combining take-profit and stop-loss as two legs
        order = oco_equity(symbol, quantity, take_profit=tp_price, stop_price=sl_price, duration=duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_bracket_order(
            self,
            symbol: str,
            base_side: str,  # 'buy' (long) or 'sell' (short)
            quantity: float,
            base_type: str = "limit",  # 'limit'|'market'|...
            close_profit_type: str = "limit",  # usually 'limit'; can be 'stop' or 'stop_limit' if you insist
            base_price: float = 0.0,  # required when base_type='limit'
            take_profit_percent: float = 1.0,  # +% for long, -% for short (handled automatically)
            stop_loss_trigger_percent: float = 0.5,  # trigger distance from ref price
            stop_loss_limit_percent: float | None = None,  # if stop type includes limit: extra % away from stop trigger
            stop_limit_offset_abs: float | None = None,  # OR absolute offset from stop trigger to limit (one of the two
            # offsets)
            duration: str = "day",
            reference_price: float | None = None,  # required if base_type='market' (or if you want different ref)
            stop_type: str = "stop_limit",  # 'stop'|'stop_limit'
            tag: str = None,
            timeout: float = 5.0,
            preview: bool = False):
        """
        Place a bracket order (OTOCO): parent + take-profit child + stop(-limit) child.

        Behavior:
          - LONG (base_side='buy'): TP above reference; Stop below reference.
          - SHORT (base_side='sell'): TP below reference; Stop above reference.
          - Reference for % math:
              * limit parent -> base_price
              * market parent -> reference_price (required)
          - Stop child:
              * If stop_type='stop' -> only 'stop' is set
              * If stop_type='stop_limit' -> 'stop' at trigger + 'price' at (trigger +/- offset)
                Use EITHER stop_loss_limit_percent OR stop_limit_offset_abs to set that offset.

        Notes:
          - close_profit_type is typically 'limit'. If you pass 'stop' or 'stop_limit', we’ll set stop/price
        accordingly.
          - This constructs an OTOCO equity order via indexed legs. Adjust as needed for options.
        """
        _require_non_empty_str("symbol", symbol)
        _require_positive_number("quantity", quantity)
        _validate_duration(duration)
        _validate_order_type(base_type)
        _require_positive_number("take_profit_percent", take_profit_percent)
        _require_positive_number("stop_loss_trigger_percent", stop_loss_trigger_percent)
        if stop_loss_limit_percent is not None:
            _require_positive_number("stop_loss_limit_percent", stop_loss_limit_percent)
        if stop_limit_offset_abs is not None:
            _require_positive_number("stop_limit_offset_abs", stop_limit_offset_abs)

        is_long = base_side == "buy"
        if not is_long and base_side != "sell":
            raise ValueError("base_side must be 'buy' or 'sell'")

        # Determine reference price
        if base_type == "limit":
            if base_price <= 0:
                raise ValueError("base_price must be > 0 for limit parent")
            ref = base_price
        else:
            if reference_price is None or reference_price <= 0:
                raise ValueError("reference_price must be provided (>0) for non-limit parents")
            ref = reference_price

        # Percent helpers
        def up(p, pct):
            return p * (1 + pct / 100.0)

        def down(p, pct):
            return p * (1 - pct / 100.0)

        # Compute targets
        if is_long:
            tp_price = up(ref, take_profit_percent)
            sl_stop = down(ref, stop_loss_trigger_percent)
            # stop-limit 'price' (limit) slightly below the stop trigger to improve fill odds
            if stop_limit_offset_abs is not None:
                sl_limit = sl_stop - stop_limit_offset_abs
            elif stop_loss_limit_percent is not None:
                sl_limit = down(sl_stop, stop_loss_limit_percent)
            else:
                sl_limit = sl_stop  # acceptable starting point; tune if broker requires a delta
            closing_side = "sell"
        else:
            tp_price = down(ref, take_profit_percent)
            sl_stop = up(ref, stop_loss_trigger_percent)
            # for shorts, limit above stop trigger to improve fill odds
            if stop_limit_offset_abs is not None:
                sl_limit = sl_stop + stop_limit_offset_abs
            elif stop_loss_limit_percent is not None:
                sl_limit = up(sl_stop, stop_loss_limit_percent)
            else:
                sl_limit = sl_stop
            closing_side = "buy"

        # Parent leg
        parent_leg_kwargs = dict(side=base_side, type=base_type, quantity=quantity, symbol=symbol, duration=duration)
        if base_type == "limit":
            parent_leg_kwargs["price"] = base_price
        parent_leg = OrderLeg(**parent_leg_kwargs)

        # Take-profit child
        tp_kwargs = dict(side=closing_side, type=close_profit_type, quantity=quantity, symbol=symbol, duration=duration)
        if close_profit_type == "limit":
            tp_kwargs["price"] = tp_price
        elif close_profit_type == "stop":
            tp_kwargs["stop"] = tp_price
        elif close_profit_type == "stop_limit":
            tp_kwargs["stop"] = tp_price
            tp_kwargs["price"] = tp_price
        else:
            raise ValueError("close_profit_type must be 'limit'|'stop'|'stop_limit'")
        tp_leg = OrderLeg(**tp_kwargs)

        # Stop(-limit) child
        sl_kwargs = dict(side=closing_side, type=stop_type, quantity=quantity, symbol=symbol, duration=duration,
                         stop=sl_stop)
        if stop_type == "stop_limit":
            sl_kwargs["price"] = sl_limit
        elif stop_type != "stop":
            raise ValueError("stop_type must be 'stop' or 'stop_limit'")
        sl_leg = OrderLeg(**sl_kwargs)

        # Build OTOCO order (NOTE: klass must be 'otoco')
        order = otoco_equity(parent_leg, tp_leg, sl_leg, klass="otoco")

        # Submit
        order_response = self.rest_client.place_order(order, preview=preview, tag=tag, timeout=timeout)
        return order_response

    def place_limit_order(
            self, symbol: str, side: str, quantity: float, limit_price: float, duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a limit order.
        :param tag: User tag for correlating orders.
        :param symbol: Symbol for the order (equity).
        :param side: Side of the trade ('buy' or 'sell').
        :param quantity: Quantity of the instrument.
        :param limit_price: Limit price at which order is placed.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _validate_equity_order_inputs(symbol, side, quantity, duration, allow_extended=True)
        _require_positive_number("limit_price", limit_price)
        # Create a limit order
        order = equity_limit(symbol, side, quantity, limit_price, duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_market_order(
            self, symbol: str, side: str, quantity: float, duration: str = "day", tag: str = None,
            timeout: float = 5.0, preview: bool = False):
        """
        Place a market order.
        :param tag:
        :param symbol: Symbol for the order (equity).
        :param side: Side of the trade ('buy' or 'sell').
        :param quantity: Quantity of the instrument.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _validate_equity_order_inputs(symbol, side, quantity, duration)
        # Create a market order
        order = equity_market(symbol, side, quantity, duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_stop_order(
            self, symbol: str, side: str, quantity: float, stop_price: float, duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a stop order.
        :param symbol: Symbol for the order (equity).
        :param side: Side of the trade ('buy' or 'sell').
        :param quantity: Quantity of the instrument.
        :param stop_price: Stop trigger price.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _validate_equity_order_inputs(symbol, side, quantity, duration)
        _require_positive_number("stop_price", stop_price)
        # Create a stop order
        order = Order(
            class_="equity",
            legs=[
                OrderLeg(side=side, type="stop", quantity=quantity, symbol=symbol, duration=duration, stop=stop_price)]
        )
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_stop_limit_order(
            self, symbol: str, side: str, quantity: float, stop_price: float, limit_price: float,
            duration: str = "day", tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a stop-limit order.
        :param symbol: Symbol for the order (equity).
        :param side: Side of the trade ('buy' or 'sell').
        :param quantity: Quantity of the instrument.
        :param stop_price: Stop trigger price.
        :param limit_price: Limit price after stop is triggered.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _validate_equity_order_inputs(symbol, side, quantity, duration)
        _require_positive_number("stop_price", stop_price)
        _require_positive_number("limit_price", limit_price)
        # Create a stop-limit order
        order = stop_limit(symbol, side, quantity, stop_price, limit_price, duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_oto_order(
            self, parent_order_leg: OrderLeg, child_order_legs: list[OrderLeg], duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a One-Triggers-Other (OTO) order: a parent order that triggers one or more child orders.
        :param parent_order_leg: The parent order leg that triggers the child orders.
        :param child_order_legs: A list of child order legs to be executed after the parent triggers.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _validate_duration(duration)
        _validate_leg(parent_order_leg)
        if not child_order_legs:
            raise ValueError("child_order_legs must contain at least one OrderLeg")
        for child_order_leg in child_order_legs:
            _validate_leg(child_order_leg)
        # Build OTO order
        order = oto_equity(parent_order_leg, *child_order_legs, klass="oto")
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_combo_order(
            self, symbol: str, legs: list[OrderLeg], duration: str = "day", tag: str = None,
            timeout: float = 5.0, preview: bool = False):
        """
        Place a combo order: a multi-leg order (e.g., options spreads).
        :param symbol: The symbol to trade (usually for underlying of options or equities).
        :param legs: A list of order legs defining the combo.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _require_non_empty_str("symbol", symbol)
        _validate_duration(duration)
        if not legs:
            raise ValueError("legs must contain at least one OrderLeg")
        for leg in legs:
            _validate_leg(leg, require_symbol=False)
        order = Order(class_="combo", legs=legs, extras={"symbol": symbol, "duration": duration})
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_oco_order(
            self, symbol: str, quantity: float, tp_price: float, sl_price: float, duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a One-Cancels-Other (OCO) order: two orders, where fulfilling one cancels the other.
        :param symbol: Symbol for the order (equity).
        :param quantity: Quantity of the instrument.
        :param tp_price: Take-profit price.
        :param sl_price: Stop-loss price.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _require_non_empty_str("symbol", symbol)
        _require_positive_number("quantity", quantity)
        _require_positive_number("tp_price", tp_price)
        _require_positive_number("sl_price", sl_price)
        _validate_duration(duration)
        order = oco_equity(symbol, quantity, take_profit=tp_price, stop_price=sl_price, duration=duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_trailing_stop_order(
            self, symbol: str, side: str, quantity: float, trail_percent: float, duration: str = "day",
            tag: str = None, preview: bool = False):
        """
        Place a trailing stop order: a stop order that dynamically adjusts based on market price movements.
        :param symbol: Symbol for the order (equity).
        :param side: Side of the trade ('buy' or 'sell').
        :param quantity: Quantity of the instrument.
        :param trail_percent: The trailing percentage from the market price.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        # Assume "stop" is the calculated trailing stop price and sent as an extra
        raise NotImplementedError("Tradier doesn't support trailing orders yet")

    def place_option_spread_order(
            self, buy_symbol: str, sell_symbol: str, quantity: int, limit_price: float, duration: str = "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place an options spread order (vertical spread).
        :param buy_symbol: OCC symbol for the option to buy.
        :param sell_symbol: OCC symbol for the option to sell.
        :param quantity: Quantity of the options.
        :param limit_price: Limit price for the entire spread.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :return: Response from the REST client.
        """
        _require_non_empty_str("buy_symbol", buy_symbol)
        _require_non_empty_str("sell_symbol", sell_symbol)
        _require_positive_number("quantity", quantity)
        _require_positive_number("limit_price", limit_price)
        _validate_duration(duration)
        order = option_vertical(buy_symbol, sell_symbol, quantity, limit_price, duration)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def place_multi_leg_order(
            self, symbol, legs: list[OrderLeg], order_type='market', duration: str =
            "day",
            tag: str = None, timeout: float = 5.0, preview: bool = False):
        """
        Place a multi-leg order (custom combinations of buys/sells for options or equities).
        :param legs: A list of order legs defining the multi-leg order.
        :param class_: The class of the instrument ('multileg', 'combo', etc.).
        :param order_type: Type of order one of: market, debit, credit, even.
        :param duration: Duration of the order ('day'|'gtc', etc.).
        :param tag: User tag for correlating orders.
        :return: Response from the REST client.
        """
        _require_non_empty_str("symbol", symbol)
        _validate_multileg_order_type(order_type)
        _validate_duration(duration)
        if not 2 <= len(legs) <= 4:
            raise ValueError("legs must contain between 2 and 4 option legs")
        for leg in legs:
            _validate_leg(leg, option_only=True)
        order = Order(symbol=symbol, class_="multileg", legs=legs, extras={"duration": duration}, type=order_type)
        return self.rest_client.place_order(order=order, preview=preview, tag=tag, timeout=timeout)

    def modify_existing_order(
            self, account_id: str, order_id: str, order_type: str = None, duration: str = None,
            price: float = None, stop: float = None, timeout: float = 5.0):
        """
        Modify an existing order.
        :param account_id: Account ID where the order resides.
        :param order_id: ID of the order to be modified.
        :param order_type: New order type, if needed.
        :param duration: New duration for the modified order.
        :param price: Updated price for the order.
        :param stop: Updated stop price for the order.
        :return: Response from the REST client.
        """
        return self.rest_client.modify_order(
            account_id, order_id, order_type=order_type, duration=duration, price=price, stop=stop, timeout=timeout
        )

    def cancel_order(self, account_id: str, order_id: str, timeout: float = 5.0):
        """
        Cancel an order.
        :param account_id: Account ID where the order resides.
        :param order_id: ID of the order to be canceled.
        :return: Response from the REST client.
        """
        return self.rest_client.cancel_order(account_id, order_id, timeout=timeout)

    def cancel_all_oco_orders(self, account_id: str, timeout: float = 5.0):
        """
        Cancel all OCO orders in the account.
        :param account_id: Account ID.
        :return: Success message or REST client response.
        """
        orders = self.rest_client.get_orders(account_id, timeout=timeout).get("orders", {}).get("order", [])
        oco_orders = [order for order in orders if order.get("class") == "oco"]
        results = []
        for oco_order in oco_orders:
            results.append(self.cancel_order(account_id, oco_order.get("id"), timeout=timeout))
        return results


# ---- Tiny helpers to construct common orders (feel free to trim/extend) ----
def equity_market(symbol: str, side: str, qty: Union[int, float], duration: str = "day") -> Order:
    """
    Create a market order for equity.

    :param symbol:
    :param side:
    :param qty:
    :param duration:
    :return:
    """
    return Order(class_="equity",
                 legs=[OrderLeg(side=side, type="market", quantity=qty, symbol=symbol, duration=duration)])


def equity_limit(
        symbol: str, side: str, qty: Union[int, float], limit_price: float, duration: str = "day") -> Order:
    """
    Create a limit order for equity.

    :param symbol:
    :param side:
    :param qty:
    :param limit_price:
    :param duration:
    :return:
    """
    return Order(class_="equity", legs=[
        OrderLeg(side=side, type="limit", quantity=qty, symbol=symbol, duration=duration, price=limit_price)])


def option_limit(occ: str, side: str, qty: int, limit_price: float, duration: str = "day") -> Order:
    """
    Create a limit order for options.

    :param occ:
    :param side:
    :param qty:
    :param limit_price:
    :param duration:
    :return:
    """
    return Order(class_="option", legs=[
        OrderLeg(side=side, type="limit", quantity=qty, option_symbol=occ, duration=duration, price=limit_price)])


def stop_limit(
        symbol: str, side: str, qty: Union[int, float], stop_price: float, limit_price: float,
        duration: str = "day") -> Order:
    """
    Create a stop limit order for equity.

    :param symbol:
    :param side:
    :param qty:
    :param stop_price:
    :param limit_price:
    :param duration:
    :return:
    """
    return Order(class_="equity", legs=[
        OrderLeg(side=side, type="stop_limit", quantity=qty, symbol=symbol, duration=duration, stop=stop_price,
                 price=limit_price)])


# Multileg example (e.g., vertical spread). Use class_='multileg' and OCCs in legs:
def option_vertical(
        occ_buy: str, occ_sell: str, qty: int, limit_price: float, duration: str = "day") -> Order:
    """
    Create a vertical spread order for options.

    :param occ_buy:
    :param occ_sell:
    :param qty:
    :param limit_price:
    :param duration:
    :return:
    """
    legs = [
        OrderLeg(side="buy_to_open", type="limit", quantity=qty, option_symbol=occ_buy, duration=duration,
                 price=limit_price),
        OrderLeg(side="sell_to_open", type="limit", quantity=qty, option_symbol=occ_sell, duration=duration,
                 price=limit_price),
    ]
    return Order(class_="multileg", legs=legs)


# Advanced: OCO (two exits for same underlying). Add/alter as needed.
def oco_equity(
        symbol: str, qty: Union[int, float], take_profit: float, stop_price: float,
        duration: str = "day") -> Order:
    """
    Create an OCO order for equity.

    :param symbol:
    :param qty:
    :param take_profit:
    :param stop_price:
    :param duration:
    :return:
    """
    legs = [
        OrderLeg(side="sell", type="limit", quantity=qty, symbol=symbol, duration=duration, price=take_profit),
        OrderLeg(side="sell", type="stop", quantity=qty, symbol=symbol, duration=duration, stop=stop_price,
                 price=stop_price),
    ]
    return Order(class_="oco", legs=legs)


# OTO: first leg is the trigger/parent; rest are attached children
def oto_equity(parent: OrderLeg, *children: OrderLeg, klass: str = "oto") -> Order:
    """
    Create an OTO order for equity.

    :param parent:
    :param children:
    :param klass:
    :return:
    """
    return Order(class_=klass, advanced="oto", legs=[parent, *children])


# OTOCO: parent + two mutually exclusive children
def otoco_equity(
        parent: OrderLeg, child_take_profit: OrderLeg, child_stop: OrderLeg, klass: str = "otoco") -> Order:
    """
    Create an OTOCO order for equity.

    :param parent:
    :param child_take_profit:
    :param child_stop:
    :param klass:
    :return:
    """
    return Order(class_=klass, advanced="otoco", legs=[parent, child_take_profit, child_stop])
