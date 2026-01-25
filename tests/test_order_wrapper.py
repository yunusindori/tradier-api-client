"""
Tests for the OrderWrapper class in the Tradier API client.
"""
import json
import os

from tradier_api_client.rest import RestClient
from tradier_api_client.rest.extensions.orders import OrderWrapper  # Assuming the implementation is in orders.py
from tradier_api_client.rest.models.orders_fixed import OrderLeg


def initialize_rest_client():
    """
    Initialize the RestClient with the sandbox URL and API key from environment variables.
    :return:
    """
    return RestClient("https://sandbox.tradier.com/v1", os.environ.get('API_KEY'),
                      verbose=True)


# Initialize the RestClient and OrderWrapper
def initialize_order_wrapper(client: RestClient = None):
    """
    Initialize the OrderWrapper with the given RestClient or create a new one.
    :param client:
    :return:
    """
    rest_client = client or initialize_rest_client()
    return OrderWrapper(rest_client)


# Test placing a close bracket (OCO) order
def test_place_close_bracket_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_close_bracket_order(
        symbol="BB",
        quantity=10,
        tp_price=5.0,  # Take-profit price (arbitrarily above market price)
        sl_price=4.5,  # Stop-loss price (arbitrarily below market price)
    )
    print(json.dumps(response))


# Test placing a simple limit order
def test_place_limit_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_limit_order(
        symbol="BB",
        side="buy",
        quantity=10,
        limit_price=4.6,  # Limit price below current market price (ideal for a buy)
    )
    print(json.dumps(response))


# Test placing a market order
def test_place_market_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_market_order(
        symbol="BB",
        side="sell",
        quantity=10,
        duration="gtc",
    )
    print(json.dumps(response))


# Test placing a stop order
def test_place_stop_buy_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_stop_order(
        symbol="BB",
        side="buy",
        quantity=10,
        stop_price=5.1,  # Stop trigger price (arbitrarily above market price for a buy)
    )
    print(json.dumps(response))


# Test placing a stop-limit order
def test_place_stop_limit_buy_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_stop_limit_order(
        symbol="BB",
        side="buy",
        quantity=10,
        stop_price=4.9,
        limit_price=4.85,
    )
    print(json.dumps(response))


# Test placing an OTO order
def test_place_oto_buy_order():
    order_wrapper = initialize_order_wrapper()
    parent_leg = OrderLeg(
        side="buy",
        type="limit",
        quantity=10,
        symbol="BB",
        price=4.6,
        duration="day"
    )
    child_leg = OrderLeg(
        side="sell",
        type="stop",
        quantity=10,
        symbol="BB",
        stop=4.4,
        duration="day"
    )
    response = order_wrapper.place_oto_order(
        parent_order_leg=parent_leg,
        child_order_legs=[child_leg],
        duration="day", tag="test-oto-order-1"
    )
    print(json.dumps(response))


# Test placing an OCO order
def test_place_oco_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_oco_order(
        symbol="BB",
        quantity=10,
        tp_price=5.2,  # Take-profit price above market price
        sl_price=4.3,  # Stop-loss price below market price
        duration="day"
    )
    print(json.dumps(response))


# Test placing a trailing stop order
def test_place_trailing_stop_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_trailing_stop_order(
        symbol="BB",
        side="sell",
        quantity=10,
        trail_percent=5,  # Trail stop by 5% from the highest price
        duration="day"
    )
    print(json.dumps(response))


# Test placing a vertical option spread order
def test_place_option_spread_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.place_option_spread_order(
        buy_symbol="BB230217C00005000",  # Example OCC symbol for a call option
        sell_symbol="BB230217C00005500",  # Example OCC symbol for another call option
        quantity=1,
        limit_price=0.30,  # Limit price for the spread
    )
    print(json.dumps(response))


# Test placing a multi-leg combo order
def test_place_multi_leg_order():
    order_wrapper = initialize_order_wrapper()
    legs = [
        OrderLeg(side="buy_to_open", quantity=10, option_symbol="BB", price=4.7, duration="day"),
        OrderLeg(side="sell_to_close", quantity=10, option_symbol="BB", price=5.2, duration="day")
    ]
    response = order_wrapper.place_multi_leg_order(
        symbol="BB",
        legs=legs
    )
    print(json.dumps(response))


# Test modifying an existing order
def test_modify_existing_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.modify_existing_order(
        account_id="your_account_id_here",
        order_id="order_id_here",
        order_type="market",
        duration="gtc",
        price=4.8
    )
    print(json.dumps(response))


# Test cancelling an order
def test_cancel_order():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.cancel_order(
        account_id="your_account_id_here",
        order_id="order_id_here"
    )
    print(json.dumps(response))


# Test cancelling all OCO orders
def test_cancel_all_oco_orders():
    order_wrapper = initialize_order_wrapper()
    response = order_wrapper.cancel_all_oco_orders(
        account_id="your_account_id_here"
    )
    print(json.dumps(response))


def test_get_all_orders():
    rest_client = initialize_rest_client()
    print(json.dumps(rest_client.get_orders(rest_client.account_number)))


def test_cancel_all_orders():
    rest_client = initialize_rest_client()
    order_wrapper = initialize_order_wrapper(client=rest_client)
    orders = rest_client.get_orders(rest_client.account_number)
    if orders and 'orders' in orders and orders['orders'] and 'order' in orders['orders'] and orders['orders']['order']:
        for order in orders['orders']['order']:
            try:
                print(f"Cancelling order: {order['id']}")
                order_wrapper.cancel_order(rest_client.account_number, order['id'])
            except Exception as e:
                print(e)
                continue
    else:
        print("No orders found")


def test_place_otoco_order():
    order_wrapper = initialize_order_wrapper()
    parent_leg = OrderLeg(
        side="buy",
        type="limit",
        quantity=10,
        symbol="BB",
        price=4.6,
        duration="gtc"
    )
    profit_leg = OrderLeg(side="sell", type="limit", quantity=10, symbol="BB", price=4.8, duration="gtc")
    stop_leg = OrderLeg(side="sell", type="stop_limit", quantity=10, symbol="BB", stop=4.5, price=4.5, duration="gtc")
    response = order_wrapper.place_bracket_order("BB", "buy", quantity=10, base_price=4.6,
                                                 stop_loss_limit_percent=0.5, stop_limit_offset_abs=0.05,
                                                 duration='gtc', tag="test-otoco")
    print(json.dumps(response))


if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    # test_place_oto_buy_order()
    # test_place_market_order()
    # test_place_oco_order()
    # test_place_otoco_order()
    # test_place_oto_buy_order()
    # test_place_stop_limit_buy_order()
    # test_place_stop_buy_order()
    # test_place_close_bracket_order()
    # test_place_limit_order()
    # try:
    #     test_place_trailing_stop_order()
    # except Exception as e:
    #     print(e)
    # test_get_all_orders()
    # test_place_multi_leg_order()
    test_cancel_all_orders()
