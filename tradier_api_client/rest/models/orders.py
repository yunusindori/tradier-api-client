"""
Order classes for Tradier API.
"""
# quick_tradier_orders.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from logging import getLogger

log = getLogger(__name__)


# ---- Models ----
@dataclass
class OrderLeg:
    """
    A single leg of an order (equity or option).
    """
    # Use either symbol (equity) OR option_symbol (OCC format). Leave the other as None.
    side: str  # 'buy'|'sell'|'buy_to_open'|'sell_to_close' etc.
    quantity: Union[int, float]
    type: str = None  # 'market'|'limit'|'stop'|'stop_limit'|...
    symbol: Optional[str] = None  # equities
    option_symbol: Optional[str] = None  # options (OCC)
    duration: str = "day"  # 'day'|'gtc'|'pre'|'post' (etc, per docs)
    price: Optional[Union[int, float]] = None  # limit/stop-limit price
    stop: Optional[Union[int, float]] = None  # stop or stop-limit trigger
    extras: Dict[str, Any] = field(default_factory=dict)  # room for experimentation


@dataclass
class Order:
    """
    An order with one or more legs.
    """
    # class_: 'equity'|'option'|'multileg'|'combo' (per Tradier)
    class_: str
    legs: List[OrderLeg]
    type: str = None
    advanced: Optional[str] = None  # None|'oco'|'oto'|'otoco'
    # For combos/multileg you can set strategy/meta in extras below as needed:
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_form(self) -> Dict[str, Any]:
        """
        Builds the POST form. If one leg AND not advanced -> unindexed fields.
        Else -> indexed fields: symbol[0], type[0], ..., symbol[1], ...
        """
        form: Dict[str, Any] = {"class": self.class_}
        if self.type:
            form["type"] = self.type
        if self.advanced:
            form["advanced"] = self.advanced
        form.update(self.extras)

        if len(self.legs) == 1 and not self.advanced:
            L = self.legs[0]
            if L.symbol:
                form["symbol"] = L.symbol
            if L.option_symbol:
                form["option_symbol"] = L.option_symbol
            form.update({"side": L.side, "type": L.type, "quantity": L.quantity, "duration": L.duration})
            if L.price is not None:
                form["price"] = round(L.price, 2)
            if L.stop is not None:
                form["stop"] = round(L.stop, 2)
            form.update(L.extras)
            return form

        # Multi-leg or any advanced order: use indexed fields
        for i, L in enumerate(self.legs):
            if L.symbol:
                form[f"symbol[{i}]"] = L.symbol
            if L.option_symbol:
                form[f"option_symbol[{i}]"] = L.option_symbol
            form.update({
                f"side[{i}]": L.side,
                f"quantity[{i}]": L.quantity,
                f"duration[{i}]": L.duration,
            })
            if L.type is not None:
                form[f"type[{i}]"] = L.type
            if L.price is not None:
                form[f"price[{i}]"] = round(L.price, 2)
            if L.stop is not None:
                form[f"stop[{i}]"] = round(L.stop, 2)
            for k, v in L.extras.items():
                form[f"{k}[{i}]"] = v
        return form





# ---- Example usage (commented) ----
# s = make_session(token="YOUR_TOKEN")
# acc = "YOUR_ACCOUNT_ID"
# resp = place_order(s, acc, equity_market("AAPL", "buy", 1), preview=True)
# print(resp)

# Notes / open questions to probe with live calls:
# - How many legs are allowed for class_='multileg' and 'combo'? (iterate legs and observe errors)
# - Which advanced modes accept options vs equities vs mixed? (try class_ + leg mixes)
# - Does response include per-leg order_ids for advanced orders? (inspect JSON)
# - How OTO behaves if parent not filled/placed? (preview/live and inspect status tree)
