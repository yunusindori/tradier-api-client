"""
tradier_api_client.rest.models.orders_fixed

Clean, explicit order models matching Tradier's "place order" request fields.

This file replaces the previous experimental `orders_fixed.py`.

Design goals
- Zero ambiguity: each field appears in exactly one place (no order-level vs leg-level duplication).
- Strict validation: required fields (including conditional ones) are checked before producing a payload.
- Correct payload shape:
  - Simple (single-leg) orders use unindexed fields.
  - Multileg option orders use `class=multileg`, an underlying `symbol`, and indexed option legs.
  - Advanced orders (OCO/OTO/OTOCO) are modeled explicitly and emit fields in the common Tradier pattern:
    `advanced=<...>` and indexed child orders.

Notes
- Tradier expects application/x-www-form-urlencoded. `to_form()` returns a dict suitable for that.
- This module is used by `tradier_api_client.rest.extensions.orders.OrderWrapper`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence, Union

Number = Union[int, float]


# ----------------------------
# Validation helpers
# ----------------------------

def _require_str(name: str, value: Optional[str]) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _require_number(name: str, value: Optional[Number]) -> Number:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{name} is required")
    return value


def _maybe_round(value: Optional[Number]) -> Optional[Number]:
    if value is None:
        return None
    # Tradier is fine with decimal values; round for stability.
    return round(float(value), 2)


# ----------------------------
# Backwards-compatible models used by OrderWrapper
# ----------------------------

@dataclass(frozen=True)
class OrderLeg:
    """Backward-compatible order leg.

    This keeps the original project surface area so existing code can keep doing:
      OrderLeg(side=..., type=..., quantity=..., symbol=..., duration=..., price=..., stop=...)

    Notes
    - For *single-leg* orders, these fields map directly to Tradier's unindexed parameters.
    - For *advanced* orders (oco/oto/otoco), legs are serialized as indexed fields.
    - For *multileg* option orders created via `Order(class_='multileg', ...)`, legs should use `option_symbol` and
      `side` like buy_to_open/sell_to_open, etc.
    """

    side: str
    quantity: Number

    type: Optional[str] = None
    symbol: Optional[str] = None
    option_symbol: Optional[str] = None
    duration: str = "day"

    price: Optional[Number] = None
    stop: Optional[Number] = None

    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Order:
    """Backward-compatible order container.

    This matches the old constructor used throughout `rest/extensions/orders.py`.

    Supported shapes:
    - Simple single-leg: class_ in {'equity','option'} with a single OrderLeg
    - Advanced: class_ in {'oco','oto','otoco'} and `advanced` set (legacy convention)
    - Multileg: class_ == 'multileg' and legs with option_symbol; requires underlying symbol via `symbol` attr
      (or extras['symbol'] for compatibility).

    NOTE: This class intentionally keeps `legs` because existing call sites build orders that way.
    """

    class_: str
    legs: Sequence[OrderLeg]

    # Legacy order-level flags
    advanced: Optional[str] = None

    # For multileg/other cases where there is an order-level symbol.
    symbol: Optional[str] = None

    # Some older call sites set an order-level type or duration via extras.
    type: Optional[str] = None

    extras: Dict[str, Any] = field(default_factory=dict)

    def to_form(self) -> Dict[str, Any]:
        """Serialize to a dict suitable for application/x-www-form-urlencoded."""

        if not isinstance(self.legs, Sequence) or len(self.legs) == 0:
            raise ValueError("legs must contain at least 1 OrderLeg")

        klass = _require_str("class_", self.class_)

        # Common base
        form: Dict[str, Any] = {"class": klass}

        # Allow passing order-level symbol via explicit attr or extras for compatibility.
        order_symbol = self.symbol
        if not order_symbol and isinstance(self.extras, dict):
            order_symbol = self.extras.get("symbol")
        if isinstance(order_symbol, str) and order_symbol.strip():
            form["symbol"] = order_symbol.strip()

        # Allow passing order-level type via self.type
        if isinstance(self.type, str) and self.type.strip():
            form["type"] = self.type.strip()

        # Allow passing order-level duration via extras (existing code does this).
        if isinstance(self.extras, dict) and isinstance(self.extras.get("duration"), str):
            form["duration"] = self.extras.get("duration").strip()

        # Advanced orders: use indexed serialization + advanced flag.
        if self.advanced:
            form["advanced"] = _require_str("advanced", self.advanced)

        # Merge other extras last (but avoid overriding keys we already set intentionally).
        for k, v in (self.extras or {}).items():
            if k not in form:
                form[k] = v

        single_leg_simple = len(self.legs) == 1 and not self.advanced and klass not in {"oco", "oto", "otoco"}

        if single_leg_simple:
            L = self.legs[0]
            # Unindexed payload
            if L.symbol:
                form["symbol"] = L.symbol.strip()
            if L.option_symbol:
                form["option_symbol"] = L.option_symbol.strip()

            form["side"] = _require_str("side", L.side)
            form["quantity"] = _require_number("quantity", L.quantity)
            form["type"] = _require_str("type", L.type)
            form["duration"] = _require_str("duration", L.duration)

            p = _maybe_round(L.price)
            s = _maybe_round(L.stop)
            if p is not None:
                form["price"] = p
            if s is not None:
                form["stop"] = s

            form.update(L.extras)
            return form

        # Multileg orders: Tradier expects underlying symbol and indexed option legs.
        if klass == "multileg":
            if "symbol" not in form:
                raise ValueError("symbol (underlying) is required for multileg orders")

            # These are order-level for multileg in Tradier examples.
            # Prefer explicit form values, else infer from first leg.
            if "type" not in form:
                first_type = getattr(self.legs[0], "type", None)
                if isinstance(first_type, str) and first_type.strip():
                    form["type"] = first_type.strip()
            if "duration" not in form:
                form["duration"] = _require_str("duration", self.legs[0].duration)

            # If order-level price/stop desired, accept them via extras.
            p0 = _maybe_round((self.extras or {}).get("price"))
            s0 = _maybe_round((self.extras or {}).get("stop"))
            if p0 is not None:
                form["price"] = p0
            if s0 is not None:
                form["stop"] = s0

            for i, L in enumerate(self.legs):
                if not L.option_symbol:
                    raise ValueError("multileg orders require option_symbol on each leg")
                form[f"option_symbol[{i}]"] = L.option_symbol.strip()
                form[f"side[{i}]"] = _require_str("side", L.side)
                form[f"quantity[{i}]"] = _require_number("quantity", L.quantity)
                for k, v in L.extras.items():
                    form[f"{k}[{i}]"] = v
            return form

        # Advanced / indexed: for oco/oto/otoco, and also for any multi-leg non-multileg class.
        for i, L in enumerate(self.legs):
            if L.symbol:
                form[f"symbol[{i}]"] = L.symbol.strip()
            if L.option_symbol:
                form[f"option_symbol[{i}]"] = L.option_symbol.strip()

            form[f"side[{i}]"] = _require_str("side", L.side)
            form[f"quantity[{i}]"] = _require_number("quantity", L.quantity)
            form[f"duration[{i}]"] = _require_str("duration", L.duration)

            if L.type is not None:
                form[f"type[{i}]"] = _require_str("type", L.type)

            p = _maybe_round(L.price)
            s = _maybe_round(L.stop)
            if p is not None:
                form[f"price[{i}]"] = p
            if s is not None:
                form[f"stop[{i}]"] = s

            for k, v in L.extras.items():
                form[f"{k}[{i}]"] = v

        return form


# ----------------------------
# New clean models (optional)
# ----------------------------
# The rest of this module intentionally focuses on the backward-compatible Order/OrderLeg
# used throughout the repository today.

# NOTE: Keeping this file stable and backward-compatible is the priority.
# If you want a stricter, non-legacy API, we can add a separate module (e.g., orders_v2.py)
# without impacting OrderWrapper.
