"""
Options utility classes and functions
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from functools import partial
from typing import List, Optional, Callable, TypedDict
from typing import Literal, Dict, Any, Iterable, Tuple

from tradier_api_client.rest import RestClient

_OCC_RE = re.compile(r'^([A-Z0-9]{1,6})(\d{6})([CP])(\d{8})$')


def parse_occ(symbol_or_list):
    """
    Parse OCC option symbol(s) like 'BB260116C00001500'.

    Returns a list of dicts with:
      - underlying (str)
      - expiration (datetime.date)
      - option_type ('call'|'put')
      - strike (float)
    """
    symbols = symbol_or_list if isinstance(symbol_or_list, (list, tuple)) else [symbol_or_list]
    out = []
    for s in symbols:
        s = s.strip().strip('"').strip("'")
        m = _OCC_RE.match(s)
        if not m:
            raise ValueError(f"Not a valid OCC option symbol: {s}")

        root, yymmdd, cp, strike8 = m.groups()

        yy = int(yymmdd[:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        # OCC uses YY; modern options are 20YY. Adjust if you ever need 19YY.
        yyyy = 2000 + yy

        expiration = date(yyyy, mm, dd)
        option_type = 'call' if cp == 'C' else 'put'
        # Strike is 8 digits with 3 implied decimals
        strike = int(strike8) / 1000.0

        out.append({
            'underlying': root.rstrip(),
            'expiration': expiration,
            'option_type': option_type,
            'strike': strike,
        })
    return out


# --- tiny helpers ---

def _coerce_date(d: Optional[object]) -> Optional[date]:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date()
    raise TypeError(f"Unsupported date type: {type(d)}")


class OptionDict(TypedDict):
    """
    Options Dict
    """
    underlying: str
    expiration: date  # or: date | str if you allow strings pre-coercion
    option_type: Literal["call", "put"]
    strike: float


def _single_dict_to_occ(option_dict: OptionDict):
    u = str(option_dict["underlying"]).upper()
    if not (1 <= len(u) <= 6) or not u.isalnum():
        raise ValueError(f"Bad underlying: {u!r}")

    # expiration -> YYMMDD
    dt = _coerce_date(option_dict["expiration"])
    yymmdd = f"{dt.year % 100:02d}{dt.month:02d}{dt.day:02d}"

    # option type -> C/P
    t0 = str(option_dict["option_type"]).strip().lower()
    if t0 not in ("call", "put", "c", "p"):
        raise ValueError(f"Bad option_type: {option_dict['option_type']!r}")
    cp = "C" if t0.startswith("c") else "P"

    # strike -> 8 digits, 3 implied decimals
    # use Decimal to avoid float rounding surprises
    strike_scaled = (Decimal(str(option_dict["strike"])) * Decimal("1000")).quantize(Decimal("1"),
                                                                                     rounding=ROUND_HALF_UP)
    n = int(strike_scaled)
    if n < 0 or n > 99999999:
        raise ValueError(f"Bad strike after scaling x1000: {n}")
    strike8 = f"{n:08d}"
    return f"{u}{yymmdd}{cp}{strike8}"


def parse_dict(dict_or_list_of_dict: OptionDict | list[OptionDict]) -> str | list[str]:
    """
    Convert {'underlying':'BB','expiration':'2025-10-31','option_type':'call','strike':8.5}
    -> 'BB251031C00008500'
    """
    # underlying
    if len(dict_or_list_of_dict) == 0:
        return []
    if isinstance(dict_or_list_of_dict, dict):
        dict_or_list_of_dict = [dict_or_list_of_dict]
    return [_single_dict_to_occ(o) for o in dict_or_list_of_dict]


def _apply(options: List[OptionDict], predicates: List[Callable[[OptionDict], bool]]) -> List[OptionDict]:
    if not predicates:
        return list(options)
    return [o for o in options if all(p(o) for p in predicates)]


# --- single convenience filter ---
def filter_options_occ(
        options: list[str],
        *,
        # your original ask
        before: Optional[object] = None,  # expire strictly before this date
        min_strike: Optional[float] = None,  # strike strictly greater than this

        # additional practical filters (all optional)
        option_type: Optional[str] = None,  # 'call' or 'put'
        exp_start: Optional[object] = None,  # inclusive start of expiration range
        exp_end: Optional[object] = None,  # inclusive end of expiration range
        strike_min: Optional[float] = None,  # inclusive strike lower bound
        strike_max: Optional[float] = None,  # inclusive strike upper bound
        underlying: Optional[str] = None,  # exact underlying match (case-insensitive)
        underlying_prefix: Optional[str] = None,  # underlying startswith (case-insensitive)

        # price-aware helpers
        price: Optional[float] = None,  # current/assumed spot price
        strike_within_pct: Optional[float] = None,  # keep strikes within +/- this pct of price (e.g., 0.05)
        moneyness: Optional[str] = None,  # 'ITM' | 'ATM' | 'OTM' (requires price)
        atm_tol_pct: float = 0.01,  # ATM tolerance (default 1%)
):
    """
    Apply zero or more filters to a list/iterable of OCC option symbols. Returns a new list.

    This function takes in and returns a list of OCC options.

    Examples:
      filter_options_occ(data, before="2026-01-31", min_strike=4.0)
      filter_options_occ(data, option_type="call", exp_start="2026-01-01", exp_end="2026-03-31")
      filter_options_occ(data, underlying="BB", price=5.0, strike_within_pct=0.10)
      filter_options_occ(data, price=5.0, moneyness="ITM")
    """
    options_dicts = parse_occ(options)
    _to_pass = {arg: val for arg, val in locals().items() if arg not in ['options', 'options_dicts']}
    filtered_options = filter_options(options=options_dicts, **_to_pass)
    return parse_dict(filtered_options)


def filter_options(
        options: List[OptionDict],
        *,
        # your original ask
        before: Optional[object] = None,  # expire strictly before this date
        min_strike: Optional[float] = None,  # strike strictly greater than this

        # additional practical filters (all optional)
        option_type: Optional[str] = None,  # 'call' or 'put'
        exp_start: Optional[object] = None,  # inclusive start of expiration range
        exp_end: Optional[object] = None,  # inclusive end of expiration range
        strike_min: Optional[float] = None,  # inclusive strike lower bound
        strike_max: Optional[float] = None,  # inclusive strike upper bound
        underlying: Optional[str] = None,  # exact underlying match (case-insensitive)
        underlying_prefix: Optional[str] = None,  # underlying startswith (case-insensitive)

        # price-aware helpers
        price: Optional[float] = None,  # current/assumed spot price
        strike_within_pct: Optional[float] = None,  # keep strikes within +/- this pct of price (e.g., 0.05)
        moneyness: Optional[str] = None,  # 'ITM' | 'ATM' | 'OTM' (requires price)
        atm_tol_pct: float = 0.01,  # ATM tolerance (default 1%)
) -> List[OptionDict]:
    """
    Apply zero or more filters to a list/iterable of option dicts. Returns a new list.

    Examples:
      filter_options(data, before="2026-01-31", min_strike=4.0)
      filter_options(data, option_type="call", exp_start="2026-01-01", exp_end="2026-03-31")
      filter_options(data, underlying="BB", price=5.0, strike_within_pct=0.10)
      filter_options(data, price=5.0, moneyness="ITM")
    """
    predicates: List[Callable[[OptionDict], bool]] = []

    # --- expiration filters ---
    b = _coerce_date(before)
    if b is not None:
        predicates.append(lambda o: o["expiration"] < b)

    s = _coerce_date(exp_start)
    e = _coerce_date(exp_end)
    if s is not None and e is not None:
        predicates.append(lambda o: s <= o["expiration"] <= e)
    elif s is not None:
        predicates.append(lambda o: o["expiration"] >= s)
    elif e is not None:
        predicates.append(lambda o: o["expiration"] <= e)

    # --- strike filters ---
    if min_strike is not None:
        predicates.append(lambda o: float(o["strike"]) > float(min_strike))
    if strike_min is not None:
        predicates.append(lambda o: float(o["strike"]) >= float(strike_min))
    if strike_max is not None:
        predicates.append(lambda o: float(o["strike"]) <= float(strike_max))

    # +/- pct around price, if requested
    if price is not None and strike_within_pct is not None:
        if strike_within_pct < 0:
            raise ValueError("strike_within_pct must be non-negative.")
        lo, hi = price * (1 - strike_within_pct), price * (1 + strike_within_pct)

        def _strike_within_pct(o: OptionDict, _lo: float, _hi: float):
            return _lo <= float(o["strike"]) <= _hi

        predicates.append(partial(_strike_within_pct, _lo=lo, _hi=hi))

    # --- option type ---
    if option_type is not None:
        t = option_type.lower()
        if t not in ("call", "put"):
            raise ValueError("option_type must be 'call' or 'put'.")

        def _option_type_filter(o: OptionDict, _t):
            return str(o["option_type"]).lower() == _t

        predicates.append(partial(_option_type_filter, _t=t))

    # --- underlying symbol filters ---
    if underlying is not None:
        eq = underlying.lower()

        def _underlying_filter(o: OptionDict, _eq):
            return str(o["underlying"]).lower() == _eq

        predicates.append(partial(_underlying_filter, _eq=eq))
    if underlying_prefix is not None:
        sw = underlying_prefix.lower()

        def _underlying_prefix_filter(o: OptionDict, _sw):
            return str(o["underlying"]).lower().startswith(_sw)

        predicates.append(partial(_underlying_prefix_filter, _sw=sw))

    # --- moneyness (needs price) ---
    if moneyness is not None:
        if price is None:
            raise ValueError("moneyness filter requires 'price'.")
        cat = moneyness.upper()
        if cat not in {"ITM", "ATM", "OTM"}:
            raise ValueError("moneyness must be one of {'ITM','ATM','OTM'}.")

        def _moneyness(o: OptionDict, _cat=cat, _price=price, _atm_tol_pct=atm_tol_pct):
            k = float(o["strike"])
            _t = str(o["option_type"]).lower()
            atm = abs(k - _price) / _price <= _atm_tol_pct
            if _cat == "ATM":
                return atm
            itm = (k < _price) if _t == "call" else (k > _price)
            return itm if _cat == "ITM" else (not itm and not atm)

        predicates.append(_moneyness)

    return _apply(options, predicates)


# --- minimal usage examples ---
# filtered = filter_options(parsed_options, before="2026-01-31", min_strike=4.0)
# weeklies = filter_options(parsed_options, exp_start="2026-01-01", exp_end="2026-01-31", option_type="call")
# near_atm = filter_options(parsed_options, price=5.00, strike_within_pct=0.05)
# itm_calls = filter_options(parsed_options, option_type="call", price=5.00, moneyness="ITM")


def filter_for_tradable_options(
        quotes: Dict[str, Any],
        side: Literal["buy", "sell"],
        *,
        min_bid: float | None = None,
        max_ask: float | None = None,
        require_size: bool = True,
        min_size: int = 1,
        max_age_sec: int | None = None,  # ignore stale quotes unless None
        now_sec: int | None = None  # defaults to quotes['client_timestamp'] if present
) -> Dict[str, float]:
    """
    Return {symbol: price} where price is the ask (buy) or bid (sell), insertion-ordered
    by best price (asc for buy, desc for sell).

    - side='buy'  -> keep quotes with ask <= max_ask
    - side='sell' -> keep quotes with bid >= min_bid
    - require_size enforces ask/bid size >= min_size
    - max_age_sec filters by ask_date/bid_date recency (ms in payload)
    """
    # Pull the list of quotes (Tradier may return a single quote dict or a list)
    raw = quotes.get("quotes", {}).get("quote", [])
    items: Iterable[Dict[str, Any]] = raw if isinstance(raw, list) else [raw]

    # Time baseline for staleness checks
    if now_sec is None:
        now_sec = int(quotes.get("client_timestamp") or 0) or None  # allow None if missing

    out: list[Tuple[str, float]] = []

    for q in items:
        if not q or q.get("type") != "option":
            continue

        if side == "buy":
            price = q.get("ask")
            limit_ok = (max_ask is None) or (price is not None and float(price) <= float(max_ask))
            size_ok = (not require_size) or int(q.get("asksize") or 0) >= min_size
            date_ms = q.get("ask_date")
        else:  # 'sell'
            price = q.get("bid")
            limit_ok = (min_bid is None) or (price is not None and float(price) >= float(min_bid))
            size_ok = (not require_size) or int(q.get("bidsize") or 0) >= min_size
            date_ms = q.get("bid_date")

        if price is None or not limit_ok or not size_ok:
            continue

        fresh_ok = True
        if max_age_sec is not None and now_sec is not None and date_ms:
            fresh_ok = (now_sec - (int(date_ms) // 1000)) <= max_age_sec

        if not fresh_ok:
            continue

        out.append((q["symbol"], float(price)))

    # Sort to be execution-friendly
    out.sort(key=(lambda t: t[1]), reverse=(side == "sell"))

    # Preserve ordering by insertion (Python 3.7+)
    return {sym: px for sym, px in out}


def filter_for_tradable_options_strike_plus_bid(
        quotes: Dict[str, Any],
        occ_symbols: List[str],
        *,
        target_price: float,
        require_size: bool = True,
        min_size: int = 1,
        max_age_sec: int | None = None,
        now_sec: int | None = None,
) -> Dict[str, float]:
    """
    Return {symbol: bid} for options in `occ_symbols` whose (strike + bid) >= target_price.
    Results are insertion-ordered by (strike+bid) desc, then bid desc.

    Requires a `parse_options_occ(occ_symbols)` function that returns a list[dict] like:
        {'underlying': ..., 'expiration': 'YYYY-MM-DD', 'option_type': 'call'|'put', 'strike': float}
    """
    # Map OCC -> strike using the parsed list (assumes order matches input)
    parsed = parse_occ(occ_symbols)
    strike_by_symbol = {sym: float(info["strike"]) for sym, info in zip(occ_symbols, parsed)}

    # Pull quotes list (Tradier can return a single dict)
    raw = quotes.get("quotes", {}).get("quote", [])
    items: Iterable[Dict[str, Any]] = raw if isinstance(raw, list) else [raw]

    # Time baseline for staleness checks
    if now_sec is None:
        now_sec = int(quotes.get("client_timestamp") or 0) or None

    selected: List[Tuple[str, float, float]] = []  # (symbol, bid, total)

    occ_set = set(occ_symbols)
    for q in items:
        if not q or q.get("type") != "option":
            continue

        sym = q.get("symbol")
        if sym not in occ_set:
            continue  # only keep your candidate list

        bid = q.get("bid")
        if bid is None:
            continue

        # Size check (on the bid side for selling)
        if require_size and int(q.get("bidsize") or 0) < min_size:
            continue

        # Freshness check (bid_date is ms since epoch)
        if max_age_sec is not None and now_sec is not None:
            bid_ms = q.get("bid_date")
            if not bid_ms or (now_sec - (int(bid_ms) // 1000)) > max_age_sec:
                continue

        strike = strike_by_symbol.get(sym)
        if strike is None:
            continue

        total = float(strike) + float(bid)
        if total >= float(target_price):
            selected.append((sym, float(bid), total))

    # Sort by total desc, then bid desc
    selected.sort(key=lambda t: (t[2], t[1]), reverse=True)

    # Return in that order -> {symbol: bid}
    return {sym: bid for sym, bid, _ in selected}


class OptionsWrapper:
    """
    Wrapper class for options
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client = rest_client

    def get_call_options_occ_symbols_list(self, underlying_symbol: str, timeout: float = 5.0):
        """
        Get the list of OCC call options symbols for a given underlying symbol.
        :param underlying_symbol:
        """
        response = self.rest_client.lookup_options_symbols(underlying_symbol, timeout=timeout)
        if response and 'symbols' in response and response['symbols'] and 'options' in response['symbols'][0] and \
                response['symbols'][0]['options']:
            call_options_symbols = response['symbols'][0]['options']
            option_side_position = len(underlying_symbol) + 6
            call_options_symbols = list(filter(lambda x: x[option_side_position] == 'C', call_options_symbols))
            return call_options_symbols
        return []

    def get_put_options_occ_symbols_list(self, underlying_symbol: str, timeout: float = 5.0):
        """
        Get the list of OCC put options symbols for a given underlying symbol.
        :param underlying_symbol:
        """
        response = self.rest_client.lookup_options_symbols(underlying_symbol, timeout=timeout)
        if response and 'symbols' in response and response['symbols'] and 'options' in response['symbols'][0] and \
                response['symbols'][0]['options']:
            call_options_symbols = response['symbols'][0]['options']
            option_side_position = len(underlying_symbol) + 6
            call_options_symbols = list(filter(lambda x: x[option_side_position] == 'P', call_options_symbols))
            return call_options_symbols
        return []
