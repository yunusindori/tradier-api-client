"""
The REST client
"""
import logging
import os
import threading
from datetime import datetime
from multiprocessing import AuthenticationError
from typing import Optional, Any, Dict, Union, TypedDict

import requests
from requests import HTTPError

from tradier_api_client.enc_dec import EncDec
from tradier_api_client.rest.models.orders_fixed import Order
from ..helper_functions import log_for_level


# noinspection PyMissingOrEmptyDocstring
class RateLimit(TypedDict):
    used: int
    available: int
    expiry: Optional[int]
    expiry_dt: Optional[datetime]


class Singleton(type):
    """
    Singleton metaclass
    """
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
            return cls._instances[cls]


# Use metaclass=Singleton to make this a singleton class, if needed
# TODO: Decide on whether this needs to be a singleton class or not
class RestClient:
    """
    Rest Client class
    """

    def __init__(
            self, base_url, api_key=None, account_id=None, api_key_env_prop=None,
            account_id_env_prop=None, config=None, verbose=False):
        """
        A config.yml must be present in the current working directory.

        The config file must contain these properties at minimum:
        tradier.api.client.base_url
        tradier.api.client.streaming_url

        :param api_key:
        :param api_key_env_prop: Used when api_key is not passed directly. This is the name of the environment variable
            holding the api key
        :param config: Optional config dictionary, currently not used.
        :param verbose: Log level set to DEBUG if True, else INFO
        """
        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.api_key = api_key
        self.account_number = account_id
        if not self.api_key and api_key_env_prop:
            self.api_key = os.environ.get(api_key_env_prop)
        if not self.account_number and account_id_env_prop:
            self.account_number = os.environ.get(account_id_env_prop)
        self.authenticated = self.api_key is not None
        if not self.authenticated:
            raise Exception("Set the api_key to environment or pass as a param to the constructor")

        self.http_base_url = base_url

        if not getattr(self, "http_base_url", None):
            raise Exception(
                "Missing Tradier base URL. Provide `config` with tradier.api.client.base_url, or pass `base_url`."
            )

        """
        X-Ratelimit-Allowed: 120
        X-Ratelimit-Used: 1
        X-Ratelimit-Available: 119
        X-Ratelimit-Expiry: 1369168800001
        """
        self.rate_limit: RateLimit = {
            "used": 0,
            "available": 200,
            "expiry": None,
            "expiry_dt": None
        }
        self.enc_dec = EncDec()
        if not self.account_number:
            self.account_number = account_id or self.__get_account_number()

    # noinspection PyMethodMayBeStatic
    def load_properties_to_env(self, file_path):
        """
        Reads the properties file and exports all the properties to the environment
        :return:
        """
        with open(file_path) as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments (lines starting with '#')
                if not line or line.startswith('#'):
                    continue
                # Split the line on the first '='
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

    def set_headers(self, headers: dict = None):
        """
        Set headers
        :param headers:
        :return:
        """
        headers = headers or {}
        if "Authorization" not in headers.keys():
            headers.update({"Authorization": f"Bearer {self.api_key}"})
        if 'Accept' not in headers.keys():
            headers.update({"Accept": "application/json"})
        if 'Accept-Encoding' not in headers.keys():
            headers.update({"Accept-Encoding": "gzip"})
        return headers

    def prepare_and_send_request(
            self,
            http_method,
            url_path,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
            headers: dict = None
    ):
        """
        :meta private:
        """
        if params is not None and data is not None:
            raise Exception("Only one of params and data can be passed.")

        if not self.authenticated:
            raise AuthenticationError(
                "Unauthenticated request to private endpoint. If you wish to access private endpoints, "
                "you must provide your API key and secret "
                "when initializing the RESTClient."
            )

        headers = self.set_headers(headers)
        if params:
            params = {key: value for key, value in params.items() if value is not None}

        if data:
            data = {key: value for key, value in data.items() if value is not None}

        return self.send_request(http_method, url_path, params, headers, data=data)

    class RateLimitExceeded(Exception):
        """
        Thrown when rate limit is exceeded.
        """
        pass

    def _can_request(self):
        from datetime import datetime, timezone
        rl = self.rate_limit
        used = rl.get("used")
        available = rl.get("available")
        expiry = rl.get("expiry")
        if expiry is not None and isinstance(expiry, int):
            now = int(datetime.now(timezone.utc).timestamp())
            if now > int(expiry):
                return True
        if used is None or available is None:
            return False
        try:
            return (int(available) - int(used)) >= 5
        except Exception:
            return False

    def send_request(self, http_method, url_path, params, headers, data=None, timeout=None):
        """
        :meta private:
        """
        url = f"{self.http_base_url}{url_path}"

        log_for_level(self.logger, logging.DEBUG, f"Sending {http_method} request to {url}")
        if not self._can_request():
            raise self.RateLimitExceeded("Rate limit: need 5-call headroom or wait for expiry")
        client_timestamp = int(datetime.now().timestamp())
        response = requests.request(
            http_method,
            url,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout
        )
        log_for_level(self.logger, logging.DEBUG, f"Request URL: {response.request.url}")
        log_for_level(self.logger, logging.DEBUG, f"Request Headers: {response.request.headers}")
        log_for_level(self.logger, logging.DEBUG, f"Request Body: {response.request.body}")

        # Print response details
        log_for_level(self.logger, logging.DEBUG, f"Response Status Code: {response.status_code}")
        log_for_level(self.logger, logging.DEBUG, f"Response Headers: {response.headers}")
        log_for_level(self.logger, logging.DEBUG, f"Response Content: {response.text}")
        self.handle_exception(response)  # Raise an HTTPError for bad responses
        self.update_rate_limit(response)
        log_for_level(self.logger, logging.DEBUG, f"Raw response: {response.json()}")
        return {**response.json(), 'client_timestamp': client_timestamp}

    def update_rate_limit(self, response):
        """Update self.rate_limit only if rate-limit headers are present."""
        headers = getattr(response, "headers", {}) or {}
        used = headers.get("X-Ratelimit-Used")
        available = headers.get("X-Ratelimit-Available")
        expiry_raw = headers.get("X-Ratelimit-Expiry")
        if used is None and available is None and expiry_raw is None:
            return

        def to_int(x):
            """
            Safely convert to int.
            :param x:
            :return:
            """
            try:
                return int(x) if x is not None else None
            except Exception:
                return None

        if used is not None:
            self.rate_limit["used"] = to_int(used)
        if available is not None:
            self.rate_limit["available"] = to_int(available)
        if expiry_raw is not None:
            try:
                num: int = int(expiry_raw)
                if num > 10 ** 12:
                    num //= 1000
                self.rate_limit["expiry"] = num
                from datetime import datetime, timezone
                self.rate_limit["expiry_dt"] = datetime.fromtimestamp(num)
            except Exception:
                self.rate_limit["expiry"] = None
                self.rate_limit["expiry_dt"] = None
        # print(f"Account id: {self.account_number}, Updated rate limit: {self.rate_limit}")

    def get(self, path, params: Optional[dict] = None, headers: dict = None) -> Union[None, dict, list, int, str]:
        """
        :param path:
        :param params:
        :param headers:
        :param headers:
        """
        response = self.prepare_and_send_request("get", path, params, headers=headers)
        return response

    def post(self, path, params: Optional[dict] = None, data: Optional[dict] = None, headers: dict = None):
        """
        :param path:
        :param params:
        :param data:
        :param headers:
        :return:

        """
        response = self.prepare_and_send_request("post", path, params=params, data=data, headers=headers)
        return response

    def put(self, path, params: Optional[dict] = None, data: Optional[dict] = None, headers: dict = None):
        """

        :param path:
        :param params:
        :param data:
        :param headers:
        :return:
        """
        response = self.prepare_and_send_request("put", path, params=params, data=data, headers=headers)
        return response

    def delete(self, path, params: Optional[dict] = None, data: Optional[dict] = None, headers: dict = None):
        """
        :param path:
        :param params:
        :param data:
        :param headers:
        :return:

        """
        response = self.prepare_and_send_request("delete", path, params=params, data=data, headers=headers)
        return response

    def handle_exception(self, response):
        """Raises :class:`HTTPError`, if one occurred.

        :meta private:
        """
        http_error_msg = ""
        reason = response.reason

        if 400 <= response.status_code < 500:
            if (
                    response.status_code == 403
                    and '"error_details":"Missing required scopes"' in response.text
            ):
                http_error_msg = f"{response.status_code} Client Error: Missing Required Scopes. Please verify your " \
                                 f"API keys include the necessary " \
                                 f"permissions."
            else:
                http_error_msg = (
                    f"{response.status_code} Client Error: {reason} {response.text}"
                )
        elif 500 <= response.status_code < 600:
            http_error_msg = (
                f"{response.status_code} Server Error: {reason} {response.text}"
            )

        if http_error_msg:
            log_for_level(self.logger, logging.ERROR, f"HTTP Error: {http_error_msg}")
            raise HTTPError(http_error_msg, response=response)

    def create_account_session(self, api_key):
        """Create account session id"""
        headers = {
            'Authorization': f'Bearer {api_key}',
        }
        path = f"/accounts/events/session"
        return self.post(path, headers=headers, data={})

    def create_market_session(self, api_key):
        """Create market events session"""
        headers = {
            'Authorization': f'Bearer {api_key}',
        }
        path = f"/markets/events/session"
        return self.post(path, headers=headers, data={})

    def authenticated(self):
        """Check if API key is present"""
        return self.api_key is not None

    def get_user_profile(self, api_key=None):
        """Get User Profile"""
        path = "/user/profile"
        headers = {'Authorization': f'Bearer {self.api_key}'} if not api_key else {'Authorization': f'Bearer {api_key}'}
        return self.get(path, headers=headers)

    def get_balances(self, account_id):
        """Get account balances"""
        path = f"/accounts/{account_id}/balances"
        return self.get(path)

    def get_positions(self, account_id):
        """Get Account Positions"""
        path = f"/accounts/{account_id}/positions"
        return self.get(path)

    def get_history(
            self, account_id, page=1, limit=10000, history_type: list = None, start: str = None,
            end: str = None, symbol: str = None,
            exactMatch=True, api_key=None):
        """
        Get Account History
        See: https://documentation.tradier.com/brokerage-api/trading/get-account-history
        """
        path = f"/accounts/{account_id}/history"
        params: Dict[str, Any] = {
            'page': page,
            'limit': limit
        }
        if history_type:
            if isinstance(history_type, str):
                history_type = [history_type]
            params['type'] = ",".join(history_type)
        if symbol:
            params['symbol'] = symbol
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        params['exactMatch'] = exactMatch
        headers = {'Authorization': f'Bearer {self.api_key}'} if not api_key else {'Authorization': f'Bearer {api_key}'}
        return self.get(path, params=params, headers=headers)

    def get_gain_loss(self, account_id):
        """Get Current Gain Los"""
        path = f"/accounts/{account_id}/gainloss"
        return self.get(path)

    def get_orders(self, account_id, include_tags=True):
        """Get all orders for the market session of the present calendar day. Not to be used for historical orders"""
        path = f"/accounts/{account_id}/orders"
        done = False
        page = 1
        order_list = []
        while not done:
            params = {
                'page': page,
                'includeTags': include_tags
            }
            response = None
            try:
                response = self.get(path, params=params)
            except self.RateLimitExceeded as e:
                if not done:
                    continue

            if response and response.get('orders') and isinstance(response.get('orders'), dict) and response.get(
                    "orders").get('order') is not None:
                items = response.get('orders').get('order')
                if isinstance(items, list):
                    order_list.extend(items)
                elif isinstance(items, dict):
                    order_list.append([items])
                if response.get('orders').get('total_pages') and response.get('orders').get('total_pages') > page:
                    page += 1
                else:
                    done = True
            else:
                done = True
        return {'orders': {'order': order_list}}

    def get_order(self, account_id, order_id, include_tags=True):
        """Get An Order's Details"""
        path = f"/accounts/{account_id}/orders/{order_id}"
        params = {
            'includeTags': include_tags
        }
        return self.get(path, params=params)

    def modify_order(self, account_id, order_id, order_type=None, duration=None, price=None, stop=None):
        """Modify an order"""
        path = f"/accounts/{account_id}/orders/{order_id}"
        data = None
        if order_type or duration or price or stop:
            data = {}
            if order_type:
                data.update({'type': order_type})
            if duration:
                data.update({'duration': duration})
            if price:
                data.update({'price': price})
            if stop:
                data.update({'stop': stop})
        return self.put(path, data=data)

    def cancel_order(self, account_id=None, order_id=None):
        """Cancel an order"""
        path = f"/accounts/{account_id}/orders/{order_id}"
        return self.delete(path)

    def get_quotes(self, symbols, greeks=False):
        """Get quotes for symbols"""
        path = "/markets/quotes"
        params = {
            'symbols': symbols,
            'greeks': greeks
        }
        return self.get(path, params=params)

    def get_quotes_larger(self, symbols, greeks=False):
        """Get quotes via POST that supports larger set of symbols"""
        path = "/markets/quotes"
        data = {
            'symbols': symbols,
            'greeks': greeks
        }
        return self.post(path, data=data)

    def get_option_chains(self, symbol, expiration, greeks=False):
        """Get options chains"""
        path = "/markets/options/chains"
        params = {
            'symbol': symbol,
            'expiration': expiration,
            'greeks': greeks
        }
        return self.get(path, params=params)

    def get_option_strikes(self, symbol, expiration, include_all_roots=True):
        """Get options strikes"""
        path = "/markets/options/strikes"
        params = {
            'symbol': symbol,
            'expiration': expiration,
            'includeAllRoots': include_all_roots
        }
        return self.get(path, params=params)

    def get_option_expirations(
            self, symbol, include_all_roots=True, strikes=False, contract_size=False,
            expiration_type=False):
        """Get options expirations"""
        path = "/markets/options/expirations"
        params = {
            'symbol': symbol,
            'includeAllRoots': include_all_roots,
            'strikes': strikes,
            'contractSize': contract_size,
            'expirationType': expiration_type
        }
        return self.get(path, params=params)

    def lookup_option_symbols(self, symbol, includeAllRoots=True, strikes=True, contractSize=True, expirationType=True):
        """Get all options symbols for an underlying symbol"""
        path = "/markets/options/expirations"
        params = {
            'symbol': symbol,
            'includeAllRoots': includeAllRoots,
            'strikes': strikes,
            'contractSize': contractSize,
            'expirationType': expirationType
        }
        return self.get(path, params=params)

    def lookup_options_symbols(self, underlying, strike=None, expiration=None, option_type=None):
        """
        Get options symbols for an underlying symbol
        :param underlying:
        :param strike:
        :param expiration: YYYY-MM-DD
        :param option_type: call or put
        :return:
        """
        path = "/markets/options/lookup"
        params = {
            'underlying': underlying
        }
        if strike:
            params.update({'strike': strike})
        if expiration:
            params.update({'expiration': expiration})
        if option_type:
            params.update({'type': option_type})
        return self.prepare_and_send_request("get", path, params=params)

    def get_historical_quotes(
            self, symbol, interval='daily', start=None, end=None, return_list=False):
        """Get historical prices of a symbol"""
        path = "/markets/history"
        params = {
            'symbol': symbol,
        }
        if interval:
            params.update({'interval': interval})
        if start:
            params.update({'start': start})
        if end:
            params.update({'end': end})
        return self.convert_to_list(self.get(path, params=params), return_list)

    # noinspection PyMethodMayBeStatic
    def convert_to_list(self, quotes_history, return_list):
        """
        Convert quotes history to list
        :param quotes_history:
        :param return_list:
        :return:
        """
        if return_list:
            if (quotes_history and isinstance(quotes_history, dict) and "history" in quotes_history and
                    quotes_history["history"]
                    and "day" in quotes_history["history"] and
                    quotes_history['history']['day']):
                if isinstance(quotes_history["history"]["day"], dict):
                    return [quotes_history["history"]["day"]]
                else:
                    return quotes_history["history"]["day"]
            else:
                return []
        else:
            return quotes_history

    def get_time_sales(self, symbol, interval='tick', start=None, end=None, session_filter='all', ):
        """Get short term interval based prices of a symbol. Check API docs for intervals and availability periods."""
        path = "/markets/timesales"
        params = {
            'symbol': symbol,
            'interval': interval,
            'session_filter': session_filter
        }
        if start:
            params.update({'start': start})
        if end:
            params.update({'end': end})
        return self.get(path, params=params)

    def get_etb_securities(self):
        """Get list of stocks that can be shorted"""
        path = "/markets/etb"
        return self.get(path)

    def get_clock(self, delayed=False):
        """Get market clock and status"""
        path = "/markets/clock"
        params = {
            'delayed': delayed
        }
        return self.get(path, params=params)

    def get_calendar(self, month=None, year=None):
        """Get market trading calendar"""
        path = "/markets/calendar"
        params = None
        if month:
            params = params or {'month': month}
        if year:
            params = params or {}
            params.update({'year': year})
        return self.get(path, params=params)

    def get_markets_search(self, query, indexes=True):
        """Get details of symbols and where they are traded"""
        path = "/markets/search"
        params = {
            'q': query,
            'indexes': indexes
        }
        return self.get(path, params=params)

    def get_markets_lookup(self, query=None, exchanges=None, types=None):
        """Lookup details of a symbol and where it's traded. Same as companies"""
        path = "/markets/lookup"
        params = {}
        if query:
            params.update({'q': query})
        if exchanges:
            params.update({'exchanges': exchanges})
        if types:
            params.update({'types': types})
        return self.get(path, params=params or None)

    def get_company(self, symbols: list):
        """Get details of companies"""
        path = "/beta/markets/fundamentals/company"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_corporate_calendars(self, symbols: list):
        """Get detailed corporate actions of companies"""
        path = "/beta/markets/fundamentals/calendars"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_dividends(self, symbols: list):
        """Get dividends given out by companies"""
        path = "/beta/markets/fundamentals/dividends"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_corporate_actions(self, symbols: list):
        """Get corporate actions taken by companies"""
        path = "/beta/markets/fundamentals/corporate_actions"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_ratios(self, symbols: list):
        """Get fundamental ratios of companies on various dates """
        path = "/beta/markets/fundamentals/ratios"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_financial_reports(self, symbols: list):
        """Get financial filings and reports of copmanies"""
        path = "/beta/markets/fundamentals/financials"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def get_price_statistics(self, symbols: list):
        """Get price statistics of companies"""
        path = "/beta/markets/fundamentals/statistics"
        params = {
            'symbols': ",".join(symbols)
        }
        self.get(path, params=params)

    def __get_account_number(self):
        """
        Get account id from the server using the API key and set it in the client
        :return:
        """
        if not self.api_key:
            raise "API Key not set"
        account_details = self.get_user_profile()
        if account_details and account_details.get('profile') and account_details.get('profile').get('account'):
            account = account_details.get('profile').get('account')
            if isinstance(account, dict):
                account = [account]
                account_detail = account[0]
                return account_detail['account_number']
        return None

    def place_order(self, order: Order, preview: bool = False, timeout: float = 15.0, tag=None) -> (
            Dict)[
        str, Any]:
        """
        A more organized variant of place_order that takes in an order object. Each order contains order legs.

        :param order:
        :param preview:
        :param timeout:
        :param tag:
        :return:
        """
        path = f"/accounts/{self.account_number}/orders"
        form = order.to_form()
        if preview:
            form["preview"] = "true"
        if tag:
            form["tag"] = tag
        import json
        log_for_level(self.logger, logging.DEBUG, f"Sending order payload: {json.dumps(form)}")
        headers = {'Authorization': f'Bearer {self.api_key}'}
        r = self.prepare_and_send_request("post", path, data=form, headers=headers)
        return r


if __name__ == '__main__':
    pass
