"""
The REST client
"""
import logging
import os
import random
import threading
from datetime import datetime
from multiprocessing import AuthenticationError
from time import sleep
from typing import Optional, Any, Dict, Union, TypedDict

import requests
from requests import HTTPError, ReadTimeout, ConnectTimeout
from requests.exceptions import ConnectionError as RequestsConnectionError

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

    Retry support
    -------------
    This client supports **optional** retries for transient failures (timeouts, connection issues, HTTP 429, HTTP 5xx).

    - Retry is **opt-in per request** via `retry_allowed=True`.
    - Default retry policy is controlled by instance attributes:
      - `retry_attempts` (int): total attempts including the initial try (1 means no retries)
      - `retry_delay_seconds` (float): base delay between retries
      - `retry_backoff_factor` (float): exponential backoff factor applied per retry
      - `retry_jitter` (float): random +/- jitter added to the delay

    You can override `attempts`, `delay_seconds`, or `timeout` per call.
    """

    def __init__(
            self, base_url, api_key=None, account_number=None, api_key_env_prop=None,
            account_id_env_prop=None, config=None, verbose=False, extras: dict = None):
        """
        :param base_url: Tradier REST API base URL (e.g. https://sandbox.tradier.com/v1)
        :param api_key: Tradier API key (Bearer token)
        :param account_number: Optional account id. If not provided, it will be fetched from the server.
        :param api_key_env_prop: If `api_key` is not provided, fetch it from this env var.
        :param account_id_env_prop: If `account_number` is not provided, fetch it from this env var.
        :param config: Optional config dictionary, currently not used.
        :param verbose: Log level set to DEBUG if True, else INFO
        :param extras: Optional dict used to set additional attributes on the instance.
            This can include retry defaults:
            - session_id_refresh_attempts / session_id_refresh_delay (used elsewhere)
            - retry_attempts / retry_delay_seconds / retry_backoff_factor / retry_jitter
        """
        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        if extras and isinstance(extras, dict):
            for key, value in extras.items():
                self.__setattr__(key, value)

        # Optional retry policy defaults. Per-request retry is opt-in via retry_allowed.
        # Attempts includes the initial try (e.g., attempts=1 means no retries).
        self.retry_attempts = getattr(self, 'retry_attempts', 1)
        self.retry_delay_seconds = getattr(self, 'retry_delay_seconds', 1.0)
        self.retry_backoff_factor = getattr(self, 'retry_backoff_factor', 2.0)
        self.retry_jitter = getattr(self, 'retry_jitter', 0.1)

        self.api_key = api_key
        self.account_number = account_number
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
            self.account_number = account_number or self.__get_account_number()

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
            headers: dict = None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
            timeout: Optional[float] = None,
    ):
        """Prepare and send an HTTP request.

        This is the central request method used by most higher-level endpoint methods.

        :param http_method: HTTP method ("get", "post", "put", "delete")
        :param url_path: API path (e.g. "/markets/quotes")
        :param params: Query params dict (mutually exclusive with `data`)
        :param data: Form body dict (mutually exclusive with `params`)
        :param headers: Optional headers dict
        :param retry_allowed: If True, retry transient failures based on the retry policy.
            If False (default), failures raise immediately.
        :param attempts: Override instance `retry_attempts` for this request.
            Total attempts including the first try.
        :param delay_seconds: Override instance `retry_delay_seconds` for this request.
        :param timeout: Optional requests timeout passed to `requests.request`.
        :return: Parsed JSON response as a dict with an added `client_timestamp` field.
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

        return self.send_request(
            http_method,
            url_path,
            params,
            headers,
            data=data,
            timeout=timeout,
            retry_allowed=retry_allowed,
            attempts=attempts,
            delay_seconds=delay_seconds,
        )

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

    def _should_retry_http(self, response: requests.Response) -> bool:
        """Return True if the HTTP response is retryable (transient)."""
        try:
            status = int(getattr(response, 'status_code', 0) or 0)
        except Exception:
            return False
        # Retry on 429 and 5xx.
        return status == 429 or status >= 500

    def _sleep_with_jitter(self, seconds: float):
        if seconds is None:
            return
        jitter = 0.0
        try:
            jitter = float(self.retry_jitter or 0.0)
        except Exception:
            jitter = 0.0
        # Add +/- jitter on top of the delay.
        if jitter > 0:
            seconds = max(0.0, seconds + random.uniform(-jitter, jitter))
        sleep(seconds)

    def _calculate_retry_delay(self, attempt_index: int, base_delay: float) -> float:
        """attempt_index is 1-based for the *retry*, not the initial attempt."""
        try:
            factor = float(self.retry_backoff_factor or 1.0)
        except Exception:
            factor = 1.0
        if factor <= 1.0:
            return float(base_delay)
        return float(base_delay) * (factor ** max(0, attempt_index - 1))

    def send_request(
            self,
            http_method,
            url_path,
            params,
            headers,
            data=None,
            timeout=None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
    ):
        """Send an HTTP request.

        Implements optional retry behavior when `retry_allowed=True`.

        Retry behavior:
        - Retries on transient network errors: `ReadTimeout`, `ConnectTimeout`, `requests.ConnectionError`.
        - Retries on transient HTTP errors: status code 429 and 5xx.
        - Does not retry on other 4xx responses.

        :param http_method: HTTP method ("get", "post", "put", "delete").
        :param url_path: API path (e.g. "/markets/quotes").
        :param params: Query params, forwarded to `requests.request(params=...)`.
        :param headers: Request headers.
        :param data: Request form body, forwarded to `requests.request(data=...)`.
        :param timeout: Optional timeout forwarded to `requests.request`.
        :param retry_allowed: Enable/disable retry for this request.
        :param attempts: Override instance retry attempts.
        :param delay_seconds: Override instance base delay.
        """
        url = f"{self.http_base_url}{url_path}"

        log_for_level(self.logger, logging.DEBUG, f"Sending {http_method} request to {url}")
        if not self._can_request():
            raise self.RateLimitExceeded("Rate limit: need 5-call headroom or wait for expiry")

        attempts_allowed = int(attempts) if attempts is not None else int(getattr(self, 'retry_attempts', 1) or 1)
        attempts_allowed = max(1, attempts_allowed)
        base_delay = float(delay_seconds) if delay_seconds is not None else float(
            getattr(self, 'retry_delay_seconds', 1.0) or 1.0
        )

        last_exc: Optional[BaseException] = None
        last_response: Optional[requests.Response] = None

        for attempt_num in range(1, attempts_allowed + 1):
            client_timestamp = int(datetime.now().timestamp())
            try:
                response = requests.request(
                    http_method,
                    url,
                    params=params,
                    data=data,
                    headers=headers,
                    timeout=timeout
                )
                last_response = response

                log_for_level(self.logger, logging.DEBUG, f"Request URL: {response.request.url}")
                log_for_level(self.logger, logging.DEBUG, f"Request Headers: {response.request.headers}")
                log_for_level(self.logger, logging.DEBUG, f"Request Body: {response.request.body}")

                # Print response details
                log_for_level(self.logger, logging.DEBUG, f"Response Status Code: {response.status_code}")
                log_for_level(self.logger, logging.DEBUG, f"Response Headers: {response.headers}")
                log_for_level(self.logger, logging.DEBUG, f"Response Content: {response.text}")

                # Raise on bad responses, but allow retries for transient statuses.
                try:
                    self.handle_exception(response)
                except HTTPError as e:
                    last_exc = e
                    if retry_allowed and attempt_num < attempts_allowed and self._should_retry_http(response):
                        delay = self._calculate_retry_delay(attempt_num, base_delay)
                        log_for_level(self.logger, logging.WARNING,
                                      f"HTTP error retryable (attempt {attempt_num}/{attempts_allowed}); "
                                      f"sleeping {delay:.2f}s before retry")
                        self._sleep_with_jitter(delay)
                        continue
                    raise

                # Success path
                self.update_rate_limit(response)
                log_for_level(self.logger, logging.DEBUG, f"Raw response: {response.json()}")
                return {**response.json(), 'client_timestamp': client_timestamp}

            except (ReadTimeout, ConnectTimeout, RequestsConnectionError) as e:
                last_exc = e
                if retry_allowed and attempt_num < attempts_allowed:
                    delay = self._calculate_retry_delay(attempt_num, base_delay)
                    log_for_level(self.logger, logging.WARNING,
                                  f"Request failed with transient network error (attempt {attempt_num}/"
                                  f"{attempts_allowed}); "
                                  f"sleeping {delay:.2f}s before retry", exc_info=e)
                    self._sleep_with_jitter(delay)
                    continue
                raise

        # Shouldn't be reachable (loop returns/raises), but keep as a final fallback.
        if last_exc:
            raise last_exc
        if last_response is not None:
            self.handle_exception(last_response)
        return None

    def update_rate_limit(self, response):
        """Update `self.rate_limit` using rate-limit headers if present.

        :param response: `requests.Response`
        """
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

    def get(
            self,
            path,
            params: Optional[dict] = None,
            headers: dict = None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
            timeout: Optional[float] = None,
    ) -> Union[None, dict, list, int, str]:
        """HTTP GET wrapper.

        :param path: API path (e.g. "/markets/quotes").
        :param params: Optional query params dict.
        :param headers: Optional headers dict.
        :param retry_allowed: If True, retry transient failures based on the retry policy.
        :param attempts: Override instance retry attempts.
        :param delay_seconds: Override instance base delay.
        :param timeout: Optional timeout forwarded to `requests.request`.
        """
        response = self.prepare_and_send_request(
            "get",
            path,
            params,
            headers=headers,
            retry_allowed=retry_allowed,
            attempts=attempts,
            delay_seconds=delay_seconds,
            timeout=timeout,
        )
        return response

    def post(
            self,
            path,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
            headers: dict = None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
            timeout: Optional[float] = None,
    ):
        """HTTP POST wrapper.

        :param path: API path.
        :param params: Optional query params dict.
        :param data: Optional form body dict.
        :param headers: Optional headers dict.
        :param retry_allowed: If True, retry transient failures based on the retry policy.
        :param attempts: Override instance retry attempts.
        :param delay_seconds: Override instance base delay.
        :param timeout: Optional timeout forwarded to `requests.request`.
        """
        response = self.prepare_and_send_request(
            "post",
            path,
            params=params,
            data=data,
            headers=headers,
            retry_allowed=retry_allowed,
            attempts=attempts,
            delay_seconds=delay_seconds,
            timeout=timeout,
        )
        return response

    def put(
            self,
            path,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
            headers: dict = None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
            timeout: Optional[float] = None,
    ):
        """HTTP PUT wrapper.

        :param path: API path.
        :param params: Optional query params dict.
        :param data: Optional form body dict.
        :param headers: Optional headers dict.
        :param retry_allowed: If True, retry transient failures based on the retry policy.
        :param attempts: Override instance retry attempts.
        :param delay_seconds: Override instance base delay.
        :param timeout: Optional timeout forwarded to `requests.request`.
        """
        response = self.prepare_and_send_request(
            "put",
            path,
            params=params,
            data=data,
            headers=headers,
            retry_allowed=retry_allowed,
            attempts=attempts,
            delay_seconds=delay_seconds,
            timeout=timeout,
        )
        return response

    def delete(
            self,
            path,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
            headers: dict = None,
            retry_allowed: bool = False,
            attempts: Optional[int] = None,
            delay_seconds: Optional[float] = None,
            timeout: Optional[float] = None,
    ):
        """HTTP DELETE wrapper.

        :param path: API path.
        :param params: Optional query params dict.
        :param data: Optional form body dict.
        :param headers: Optional headers dict.
        :param retry_allowed: If True, retry transient failures based on the retry policy.
        :param attempts: Override instance retry attempts.
        :param delay_seconds: Override instance base delay.
        :param timeout: Optional timeout forwarded to `requests.request`.
        """
        response = self.prepare_and_send_request(
            "delete",
            path,
            params=params,
            data=data,
            headers=headers,
            retry_allowed=retry_allowed,
            attempts=attempts,
            delay_seconds=delay_seconds,
            timeout=timeout,
        )
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
        return self.post(path, headers=headers, data={}, retry_allowed=True)

    def create_market_session(self, api_key):
        """Create market events session"""
        headers = {
            'Authorization': f'Bearer {api_key}',
        }
        path = f"/markets/events/session"
        return self.post(path, headers=headers, data={}, retry_allowed=True)

    def authenticated(self):
        """Check if API key is present"""
        return self.api_key is not None

    def get_user_profile(self, api_key=None):
        """Get User Profile"""
        path = "/user/profile"
        headers = {'Authorization': f'Bearer {self.api_key}'} if not api_key else {'Authorization': f'Bearer {api_key}'}
        return self.get(path, headers=headers)

    def get_balances(self, account_id, retry: bool = False):
        """Get account balances"""
        path = f"/accounts/{account_id}/balances"
        return self.get(path, retry_allowed=retry)

    def get_positions(self, account_id, retry: bool = False):
        """Get Account Positions"""
        path = f"/accounts/{account_id}/positions"
        return self.get(path, retry_allowed=retry)

    def get_history(
            self, account_id, page=1, limit=10000, history_type: list = None, start: str = None,
            end: str = None, symbol: str = None,
            exactMatch=True, api_key=None, retry: bool = False):
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
        return self.get(path, params=params, headers=headers, retry_allowed=retry)

    def get_gain_loss(self, account_id, retry: bool = False):
        """Get Current Gain Los"""
        path = f"/accounts/{account_id}/gainloss"
        return self.get(path, retry_allowed=retry)

    def get_orders(self, account_id, include_tags=True, retry: bool = False):
        """Get all orders for the market session of the present calendar day.

        Not to be used for historical orders.

        :param account_id: Account id.
        :param include_tags: Include order tags in the response.
        :param retry: If True, retry transient failures based on the retry policy.
        """
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
                response = self.get(path, params=params, retry_allowed=retry)
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

    def get_order(self, account_id, order_id, include_tags=True, retry: bool = False):
        """Get an order's details.

        :param account_id: Account id.
        :param order_id: Order id.
        :param include_tags: Include order tags in the response.
        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = f"/accounts/{account_id}/orders/{order_id}"
        params = {
            'includeTags': include_tags
        }
        return self.get(path, params=params, retry_allowed=retry)

    def modify_order(
            self, account_id, order_id, order_type=None, duration=None, price=None, stop=None,
            retry: bool = False):
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
        return self.put(path, data=data, retry_allowed=retry)

    def cancel_order(self, account_id=None, order_id=None, retry: bool = False):
        """Cancel an order"""
        path = f"/accounts/{account_id}/orders/{order_id}"
        return self.delete(path, retry_allowed=retry)

    def get_quotes(self, symbols, greeks=False, retry: bool = False):
        """Get quotes for symbols"""
        path = "/markets/quotes"
        params = {
            'symbols': symbols,
            'greeks': greeks
        }
        return self.get(path, params=params, retry_allowed=retry)

    def get_quotes_larger(self, symbols, greeks=False, retry: bool = False):
        """Get quotes via POST that supports larger set of symbols"""
        path = "/markets/quotes"
        data = {
            'symbols': symbols,
            'greeks': greeks
        }
        return self.post(path, data=data, retry_allowed=retry)

    def get_option_chains(self, symbol, expiration, greeks=False, retry: bool = False):
        """Get options chains"""
        path = "/markets/options/chains"
        params = {
            'symbol': symbol,
            'expiration': expiration,
            'greeks': greeks
        }
        return self.get(path, params=params, retry_allowed=retry)

    def get_option_strikes(self, symbol, expiration, include_all_roots=True, retry: bool = False):
        """Get options strikes"""
        path = "/markets/options/strikes"
        params = {
            'symbol': symbol,
            'expiration': expiration,
            'includeAllRoots': include_all_roots
        }
        return self.get(path, params=params, retry_allowed=retry)

    def get_option_expirations(
            self, symbol, include_all_roots=True, strikes=False, contract_size=False,
            expiration_type=False, retry: bool = False):
        """Get options expirations"""
        path = "/markets/options/expirations"
        params = {
            'symbol': symbol,
            'includeAllRoots': include_all_roots,
            'strikes': strikes,
            'contractSize': contract_size,
            'expirationType': expiration_type
        }
        return self.get(path, params=params, retry_allowed=retry)

    def lookup_option_symbols(
            self, symbol, includeAllRoots=True, strikes=True, contractSize=True,
            expirationType=True, retry: bool = False):
        """Get all options symbols for an underlying symbol"""
        path = "/markets/options/expirations"
        params = {
            'symbol': symbol,
            'includeAllRoots': includeAllRoots,
            'strikes': strikes,
            'contractSize': contractSize,
            'expirationType': expirationType
        }
        return self.get(path, params=params, retry_allowed=retry)

    def lookup_options_symbols(self, underlying, strike=None, expiration=None, option_type=None, retry: bool = False):
        """Get options symbols for an underlying symbol.

        :param underlying: Underlying symbol.
        :param strike: Optional strike filter.
        :param expiration: Optional expiration date (YYYY-MM-DD).
        :param option_type: Optional option type (call/put).
        :param retry: If True, retry transient failures based on the retry policy.
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
        return self.prepare_and_send_request("get", path, params=params, retry_allowed=retry)

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

    def get_time_sales(self, symbol, interval='tick', start=None, end=None, session_filter='all', retry: bool = False):
        """Get short term interval based prices of a symbol.

        Check API docs for intervals and availability periods.

        :param symbol: Symbol.
        :param interval: Interval (e.g. "tick").
        :param start: Optional start time/date.
        :param end: Optional end time/date.
        :param session_filter: Session filter.
        :param retry: If True, retry transient failures based on the retry policy.
        """
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
        return self.get(path, params=params, retry_allowed=retry)

    def get_etb_securities(self, retry: bool = False):
        """Get list of stocks that can be shorted.

        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = "/markets/etb"
        return self.get(path, retry_allowed=retry)

    def get_clock(self, delayed=False, retry: bool = False):
        """Get market clock and status.

        :param delayed: If True, request delayed clock.
        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = "/markets/clock"
        params = {
            'delayed': delayed
        }
        return self.get(path, params=params, retry_allowed=retry)

    def get_calendar(self, month=None, year=None, retry: bool = False):
        """Get market trading calendar.

        :param month: Optional month.
        :param year: Optional year.
        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = "/markets/calendar"
        params = None
        if month:
            params = params or {'month': month}
        if year:
            params = params or {}
            params.update({'year': year})
        return self.get(path, params=params, retry_allowed=retry)

    def get_markets_search(self, query, indexes=True, retry: bool = False):
        """Get details of symbols and where they are traded.

        :param query: Search query.
        :param indexes: Include indexes.
        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = "/markets/search"
        params = {
            'q': query,
            'indexes': indexes
        }
        return self.get(path, params=params, retry_allowed=retry)

    def get_markets_lookup(self, query=None, exchanges=None, types=None, retry: bool = False):
        """Lookup details of a symbol and where it's traded.

        Same as companies.

        :param query: Optional query.
        :param exchanges: Optional exchanges filter.
        :param types: Optional types filter.
        :param retry: If True, retry transient failures based on the retry policy.
        """
        path = "/markets/lookup"
        params = {}
        if query:
            params.update({'q': query})
        if exchanges:
            params.update({'exchanges': exchanges})
        if types:
            params.update({'types': types})
        return self.get(path, params=params or None, retry_allowed=retry)

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

    def place_order(self, order: Order, preview: bool = False, timeout: float = 15.0, tag=None) -> Dict[str, Any]:
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
