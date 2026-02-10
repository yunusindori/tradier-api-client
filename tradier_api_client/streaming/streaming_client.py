"""
Stream Client
"""
import json
import logging
import threading
import time
import traceback
import random
from functools import partial
from queue import Queue
from typing import Optional

from ..helper_functions import log_for_level
from ..rest.rest_client import RestClient
from ..streaming.websocket_stream import StreamListener


class StreamingClient:
    """WebSocket client.

    Only one session is allowed to be created for each stream type so it's up to you to only create
    one instance each of this class for each stream type.

    Reconnect policy
    ----------------
    The client can automatically restart streams when the server closes the websocket.

    If reconnection fails for longer than the configured attempt/downtime budget, the client will
    invoke the mandatory `irrecoverable_callback` so the caller can decide what to do next.
    """

    def __init__(
            self,
            main_api_key,
            base_url,
            stream_base_url,
            main_account_id=None,
            additional_api_keys=None,
            additional_account_ids=None,
            stream_type='account',
            config=None,
            verbose=False,
            events_destination=None,
            events_callback=None,
            logger: logging.Logger = None,
            reconnect_attempts: int = 10,
            reconnect_base_delay_seconds: float = 1.0,
            reconnect_backoff_factor: float = 2.0,
            reconnect_jitter_seconds: float = 0.1,
            reconnect_max_downtime_seconds: float = 300.0,
            irrecoverable_callback=None,
    ):
        """Create a StreamingClient.

        Can be used to listen to account events or market events.

        :param main_api_key: Primary trading account's api_key.
        :param base_url: REST base URL used to obtain session ids.
        :param stream_base_url: Websocket base URL.
        :param main_account_id: Optional primary account id. If not provided, it is fetched via REST.
        :param additional_api_keys: Optional list of additional API keys.
        :param additional_account_ids: Optional list of additional account ids.
        :param stream_type: One of: "market" or "account".
        :param config: Reserved for future config usage.
        :param verbose: Log level will be set to DEBUG if True, else INFO.
        :param events_destination: A queue.Queue instance where messages will be delivered.
        :param events_callback: Callback invoked with each decoded message. Only one of destination/callback allowed.
        :param logger: Optional logger to use.

        Reconnect parameters
        --------------------
        :param reconnect_attempts: Maximum reconnect attempts before declaring the stream irrecoverable.
            Total attempts including the first reconnect attempt.
        :param reconnect_base_delay_seconds: Base delay (seconds) between reconnect attempts.
        :param reconnect_backoff_factor: Exponential backoff factor applied per attempt (>= 1.0).
        :param reconnect_jitter_seconds: Optional jitter (seconds) added to reconnect delay to reduce thundering herd
            problems. Default is 0.1 seconds.
        :param reconnect_max_downtime_seconds: Maximum wall-clock time (seconds) allowed since the first reconnect
            attempt before declaring irrecoverable.
        :param irrecoverable_callback: Mandatory callback invoked once when the stream is irrecoverable.

            Signature:
                irrecoverable_callback(stream_type: str, reason: str, stream_key: str,
                                       close_status_code: Optional[int], close_msg: Optional[str],
                                       attempts: int, downtime_seconds: float) -> None
        """
        self.logger = (logger if logger else logging.getLogger(__name__))
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        if irrecoverable_callback is None or not callable(irrecoverable_callback):
            raise Exception("irrecoverable_callback is mandatory and must be callable")

        # Indicates an intentional/active shutdown. Used to suppress noisy callback exceptions
        # that can race with websocket-client internals during SIGINT/Ctrl-C.
        self._is_shutting_down = False

        self.reconnect_attempts = int(reconnect_attempts)
        self.reconnect_base_delay_seconds = float(reconnect_base_delay_seconds)
        self.reconnect_backoff_factor = float(reconnect_backoff_factor)
        self.reconnect_jitter_seconds = float(reconnect_jitter_seconds)
        self.reconnect_max_downtime_seconds = float(reconnect_max_downtime_seconds)
        self.irrecoverable_callback = irrecoverable_callback

        # Reconnect state
        self._reconnect_lock = threading.Lock()
        self._reconnect_in_progress = False
        self._reconnect_first_failure_ts: Optional[float] = None
        self._reconnect_attempt_count = 0
        self._irrecoverable_signaled = False

        self.__validate_init_inputs(main_api_key, main_account_id, additional_api_keys,
                                    additional_account_ids, stream_type, events_destination, events_callback)
        self.NUM_SYMBOLS_PER_STREAM = 1200
        self.api_keys = [main_api_key]
        self.rest_client = RestClient(base_url=base_url, api_key=self.api_keys[0], verbose=verbose)
        self.api_keys.extend([] if not additional_api_keys else additional_api_keys)
        if not main_account_id:
            log_for_level(self.logger, logging.DEBUG, "Getting main account id...")
            profile = self.rest_client.get_user_profile(main_api_key)
            main_account_id = profile['profile']['account'][0]['account_number'] if \
                isinstance(profile['profile']['account'], list) else \
                profile['profile']['account']['account_number']
        self.account_ids = [main_account_id]
        self.account_ids.extend([] if not additional_account_ids else additional_account_ids)
        self.stream_type = stream_type
        self.events_destination = events_destination
        self.events_callback = events_callback
        if len(self.api_keys) > 1 and len(self.account_ids) == 1:
            log_for_level(self.logger, logging.DEBUG, "Getting additional account ids...")
            for api_key in self.api_keys[1:]:
                if api_key is not None and api_key.strip():
                    profile = self.rest_client.get_user_profile(api_key)
                    self.account_ids.append(profile['profile']['account'][0]['account_number'] if
                                            isinstance(profile['profile']['account'], list) else
                                            profile['profile']['account']['account_number'])
        self.stream_type = stream_type
        self.streaming_base_url = stream_base_url
        log_for_level(self.logger, logging.INFO, f"Using websockets base URL: {self.streaming_base_url}")
        self.account_id_to_api_key = dict(zip(self.account_ids, self.api_keys))
        # Dictionary of api key to market stream
        self.stream_started = False
        self.symbols_listened_to = []
        self.event_types = []
        self.market_streams_count = 1
        self.stream_path = "/markets/events" if stream_type == "market" else "/accounts/events" if \
            stream_type == "account" else None
        self.events_streams = None
        self.stop_me = False

        # Used to reliably stop background threads and avoid "old" threads running after restarts.
        self._stop_event = threading.Event()
        self._run_id = 0

        # Single maintenance thread (replaces keep_alive_thread + session_id_refresher).
        self.maintenance_thread = threading.Thread(target=self.run_maintenance_loop, daemon=True)

    def __validate_init_inputs(
            self, main_api_key, main_account_id, additional_api_keys,
            additional_account_ids, stream_type,
            events_destination, events_callback):
        """Initial validation of inputs."""
        log_for_level(self.logger, logging.DEBUG, "Validating inputs...")
        if not main_api_key or not isinstance(main_api_key, str) or not main_api_key.strip():
            raise Exception("main_api_key is mandatory")
        if main_account_id and (not isinstance(main_account_id, str) or not main_account_id.strip()):
            raise Exception("Invalid main account id, please pass a valid string or leave blank")
        if stream_type not in ['account', 'market']:
            raise Exception("stream_type can only be one of account and market")
        if events_destination and events_callback or not (events_destination or events_callback):
            raise Exception('One and only one of callback or destination can be specified.')
        if events_destination and not isinstance(events_destination, Queue):
            raise Exception("events_destination must be an instance of queue.Queue, or use a callback to handle the "
                            "message yourself.")
        if additional_api_keys and len(additional_api_keys) > 0 and not all([str and isinstance(
                val, str) and val.strip() for val in additional_api_keys]):
            raise Exception("Please pass valid api keys for additional_api_keys or leave blank if no needed")
        if additional_account_ids and len(additional_account_ids) > 0 and not all(
                [str and isinstance(val, str) and val.strip() for val in additional_account_ids]):
            raise Exception("Please pass valid account ids for additional_account_ids or leave blank if no needed")
        unique_additional_api_keys_length = len(list(set(additional_api_keys))) if isinstance(additional_api_keys,
                                                                                              list) else 0
        unique_additional_account_ids_length = len(list(set(additional_account_ids))) if isinstance(
            additional_account_ids, list) else 0
        if (unique_additional_account_ids_length > 0 and unique_additional_account_ids_length != len(
                additional_account_ids)) or (unique_additional_api_keys_length > 1 and
                                             unique_additional_api_keys_length != len(additional_api_keys)):
            raise Exception("Duplicate values passed in for api keys or account ids, please pass only unique values")
        if 0 < unique_additional_account_ids_length != unique_additional_api_keys_length:
            raise Exception("Additional account ids if passed must be unique and the same number as unique api keys")
        # Ensure the callback is callable here too, to keep validation centralized.
        if not getattr(self, 'irrecoverable_callback', None) or not callable(getattr(self, 'irrecoverable_callback')):
            raise Exception('irrecoverable_callback is mandatory and must be callable')
        log_for_level(self.logger, logging.DEBUG, "Inputs validated...")

    def start_listening(self, symbols: list = None, event_types: list = None):
        """
        Starts listening to account or market events depending on the type of the stream. If it's a market
        stream, list of symbols is mandatory. if events_types is not passed it will default to all events.

        :param symbols: Only needed in case of market stream type. Each API Key allows to listen to
        self.NUM_SYMBOLS_PER_STREAM symbols.
        :param event_types: If omitted, all market events will be sent to the destination or callback.
        Raises:
            Exception: In case the stream fails to start or even create. Make sure the surround the call to this
            method in a try/except block, else failures can lead to unexpected behavior.
        """
        if self.stream_started:
            raise Exception("Stream already started, use update to make changes to the stream")
        self.events_streams = self.events_streams or {}
        try:
            # New run generation for worker threads.
            self._run_id += 1
            self._stop_event.clear()

            if self.stop_me:
                self.stop_me = False
            log_for_level(self.logger, logging.INFO, f"Using streaming base URL: {self.streaming_base_url}")
            if self.stream_type == 'market':
                log_for_level(self.logger, logging.INFO, "Starting market stream...")
                if not symbols or not isinstance(symbols, list) or len(symbols) == 0:
                    raise Exception("Pass a list of symbols")
                symbols_listened_to = []
                symbol_buckets = [symbols[i:i + self.NUM_SYMBOLS_PER_STREAM] for i in
                                  range(0, len(symbols), self.NUM_SYMBOLS_PER_STREAM)]
                if len(self.account_ids) < len(symbol_buckets):
                    log_for_level(
                        self.logger,
                        logging.INFO,
                        f"Insufficient API keys to listen to all symbols, only {len(self.account_ids)} streams will "
                        f"be created",
                    )
                elif len(self.account_ids) > len(symbol_buckets):
                    log_for_level(
                        self.logger,
                        logging.INFO,
                        f"{len(symbols)} symbols require {len(symbol_buckets)} API keys but only {len(self.api_keys)} "
                        f"are available",
                    )
                num_streams = len(symbol_buckets) if len(self.account_ids) > len(symbol_buckets) else len(
                    self.account_ids)
                for stream_index in range(num_streams):
                    account_id = self.account_ids[stream_index]
                    self._build_stream_dict_for_account_id(account_id, symbol_buckets[stream_index])
                    if event_types and isinstance(event_types, list):
                        self.events_streams[account_id]['event_types'] = event_types
                    self.event_types = event_types
                    # Initialize the stream later now so that symbols can be used in the open callback
                    log_for_level(self.logger, logging.INFO, "Starting websocket connection...")
                    self.events_streams[account_id]['stream'] = self._build_stream_listener_for_account_id(account_id)
                    symbols_listened_to.extend(symbol_buckets[stream_index])
                    self.events_streams[account_id]['stream'].start()
                    log_for_level(self.logger, logging.INFO, "Market stream started...")
                self.symbols_listened_to = symbols_listened_to
            else:
                log_for_level(self.logger, logging.INFO, "Starting account stream...")
                account_id = self.account_ids[0]
                self._build_stream_dict_for_account_id(account_id)
                self.events_streams[self.account_ids[0]]['stream'] = self._build_stream_listener_for_account_id(
                    account_id)
                self.events_streams[self.account_ids[0]]['stream'].start()
                log_for_level(self.logger, logging.INFO, "Account stream started...")
            # Start the maintenance thread for session refresh and keep-alive
            self.maintenance_thread.start()
            self.stream_started = True
            # Reconnect state resets on successful start.
            self._reconnect_first_failure_ts = None
            self._reconnect_attempt_count = 0
            self._irrecoverable_signaled = False
        except Exception as e:
            log_for_level(self.logger, logging.ERROR, "Failed to create or start stream: ", exc_info=e)
            log_for_level(self.logger, logging.ERROR, "Stopping the stream client...")
            try:
                self.stop()
                log_for_level(self.logger, logging.INFO, "Stopped the stream client")
            except Exception as ae:
                self.logger.exception("Another error occurred while trying to stop the stream...", exc_info=ae)
            raise e

    def _build_stream_listener_for_account_id(self, account_id):
        return StreamListener(
            base_url=self.streaming_base_url,
            stream_path=self.stream_path,
            on_connect_callback=partial(self.handle_open, account_id),
            on_message_callback=partial(self.handle_message, account_id),
            on_error_callback=partial(self.handle_error, account_id),
            on_disconnect_callback=partial(self.handle_close, account_id),
            logger=self.logger)

    def _build_stream_dict_for_account_id(self, account_id, symbols: Optional[list] = None):
        """

        :param account_id:
        :param symbols:
        """
        self.events_streams[account_id] = {
            "stream": None,
            'session_id': None,
            'session_id_last_updated': None,
            'symbols': symbols if symbols and isinstance(symbols, list) else None,
            'event_types': None,
            'last_event_timestamp': None
        }

    def restart_streams(self):
        """
        Restart listening to market or account events if stopped by calling the stop method.
        """
        log_for_level(self.logger, logging.INFO, "Restarting stream(s)...")
        if self.stream_started:
            raise Exception("Stream already started, use update to make changes to the stream")
        # Create a fresh Thread object for the maintenance worker so it can be started again.
        self.maintenance_thread = threading.Thread(target=self.run_maintenance_loop, daemon=True)
        self.events_streams = None
        # If stream_type is account, symbols and events will be ignored.
        self.start_listening(symbols=self.symbols_listened_to, event_types=self.event_types)
        log_for_level(self.logger, logging.INFO, "Stream(s) restarted...")

    def update(self, symbols=None, event_types=None):
        """
        Only makes sense to call this method for a market stream since Tradier doesn't support event types other than
         order for account stream. It raises an exception if called for account stream type.

        Pass a new list of symbols and event types to listen to.

        One of symbols or event_types is mandatory and the stream will be updated accordingly.
        :param symbols:
        :param event_types:
        """
        if self.stream_type == 'account':
            raise Exception("Account stream can only be created once, and cannot be updated.")
        if not symbols and not event_types:
            raise Exception("One of symbols or event_types is required. Both can be passed too.")
        elif symbols and not isinstance(symbols, list):
            raise Exception("symbols must be a list.")
        elif event_types and not isinstance(event_types, list):
            raise Exception("event_types must be a list.")
        self.stop()
        self.symbols_listened_to = symbols
        self.event_types = event_types
        self.restart_streams()

    # noinspection PyUnusedLocal
    def handle_open(self, stream_key):
        """Called by StreamListener when the websocket connection is opened.

        This method creates/refreshes a session id as needed and sends the initial subscription payload.

        :param stream_key: Account id key for the stream (used to look up the API key and stream metadata).
        """
        # During shutdown, websocket-client can still deliver on_open. Bail out quietly.
        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            return

        self.check_session_id_for_stream(stream_key)
        session_id = self.events_streams[stream_key]['session_id']
        if self.stream_type == 'account':
            initial_payload = self.build_account_stream_payload(events=['orders'], session_id=session_id)
            log_for_level(self.logger, logging.INFO, f"Sending payload to stream: {initial_payload}")
            try:
                stream = self.events_streams[stream_key]['stream']
                if stream is None or not stream.is_running():
                    # Don't raise from callbacks; it just creates noisy stack traces.
                    log_for_level(self.logger, logging.INFO, "Stream is not connected (account) - skipping send")
                    return
                stream.update_stream(json.dumps(initial_payload))
            except Exception as e:
                # Suppress expected send errors during teardown.
                if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
                    log_for_level(self.logger, logging.INFO,
                                  "Suppressed exception while updating account stream during shutdown",
                                  exc_info=e)
                    return
                log_for_level(self.logger, logging.ERROR, "An error occurred while updating account stream: ",
                              exc_info=e)
        elif self.stream_type == 'market':
            symbols = self.events_streams[stream_key]['symbols']
            initial_payload = self.build_market_stream_payload(symbols=symbols, session_id=session_id)
            log_for_level(self.logger, logging.INFO, f"Sending initial payload to stream: {initial_payload}")
            try:
                stream = self.events_streams[stream_key]['stream']
                if stream is None or not stream.is_running():
                    log_for_level(self.logger, logging.INFO, "Stream is not connected (market) - skipping send")
                    return
                stream.update_stream(json.dumps(initial_payload))
            except Exception as e:
                if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
                    log_for_level(self.logger, logging.INFO,
                                  "Suppressed exception while updating market stream during shutdown",
                                  exc_info=e)
                    return
                log_for_level(self.logger, logging.ERROR, "An error occurred while updating stream: ", exc_info=e)

    def _get_session_id_from_server(self, api_key: str) -> str:
        """Get a new session id from the server.

        :param api_key: API key to use for creating the session.
        :return: Session id string.
        """
        return self.rest_client.create_market_session(api_key)['stream']['sessionid'] if self.stream_type == 'market' else \
            self.rest_client.create_account_session(api_key)['stream']['sessionid']

    def check_session_id(self):
        """Check and refresh session ids for all streams."""
        try:
            if self.events_streams and isinstance(self.events_streams, dict) and len(self.events_streams.keys()) > 0:
                log_for_level(self.logger, logging.INFO, "Checking session ids for all streams...")
                for account_id in self.events_streams.keys():
                    self.check_session_id_for_stream(account_id)
        except Exception:
            log_for_level(self.logger, logging.WARNING, traceback.format_exc())

    def check_session_id_for_stream(self, stream_key):
        """Check and refresh session id for a stream.

        :param stream_key: Account id key for the stream.
        """
        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            return
        stream_dict = self.events_streams[stream_key]
        if self._session_id_refresh_condition_met(stream_key, stream_dict):
            log_for_level(self.logger, logging.INFO, f"Getting a new session id for: {stream_key}...")
            api_key = self.account_id_to_api_key[stream_key]
            stream_dict['session_id'] = self._get_session_id_from_server(api_key)
            stream_dict['session_id_last_updated'] = time.time()
            log_for_level(self.logger, logging.INFO, f"Got new session id for: {stream_key}...")

    def _session_id_refresh_condition_met(self, stream_key, stream_dict):
        """Return True if a session id refresh is needed."""
        log_for_level(self.logger, logging.INFO, "Checking whether to obtain a new session id...")

        missing_session_id = (
                'session_id' not in stream_dict.keys() or
                not isinstance(stream_dict.get('session_id'), str) or
                not stream_dict.get('session_id')
        )
        ws_running = bool(stream_dict.get('stream') is not None and stream_dict.get('stream').is_running())

        get_new = missing_session_id or (ws_running and (
                'session_id_last_updated' not in stream_dict.keys() or
                not isinstance(stream_dict.get('session_id_last_updated'), float))) or \
                  (isinstance(stream_dict.get('session_id_last_updated'), float) and
                   time.time() - stream_dict['session_id_last_updated'] > 1800) or self._was_last_event_long_ago(
            stream_dict)
        if get_new:
            log_for_level(self.logger, logging.INFO, f"Need new session id for {stream_key}...")
        return get_new

    # noinspection PyMethodMayBeStatic
    def build_market_stream_payload(
            self, symbols, session_id, payload_type_filter: Optional[list] = None, linebreak=True,
            valid_only=True, advanced_details=True):
        """Build market stream request payload."""
        payload = {
            'symbols': symbols,
            'sessionid': session_id,
            'linebreak': linebreak,
            'validOnly': valid_only,
            'advancedDetails': advanced_details
        }
        if isinstance(payload_type_filter, list) and len(payload_type_filter) > 0:
            payload['filter'] = payload_type_filter
        return payload

    # noinspection PyMethodMayBeStatic
    def build_account_stream_payload(self, events, session_id, exclude_accounts: Optional[list] = None):
        """Build account stream request payload."""
        payload = {
            'events': events,
            'sessionid': session_id
        }
        if exclude_accounts and isinstance(exclude_accounts, list) and len(exclude_accounts) > 0:
            payload['excludeAccounts'] = exclude_accounts
        return payload

    def refresh_session_id(self):
        """Deprecated: use the combined maintenance loop.

        This method is kept for backward compatibility and for any external callers.
        It now runs a single session-id check.
        """
        self.check_session_id()

    def run_maintenance_loop(self):
        """Background maintenance loop.

        This thread performs:
        1) Periodic session-id refresh checks for all streams.
        2) Market stream keep-alive behavior (send a ping/update when no events are received).

        It replaces the earlier two-thread approach (session_id_refresher + keep_alive_thread).
        """
        my_run_id = self._run_id
        last_session_check = 0.0
        while not self.stop_me and not self._stop_event.is_set() and my_run_id == self._run_id:
            now = time.time()

            if now - last_session_check >= 60:
                try:
                    self.check_session_id()
                except Exception:
                    log_for_level(self.logger, logging.WARNING, traceback.format_exc())
                last_session_check = now

            if self.stream_type == 'market':
                self._maybe_send_market_keepalive()

            self._stop_event.wait(1)

    def handle_close(self, stream_key, close_status_code=None, close_msg=None):
        """Handle websocket close event.

        For market streams, any single stream disconnect triggers a full stop + restart of all streams to ensure
        all streams use fresh session ids.

        :param stream_key: Account id key of the closed stream.
        :param close_status_code: Optional websocket close status code.
        :param close_msg: Optional websocket close message.
        """
        log_for_level(self.logger, logging.INFO, f"Stream for account id {stream_key} closed...")
        log_for_level(self.logger, logging.INFO,
                      f"Close status code: {close_status_code}, close message: {close_msg}")
        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            return

        with self._reconnect_lock:
            if self._reconnect_in_progress:
                return
            self._reconnect_in_progress = True

        try:
            if self._reconnect_first_failure_ts is None:
                self._reconnect_first_failure_ts = time.time()
                self._reconnect_attempt_count = 0

            self.stream_started = False

            while not self.stop_me:
                self._reconnect_attempt_count += 1
                downtime = time.time() - float(self._reconnect_first_failure_ts)

                if self._reconnect_attempt_count > self.reconnect_attempts or downtime > self.reconnect_max_downtime_seconds:
                    if not self._irrecoverable_signaled:
                        self._irrecoverable_signaled = True
                        reason = "reconnect budget exceeded"
                        try:
                            self.irrecoverable_callback(
                                self.stream_type,
                                reason,
                                str(stream_key),
                                close_status_code,
                                close_msg,
                                int(self._reconnect_attempt_count),
                                float(downtime),
                            )
                        except Exception:
                            log_for_level(self.logger, logging.ERROR, "Error while invoking irrecoverable_callback",
                                          exc_info=True)
                    return

                delay = self.reconnect_base_delay_seconds * (
                        self.reconnect_backoff_factor ** max(0, self._reconnect_attempt_count - 1)
                )
                if self.reconnect_jitter_seconds and self.reconnect_jitter_seconds > 0:
                    delay = max(0.0, delay + random.uniform(-self.reconnect_jitter_seconds, self.reconnect_jitter_seconds))
                log_for_level(
                    self.logger,
                    logging.INFO,
                    f"Reconnect attempt {self._reconnect_attempt_count}/{self.reconnect_attempts} in {delay:.2f}s",
                )
                self._stop_event.wait(delay)
                if self.stop_me:
                    return

                try:
                    self.stop()
                    if self.events_streams and isinstance(self.events_streams, dict):
                        for k in list(self.events_streams.keys()):
                            stream_dict = self.events_streams.get(k)
                            if isinstance(stream_dict, dict):
                                stream_dict['session_id'] = None
                                stream_dict['session_id_last_updated'] = None

                    self.restart_streams()
                    self._reconnect_first_failure_ts = None
                    self._reconnect_attempt_count = 0
                    self._irrecoverable_signaled = False
                    return
                except Exception as e:
                    log_for_level(self.logger, logging.WARNING, "Reconnect attempt failed", exc_info=e)
        finally:
            with self._reconnect_lock:
                self._reconnect_in_progress = False

    def keep_market_stream_alive(self):
        """Deprecated: use the combined maintenance loop.

        This method is kept for backward compatibility and for any external callers.
        It now performs a single keep-alive pass.
        """
        if self.stream_type == 'market':
            self._maybe_send_market_keepalive()

    def _maybe_send_market_keepalive(self):
        """Send keep-alive pings for market streams when no events are received.

        This preserves the behavior of the original keep_alive_thread, but is invoked from
        the combined maintenance loop.
        """
        # Defensive: events_streams may be None or mutated during restart/stop.
        if not self.events_streams or not isinstance(self.events_streams, dict):
            return

        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            return

        for key, stream_dict in list(self.events_streams.items()):
            if not key or not stream_dict:
                continue
            try:
                self.check_session_id_for_stream(key)
                if self._was_last_event_long_ago(stream_dict):
                    stream = stream_dict.get('stream')
                    if stream is not None and stream.is_running():
                        stream.send_ping(json.dumps(
                            self.build_market_stream_payload(symbols=stream_dict.get('symbols'),
                                                             session_id=stream_dict.get('session_id'))))
            except Exception:
                # Catch per-stream errors to avoid killing the loop
                log_for_level(self.logger, logging.ERROR,
                              "Error issuing keep-alive for market stream", exc_info=True)

    def stop(self):
        """Stop the stream client and all connected sockets and associated threads.

        This method is idempotent and safe to call from signal handlers (e.g., Ctrl-C).
        """
        # Idempotent shutdown.
        if getattr(self, '_is_shutting_down', False) and self.stop_me:
            self.stream_started = False
            return

        log_for_level(self.logger, logging.INFO, "Stopping all streams and threads...")
        self._is_shutting_down = True
        self.stop_me = True
        self._stop_event.set()

        # Best-effort: stop streams first so their callbacks stop firing.
        if self.events_streams and isinstance(self.events_streams, dict) and len(self.events_streams.keys()) > 0:
            for account_id in list(self.events_streams.keys()):
                stream_dict = self.events_streams.get(account_id) or {}
                stream = stream_dict.get('stream')
                if stream is not None:
                    try:
                        stream.stop()
                    except Exception:
                        log_for_level(self.logger, logging.INFO, "Suppressed error while calling stream.stop()",
                                      exc_info=True)

        # Ensure the maintenance thread exits fully before a restart clears the stop signal.
        try:
            if self.maintenance_thread and hasattr(self.maintenance_thread, 'is_alive') and self.maintenance_thread.is_alive():
                self.maintenance_thread.join(timeout=5)
        except Exception:
            log_for_level(self.logger, logging.INFO, "Suppressed error while joining maintenance_thread",
                          exc_info=True)

        # Also join any StreamListener threads so they don't access cleared state after stop.
        try:
            if self.events_streams and isinstance(self.events_streams, dict):
                for account_id in list(self.events_streams.keys()):
                    stream = (self.events_streams.get(account_id) or {}).get('stream')
                    if stream is not None and hasattr(stream, 'is_alive') and stream.is_alive():
                        try:
                            stream.join(timeout=5)
                        except Exception:
                            log_for_level(self.logger, logging.INFO, "Suppressed error while joining stream thread",
                                          exc_info=True)
        except Exception:
            log_for_level(self.logger, logging.ERROR, "Suppressed error while attempting to join stream threads",
                          exc_info=True)

        self.stream_started = False

    def handle_message(self, stream_key, msg):
        """Handles a message received from the underlying websocket."""
        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            return

        log_for_level(self.logger, logging.DEBUG, f"Received message from listener with key: {stream_key}")
        log_for_level(self.logger, logging.DEBUG, f"Message received: {msg}")
        if 'error' in msg:
            self.stop()
            return
        self.events_streams[stream_key]['last_event_timestamp'] = time.time()
        if self.events_destination:
            log_for_level(self.logger, logging.DEBUG, "Sending message to destination...")
            try:
                self.events_destination.put(msg)
            except Exception as e:
                log_for_level(self.logger, logging.ERROR, "Failed to put message to the destination queue",
                              exc_info=e)
        else:
            log_for_level(self.logger, logging.DEBUG, "Calling event callback for message...")
            try:
                self.events_callback(msg)
            except Exception as e:
                log_for_level(self.logger, logging.ERROR,
                              "An error occurred when invoking the callback function", exc_info=e)

    def handle_error(self, exc, stream_key):
        """Handles error related to the underlying websocket."""
        if self.stop_me or self._stop_event.is_set() or getattr(self, '_is_shutting_down', False):
            # websocket-client may report errors during teardown; keep it quiet.
            log_for_level(self.logger, logging.INFO, "Suppressed stream error during shutdown", exc_info=exc)
            return

        log_for_level(self.logger, logging.ERROR, "Error received from the stream: ", exc_info=exc)
        log_for_level(self.logger, logging.ERROR,
                      f"Error received from the stream {stream_key}: ", exc_info=exc)

    def on_ping(self, stream_key):
        """
        Opportunity to respond to ping messages, currently unused
        :param stream_key:
        """
        pass

    def on_pong(self, stream_key):
        """
        Opportunity to respond to pong messages, currently unused
        :param stream_key:
        """
        pass

    def _was_last_event_long_ago(self, stream_dict):
        """Return True if the last event was received too long ago.

        Used as a signal for refreshing session ids and for keep-alive behavior.

        :param stream_dict: Stream metadata dict.
        :return: True if last_event_timestamp is missing/old.
        """
        # Follow original behavior: treat missing timestamp as "long ago".
        was_long_ago = not isinstance(stream_dict.get('last_event_timestamp'), float) or \
            time.time() - stream_dict.get('last_event_timestamp', 0.0) > 240
        log_for_level(self.logger, logging.DEBUG, f"Last event was several minutes ago: {was_long_ago}")
        return was_long_ago
