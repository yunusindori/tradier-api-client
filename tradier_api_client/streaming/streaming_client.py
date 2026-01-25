"""
Stream Client
"""
import json
import logging
import threading
import time
import traceback
from functools import partial
from queue import Queue
from typing import Optional

from ..helper_functions import log_for_level
from ..rest.rest_client import RestClient
from ..streaming.websocket_stream import StreamListener


class StreamingClient:
    """
    WebSocket client. Only one session is allowed to be created for each stream type so it's up to you to only create
    one instance each of this class for each stream type.
    """

    def __init__(
            self, main_api_key, base_url, stream_base_url, main_account_id=None, additional_api_keys=None,
            additional_account_ids=None, stream_type='account', config=None,
            verbose=False, events_destination=None, events_callback=None, logger: logging.Logger = None):
        """

        Can be used to listen to account events or market events

        :param main_api_key: Primary trading account's api_key
        :param additional_api_keys: Pass list of additional API keys for listening to more symbols than
        self.NUM_SYMBOLS_PER_STREAM
        :param stream_type: market or account
        :param verbose: Log level will be set to DEBUG if True, else INFO
        :param events_destination: A queue.Queue instance that the put method can be invoked on to store the
            event. Make sure the queue has sufficient capacity left for the event to be stored.
        :param events_callback: A callback method instead of destination to send the event to. Only one of
            callback or destination can be used. Only use callback if it returns quickly else it's best to use
            destination.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)
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

        self.keep_alive_thread = threading.Thread(target=self.keep_market_stream_alive, daemon=True)
        self.session_id_refresher = threading.Thread(target=self.refresh_session_id, daemon=True)

    def __validate_init_inputs(
            self, main_api_key, main_account_id, additional_api_keys,
            additional_account_ids, stream_type,
            events_destination, events_callback):
        """
        Initial validation of inputs

        """
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
                log_for_level(self.logger, logging.DEBUG, "Starting market stream...")
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
                    log_for_level(self.logger, logging.DEBUG, "Starting websocket connection...")
                    self.events_streams[account_id][
                        'stream']: StreamListener = self._build_stream_listener_for_account_id(account_id)
                    symbols_listened_to.extend(symbol_buckets[stream_index])
                    self.events_streams[account_id]['stream'].start()
                    log_for_level(self.logger, logging.DEBUG, "Market stream started...")
                self.symbols_listened_to = symbols_listened_to
            else:
                log_for_level(self.logger, logging.DEBUG, "Starting account stream...")
                account_id = self.account_ids[0]
                self._build_stream_dict_for_account_id(account_id)
                self.events_streams[self.account_ids[0]]['stream'] = self._build_stream_listener_for_account_id(
                    account_id)
                self.events_streams[self.account_ids[0]]['stream'].start()
                log_for_level(self.logger, logging.DEBUG, "Account stream started...")
            self.session_id_refresher.start()
            self.keep_alive_thread.start()
            self.stream_started = True
        except Exception as e:
            log_for_level(self.logger, logging.ERROR, "Failed to create or start stream: ")
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
        self.session_id_refresher = threading.Thread(target=self.refresh_session_id, daemon=True)
        self.keep_alive_thread.start = threading.Thread(target=self.keep_market_stream_alive, daemon=True)
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
        """
        This is called internally by this class to create a session id and post to the stream to start streaming.

        :param stream_key:

        Raises:
            Exception: If any issue occurs, like the stream is prematurely closed, or no symbols exist to send to the
            stream, or if there was an error obtaining a session id. Be sure to handle the exception and retry.
        """
        api_key = self.account_id_to_api_key[stream_key]
        self.check_session_id_for_stream(stream_key)
        session_id = self.events_streams[stream_key]['session_id']
        if self.stream_type == 'account':
            initial_payload = self.build_account_stream_payload(events=['orders'], session_id=session_id)
            log_for_level(self.logger, logging.DEBUG, f"Sending payload to stream: {initial_payload}")
            self.events_streams[stream_key]['stream'].update_stream(json.dumps(initial_payload))
        elif self.stream_type == 'market':
            symbols = self.events_streams[stream_key]['symbols']
            initial_payload = self.build_market_stream_payload(symbols=symbols, session_id=session_id)
            log_for_level(self.logger, logging.DEBUG, f"Sending payload to stream: {initial_payload}")
            try:
                self.events_streams[stream_key]['stream'].update_stream(json.dumps(initial_payload))
            except Exception as e:
                self.logger.exception("An error occurred while updating stream: ", exc_info=e)

    def _get_session_id_from_server(self, stream_key):
        """
        Common method to get session id for both stream types
        :param stream_key:
        :return:
        """
        return self.rest_client.create_market_session(stream_key)['stream'][
            'sessionid'] if self.stream_type == 'market' else \
            self.rest_client.create_account_session(stream_key)['stream'][
                'sessionid']

    def get_current_symbols_list(self):
        """
        Returns list of symbols currently being listened to
        :return:
        """
        return self.symbols_listened_to

    def refresh_session_id(self):
        """Utility thread method to refresh session ids for all accounts every 30 minutes."""
        my_run_id = self._run_id
        while not self.stop_me and not self._stop_event.is_set() and my_run_id == self._run_id:
            self.check_session_id()
            # Wake early on stop.
            self._stop_event.wait(60)

    def check_session_id(self):
        """
        Check and refresh session id for all streams
        """
        try:
            if self.events_streams and isinstance(self.events_streams, dict) and len(
                    self.events_streams.keys()) > 0:
                log_for_level(self.logger, logging.DEBUG, "Checking session ids for all streams...")
                for account_id in self.events_streams.keys():
                    self.check_session_id_for_stream(account_id)
        except Exception:
            log_for_level(self.logger, logging.WARNING, traceback.format_exc())

    def check_session_id_for_stream(self, stream_key):
        """
        Check and refresh session id for a stream
        :param stream_key:
        """
        stream_dict = self.events_streams[stream_key]
        if self._session_id_refresh_condition_met(stream_key, stream_dict):
            log_for_level(self.logger, logging.DEBUG, f"Getting a new session id for: {stream_key}...")
            api_key = self.account_id_to_api_key[stream_key]
            stream_dict['session_id'] = self._get_session_id_from_server(api_key)
            stream_dict['session_id_last_updated'] = time.time()
            log_for_level(self.logger, logging.DEBUG, f"Got new session id for: {stream_key}...")

    def _session_id_refresh_condition_met(self, stream_key, stream_dict):
        log_for_level(self.logger, logging.DEBUG, "Checking whether to obtain a new session id...")
        get_new = stream_dict['stream'] is not None and stream_dict['stream'].is_running() and (
                'session_id' not in stream_dict.keys() or
                not isinstance(stream_dict['session_id'], str) or
                'session_id_last_updated' not in stream_dict.keys() or
                not isinstance(stream_dict['session_id_last_updated'], float)) or \
                  (isinstance(stream_dict['session_id_last_updated'], float) and
                   time.time() - stream_dict['session_id_last_updated'] > 1800) or self._was_last_event_long_ago(
            stream_dict)
        if get_new:
            log_for_level(self.logger, logging.DEBUG, f"Need new session id for {stream_key}...")
        return get_new

    def _was_last_event_long_ago(self, stream_dict):
        was_long_ago = not isinstance(stream_dict['last_event_timestamp'], float) or time.time() - stream_dict[
            'last_event_timestamp'] > 240
        log_for_level(self.logger, logging.DEBUG, f"Last event was several minutes ago: {was_long_ago}")
        return was_long_ago

    # noinspection PyMethodMayBeStatic
    def build_market_stream_payload(
            self, symbols, session_id, payload_type_filter: Optional[list] = None, linebreak=True,
            valid_only=True, advanced_details=True):
        """
        Build market stream request payload to the send to the server

        :return:
        :param symbols:
        :param session_id:
        :param payload_type_filter:
        :param linebreak:
        :param valid_only:
        :param advanced_details:
        :return:
        """
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
        """
        Build account stream request payload to the send to the server

        :param events:
        :param session_id:
        :param exclude_accounts:
        :return:
        """
        payload = {
            'events': events,
            'sessionid': session_id
        }
        if exclude_accounts and isinstance(exclude_accounts, list) and len(exclude_accounts) > 0:
            payload['excludeAccounts'] = exclude_accounts
        return payload

    def stop(self):
        """
        Stops the stream client and all connected sockets and associated threads. Once the stream client is stopped,
        a new instance of the client must be created if you want to listen to market or account streams again.
        """
        log_for_level(self.logger, logging.DEBUG, "Stopping all streams and threads...")
        self.stop_me = True
        self._stop_event.set()
        if self.events_streams and isinstance(self.events_streams, dict) and len(self.events_streams.keys()) > 0:
            for account_id in self.events_streams.keys():
                stream_dict = self.events_streams.get(account_id) or {}
                stream = stream_dict.get('stream')
                if stream is not None:
                    stream.stop()

        # Ensure the refresh thread fully exits before a restart clears the stop signal.
        try:
            if self.session_id_refresher and self.session_id_refresher.is_alive():
                self.session_id_refresher.join(timeout=5)
        except Exception:
            # Best-effort join; don't fail stop() if joining has issues.
            pass

        self.stream_started = False

    def handle_message(self, stream_key, msg):
        """
        Handles the message received from the underlying websocket

        :param stream_key:
        :param msg:
        """
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
        """
        Handles error related to the underlying websocket
        :param stream_key:
        :param exc:
        """
        self.logger.exception("Error received from the stream: ", exc_info=exc)
        log_for_level(self.logger, logging.ERROR,
                      f"Error received from the stream {stream_key}: ", exc_info=exc)

    def handle_close(self, stream_key, close_status_code=None, close_msg=None):
        """
        Handles the close event of the underlying websocket
        :param stream_key:
        :param close_status_code:
        :param close_msg:
        """
        log_for_level(self.logger, logging.INFO, f"Stream for account id {stream_key} closed...")
        log_for_level(self.logger, logging.INFO,
                      f"Close status code: {close_status_code}, close message: {close_msg}")
        if not self.stop_me:
            log_for_level(self.logger, logging.INFO, "Not supposed to stop yet, attempting to reconnect...")
            self.stream_started = False
            try:
                # Stop the currently running threads so that new ones can be started
                self.stop()
                # Restart the streams
                self.restart_streams()
            except Exception as e:
                log_for_level(self.logger, logging.WARNING, "An error occurred trying to restart the streams: ",
                              exc_info=e)

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

    def keep_market_stream_alive(self):
        """
        Keep the stream alive in case there are no events received for a minute
        """
        if self.stream_type == 'market':
            while not self.stop_me:
                for key, stream_dict in self.events_streams.items():
                    if key and stream_dict:
                        self.check_session_id_for_stream(key)
                        if self._was_last_event_long_ago(stream_dict):
                            stream_dict['stream'].send(self.build_market_stream_payload(symbols=stream_dict['symbols'],
                                                                                        session_id=stream_dict[
                                                                                            'session_id']))
                    time.sleep(1)
