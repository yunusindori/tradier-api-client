"""
Connect to a tradier stream, orders and live market events are the only two options.
"""
import logging
from threading import Thread
from time import time

import websocket


class ApplicationThread(Thread):
    """
    A simple thread parent providing stop method
    """

    def __init__(self):
        super(ApplicationThread, self).__init__()
        self.running = False
        self.stop_me = False
        self.starting = True

    def run(self) -> None:
        """
        Start the thread to keep the socket connected
        """
        self.running = True
        logging.getLogger(__name__).debug(f'Thread: {self.name}')

    def stop(self):
        """
        Stop the socket connection
        """
        self.stop_me = True
        self.running = False


class StreamListener(ApplicationThread):
    """
    Tradier stream listener, there are only 2 streams to listen to: account events and market events. Both are
    supported.
    """

    def __init__(
            self, base_url: str, stream_path: str, on_connect_callback, on_message_callback, on_error_callback,
            reconnect_on_failure=True, on_data_callback=None, on_cont_message_callback=None,
            on_disconnect_callback=None, logger: logging.Logger = None, verbose=False):
        """
        auth_callback must have the following signature:
        def auth_callback() -> cookie, headers:
        It will be used for getting the secure cookie and/or headers for secure streams in case the stream get
        disconnected.
        """
        super(StreamListener, self).__init__()
        self.logger = logging.getLogger(__name__) if not logger else logger.getChild(__name__)
        self.logger.setLevel(logger.getEffectiveLevel() if logger else logging.DEBUG if verbose else logging.INFO)
        # self.logger.addHandler(logging.StreamHandler())
        self.on_cont_message_callback = on_cont_message_callback
        self.on_data_callback = on_data_callback
        self.on_disconnect_callback = on_disconnect_callback
        self.on_error_callback = on_error_callback
        self.on_message_callback = on_message_callback
        self.on_connect_callback = on_connect_callback
        # TODO: Define initial fields
        self.reconnect_on_error = reconnect_on_failure
        if not stream_path.startswith("/"):
            self.url = base_url + "/" + stream_path
        else:
            self.url = base_url + stream_path
        self.logger.debug(f'Connecting to : {self.url}')
        self.last_connected = time()
        self.app = websocket.WebSocketApp(url=self.url, on_open=lambda ws: self.on_connect_callback(),
                                          on_message=lambda ws, msg: self.on_message_callback(msg),
                                          on_error=lambda ws, exc: self.on_error_callback(exc),
                                          on_close=lambda ws, close_status_code,
                                                          close_msg: self.on_disconnect_callback(close_status_code,
                                                                                                 close_msg) if
                                          self.on_disconnect_callback else None,
                                          on_ping=lambda ws: self.send_pong(),
                                          on_data=lambda ws, msg, data_type, cont_flag:
                                          self.on_data_callback(msg,
                                                                data_type,
                                                                cont_flag) if self.on_data_callback else None,
                                          on_cont_message=lambda ws, msg, cont_flag:
                                          self.on_cont_message_callback(msg, cont_flag) if
                                          self.on_cont_message_callback else None)

    def is_running(self):
        """
        Check if the underlying socket client is connected
        :return:
        """
        # Defensive: self.app.sock may be None during connection setup/teardown. Return False in that case
        sock = getattr(self.app, 'sock', None)
        return bool(sock and getattr(sock, 'connected', False))

    def update_stream(self, updated_request):
        """Update the stream per Tradier documentation.

        See: https://documentation.tradier.com/brokerage-api/streaming/wss-market-websocket

        :param updated_request: Serialized request payload (string) to send to the websocket.
        :raises websocket.WebSocketConnectionClosedException: If the connection is not currently open.
        """
        self.logger.debug(f"Sending message: {updated_request}")
        if self.stop_me:
            raise websocket.WebSocketConnectionClosedException("Connection is stopping.")
        if not self.is_running():
            raise websocket.WebSocketConnectionClosedException("Connection is already closed.")
        try:
            self.app.send(data=updated_request)
        except websocket.WebSocketConnectionClosedException:
            # Common during teardown; let callers decide whether to log.
            raise
        except Exception as e:
            # websocket-client uses AttributeError/None sock paths during races; normalize into the same exception.
            raise websocket.WebSocketConnectionClosedException("Connection is already closed.") from e

    def run(self) -> None:
        """
        The normal run method
        """
        # noinspection PyBroadException
        try:
            self.app.run_forever()
        except Exception as e:
            self.logger.exception(msg=f"Disconnect from {self.url}", exc_info=e)

    def stop(self):
        """
        Stop this thread
        """
        super(StreamListener, self).stop()
        try:
            if self.app:
                self.app.keep_running = False
                self.app.close()
        except Exception:
            # Best-effort close; swallow to keep shutdown quiet.
            self.logger.debug("Suppressed exception while closing WebSocketApp", exc_info=True)

    def send_pong(self):
        """
        Send a pong on ping
        """
        # TODO: Define the pong message
        pass

    def send_ping(self, data):
        """Send a ping-like message over the websocket.

        This is used by the higher-level client as a keep-alive mechanism.

        :param data: Serialized payload (string) to send.
        :raises websocket.WebSocketConnectionClosedException: If the connection is not currently open.
        """
        if self.stop_me:
            raise websocket.WebSocketConnectionClosedException("Connection is stopping.")
        if not self.is_running():
            raise websocket.WebSocketConnectionClosedException("Connection is already closed.")
        try:
            self.app.send(data)
        except websocket.WebSocketConnectionClosedException:
            raise
        except Exception as e:
            raise websocket.WebSocketConnectionClosedException("Connection is already closed.") from e
