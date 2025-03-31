#!/usr/bin/env python3

import zmq
import argparse
from typing import Optional, Dict
from typing_extensions import Protocol
import logging

from agentlace.internal.utils import make_compression_method
from threading import Lock

##############################################################################


class CallbackProtocol(Protocol):
    def __call__(self, message: Dict) -> Dict:
        ...


class ReqRepServer:
    def __init__(self,
                 port=5556,
                 impl_callback: Optional[CallbackProtocol] = None,
                 log_level=logging.DEBUG,
                 compression: str = 'lz4'):
        """
        Request reply server
        """
        self.impl_callback = impl_callback
        self.compress, self.decompress = make_compression_method(compression)
        self.port = port
        self.reset()
        logging.basicConfig(level=log_level)
        logging.debug(f"Req-rep server is listening on port {port}")

    def reset_socket(self):
        """Reset the ZMQ socket if it's in a bad state"""
        try:
            if self.socket:
                try:
                    # Try to unbind first if possible
                    self.socket.unbind(f"tcp://*:{self.port}")
                except zmq.ZMQError:
                    pass  # Socket might not be bound
                self.socket.close()
                self.socket = None
                time.sleep(0.1)  # Give OS time to free up the port

            # Create new socket
            self.socket = self.context.socket(zmq.REP)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
            self.socket.setsockopt(zmq.RECONNECT_IVL, 100)  # 100ms reconnect interval
            self.socket.setsockopt(zmq.RECONNECT_IVL_MAX, 1000)  # 1s max reconnect interval
            
            try:
                self.socket.bind(f"tcp://*:{self.port}")
            except zmq.ZMQError as e:
                if e.errno == zmq.EADDRINUSE:
                    # Port is still in use, try to force close the context and recreate
                    logging.warning(f"Port {self.port} still in use, recreating context...")
                    self.context.term()
                    time.sleep(0.2)  # Give more time for cleanup
                    self.context = zmq.Context()
                    self.socket = self.context.socket(zmq.REP)
                    self.socket.setsockopt(zmq.LINGER, 0)
                    self.socket.setsockopt(zmq.RCVTIMEO, 1000)
                    self.socket.bind(f"tcp://*:{self.port}")
                else:
                    raise
            
            logging.debug(f"Reset socket on port {self.port}")
        except Exception as e:
            logging.error(f"Failed to reset socket: {str(e)}")
            # If we completely failed to reset, sleep and let caller retry
            time.sleep(1)
            raise

    def receive_complete_message(self):
        """Receive a complete message with timeout and validation"""
        try:
            # Use NOBLOCK to avoid hanging
            message = self.socket.recv(flags=zmq.NOBLOCK)
            if not message:  # Sanity check for empty messages
                logging.warning("Received empty message")
                return None
            return message
        except zmq.Again:
            # No message available
            return None
        except Exception as e:
            logging.error(f"Error receiving message: {str(e)}")
            return None

    def run(self):
        if self.is_kill:
            logging.debug("Server is prev killed, reseting...")
            self.reset()
        
        consecutive_errors = 0
        max_reset_attempts = 3
        reset_attempt = 0
        poll_timeout = 1000  # 1 second timeout
        
        while not self.is_kill:
            try:
                poller = zmq.Poller()
                poller.register(self.socket, zmq.POLLIN)
                
                while not self.is_kill:
                    try:
                        # Use poller to handle timeouts gracefully
                        socks = dict(poller.poll(poll_timeout))
                        if self.socket not in socks or socks[self.socket] != zmq.POLLIN:
                            continue

                        message = self.receive_complete_message()
                        if message is None:
                            continue

                        logging.debug(f"Received raw message of length: {len(message)} bytes")
                        try:
                            message = self.decompress(message)
                            logging.debug(f"Successfully decompressed message: {message}")
                            consecutive_errors = 0
                            reset_attempt = 0  # Reset attempt counter on success
                        except Exception as e:
                            logging.error(f"Failed to decompress message: {str(e)}")
                            if len(message) > 0:
                                logging.debug(f"First 100 bytes of raw message: {message[:100]}")
                            consecutive_errors += 1
                            if consecutive_errors >= 3:
                                raise  # Let outer try block handle reset
                            # Send error response to client to maintain REQ-REP state
                            error_response = self.compress({"error": str(e)})
                            self.socket.send(error_response)
                            continue

                        #  Send reply back to client
                        if self.impl_callback:
                            res = self.impl_callback(message)
                            res = self.compress(res)
                            self.socket.send(res)
                        else:
                            logging.warning("No implementation callback provided.")
                            self.socket.send(b"World")
                    except zmq.Again as e:
                        logging.warning(f"Socket timeout: {str(e)}")
                        continue
            except Exception as e:
                logging.error(f"Error in main loop, attempting socket reset: {str(e)}")
                try:
                    self.reset_socket()
                    reset_attempt += 1
                    if reset_attempt >= max_reset_attempts:
                        logging.error("Max reset attempts reached, server will exit")
                        self.stop()
                        raise
                    time.sleep(1)  # Wait before retrying
                except Exception as reset_error:
                    logging.error(f"Failed to reset socket: {str(reset_error)}")
                    self.stop()
                    raise
                    res = self.impl_callback(message)
                    res = self.compress(res)
                    self.socket.send(res)
                else:
                    logging.warning("No implementation callback provided.")
                    self.socket.send(b"World")
            except zmq.Again as e:
                continue
            except zmq.ZMQError as e:
                # Handle ZMQ errors gracefully
                if self.is_kill:
                    logging.debug("Stopping the ZMQ server...")
                    break
                else:
                    raise e

    def stop(self):
        self.is_kill = True
        self.socket.close()
        self.context.term()
        del self.socket
        del self.context

    def reset(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{self.port}")
        self.socket.setsockopt(zmq.SNDHWM, 5)

        # Set a timeout for the recv method (e.g., 1.5 second)
        self.socket.setsockopt(zmq.RCVTIMEO, 1500)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.is_kill = False


##############################################################################

class ReqRepClient:
    def __init__(self,
                 ip: str,
                 port=5556,
                 timeout_ms=800,
                 log_level=logging.DEBUG,
                 compression: str = 'lz4'):
        """
        :param ip: IP address of the server
        :param port: Port number of the server
        :param timeout_ms: Timeout in milliseconds
        :param log_level: Logging level, defaults to DEBUG
        :param compression: Compression algorithm, defaults to lz4
        """
        self.context = zmq.Context()
        logging.basicConfig(level=log_level)
        logging.debug(f"Req-rep client is connecting to {ip}:{port}")

        self.compress, self.decompress = make_compression_method(compression)
        self.socket = None
        self.ip, self.port, self.timeout_ms = ip, port, timeout_ms
        self._internal_lock = Lock()
        self.reset_socket()

    def reset_socket(self):
        """
        Reset the socket connection, this is needed when REQ is in a
        broken state.
        """
        try:
            if self.socket:
                self.socket.close()
                self.socket = None
        except Exception as e:
            logging.warning(f"Error closing socket: {e}")
            self.socket = None
            
        try:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            self.socket.setsockopt(zmq.LINGER, 0)  # Don't wait on close
            self.socket.connect(f"tcp://{self.ip}:{self.port}")
        except Exception as e:
            logging.error(f"Failed to create new socket: {e}")
            self.socket = None
            raise

    def send_msg(self, request: dict, wait_for_response=True) -> Optional[str]:
        if self.socket is None:
            logging.warning("Socket is None, attempting reset...")
            try:
                self.reset_socket()
            except Exception as e:
                logging.error(f"Failed to reset socket: {e}")
                return None

        serialized = self.compress(request)
        with self._internal_lock:
            try:
                self.socket.send(serialized)
                if wait_for_response is False:
                    return None
                message = self.socket.recv()
                return self.decompress(message)
            except Exception as e:
                logging.warning(
                    f"Failed to send message to {self.ip}:{self.port}: {e}")
                try:
                    self.reset_socket()
                except Exception as reset_error:
                    logging.error(f"Failed to reset socket after error: {reset_error}")
                return None

    def __del__(self):
        if self.socket:
            self.socket.close()
        self.context.term()


##############################################################################

if __name__ == "__main__":
    # NOTE: This is just for Testing
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', action='store_true')
    parser.add_argument('--client', action='store_true')
    parser.add_argument('--ip', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=5556)
    args = parser.parse_args()

    def do_something(message):
        return b'World'

    if args.server:
        ss = ReqRepServer(port=args.port, impl_callback=do_something)
        ss.run()
    elif args.client:
        sc = ReqRepClient(ip=args.ip, port=args.port)
        r = sc.send_msg({'hello': 1})
        print(r)
    else:
        raise Exception('Must specify --server or --client')
