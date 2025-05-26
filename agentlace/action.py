#!/usr/bin/env python3

from __future__ import annotations
from typing import Optional, Callable, Set, Dict, List
from typing_extensions import Protocol
from collections import deque

from agentlace.zmq_wrapper.req_rep import ReqRepServer, ReqRepClient
from agentlace.zmq_wrapper.broadcast import BroadcastServer, BroadcastClient
from agentlace.internal.utils import compute_hash

import threading
import logging
from dataclasses import dataclass, asdict
import json
import time


##############################################################################

@dataclass
class ActionConfig():
    """Configuration for the env server and client
    NOTE: Client and server should have the same config
            client will raise Error if configs are different, thus can use
            `version` to check for compatibility
    """
    port_number: int
    action_keys: List[str]
    observation_keys: List[str]
    broadcast_port: Optional[int] = None
    version: str = "0.0.1"


class ObsCallback(Protocol):
    """
    Define the data type of the callback function
        :param keys: Set of keys of the observation, defaults to all keys
        :return: Response with Observation to send to the client
    """

    def __call__(self, keys: Set) -> Dict:
        ...


class ActCallback(Protocol):
    """
    Define the data type of the callback function
        :param key: Key of the action
        :param payload: Payload related to the action in dict format
        :return: Response to send to the client
    """

    def __call__(self, key: str, payload: Dict) -> Dict:
        ...

##############################################################################


class ActionServer:
    def __init__(self,
                 config: ActionConfig,
                 obs_callback: Optional[ObsCallback],
                 act_callback: Optional[ActCallback],
                 log_level=logging.WARNING):
        """
        Args:
            :param config: Config object
            :param obs_callback: Callback function when client requests observation
            :param act_callback: Callback function when client requests action
        """
        def __obs_parser_cb(payload: dict) -> dict:
            if payload["type"] == "obs" and obs_callback is not None:
                return obs_callback(payload["keys"])
            elif payload["type"] == "act" and act_callback is not None:
                return act_callback(payload["key"], payload["payload"])
            elif payload["type"] == "hash":
                config_json = json.dumps(asdict(config), separators=(',', ':'))
                return {"success": True, "payload": config_json}
            return {"success": False, "message": "Invalid payload"}

        self.config = config
        self.server = ReqRepServer(config.port_number, __obs_parser_cb)
        logging.basicConfig(level=log_level)
        logging.debug(f"Action server is listening on port {config.port_number}")

        if config.broadcast_port is not None:
            self.broadcast = BroadcastServer(config.broadcast_port)
            logging.debug(f"Broadcast server is on port {config.broadcast_port}")

    def start(self, threaded: bool = False):
        """
        Starts the server, defaulting to blocking mode
            :param threaded: Whether to start the server in a separate thread
        """
        if threaded:
            self.thread = threading.Thread(target=self.server.run, daemon=True)
            self.thread.start()
        else:
            self.server.run()

    def publish_obs(self, payload: dict) -> bool:
        """
        Publishes the observation to the broadcast server,
        Enable broadcasting by defining `broadcast_port` in `ActionConfig`
        """
        if self.config.broadcast_port is None:
            logging.warning("Broadcast server not initialized")
            return False
        self.broadcast.broadcast(payload)
        return True

    def stop(self):
        """Stop the server"""
        self.server.stop()

##############################################################################


class ActionClient:
    def __init__(self,
                 server_ip: str,
                 config: ActionConfig, 
                 wait_for_server: bool = False,
                 timeout_ms: int = 800,
                 ):
        """
        Args:
            :param server_ip: IP of the server
            :param config: Config object
            :param wait_for_server: Whether to retry connecting to the server
        """
        self.client = ReqRepClient(server_ip, config.port_number, timeout_ms=timeout_ms)
        self._wait_for_server = wait_for_server
        res = self._verify_server_connection()

        # use hash for faster lookup. config uses list because it is
        # used for hash md5 comparison
        config.observation_keys = set(config.observation_keys)
        config.action_keys = set(config.action_keys)
        self.config = config
        self.server_ip = server_ip
        self.broadcast_client = None

    def _verify_server_connection(self):
        """Verify connection to server and handle retries"""
        res = self.client.send_msg({"type": "hash"})

        # Retry to connect to the server if not connected
        if self._wait_for_server:
            while res is None:
                logging.error(f"Failed to connect to action server of "
                            f"{self.server_ip}:{self.config.port_number}. Retrying...")
                time.sleep(2)
                # Reset client socket to ensure clean reconnection
                self.client.reset_socket()
                res = self.client.send_msg({"type": "hash"})

        if res is None:
            raise Exception("Failed to connect to action server")

        # Check hash of server config to ensure compatibility
        config_json = json.dumps(asdict(self.config), separators=(',', ':'))
        if compute_hash(config_json) != compute_hash(res["payload"]):
            raise Exception(
                f"Incompatible config with hash with server. "
                "Please check the config of the server and client")
        
        return res

    def _send_with_retry(self, message, max_retries=3):
        """Send message with automatic retry and reconnection on failure"""
        for attempt in range(max_retries):
            result = self.client.send_msg(message)
            if result is not None:
                return result
            
            # Connection failed, try to reconnect
            logging.warning(f"Connection failed on attempt {attempt + 1}, trying to reconnect...")
            try:
                self.client.reset_socket()
                # Verify we can still connect to server
                self._verify_server_connection()
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    logging.error(f"Failed to reconnect after {max_retries} attempts: {e}")
                    raise
                time.sleep(1)  # Wait before next retry
        
        return None

    def obs(self, keys: Optional[Set[str]] = None) -> Optional[dict]:
        """
        Get the observation from the Edge Server
            :param keys: Set of keys to request, `None`: select all
            :return: Observation from the server, `None` when not connected
        """
        if keys is None:
            keys = self.config.observation_keys  # if None and select all
        else:
            for key in keys:
                assert key in self.config.observation_keys, f"Invalid obs key: {key}"
        message = {"type": "obs", "keys": keys}
        return self._send_with_retry(message)

    def act(self, key: str, payload: dict = {}) -> Optional[dict]:
        """
        Sends the action to the server
            :param key: Key of the action
            :param payload: Payload to send to the server
            :return: Response from the server, `None` when not connected
        """
        if key not in self.config.action_keys:
            raise Exception(f"Invalid action key: {key}")
        message = {"type": "act", "key": key, "payload": payload}
        return self._send_with_retry(message)

    def register_obs_callback(self, callback: Callable):
        """
        Registers the callback function for observation streaming
            :param callback: Callback function for observation streaming
        """
        if self.config.broadcast_port is None:
            raise Exception("Broadcast server not initialized")
        self.broadcast_client = BroadcastClient(
            self.server_ip, self.config.broadcast_port)
        self.broadcast_client.async_start(callback)

    def stop(self):
        """Stop the client"""
        if self.broadcast_client is not None:
            self.broadcast_client.stop()
