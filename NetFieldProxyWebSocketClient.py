import asyncio
import base64
import json
import logging
import os
from typing import Union, Dict, Optional
import uuid

import httpx
import websockets
from websockets import WebSocketClientProtocol

DEFAULT_LOG_LEVEL = "INFO"
LOG_LEVEL = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=LOG_LEVEL)

API_ENDPOINTS = {
    "production": "api.netfield.io/v1",
    "training": "api-training.netfield.io/v1",
}


class NesProtocolWebSocket:
    """ Communicate with a WebSocket conforming to the nes protocol. """

    def __init__(
        self,
        ws_uri: str,
        client_id: Optional[Union[str, int]],
        auth_credentials: Dict,
        handlers: Optional[Dict],
    ):
        self.ws_uri = ws_uri
        if client_id is None:
            logging.info("No client_id provided, generating a UUID ...")
            client_id = str(uuid.uuid4())
        self.client_id = client_id
        self.auth_credentials = auth_credentials
        if handlers is None:
            handlers = {
                "on_pub_message": logging.info,
                "on_error": logging.error,
                "on_close": logging.info,
                "on_unexpected_message": logging.warn,
            }
        self.handlers = handlers

        self.ws: Optional[WebSocketClientProtocol] = None

    async def initialize_websocket_connection(self):
        """ Connect to the WebSocket and say hello (this includes providing credentials). """
        try:
            logging.info(
                f"Trying to establish a WebSocket connection to {self.ws_uri} ..."
            )
            self.ws = await websockets.connect(self.ws_uri, ping_interval=None)
            await self._say_hello()
        except Exception as e:
            logging.exception(e)
            raise (e)

    async def _send_as_json(self, payload: Dict):
        if self.ws:
            await self.ws.send(json.dumps(payload))
        else:
            await self.initialize_websocket_connection()

    async def subscribe_to_path(self, path: str):
        """ Subscribe to the given *path*.

        Args:
            path (str): path to subscribe to.
        """
        sub_msg = {
            "type": "sub",
            "id": str(uuid.uuid4()),
            "path": path,
        }
        logging.info("Subscribing to path '%s' ...", path)
        if self.ws:
            await self._send_as_json(sub_msg)
            response = await self.ws.recv()
            await self.on_message_handler(response)

    async def listen_for_messages(self):
        """ Keep WebSocket connection alive and pass received messages to the message handler. """
        async for msg in self.ws:
            await self.on_message_handler(msg)

    async def _say_hello(self):
        """ Say hello according to the nes protocol. This includes providing credentials. """

        hello_msg = {
            "type": "hello",
            "id": self.client_id,
            "version": "2",
            "auth": self.auth_credentials,
        }
        logging.info("Sending hello message ...")
        await self._send_as_json(hello_msg)
        response = await self.ws.recv()
        await self.on_message_handler(response)

    async def on_message_handler(self, message_raw: Union[str, bytes]):
        """ Handle the received message.

        Args:
            message_raw (str|bytes): received message.
        """
        logging.debug("Received a message: %s", message_raw)
        try:
            msg = json.loads(message_raw)
            if msg.get("payload", {}).get("error"):
                self.handlers["on_error"](
                    f"Received a message with error information: {message_raw}"
                )
                return

            msg_type = msg["type"]
            if msg_type == "hello":
                socket_identifier = msg["socket"]
                logging.info(
                    "-> server answered on hello message, assigned socket identifier '%s'",
                    socket_identifier,
                )
            elif msg_type == "sub":
                path = msg["path"]
                logging.info("-> subscription to path '%s' accepted", path)
            elif msg_type == "ping":
                logging.info("Got a ping message, responding ...")
                await self._respond_to_ping()
            elif msg_type == "pub":
                logging.debug(
                    "Got a 'pub' message, calling 'on_pub_message' handler ..."
                )
                self.handlers["on_pub_message"](msg)
            else:
                logging.debug(
                    "Got an unexpected message, calling 'on_unexpected_message' handler ..."
                )
                self.handlers["on_unexpected_message"](
                    f"Received an unexpected message: {message_raw}"
                )
        except Exception as inst:
            logging.exception(inst)

    async def close(self):
        """ Close connection to the WebSocket. """

        self.handlers["on_close"]("Closing WebSocket connection ...")
        await self.ws.close()
        await self.ws.wait_closed()
        self.handlers["on_close"]("-> WebSocket connection closed!`")

    async def _respond_to_ping(self):
        ping_msg = {
            "type": "ping",
            "id": str(uuid.uuid4()),
        }
        await self._send_as_json(ping_msg)


class NetFieldProxyWebSocketClient(NesProtocolWebSocket):
    """ Receive netFIELD Proxy messages provided on the WebSocket. """

    def __init__(
        self, access_token: str, api_endpoint=API_ENDPOINTS["training"], handlers=None
    ):
        endpoint = f"wss://{api_endpoint}"
        client_id = 42
        super().__init__(
            endpoint,
            client_id,
            {"headers": {"authorization": access_token},},
            handlers,
        )
        self.api_endpoint = api_endpoint

    async def subscribe_to_topic(self, device_id: str, topic: str):
        """ Subscribe to the given topic for the given device_id. """

        topic_as_base64 = base64.b64encode(
            topic.encode()
        ).decode()  # must be base64-encoded
        logging.info(topic_as_base64)
        path = f"/devices/{device_id}/netfieldproxy/{topic_as_base64}"
        await super().subscribe_to_path(path)

    @classmethod
    async def from_email_and_password(
        cls,
        email: str,
        password: str,
        api_endpoint=API_ENDPOINTS["training"],
        handlers=None,
    ):
        """ Instantiate class by providing email, password and API endpoint. """

        access_token = await cls.get_access_token(email, password, api_endpoint)
        return cls(access_token, api_endpoint, handlers)

    @classmethod
    async def from_access_token(
        cls, access_token: str, api_endpoint=API_ENDPOINTS["training"], handlers=None
    ):
        """ Instantiate class by providing access token and API endpoint. """

        return cls(access_token, api_endpoint, handlers)

    @staticmethod
    async def get_access_token(
        email: str, password: str, api_endpoint=API_ENDPOINTS["training"]
    ) -> str:
        """ Get an access token using the provided email and password. """

        payload = {
            "grantType": "password",
            "email": email,
            "password": password,
        }

        endpoint = f"https://{api_endpoint}/auth"
        logging.info(endpoint)
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(endpoint, data=payload)
                response_payload = response.json()
                return response_payload["accessToken"]
            except Exception as inst:
                logging.exception("Could not retrieve an access token.")
                raise (inst)


async def main():
    # define handlers to be called on specific events
    handlers = {
        "on_pub_message": logging.info,
        "on_error": logging.error,
        "on_close": logging.info,
        "on_unexpected_message": logging.warn,
    }

    # setup the WebSocket client using email/password
    email = "your@email.address"
    password = "your_secret_password"
    ws_client = await NetFieldProxyWebSocketClient.from_email_and_password(
        email, password, API_ENDPOINTS["training"], handlers
    )

    # or setup the WebSocket client using an access token
    # access_token = "your_access_token"
    # ws_client = await NetFieldProxyWebSocketClient.from_access_token(access_token, API_ENDPOINTS["training"], handlers)

    # initialize WebSocket connection
    await ws_client.initialize_websocket_connection()

    # subscribe to messages related to the given device_id and topic
    device_id = "device_id_running_netFIELD_Proxy" # example: "5ef5f6346ae7b913809b4459"
    topic = "topic_to_subscribe_to_in_plain_text" # example: "netfield/000000000000-TSBH06005548/device-monitoring/monitoring_data"
    await ws_client.subscribe_to_topic(device_id, topic)

    listen_task = None
    try:
        # listen indefinitly until a exception or KeyboardInterrupt occurs
        logging.info("Listening for messages indefinitly, press Ctrl+C to terminate ...")
        await ws_client.listen_for_messages()

        # or listen for a given time period
        # listen_for_seconds = 10
        # logging.info(f"Listening for messages for {listen_for_seconds} s ...")
        # listen_task = asyncio.Task(ws_client.listen_for_messages())
        # await asyncio.sleep(listen_for_seconds)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt")
    except Exception as inst:
        logging.exception("An unexpected exception occured")
        raise(inst)
    finally:
        logging.info("Cancel listening for messages and close WebSocket connection ...")
        if listen_task:
            listen_task.cancel()
        if ws_client:
            await ws_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except:
        pass # exception handling and cleanup takes place in main()

