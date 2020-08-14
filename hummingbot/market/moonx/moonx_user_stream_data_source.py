import asyncio
import json
import logging
import time
import traceback
from typing import AsyncIterable, Optional

import ujson
import websockets

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.moonx.moonx_auth import MoonxAuth

MOONX_WS_URL = "wss://exchange.moonx.pro/stream"
MESSAGE_TIMEOUT = 30.0
PING_TIMEOUT = 10.0


class MoonxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _mausds_logger: Optional[HummingbotLogger] = None

    def __init__(self, auth: MoonxAuth):
        super().__init__()
        self.moonx_auth: MoonxAuth = auth
        self._last_recv_time: float = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._mausds_logger is None:
            cls._mausds_logger = logging.getLogger(__name__)
        return cls._mausds_logger

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(MOONX_WS_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    is_authenicated: bool = await self.do_auth(ws)
                    if is_authenicated:
                        topics = ["user-order", "user-trade"]
                        subscribe_msg = {
                            "T": "final-subs",
                            "D": [
                                {"key": k}
                                for k in topics
                            ]
                        }
                        await ws.send(json.dumps(subscribe_msg))

                        async for raw_msg in self._inner_messages(ws):
                            self._last_recv_time = time.time()
                            diff_msg = ujson.loads(raw_msg)
                            has = diff_msg.get("S", None)
                            if has is not None:
                                print(diff_msg)
                                output.put_nowait(diff_msg)
                            elif "activate-alert" in diff_msg.get("T"):
                                activate_msg = {"T": "activate", "t": diff_msg["t"]}
                                await ws.send(ujson.dumps(activate_msg))
                                print(f"Sending Activate alert to Moonx websocket: {activate_msg}")
                            else:
                                print(raw_msg)
                                self.logger().debug(f"Unrecognized message received from Moonx websocket: {diff_msg}")

                    else:
                        self.logger().error("Moonx Websocket Auth error. "
                                            "Retrying after 30 seconds...", exc_info=True)
                        raise Exception
            except asyncio.CancelledError:
                await asyncio.sleep(5.0)
            except Exception as e:
                print("Error in Socket user stream", e)
                traceback.print_stack()
                await asyncio.sleep(10.0)

    async def do_auth(self, ws) -> bool:
        ts = int(time.time())
        auth_data = {
            "T": "api-auth",
            "D": self.moonx_auth.sign_it({}),
            "tag": "auth-" + str(ts)
        }
        await ws.send(json.dumps(auth_data))
        msg: str = await asyncio.wait_for(ws.recv(), timeout=MESSAGE_TIMEOUT)
        return json.loads(msg)["status"] == 0

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except websockets.ConnectionClosed as e:
            print(e.__class__, " - ", e.reason)
            return
        finally:
            await ws.close()
