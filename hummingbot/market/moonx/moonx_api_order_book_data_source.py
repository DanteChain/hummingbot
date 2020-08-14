import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Any, AsyncIterable

import aiohttp
import pandas as pd
import websockets

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.event.events import TradeType
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

MOONX_BASE_URL = "https://exchange.moonx.pro"
MOONX_INSTRUMENTS_URL = "%s/exchangeApi/match/v-instrument" % MOONX_BASE_URL
MOONX_TICKER_URL = "%s/trade/detail" % MOONX_BASE_URL
MOONX_ORDER_STACK_URL = "%s/market/depth" % MOONX_BASE_URL
MOONX_WS_URL = "wss://exchange.moonx.pro/stream"

MESSAGE_TIMEOUT = 30.0
PING_TIMEOUT = 10.0


class MoonxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _maobds_logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._maobds_logger is None:
            cls._maobds_logger = logging.getLogger(__name__)
        return cls._maobds_logger

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            resp = await client.get(MOONX_TICKER_URL)
            resp_json = await resp.json()
            market_data = {
                key: float(value["newPrice"])
                for key, value in resp_json.items() if key in trading_pairs
            }
        return market_data

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:

        async with aiohttp.ClientSession() as client:

            trading_pairs_response, ticker_response = await safe_gather(
                client.get(MOONX_INSTRUMENTS_URL),
                client.get(MOONX_TICKER_URL)
            )

            trading_pairs_response: aiohttp.ClientResponse = trading_pairs_response
            ticker_response: aiohttp.ClientResponse = ticker_response

            if trading_pairs_response.status != 200:
                raise IOError(f"Error fetching Moonx Instruments information. "
                              f"HTTP status is {trading_pairs_response.status}.")

            if ticker_response.status != 200:
                raise IOError(f"Error fetching Moonx Ticker information. "
                              f"HTTP status is {ticker_response.status}.")

            trading_pairs_data = await trading_pairs_response.json()
            ticker_data = await ticker_response.json()

            attr_name_map = {"sizeAsset": "baseAsset", "priceAsset": "quoteAsset"}

            trading_pairs: Dict[str, Any] = {
                item["symbol"]: {attr_name_map[k]: item[k] for k in ["priceAsset", "sizeAsset"]}
                for item in trading_pairs_data["data"]
                if (item["status"] == 1) & (item["type"] == "SPOT")
            }

            market_data: List[Dict[str, Any]] = [
                {"symbol": key, **ticker_data[key], **trading_pairs[key]}
                for key in ticker_data
                if key in trading_pairs
            ]

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="symbol")
            all_markets.loc[:, "volume"] = all_markets.get("24Total")

            return all_markets

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    "Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg="Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        params: Dict = {"symbol": trading_pair}
        async with client.get(MOONX_ORDER_STACK_URL, params=params) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching MOONX market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)
            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        trading_pairs: List[str] = await self.get_trading_pairs()

        async with aiohttp.ClientSession() as client:
            retval: Dict[str, OrderBookTrackerEntry] = {}
            number_of_pairs = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):

                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    ts = int(round(time.time() * 1000))
                    snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
                        "trading_pair": trading_pair,
                        "update_id": ts,
                        "bids": snapshot["buy"],
                        "asks": snapshot["sell"]
                    }, ts)

                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_msg.timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index + 1}/{number_of_pairs} completed.")
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)

            return retval

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

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(MOONX_WS_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_msg: Dict[str, Any] = {
                        "T": "final-subs",
                        "D": [
                            {"key": "stack@" + item.lower()}
                            for item in trading_pairs
                        ]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw_msg in self._inner_messages(ws):
                        json_msg: Dict[str, Any] = json.loads(raw_msg)
                        has = json_msg.get("S")
                        if has is not None:
                            stream_type: str = json_msg["S"]
                            order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, {
                                "trading_pair": stream_type.split("@")[1],
                                "update_id": json_msg["s"],
                                "bids": json_msg["D"]["buy"],
                                "asks": json_msg["D"]["sell"]
                            }, int(round(time.time() * 1000)))
                            output.put_nowait(order_book_message)
                        elif "activate-alert" in json_msg.get("T"):
                            activate_msg = {"T": "activate", "t": json_msg["t"]}
                            await ws.send(json.dumps(activate_msg))
                            print("Activate Alert")
                            self.logger().debug(f"Sending Activate alert to Moonx websocket: {activate_msg}")
                        else:
                            print(raw_msg)
                            self.logger().debug(f"Unrecognized message received from Moonx websocket: {json_msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            ts = int(round(time.time() * 1000))
                            snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
                                "trading_pair": trading_pair,
                                "update_id": 0,
                                "bids": snapshot["buy"],
                                "asks": snapshot["sell"]
                            }, ts)
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)

                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(MOONX_WS_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_msg: Dict[str, Any] = {
                        "T": "final-subs",
                        "D": [
                            {"key": "trade@" + item.lower()}
                            for item in trading_pairs
                        ]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw_msg in self._inner_messages(ws):
                        json_msg: Dict[str, Any] = json.loads(raw_msg)
                        has = json_msg.get("S", None)
                        if has is not None:
                            symbol = json_msg["S"].split("@")[1]
                            if json_msg['T'] == 's':
                                for trade in json_msg['D']:
                                    trade_message = await self.convert_to_order_book_message(symbol, trade)
                                    output.put_nowait(trade_message)
                            else:
                                trade = json_msg['D']
                                trade_message = await self.convert_to_order_book_message(symbol, trade)
                                output.put_nowait(trade_message)
                        elif "activate-alert" in json_msg.get("T"):
                            activate_msg = {"T": "activate", "t": json_msg["t"]}
                            await ws.send(json.dumps(activate_msg))
                            self.logger().debug(f"Sending Activate alert to Moonx websocket: {activate_msg}")
                        else:
                            print(raw_msg)
                            self.logger().debug(f"Unrecognized message received from Moonx websocket: {json_msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with Moonx WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def convert_to_order_book_message(self, symbol, trade):
        trade_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": symbol,
            "trade_type": float(TradeType.SELL.value) if trade["type"] == "BUY" else float(
                TradeType.BUY.value),
            "trade_id": trade["s"],
            "update_id": trade["s"],
            "amount": trade["num"],
            "price": trade["price"]
        }, trade["time"])
        return trade_message
