import asyncio
import logging
import time
from typing import Optional, List

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker, OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.moonx.moonx_api_order_book_data_source import MoonxAPIOrderBookDataSource


class MoonxOrderBookTracker(OrderBookTracker):
    _mobt_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 data_source_type: OrderBookTrackerDataSourceType = OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None):
        super().__init__(data_source_type=data_source_type)
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[OrderBookTrackerDataSource] = None
        self._trading_pairs: Optional[List[str]] = trading_pairs

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._mobt_logger is None:
            cls._mobt_logger = logging.getLogger(__name__)
        return cls._mobt_logger

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is OrderBookTrackerDataSourceType.EXCHANGE_API:
                self._data_source = MoonxAPIOrderBookDataSource(trading_pairs=self._trading_pairs)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "moonx"

    async def _track_single_book(self, trading_pair: str):
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: OrderBookMessage = await message_queue.get()
                if message.type is OrderBookMessageType.DIFF:
                    # Huobi websocket messages contain the entire order book state so they should be treated as snapshots
                    order_book.apply_snapshot(message.bids, message.asks, message.update_id)
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug("Processed %d order book diffs for %s.",
                                            diff_messages_accepted, trading_pair)
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    order_book.apply_snapshot(message.bids, message.asks, message.update_id)
                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)
