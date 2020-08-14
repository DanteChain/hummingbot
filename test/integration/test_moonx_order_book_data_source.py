import asyncio
import logging
import unittest
from typing import List, Dict, Any, Optional

import aiohttp
import pandas as pd

from hummingbot.market.moonx.moonx_api_order_book_data_source import MoonxAPIOrderBookDataSource

test_trading_pairs = ["BTC_ETH", "USDT_BTC", "USDT_ETH"]


class MoonxOrderBookTrackerUnitTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_data_source: MoonxAPIOrderBookDataSource = MoonxAPIOrderBookDataSource(test_trading_pairs)

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    def test_get_active_exchange_markets(self):
        trading_pairs: pd.DataFrame = self.run_async(self.order_book_data_source.get_active_exchange_markets())
        self.assertTrue(
            set(trading_pairs.index.tolist()) & set(test_trading_pairs) == set(test_trading_pairs))

    def test_get_trading_pairs(self):
        trading_pairs: pd.DataFrame = self.run_async(self.order_book_data_source.get_trading_pairs())
        self.assertEqual(trading_pairs, test_trading_pairs)

    def test_get_tracking_pairs(self):
        trackerEntry = self.run_async(self.order_book_data_source.get_tracking_pairs())
        self.assertEqual(len(trackerEntry), len(test_trading_pairs))

    async def get_snapshot(self):
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.order_book_data_source.get_trading_pairs()
            trading_pair: str = trading_pairs[0]
            try:
                snapshot: Dict[str, Any] = await self.order_book_data_source.get_snapshot(client, trading_pair)
                return snapshot
            except Exception:
                return None

    def test_get_snapshot(self):
        snapshot: Optional[Dict[str, Any]] = self.run_async(self.get_snapshot())
        self.assertIsNotNone(snapshot)
        self.assertIn(snapshot["trading_pair"], self.run_async(self.order_book_data_source.get_trading_pairs()))


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
