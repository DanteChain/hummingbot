import asyncio
import logging
import unittest

import conf
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.market.moonx.moonx_auth import MoonxAuth
from hummingbot.market.moonx.moonx_user_stream_tracker import MoonxUserStreamTracker


class MoonxUserStreamTrackerUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.moonx_auth = MoonxAuth(conf.moonx_business_num, conf.moonx_api_secret)
        cls.user_stream_tracker: MoonxUserStreamTracker = MoonxUserStreamTracker(moonx_auth=cls.moonx_auth)
        cls.user_stream_tracker_task: asyncio.Task = safe_ensure_future(cls.user_stream_tracker.start())

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    def test_user_stream(self):
        print(self.user_stream_tracker.user_stream)
        self.ev_loop.run_until_complete(asyncio.sleep(20.0))
        print(self.user_stream_tracker.user_stream)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
