import asyncio
import logging
from typing import Optional

from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker, UserStreamTrackerDataSourceType
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather, safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.market.moonx.moonx_auth import MoonxAuth
from hummingbot.market.moonx.moonx_user_stream_data_source import MoonxAPIUserStreamDataSource


class MoonxUserStreamTracker(UserStreamTracker):
    _must_logger: Optional[HummingbotLogger] = None

    def __init__(self, moonx_auth: MoonxAuth):
        super().__init__()
        self.moonx_auth = moonx_auth
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._must_logger is None:
            cls._must_logger = logging.getLogger(__name__)
        return cls._must_logger

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is UserStreamTrackerDataSourceType.EXCHANGE_API:
                self._data_source = MoonxAPIUserStreamDataSource(self.moonx_auth)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
