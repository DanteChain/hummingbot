import asyncio
import logging
import traceback
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple, AsyncIterable
from libc.stdint cimport int64_t

import aiohttp
import pandas as pd
import ujson
from async_timeout import timeout

from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker
from hummingbot.core.event.events import OrderType, TradeType, BuyOrderCreatedEvent, MarketEvent, \
    MarketOrderFailureEvent, OrderCancelledEvent, SellOrderCreatedEvent, OrderFilledEvent, BuyOrderCompletedEvent, \
    SellOrderCompletedEvent, MarketTransactionFailureEvent
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import safe_gather, safe_ensure_future
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.logger import HummingbotLogger
from hummingbot.market.market_base cimport MarketBase
from hummingbot.market.moonx.moonx_api_order_book_data_source import MoonxAPIOrderBookDataSource
from hummingbot.market.moonx.moonx_auth import MoonxAuth
from hummingbot.market.moonx.moonx_in_flight_order import MoonxInFlightOrder
from hummingbot.market.moonx.moonx_order_book_tracker import MoonxOrderBookTracker
from hummingbot.market.moonx.moonx_trading_rule import MoonxTradingRule
from hummingbot.market.moonx.moonx_user_stream_tracker import MoonxUserStreamTracker
from hummingbot.market.trading_rule cimport TradingRule

MOONX_BASE_URL = "https://exchange.moonx.pro"
MOONX_GET_ORDER = "/exchangeApi/api/v1/order-mgmt/get-order"
MOONX_POST_ORDER = "/exchangeApi/api/v1/order-mgmt/order"
MOONX_CANCEL_ORDER = "/exchangeApi/api/v1/order-mgmt/cancel-order"
MOONX_WALLET_URL = "/exchangeApi/api/v1/asset"
MOONX_TICKER_URL = "/market/tickers"
MOONX_INSTRUMENT_URL = "/exchangeApi/match/v-instrument"
SUCCESS_CODE = "100200"

s_logger = None
s_decimal_0 = Decimal(0)


class MoonxAPIException(Exception):
    def __init__(self, *args):
        self.code = args[0]
        self.message = args[1]

    def __str__(self):
        return f"code : {self.code}, message: {self.message}"


class MoonxMarketTransactionTracker(TransactionTracker):

    def __init__(self, owner: MoonxMarket):
        super().__init__()
        self._owner = owner

    def c_did_timeout_tx(self, tx_id: str):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)


cdef class MoonxMarket(MarketBase):
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value
    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books.get(trading_pair)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 business_num: str,
                 api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 poll_interval: float = 10.0,
                 trading_required: bool = True):
        super().__init__()
        self._moonx_auth = MoonxAuth(business_num, api_secret)
        self._order_book_tracker = MoonxOrderBookTracker(OrderBookTrackerDataSourceType.EXCHANGE_API, trading_pairs)
        self._user_stream_tracker = MoonxUserStreamTracker(moonx_auth=self._moonx_auth)
        self._poll_interval = poll_interval
        self._poll_notifier = asyncio.Event()
        self._in_flight_orders = {}
        self._order_not_found_records = {}  # Dict {client_order_id, count}
        self._tx_tracker = MoonxMarketTransactionTracker(self)
        self._trading_rules = {}
        self._shared_client = None
        self._trading_rules_polling_task = None
        self._status_polling_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._trading_required = trading_required

    cdef c_start(self, Clock clock, double timestamp):
        self._tx_tracker.c_start(clock, timestamp)
        MarketBase.c_start(self, clock, timestamp)

    cdef c_stop(self, Clock clock):
        MarketBase.c_stop(self, clock)
        self._async_scheduler.stop()

    @property
    def name(self) -> str:
        return "moonx"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def moonx_auth(self) -> MoonxAuth:
        return self._moonx_auth

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, MoonxInFlightOrder]:
        return self._in_flight_orders

    @staticmethod
    def split_trading_pair(trading_pair: str) -> Tuple[str, str]:
        assets = trading_pair.split("_")
        return assets[1], assets[0]

    @staticmethod
    def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
        if MoonxMarket.split_trading_pair(exchange_trading_pair) is None:
            return None
        # Moonx uses uppercase (USDT_BTC) -> (QUOTE_BASE)
        base_asset, quote_asset = MoonxMarket.split_trading_pair(exchange_trading_pair)
        return f"{base_asset.upper()}-{quote_asset.upper()}"

    @staticmethod
    def convert_to_exchange_trading_pair(mx_trading_pair: str) -> str:
        # Moonx uses uppercase (USDT_BTC) -> (QUOTE_BASE)
        if "-" in mx_trading_pair:
            base_asset, quote_asset = mx_trading_pair.split("-")
            return f"{quote_asset.upper()}_{base_asset.upper()}"
        return mx_trading_pair

    @property
    def order_book_tracker(self) -> MoonxOrderBookTracker:
        return self._order_book_tracker

    @property
    def user_stream_tracker(self) -> MoonxUserStreamTracker:
        return self._user_stream_tracker

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, Any]:
        return {
            order_id: value.to_json()
            for order_id, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        self._in_flight_orders.update({
            key: MoonxInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    cdef c_stop_tracking_order(self, str order_id):
        o = self._in_flight_orders.pop(order_id, None)
        if o is None:
            self.logger().error(f"order_id :: {order_id} not found during delete op ")

    @property
    def shared_client(self) -> str:
        return self._shared_client

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t> (timestamp / self._poll_interval)

        MarketBase.c_tick(self, timestamp)
        self._tx_tracker.c_tick(timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    @shared_client.setter
    def shared_client(self, client: aiohttp.ClientSession):
        self._shared_client = client

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await MoonxAPIOrderBookDataSource.get_active_exchange_markets()

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Kraken. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message["S"]
                updates = event_message["D"]
                if event_type == "user-order":
                    orders = []
                    if event_message["T"] == "u":
                        orders.append(updates)
                    if event_message["T"] == "s":
                        for update in updates["list"]:
                            orders.append(update)

                    for order in orders:
                        exchange_order_id = order["orderNo"]
                        status = order["tradeCoinStatus"]
                        is_partial_traded = order["filledQty"] > 0
                        is_buy = order["orderSide"] == "BUY"

                        self.logger().debug("open orders in humming bot :")
                        for key, value in self._in_flight_orders.items():
                            self.logger().debug(f"{key}, {value}")

                        try:
                            client_order_id, tracked_order = next(
                                (key, value) for key, value in self._in_flight_orders.items()
                                if value.exchange_order_id == str(exchange_order_id))
                        except StopIteration:
                            continue
                        except Exception as e:
                            self.logger().error("Exception in getting client_order_id from _in_flight_orders", e)

                        if tracked_order is None:
                            # Hiding the messages for now. Root cause to be investigated in later sprints.
                            self.logger().debug(f"Unrecognized order ID from user stream: {client_order_id}.")
                            self.logger().debug(f"Event: {event_message}")
                            self.logger().debug(f"Order Event: {update}")
                            continue

                        if status == "SUCCESS":
                            price = Decimal(order["price"])
                            if is_buy:
                                self.logger().info(
                                    f"The market buy order {tracked_order.client_order_id} has completed "
                                    f"according to user stream.")
                                buy_order_completed_event = BuyOrderCompletedEvent(self._current_timestamp,
                                                                                   tracked_order.client_order_id,
                                                                                   tracked_order.base_asset,
                                                                                   tracked_order.quote_asset,
                                                                                   (tracked_order.fee_asset
                                                                                    or tracked_order.quote_asset),
                                                                                   Decimal(tracked_order.amount),
                                                                                   Decimal(
                                                                                       tracked_order.amount * price),
                                                                                   tracked_order.fee_paid,
                                                                                   tracked_order.order_type)
                                self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                     buy_order_completed_event)
                            else:
                                self.logger().info(
                                    f"The market sell order {tracked_order.client_order_id} has completed "
                                    f"according to user stream.")
                                sell_order_completed_event = SellOrderCompletedEvent(self._current_timestamp,
                                                                                     tracked_order.client_order_id,
                                                                                     tracked_order.base_asset,
                                                                                     tracked_order.quote_asset,
                                                                                     (tracked_order.fee_asset
                                                                                      or tracked_order.quote_asset),
                                                                                     Decimal(tracked_order.amount),
                                                                                     Decimal(
                                                                                         tracked_order.amount * price),
                                                                                     tracked_order.fee_paid,
                                                                                     tracked_order.order_type)
                                self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                     sell_order_completed_event)
                            self.c_stop_tracking_order(tracked_order.client_order_id)
                        elif status == "CANCEL":
                            self.c_stop_tracking_order(client_order_id)
                            self.logger().info(f"The market order {tracked_order.client_order_id} "
                                               f"has been cancelled according to order status API.")
                            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                 OrderCancelledEvent(self._current_timestamp,
                                                                     tracked_order.client_order_id))
                elif event_type == "user-trade":
                    trades = []
                    if event_message["T"] == "u":
                        trades.append(updates)
                    if event_message["T"] == "s":
                        for update in updates["list"]:
                            trades.append(update)

                    for trade in trades:
                        exchange_order_id = trade["orderId"]
                        trade_price = trade["price"]
                        trade_qty = trade["quantity"]
                        is_buy = trade["side"] == "BUY"

                        try:
                            client_order_id, tracker_order = next(
                                (key, value) for key, value in self._in_flight_orders.items()
                                if value.exchange_order_id == str(exchange_order_id))
                        except StopIteration:
                            continue
                        except Exception as e:
                            print("Exception in stram", e)

                        if tracker_order is None:
                            # Hiding the messages for now. Root cause to be investigated in later sprints.
                            self.logger().debug(f"Unrecognized order ID from user stream: {client_order_id}.")
                            self.logger().debug(f"Event: {event_message}")
                            self.logger().debug(f"Order Event: {update}")
                            continue

                        order_filled_event = OrderFilledEvent(
                            self._current_timestamp,
                            client_order_id,
                            tracker_order.trading_pair,
                            tracker_order.trade_type,
                            tracker_order.order_type,
                            trade_price,
                            trade_qty,
                            self.c_get_fee(
                                tracker_order.base_asset,
                                tracker_order.quote_asset,
                                tracker_order.order_type,
                                tracker_order.trade_type,
                                trade_price,
                                trade_qty,
                            )
                        )
                        self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Moonx. "
                                                      "Check API key and network connection.")
                self.logger().error("_status_polling_loop", e)
                await asyncio.sleep(0.5)

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Moonx. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0
        }

    @property
    def ready(self) -> bool:
        self.logger().debug(self.status_dict.values())
        return all(self.status_dict.values())

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request(method="GET", path_url=MOONX_TICKER_URL)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _update_balances(self):
        cdef:
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            list balance_list
            set assets_to_be_removed
            str asset_name
            object free_available
            object locked
            object total_available

        response = await self._api_request("POST", path_url=MOONX_WALLET_URL, is_auth_required=True)
        balance_list = response.get("data")

        for balance in balance_list:
            asset_name = balance["assetCode"]
            free_available = Decimal(str(balance["amountAvailable"]))
            locked = Decimal(str(balance["spotLock"]))
            total_available = free_available + locked
            remote_asset_names.add(asset_name)

            self._account_balances[asset_name] = total_available
            self._account_available_balances[asset_name] = free_available

        assets_to_be_removed = local_asset_names.difference(remote_asset_names)
        for asset_name in assets_to_be_removed:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_order_status(self):
        cdef:
            # The poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t> (self._last_poll_timestamp / 10)
            int64_t current_tick = <int64_t> (self._current_timestamp / 10)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            tasks = [
                self.get_order(await tracked_order.get_exchange_order_id())
                for tracked_order in tracked_orders
            ]
            results = await safe_gather(*tasks, return_exceptions=True)

            for order, tracker_order in zip(results, tracked_orders):
                client_order_id = tracker_order.client_order_id
                if isinstance(order, MoonxAPIException):
                    if order.code in ["101102", "100103", "60080"]:
                        self.logger().info(f"Order doesnt exist {order}")
                        self._order_not_found_records[client_order_id] = self._order_not_found_records.get(
                            client_order_id, 0) + 1
                        if self._order_not_found_records[client_order_id] >= self.ORDER_NOT_EXIST_CONFIRMATION_COUNT:
                            self.c_trigger_event(
                                self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                MarketOrderFailureEvent(self._current_timestamp, client_order_id,
                                                        tracker_order.order_type)
                            )
                            self.logger().info(f"Stopped Tracking {client_order_id}")
                            self.c_stop_tracking_order(client_order_id)

    cdef c_get_fee(self,
                   base_currency: str,
                   quote_currency: str,
                   order_type: OrderType,
                   order_side: TradeType,
                   amount: Decimal,
                   price: Decimal):
        is_maker = order_type is OrderType.LIMIT
        return estimate_fee("moonx", is_maker)

    async def _update_trading_rules(self):
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t> (self._current_timestamp / 60.0)

        if current_tick > last_tick or len(self._trading_rules) < 1:
            exchange_info = await self._api_request("GET", path_url=MOONX_INSTRUMENT_URL, is_auth_required=True)
            trading_rules_list = self._format_trading_rules(exchange_info)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.symbol] = trading_rule

    def _format_trading_rules(self, raw_trading_pair_info: Dict[str, Any]) -> List[MoonxTradingRule]:
        cdef:
            list trading_rules = []

        pair_list = raw_trading_pair_info["data"]
        for trading_pair in pair_list:
            try:
                trading_rules.append(
                    MoonxTradingRule(symbol=trading_pair["symbol"],
                                     priceMultiplier=int(trading_pair["priceMultiplier"]),
                                     priceTick=Decimal(str(trading_pair["priceTick"])),
                                     quantityMultiplier=int(trading_pair["quantityMultiplier"]),
                                     quantityTick=Decimal(str(trading_pair["quantityTick"])),
                                     minimumQuantity=Decimal(str(trading_pair["minimumQuantity"])),
                                     maximumQuantity=Decimal(str(trading_pair["maximumQuantity"])))
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {trading_pair}. Skipping.", exc_info=True)
        return trading_rules

    async def _api_request(self,
                           method: str,
                           path_url: str,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False) -> Dict[str, Any]:

        client = await self._http_client()
        data = data if data is not None else {}

        signed_data = {} if not is_auth_required else self._moonx_auth.sign_it(data)

        response_coro = client.request(
            method=method,
            url=MOONX_BASE_URL + path_url,
            params=params,
            data=ujson.dumps(signed_data),
            timeout=500
        )

        async with response_coro as resp:
            if resp.status != 200:
                raise IOError(f"Error fetching data from {path_url}. HTTP status is {resp.status}.")

            try:
                resp_json = await resp.json()
            except Exception:
                raise IOError(f"Error parsing response json from {path_url}. HTTP status is {resp.status}.")

            is_exchange_api = "exchangeApi" in path_url
            resp_code = resp_json.get("code", None)

            if is_exchange_api and resp_code != SUCCESS_CODE:
                raise MoonxAPIException(resp_code, resp_json)

            # if data is not None:
            #     self.logger().error(f"Error in {path_url} response data : {data}")
            #     raise IOError({"error": resp_json})

            return resp_json

    async def get_order(self, exchange_order_id: str) -> Dict[str, Any]:
        return await self._api_request(method="POST",
                                       path_url=MOONX_GET_ORDER,
                                       data={
                                           "orderNo": exchange_order_id
                                       },
                                       is_auth_required=True)

    async def place_order(self,
                          symbol: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal):

        data = {
            "symbol": symbol,
            "quantity": str(amount),
            "price": str(price) if order_type is OrderType.LIMIT else "0",
            "orderSide": "BUY" if is_buy else "SELL",
            "orderType": "LIMIT" if order_type is OrderType.LIMIT else "MARKET",
            "timeInForce": "GTC"
        }

        return await self._api_request(method="POST", path_url=MOONX_POST_ORDER, data=data, is_auth_required=True)

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object price,
                                object amount,
                                object order_type):
        self._in_flight_orders[client_order_id] = MoonxInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            trade_type=trade_type,
            price=price,
            amount=amount,
            order_type=order_type,
        )

    async def execute_buy(self,
                          client_order_id: str,
                          symbol: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = 0):

        try:
            order_result = None
            if order_type is OrderType.LIMIT:
                self.c_start_tracking_order(
                    client_order_id,
                    "",
                    symbol,
                    TradeType.BUY,
                    price,
                    amount,
                    order_type
                )
            elif order_type is OrderType.MARKET:
                self.c_start_tracking_order(
                    client_order_id,
                    "",
                    symbol,
                    TradeType.BUY,
                    Decimal("0"),
                    amount,
                    order_type,
                )
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            order_result = await self.place_order(symbol=symbol,
                                                  amount=amount,
                                                  order_type=order_type,
                                                  is_buy=True,
                                                  price=price)

            if not order_result["msg"] == "Success":
                raise ValueError("Order has failed", order_result)

            exchange_order_id = str(order_result["data"]["exchangeOrderId"])
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {client_order_id} for "
                                   f"{amount} {symbol}.")
                tracked_order.update_exchange_order_id(exchange_order_id)
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     amount,
                                     price,
                                     client_order_id)
                                 )

        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(client_order_id)
            order_type_str = 'MARKET' if order_type == OrderType.MARKET else 'LIMIT'
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Moonx for "
                f"{amount} {symbol}"
                f" {price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg="Failed to submit buy order to Moonx. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, client_order_id, order_type))

    async def execute_sell(self,
                           client_order_id: str,
                           symbol: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = 0):

        try:
            order_result = None
            if order_type is OrderType.LIMIT:
                self.c_start_tracking_order(
                    client_order_id,
                    "",
                    symbol,
                    TradeType.SELL,
                    price,
                    amount,
                    order_type
                )
            elif order_type is OrderType.MARKET:
                self.c_start_tracking_order(
                    client_order_id,
                    "",
                    symbol,
                    TradeType.SELL,
                    Decimal("0"),
                    amount,
                    order_type,
                )
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            order_result = await self.place_order(symbol=symbol,
                                                  amount=amount,
                                                  order_type=order_type,
                                                  is_buy=False,
                                                  price=price)

            if not order_result["msg"] == "Success":
                raise ValueError("Order has failed", order_result)

            exchange_order_id = str(order_result["data"]["exchangeOrderId"])
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {client_order_id} for "
                                   f"{amount} {symbol}.")
                tracked_order.update_exchange_order_id(exchange_order_id)
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     amount,
                                     price,
                                     client_order_id)
                                 )

        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(client_order_id)
            order_type_str = 'MARKET' if order_type == OrderType.MARKET else 'LIMIT'
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Kraken for "
                f"{amount} {symbol}"
                f" {price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg="Failed to submit buy order to Kraken. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, client_order_id, order_type))

    async def execute_cancel(self, trading_pair: str, client_order_id: str):
        cancel_result = {}
        try:
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is None:
                raise ValueError("Failed to cancel order – {client_order_id}. Order not found.")
            data = {"orderNo": tracked_order.exchange_order_id}
            cancel_result = await self._api_request("POST",
                                                    MOONX_CANCEL_ORDER,
                                                    data=data,
                                                    is_auth_required=True)
        except Exception:
            self.logger().warning("Error cancelling order on Moonx",
                                  exc_info=True)
            raise

        code = cancel_result.get("code", None)
        if code is not None and code == SUCCESS_CODE:
            self.logger().info(f"Successfully cancelled order {client_order_id}.")
            self.c_stop_tracking_order(client_order_id)
            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                 OrderCancelledEvent(self._current_timestamp, client_order_id))
        return {
            "origClientOrderId": client_order_id
        }

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        self.logger().info("Cancel all called")
        for key, value in self._in_flight_orders.items():
            self.logger().info(f"key:: {key}, vale :: {value}")
        incomplete_orders = [(key, o) for (key, o) in self._in_flight_orders.items() if not o.is_done]
        tasks = [self.execute_cancel(o.trading_pair, client_order_id) for (client_order_id, o) in incomplete_orders]
        self.logger().info(f"count task {len(tasks)}")
        order_id_set = set([key for (key, o) in incomplete_orders])
        self.logger().error(f"order_id_set {order_id_set}")
        successful_cancellations = []
        self.logger().error(f"timeout_seconds {timeout_seconds}")

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cr in cancellation_results:
                    self.logger().error(f"cancellation_result :: {cr}")
                    if isinstance(cr, MoonxAPIException):
                        continue
                    if isinstance(cr, dict) and "origClientOrderId" in cr:
                        client_order_id = cr.get("origClientOrderId")
                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
        except Exception:
            self.logger().error(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order with Moonx. Check API key and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    cdef str c_buy(self,
                   str trading_pair,
                   object amount,
                   object order_type=OrderType.MARKET,
                   object price=s_decimal_0,
                   dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = f"buy-{trading_pair}-{tracking_nonce}"

        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price))
        return order_id

    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.MARKET, object price=s_decimal_0,
                    dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = f"sell-{trading_pair}-{tracking_nonce}"
        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price))
        return order_id

    cdef c_cancel(self, str trading_pair, str order_id):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    cdef c_did_timeout_tx(self, str tracking_id):
        self.c_trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                             MarketTransactionFailureEvent(self._current_timestamp, tracking_id))

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            trading_rule = self._trading_rules[trading_pair]
        return trading_rule.priceTick

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        cdef:
            trading_rule = self._trading_rules[trading_pair]
        return trading_rule.quantityTick

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        trading_rule = self._trading_rules[trading_pair]
        quantityTick = trading_rule.quantityTick
        quantized_amount = (amount / quantityTick) * quantityTick

        global s_decimal_0
        if quantized_amount < trading_rule.minimumQuantity:
            return s_decimal_0

        return quantized_amount
