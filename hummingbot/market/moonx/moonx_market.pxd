from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker
from hummingbot.market.market_base cimport MarketBase

cdef class MoonxMarket(MarketBase):
    cdef:
        object _moonx_auth
        object _user_stream_tracker
        dict _in_flight_orders
        dict _order_not_found_records
        double _last_poll_timestamp
        double _last_timestamp
        double _poll_interval
        object _poll_notifier
        object _shared_client
        public object _status_polling_task
        dict _trading_rules
        public object _trading_rules_polling_task
        public object _user_stream_tracker_task
        public object _user_stream_event_listener_task
        TransactionTracker _tx_tracker
        object _async_scheduler

    cdef c_did_timeout_tx(self, str tracking_id)
    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object price,
                                object amount,
                                object order_type)
