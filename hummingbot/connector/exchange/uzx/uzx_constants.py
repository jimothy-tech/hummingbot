from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "x-UZX3PCSN"
MAX_ORDER_ID_LEN = 32

# Base URL
REST_URL = "https://api-v2.uzx.com"
WSS_PUBLIC_URL = "wss://stream.uzx.com/notification/ws"
WSS_PRIVATE_URL = "wss://stream.uzx.com/notification/pri/ws"

# Public API endpoints or UzxClient function
TICKER_PATH_URL = "/notification/spot/{}/ticker"
TICKER_BOOK_PATH_URL = "/notification/spot/tickers"
PRODUCTS_INFO_PATH_URL = "/v2/products"
PING_PATH_URL = "/v2/time"
SNAPSHOT_PATH_URL = "/notification/spot/{}/orderbook"
SERVER_TIME_PATH_URL = "/v2/time"

# Private API endpoints or UzxClient function
BALANCES_PATH_URL = "/v2/account/balances"
ORDER_DETAILS_PATH_URL = "/v2/trade/order/details"
ORDERS_HISTORY_PATH_URL = "/v2/trade/history/orders"
PLACE_ORDER_PATH_URL = "/v2/trade/spot/order"
CANCEL_ORDER_PATH_URL = "/v2/trade/cancel-order"

WS_HEARTBEAT_TIME_INTERVAL = 9

# Uzx params

SIDE_BUY = 1
SIDE_SELL = 2

TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# WS event types
ORDER_CHANGE_EVENT_TYPE = "orderV2.spot"


# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 5000

# Order States
ORDER_STATE = {
    0: OrderState.OPEN,
    4: OrderState.FILLED,
    1: OrderState.PARTIALLY_FILLED,
    2: OrderState.PENDING_CANCEL,
    3: OrderState.CANCELED,
}


ORDER_TYPE = {
    OrderType.MARKET: 1,
    OrderType.LIMIT: 2,
    OrderType.LIMIT_MAKER: 5
}

# Websocket event types
DIFF_EVENT_TYPE = "spot.orderBook"
TRADE_EVENT_TYPE = "trade"

RATE_LIMITS = [
    # Pools
    RateLimit(limit_id=REQUEST_WEIGHT, limit=6000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=100, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=ORDERS_24HR, limit=200000, time_interval=ONE_DAY),
    RateLimit(limit_id=RAW_REQUESTS, limit=61000, time_interval=5 * ONE_MINUTE),

    # Weighted Limits
    RateLimit(limit_id=PLACE_ORDER_PATH_URL, limit=3, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=3, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDER_DETAILS_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDERS_HISTORY_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=BALANCES_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKER_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=PRODUCTS_INFO_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKER_BOOK_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=PING_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]


ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
