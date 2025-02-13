from enum import Enum


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    STOP = "STOP"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderOpt(Enum):
    CLOSE = "close"
    OPEN = "open"
    CANCEL = "cancel"


class OrderStatus(Enum):
    FILL = "fill"
    CANCELED = "canceled"


class RateBase(Enum):
    BALANCE = "balance"
    AVAILABLE_BALANCE = "available_balance"
    MARGIN_BALANCE = "margin_balance"


class WorkingType(Enum):
    MARK_PRICE = "MARK_PRICE"
    CONTRACT_PRICE = "CONTRACT_PRICE"


class ErrorCode(Enum):
    EC_OK = 0
