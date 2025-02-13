from dataclasses import dataclass, field
import datetime
from common import enum


@dataclass
class Order:
    ex_alias: str
    strategy_name: str
    symbol: str
    opt: enum.OrderOpt
    order_ids: list[int] = field(default_factory=list)
    open_order_ids: list[int] = field(default_factory=list)
    side: enum.OrderSide = None
    order_type: enum.OrderType = None
    quantity: float = 0.0
    rate: float = 0.0
    rate_base: enum.RateBase = None
    is_rate_added: bool = False
    price: float = 0.0
    stop_price: float = 0.0
    working_type: enum.WorkingType = None
    reduce_only: bool = False
    activation_price: float = 0.0
    callback_rate: float = 0.0

    # 백테스트에서 주문 체결 후 저장 및 업데이트 되는 값들
    open_time: datetime.date = None
    close_time: datetime.date = None
    open_price: float = 0.0
    close_price: float = 0.0
    open_type: enum.OrderType = None
    close_type: enum.OrderType = None
    profit: float = 0.0
    cost: float = 0.0
    pnl: float = 0.0
    pnl_w_comm: float = 0.0
    returns: float = 0.0
    funding_fee: float = 0.0
    is_trailing_activated = False
    trailing_price = 0.0

    def check_valid(self):
        if not self.strategy_name:
            return "strategy_name is missing"

        if not self.symbol:
            return "symbol is missing."

        if not self.quantity and not self.rate:
            return "Either quantity or rate is required."

        if self.order_type is not enum.OrderType.MARKET and self.order_type is not enum.OrderType.TRAILING_STOP_MARKET:
            if not self.price:
                return "Any order type other than MARKET requires price."

        if self.order_type is enum.OrderType.STOP or self.order_type is enum.OrderType.STOP_MARKET:
            if not self.working_type:
                return "STOP(_MARKET) order requires working_type."

        if self.order_type is enum.OrderType.TRAILING_STOP_MARKET:
            if not self.callback_rate:
                return "TRAILING_STOP_MARKET order requires callback_rate."
            if self.opt is not enum.OrderOpt.CLOSE:
                return "TRAILING_STOP_MARKET order can only be used to close."
            if self.callback_rate < 0.1 or self.callback_rate > 5.0:
                return "TRAILING_STOP_MARKET's callback_rate must be within the range (0.1, 5.0)"

        # TODO: 현재 close와 buy/sell을 동시에 보내는 경우가 있어 아래 검사 주석처리.
        #       항상 close가 먼저 되고 buy/sell이 이루어지기 때문에 문제가 없다.
        #       추후 여러 포지션을 동시에 가지고 있을 수 있도록 개선 필요.
        # if order_opt is base.OrderOpt.OPEN and position_side is not None:
        #     return "Cannot open new order when you have position."

        # TODO: 항상 close보다 먼저 체결이 되는 open 주문이 있다는 가정하에
        #       포지션이 없어도 close를 전송할 수 있도록 수정한다.
        #       잘못된 사용에 대해 체크할 수 있는 로직 등의 개선 필요.
        # if order_opt is enum.OrderOpt.CLOSE and position_side is None:
        #     return "No position to close."

        if self.quantity and self.quantity < 0.0:
            return "Quantity must be no less than 0.0"

        if self.rate and self.rate < 0.0:
            return "Rate must be no less than 0.0"

        return ""

    def to_json(self):
        """
        주문 시스템에서 사용하는 형태의 json 포멧으로 주문 반환.
        """
        order = {}
        order["exchange_alias"] = self.ex_alias
        order["strategy_name"] = self.strategy_name
        order["symbol"] = self.symbol

        if self.side:
            order["side"] = self.side.value

        if self.order_type:
            order["type"] = self.order_type.value

        order["quantity"] = self.quantity
        order["rate"] = self.rate

        if self.rate_base:
            order["rate_base"] = self.rate_base.value

        if (
            self.order_type is enum.OrderType.STOP
            or self.order_type is enum.OrderType.STOP_MARKET
            or self.order_type is enum.OrderType.TAKE_PROFIT
            or self.order_type is enum.OrderType.TAKE_PROFIT_MARKET
        ):
            order["stop_price"] = self.stop_price
            order["working_type"] = self.working_type.value
        elif self.order_type is enum.OrderType.TRAILING_STOP_MARKET:
            order["activation_price"] = self.activation_price
            order["callback_rate"] = self.callback_rate
            order["working_type"] = self.working_type.value
        else:
            order["price"] = self.price

        order["reduce_only"] = self.reduce_only

        return order


@dataclass
class Cancel:
    ex_alias: str
    strategy_name: str
    symbol: str
    order_id: int = 0

    def to_json(self):
        """
        주문 시스템에서 사용하는 형태의 json 포멧으로 취소 주문 반환.
        """
        cancel = {}
        cancel["exchange_alias"] = self.ex_alias
        cancel["strategy_name"] = self.strategy_name
        cancel["symbol"] = self.symbol
        cancel["order_id"] = self.order_id

        return cancel


@dataclass
class OrderResult:
    order: Order
    status: enum.OrderStatus
    commission_asset: str
    commission: float
    average_price: float
