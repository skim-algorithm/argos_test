from abc import ABC, abstractmethod
import logging
import copy
import time
from collections import defaultdict

from common import enum
from common import arg
from common import log
from order import order as o


class Base(ABC):
    def __init__(self, args: arg.Args):
        self.args = args
        self.last_data = None

        # 주문 처리용 변수. symbol : 값 형태의 dict이다.
        self.pos = defaultdict(lambda: None)
        self.opens = defaultdict(list[o.Order])
        self.dones = defaultdict(list[o.Order])
        self.totals = defaultdict(list[o.Order])
        self.cancel_cnts = defaultdict(int)

        # 현재 펀딩피
        self.funding_rate = defaultdict(lambda: 0.0)

        # logging
        self.logging = log.makeLogger(self.args.strategy)

    @abstractmethod
    def init(self, order_done_cb):
        pass

    @abstractmethod
    def on_data(self, datas):
        pass

    @abstractmethod
    def get_funding_rate(self, symbol) -> float:
        pass

    @abstractmethod
    def get_balance(self):
        pass

    @abstractmethod
    def set_max_position_count(self, count):
        pass

    @abstractmethod
    def _send_order_to_exchange(self, order: o.Order) -> int | list[int]:
        pass

    @abstractmethod
    def _send_cancel_to_exchange(self, cancel: o.Cancel) -> bool:
        pass

    def _add_position(self, order: o.Order) -> bool:
        current_pos = self.pos[order.symbol]
        # 포지션이 있는 상태에서도 포지션을 더 늘릴 수 있다.
        if current_pos and current_pos.side != order.side:
            # 단 같은 방향으로만 늘릴 수 있다. 반대는 close를 통해 닫아야 한다.
            self.logging.warning(f"already has position. canceled. {order}")
            return False

        if current_pos:
            ratio = order.quantity / (current_pos.quantity + order.quantity)
            current_pos.open_price = round((current_pos.open_price * (1.0 - ratio)) + (order.open_price * ratio), 10)
            current_pos.quantity = round(current_pos.quantity + order.quantity, 10)
            current_pos.cost = round(current_pos.cost + order.cost, 10)
            # 분할주문 시 rate를 한 번만 더한다.
            if order.is_rate_added is False:
                order.is_rate_added = True
                current_pos.rate = round(current_pos.rate + order.rate, 10)
            to_add = filter(lambda x: x not in current_pos.order_ids, order.order_ids)
            current_pos.order_ids.extend(to_add)
            to_add = filter(lambda x: x not in current_pos.open_order_ids, order.open_order_ids)
            current_pos.open_order_ids.extend(to_add)
        else:
            self.pos[order.symbol] = copy.deepcopy(order)
            order.is_rate_added = True
        
        return True

    def __process_open_order(self, order) -> int | list[int]:
        # open 주문은 reduce_only를 true로 보낸다.
        order.reduce_only = False

        self.logging.info(f"open: {order.to_json()}")
        return self._send_order_to_exchange(order)

    def __process_close_order(self, order) -> int | list[int]:
        pos = self.pos[order.symbol]
        if not pos:
            self.logging.error(f"No position to close. order={order}")
            return 0

        order.side = enum.OrderSide.BUY if pos.side is enum.OrderSide.SELL else enum.OrderSide.SELL

        # close 주문은 rate로 보내지 않는다.
        quantity = round(order.rate * pos.quantity, 10)
        for open_order in self.opens[order.symbol]:
            if open_order.opt is order.opt and open_order.order_type is order.order_type:
                quantity = round(quantity - open_order.quantity, 10)
        order.quantity = quantity
        self.logging.info(f'quantity: {round(order.rate * pos.quantity, 10)} -> {quantity}')

        # close 주문은 reduce_only를 true로 보낸다.
        order.reduce_only = True

        self.logging.info(f"close: {order.to_json()}")
        return self._send_order_to_exchange(order)

    def __process_order(self, order: o.Order) -> int | list[int]:     
        # cancel 주문이 CANCELED 될 때까지 기다린다.
        cnt = 0
        self.logging.info(f"self.cancel_cnts[cancel.symbol]: {self.cancel_cnts[order.symbol]}")
        while self.cancel_cnts[order.symbol] > 0 and cnt < 500:
            if cnt % 100 == 0:
                self.logging.info(f"waiting for previous order to be CANCELED")
            cnt += 1
            time.sleep(0.01)
        else:
            self.cancel_cnts[order.symbol] = 0

        if order.opt is enum.OrderOpt.OPEN:
            order_id = self.__process_open_order(order)
        elif order.opt is enum.OrderOpt.CLOSE:
            order_id = self.__process_close_order(order)

        if not order_id:
            self.logging.error(f"Failed to process order. order={order}")
            return 0

        elif isinstance(order_id, list):
            order.order_ids = order_id[:]
            order.open_order_ids = order_id[:]
        else:
            order.order_ids = [order_id]
            order.open_order_ids = [order_id]

        self.opens[order.symbol].append(order)
        return order_id

    def __send_order(
        self,
        ex_alias,
        symbol,
        side,
        opt,
        order_type,
        price,
        rate,
        rate_base,
        quantity,
        order_id,
        working_type,
        activation_price,
        callback_rate,
    ) -> int | list[int]:
        order: o.Order = o.Order(
            ex_alias=ex_alias,
            strategy_name=self.args.nickname,
            symbol=symbol.upper(),
            opt=opt,
            side=side,
            order_type=order_type,
            quantity=quantity,
            rate=rate,
            rate_base=rate_base,
            price=price,
            stop_price=price,
            working_type=working_type,
            activation_price=activation_price,
            callback_rate=callback_rate,
        )

        if err := order.check_valid():
            raise ValueError(err)

        return self.__process_order(order)

    def __send_cancel(self, ex_alias, symbol, order_id) -> bool:
        cancel: o.Cancel = o.Cancel(
            ex_alias=ex_alias,
            strategy_name=self.args.nickname,
            symbol=symbol.upper(),
            order_id=order_id,
        )

        # True면 성공, False면 실패 (2052b85c3ea386f2ee0121851be40ba653fbf5e2 에서 변경됨)
        return self._send_cancel_to_exchange(cancel)

    def open(self, symbol, side, order_type, price, rate, rate_base, quantity, working_type) -> int | list[int]:
        """
        새로운 주문을 추가한다.
        """
        # slippage 기록을 위해 MARKET 주문일 때에도 최근가로 price를 채운다.
        if order_type is enum.OrderType.MARKET and self.last_data:
            for s, data, _ in self.last_data:
                if symbol == s:
                    price = data.iloc[-1].close

        return self.__send_order(
            ex_alias=self.args.ex_alias,
            symbol=symbol,
            side=side,
            opt=enum.OrderOpt.OPEN,
            order_type=order_type,
            price=price,
            rate=rate,
            rate_base=rate_base,
            quantity=quantity,
            order_id=0,
            working_type=working_type,
            activation_price=0.0,
            callback_rate=0.0,
        )

    def close(
        self,
        symbol,
        order_type,
        price,
        rate,
        working_type,
        activation_price,
        callback_rate,
    ) -> int | list[int]:
        """
        잡고 있던 포지션을 청산한다.
        """

        # slippage 기록을 위해 MARKET 주문일 때에도 최근가로 price를 채운다.
        if order_type is enum.OrderType.MARKET and self.last_data:
            for s, data, _ in self.last_data:
                if symbol == s:
                    price = data.iloc[-1].close

        return self.__send_order(
            ex_alias=self.args.ex_alias,
            symbol=symbol,
            side=None,
            opt=enum.OrderOpt.CLOSE,
            order_type=order_type,
            price=price,
            rate=rate,
            rate_base=None,
            quantity=0.0,
            order_id=0,
            working_type=working_type,
            activation_price=activation_price,
            callback_rate=callback_rate,
        )

    def cancel(self, symbol, order_id) -> bool:
        return self.__send_cancel(
            ex_alias=self.args.ex_alias,
            symbol=symbol,
            order_id=order_id,
        )

    def get_position(self, symbol):
        return self.pos[symbol]

    def get_open_orders(self, symbol):
        return self.opens[symbol]
