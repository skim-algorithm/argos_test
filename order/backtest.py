import sys
from collections import defaultdict

from . import base
from order import order as o
from common import enum
from common import arg


class BacktestOrder(base.Base):
    def __init__(self, args: arg.Args):
        base.Base.__init__(self, args)
        self.usd = args.backtest.initial_usd
        self.commission = args.backtest.commission
        self.order_seq = 1
        self.total_value = self.usd
        self.total_profit = 0.0

        self.symbol_usd = defaultdict(lambda: 0.0)
        self.symbol_value = defaultdict(lambda: 0.0)
        for sym in args.symbols:
            self.symbol_usd[sym] = args.backtest.initial_usd
            self.symbol_value[sym] = self.symbol_usd[sym]

        self.symbol_profit = defaultdict(lambda: 0.0)

        self.max_position_count = sys.maxsize
        self.position_count = 0

    def _send_order_to_exchange(self, order: o.Order) -> int:
        # 백테스트에서는 실제 전송을 하지 않고 seq를 orderId로 사용한다.
        order_id = self.order_seq
        self.order_seq += 1
        return order_id

    def _send_cancel_to_exchange(self, cancel: o.Cancel) -> bool:
        # 백테스트에서는 항상 취소가 성공한다.
        if order_id := cancel.order_id:
            self.opens[cancel.symbol] = [o for o in self.opens[cancel.symbol] if order_id not in o.order_ids]
        else:
            self.opens[cancel.symbol].clear()

        return True

    def __get_order_done_price(self, order, ohlcv) -> float:
        if order.order_type is enum.OrderType.MARKET:
            return ohlcv["open"]
        elif order.order_type is enum.OrderType.STOP or order.order_type is enum.OrderType.STOP_MARKET:
            if order.side is enum.OrderSide.BUY:
                if ohlcv["open"] > order.price:
                    # STOP BUY 주문이 오픈가 보다 낮을 경우 "already triggered"로 마켓 가격으로 사진다.
                    return ohlcv["open"]
                elif ohlcv["high"] >= order.price:
                    # 지정가 보다 높게 가격이 올라갔다면 지정가로 구입한다.
                    return order.price
            else:  # SELL
                if ohlcv["open"] < order.price:
                    # STOP SELL 주문이 오픈가보다 높을 경우 "already triggered"로 마켓 가격으로 팔아진다.
                    return ohlcv["open"]
                elif ohlcv["low"] <= order.price:
                    # 지정가 보다 낮게 가격이 내려갔다면 지정가로 판매한다.
                    return order.price
        elif order.order_type is enum.OrderType.TAKE_PROFIT or order.order_type is enum.OrderType.TAKE_PROFIT_MARKET:
            if order.side is enum.OrderSide.BUY:
                if ohlcv["open"] < order.price:
                    # PROFIT BUY 주문이 오픈가 보다 높을 경우 "already triggered"로 마켓 가격으로 사진다.
                    return ohlcv["open"]
                elif ohlcv["low"] <= order.price:
                    # 지정가 보다 낮게 가격이 내려갔다면 지정가로 구입한다.
                    return order.price
            else:  # SELL
                if ohlcv["open"] > order.price:
                    # PROFIT SELL 주문이 오픈가보다 낮을 경우 "already triggered"로 마켓 가격으로 팔아진다.
                    return ohlcv["open"]
                elif ohlcv["high"] >= order.price:
                    # 지정가 보다 높게 가격이 올라갔다면 내려갔다면 지정가로 판매한다.
                    return order.price
        elif order.order_type is enum.OrderType.LIMIT:
            if order.side is enum.OrderSide.BUY:
                if ohlcv["low"] <= order.price:
                    return order.price
            else:
                if ohlcv["high"] >= order.price:
                    return order.price
        elif order.order_type is enum.OrderType.TRAILING_STOP_MARKET:
            # trailing stop이 시작되지 않았다면, 시작 여부를 검사한다.
            if not order.is_trailing_activated:
                # activation price가 주어지지 않았다면, 항상 MARKET 가격으로 발동한다.
                if not order.activation_price:
                    order.is_trailing_activated = True
                    order.activation_price = ohlcv["open"]
                # activation price가 주어졌다면, 발동 여부를 검사한다.
                elif order.activation_price:
                    if order.activation_price >= ohlcv["low"] and order.activation_price <= ohlcv["high"]:
                        order.is_trailing_activated = True

            # trailing stop이 시작되었다면, 방향에 따라 가격을 설정한다.
            if order.is_trailing_activated:
                trailing_price = 0.0
                if order.side is enum.OrderSide.BUY:
                    trailing_price = ohlcv["low"] * (1.0 + order.callback_rate / 100)
                    if order.trailing_price == 0.0 or order.trailing_price > trailing_price:
                        order.trailing_price = trailing_price
                        # self.logging.info(f'Trailing price set: {trailing_price}, act: {order.activation_price},  h: {ohlcv["high"]}, l: {ohlcv["low"]}')  # noqa: E501

                    # 설정한 가격을 초과했었다면 해당 가격으로 거래한다.
                    if ohlcv["high"] >= order.trailing_price:
                        return order.trailing_price
                else:
                    trailing_price = ohlcv["high"] * (1.0 - order.callback_rate / 100)
                    if order.trailing_price == 0.0 or order.trailing_price < trailing_price:
                        order.trailing_price = trailing_price
                        # self.logging.info(f'Trailing price set: {trailing_price}, act: {order.activation_price}, h: {ohlcv["high"]}, l: {ohlcv["low"]}')  # noqa: E501

                    # 설정한 가격을 초과했었다면 해당 가격으로 거래한다.
                    if ohlcv["low"] <= order.trailing_price:
                        return order.trailing_price

        return 0.0

    def __get_balance_from_rate_base(self, rate_base, symbol=None):
        if symbol is None:
            if rate_base is enum.RateBase.BALANCE:
                # 포지션 상관 없이 현재 가지고 있는 usd. 포지션이 있으면 수수료만 차감된 금액
                return self.usd
            if rate_base is enum.RateBase.MARGIN_BALANCE:
                # 포지션이 있다면 현재 가지고 있는 포지션 가치를 더한 금액 (portfolio value)
                return self.get_value()
            if rate_base is enum.RateBase.AVAILABLE_BALANCE:
                # 포지션이 있다면 해당 포지션을 잡는데 사용한 금액만큼을 제외한 잔액
                pos_cost = 0.0
                for p in self.pos.values():
                    if p:
                        pos_cost += p.cost
                return self.usd - pos_cost + self.total_profit

            self.logging.error(f"Invalid rate base type: {rate_base}")
            return self.get_value()
        else:
            if rate_base is enum.RateBase.BALANCE:
                # 포지션 상관 없이 현재 가지고 있는 usd. 포지션이 있으면 수수료만 차감된 금액
                return self.symbol_usd[symbol]
            if rate_base is enum.RateBase.MARGIN_BALANCE:
                # 포지션이 있다면 현재 가지고 있는 포지션 가치를 더한 금액 (portfolio value)
                return self.get_value(symbol)
            if rate_base is enum.RateBase.AVAILABLE_BALANCE:
                # 포지션이 있다면 해당 포지션을 잡는데 사용한 금액만큼을 제외한 잔액
                if self.pos[symbol] is None:
                    pos_cost = 0.0
                else:
                    pos_cost = self.pos[symbol].cost
                return self.symbol_usd[symbol] - pos_cost + self.symbol_profit[symbol]

            self.logging.error(f"Invalid symbol rate base type: {rate_base}")
            return self.get_value(symbol)

    def __process_open_order_done(self, order: o.Order, ohlcv) -> bool:
        price = self.__get_order_done_price(order, ohlcv)
        if not price:
            return False

        if self.position_count >= self.max_position_count:
            self.logging.warning(f"max_position_count({self.max_position_count})" f"exceeded. cancel order: {order}")
            return True

        sym_quantity = None
        if not order.quantity:
            balance = self.__get_balance_from_rate_base(order.rate_base)
            order.quantity = round((order.rate * balance) / (price * (1.0 + self.commission)), 10)

            symbol_balance = self.__get_balance_from_rate_base(order.rate_base, order.symbol)
            sym_quantity = round((order.rate * symbol_balance) / (price * (1.0 + self.commission)), 10)

        order.open_price = price
        order.cost = price * order.quantity
        order.open_time = ohlcv.name
        order.open_type = order.order_type

        # 거래 금액 만큼 수수료 차감.
        self.usd -= order.quantity * price * self.commission
        self.symbol_usd[order.symbol] -= (sym_quantity or order.quantity) * price * self.commission

        # 포지션 업데이트
        new_position = self.pos[order.symbol] is None
        if self._add_position(order):
            if new_position:
                self.position_count += 1
        else:
            return True

        self.dones[order.symbol].append(order)

        return True

    def __calculate_close_profit(self, symbol, close_price, close_quantity=0.0) -> float:
        pos = self.pos.get(symbol)
        if not pos:
            return 0.0

        quantity = close_quantity if close_quantity else pos.quantity

        if pos.side is enum.OrderSide.BUY:
            return (quantity * close_price) - (quantity * pos.open_price)
        else:
            return (quantity * pos.open_price) - (quantity * close_price)

    def __process_close_order_done(self, order: o.Order, ohlcv) -> bool:
        pos = self.pos[order.symbol]
        if not pos:
            # 여러 close 주문을 동시에 넣은 경우 하나가 체결되면 다른 close 주문은 취소한다.
            self.logging.warning(
                f"Open position is already closed. Other close orders will be cancelled. {order.to_json()}"
            )
            return True

        price = self.__get_order_done_price(order, ohlcv)
        if not price:
            return False

        if order.quantity > pos.quantity:
            self.logging.error(f"close quantity too big. position={pos}, order={order}")
            order.quantity = pos.quantity

        profit = self.__calculate_close_profit(order.symbol, price, order.quantity)
        self.usd += profit
        self.symbol_usd[order.symbol] += profit

        # 수수료 차감
        order.cost = price * order.quantity
        self.usd -= order.cost * self.commission
        self.symbol_usd[order.symbol] -= order.cost * self.commission

        # 포지션 초기화 (혹은 감소 처리)
        if pos.quantity == order.quantity:
            self.pos[order.symbol] = None
        else:
            pos.quantity = round(pos.quantity - order.quantity, 10)
            pos.cost -= order.cost

        # close할 포지션의 정보 기록
        order.open_price = pos.open_price
        order.open_time = pos.open_time
        order.open_type = pos.open_type
        order.funding_fee = pos.funding_fee

        # close 주문 정보 업데이트
        order.close_time = ohlcv.name
        order.close_price = price
        order.close_type = order.order_type

        # PnL은 거래 가격만 고려한 PnL과 수수료를 제외한 실제 벌어들인 금액을 저장한다.
        order.pnl = profit
        open_commission = (pos.open_price * order.quantity) * self.commission
        close_commission = order.cost * self.commission
        order.pnl_w_comm = profit - open_commission - close_commission

        self.dones[order.symbol].append(order)

        self.position_count -= 1
        return True

    def __process_order_done(self, order, ohlcv):
        if order.opt is enum.OrderOpt.OPEN:
            return self.__process_open_order_done(order, ohlcv)
        elif order.opt is enum.OrderOpt.CLOSE:
            return self.__process_close_order_done(order, ohlcv)
        return False

    def __update_value(self, datas):
        self.total_profit = 0.0

        self.all_pos_cost = 0.0
        for symbol, df, _ in datas:
            # get all pos cost for calculating all_cash_values
            if self.get_position(symbol):
                self.all_pos_cost += self.get_position(symbol).cost

            self.total_profit += self.__calculate_close_profit(symbol, df.iloc[-1]["close"])

            sym_profit = self.__calculate_close_profit(symbol, df.iloc[-1]["close"])
            self.symbol_profit[symbol] = sym_profit
            self.symbol_value[symbol] = self.symbol_usd[symbol] + sym_profit

        self.total_value = self.usd + self.total_profit

    def __update_funding_rate(self, symbol, funding_rate, close_price):
        self.funding_rate[symbol] = funding_rate

        # 포지션이 있을 경우 변경되는 펀딩피 적용.
        if pos := self.pos[symbol]:
            fee = pos.quantity * close_price * funding_rate
            if pos.side == enum.OrderSide.SELL:
                # 포지션이 sell이면 fee가 반대로 적용된다.
                fee *= -1

            self.usd -= fee
            self.symbol_usd[symbol] -= fee
            pos.funding_fee += fee
            self.logging.info(f"funding fee applied. symbol={symbol} fee={fee}, rate={funding_rate}")

    def init(self, order_done_cb):
        self.order_done_cb = order_done_cb

    def on_data(self, datas):
        """
        datas = [(symbol, dataframe, funding_rate)]
        """
        self.last_data = datas

        for symbol, df, funding_rate in datas:
            ohlcv = df.iloc[-1]

            # 펀딩피 업데이트 및 적용.
            # 이전 포지션에 대해 적용하므로 주문 처리 전에 수행.
            if funding_rate:
                self.__update_funding_rate(symbol, funding_rate, ohlcv["close"])

            open_orders = [o for o in self.opens[symbol]]
            done_order_ids = []

            for order in open_orders:
                if self.__process_order_done(order, ohlcv):
                    done_order_ids.extend(order.order_ids)

                    if not order.cost:
                        # 이미 포지션이 있거나 close가 되어 자동 취소되는 주문.
                        self.logging.warning(f"[{ohlcv.name}] order cancel: {order.to_json()}")
                        continue

                    self.logging.info(f"done: {order.to_json()}")

                    # 포지션 변화에 의한 profit 업데이트
                    self.__update_value(datas)
                    self.order_done_cb(order)

            # 체결된 주문이 없어도 profit은 계속 변한다.
            self.__update_value(datas)

            # 완료 처리된 주문을 목록에서 제거한다.
            self.opens[symbol] = [
                o for o in self.opens[symbol] if not all(elem in done_order_ids for elem in o.order_ids)
            ]

    def get_value(self, symbol=None):
        if symbol is None:
            return self.total_value
        else:
            return self.symbol_value[symbol]

    def get_usd(self, symbol=None):
        if symbol is None:
            return self.usd
        else:
            return self.symbol_usd[symbol]

    def get_funding_rate(self, symbol) -> float:
        return self.funding_rate[symbol]

    def get_balance(self):
        return {
            "totalMarginBalance": self.__get_balance_from_rate_base(enum.RateBase.MARGIN_BALANCE),
            "totalWalletBalance": self.__get_balance_from_rate_base(enum.RateBase.BALANCE),
            "availableBalance": self.__get_balance_from_rate_base(enum.RateBase.AVAILABLE_BALANCE),
        }

    def set_max_position_count(self, count):
        self.max_position_count = count
