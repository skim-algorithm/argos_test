import json
import redis
import threading
import requests
import os
import copy

from datetime import datetime
from collections import defaultdict

from . import base
from common import enum
from common import helper
from common.config import Config as config
from order import order as o


class Listener(threading.Thread):
    def __init__(self, r: redis.Redis, notify_order, target):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(target)
        self.notify_order = notify_order

    def run(self):
        for item in self.pubsub.listen():
            try:
                if item.get("data") == b"2" or item.get("data") == 2:
                    # 긴급 종료
                    os._exit(1)

                elif item.get("data") != b"1" and item.get("data") != 1:
                    msg = item.get("data", "").decode("utf8").replace("'", '"')
                    self.notify_order(json.loads(msg))
            except Exception:
                pass


class LiveOrder(base.Base):
    def __init__(self, args):
        base.Base.__init__(self, args)
        self.funding_rate_next_ts = defaultdict(lambda: 0)
        self.account_info = {}

    def _send_order_to_exchange(self, order: o.Order) -> int | list[int]:
        order_config = config.order()
        url = f'http://{order_config["Url"]}:{order_config["Port"]}/order'

        res = requests.post(url, json=order.to_json())
        data = res.json()
        self.logging.info(f"_send_order_to_exchange: order={order.to_json()}, res={data}")

        if res.status_code != 200:
            return None

        if data is None:
            return None

        def get_order_id_list(_data):
            if isinstance(_data, list):
                return [x for l in (map(lambda x: get_order_id_list(x), _data)) for x in l]  # noqa: E741
            return [_data.get("orderId")]

        orderIdList = get_order_id_list(data)
        self.logging.info(f"_send_order_to_exchange: orderIds={orderIdList}")

        if len(orderIdList) == 1:
            return orderIdList[0]
        elif len(orderIdList) > 1:
            return orderIdList
        else:
            return None

    def _send_cancel_to_exchange(self, cancel: o.Cancel) -> bool:
        order_config = config.order()
        url = f'http://{order_config["Url"]}:{order_config["Port"]}/cancel'

        res = requests.post(url, json=cancel.to_json())
        data = res.json()
        self.logging.info(f"_send_cancel_to_exchange: cancel={cancel.to_json()}, res={data}")

        if res.status_code != 200:
            return False

        if data is None:
            return True

        def get_order_id_list(_data):
            if isinstance(_data, list):
                return [x for l in (map(lambda x: get_order_id_list(x), _data)) for x in l]  # noqa: E741
            return [_data.get("orderId")]

        orderIdList = get_order_id_list(data)
        self.logging.info(f"_send_cancel_to_exchange: orderIds={orderIdList}")

        self.logging.info(f"self.cancel_cnts[cancel.symbol]: {self.cancel_cnts[cancel.symbol]} + {len(orderIdList)}")
        self.cancel_cnts[cancel.symbol] += len(orderIdList)
        return True
        
    def __process_open_order_done(self, order: o.Order, msg):
        price = msg.get('AveragePrice')
        # TODO: 현재는 모든 주문이 채워졌을 때에만 노티를 주기 때문에 accum을 사용.
        #       나중에 모든 체결 내용을 보내주는 경우 다르게 구현 필요.
        quantity = msg.get('OrderFilledAccumQuantity')

        current_pos = self.pos[order.symbol]

        if current_pos and current_pos.side != order.side:
            # 단 같은 방향으로만 늘릴 수 있다. 반대는 close를 통해 닫아야 한다.
            self.logging.warning(f"already has position. canceled. {order}")
            return True
        
        order.open_price = price
        order.cost = round(price * quantity, 10)
        order.quantity = quantity
        order.open_time = datetime.now()
        order.open_type = order.order_type

        # on_all_order_done() 에서 전달하는 전체 주문 정보
        total_order = None
        for total in self.totals[order.symbol]:
            if total.order_ids == order.order_ids:
                total_order = total
        if total_order is None:
            total_order = copy.deepcopy(order)            
            self.totals[order.symbol].append(total_order)
        else:
            ratio = order.quantity / (total_order.quantity + order.quantity)
            total_order.open_price = round((total_order.open_price * (1.0 - ratio)) + (order.open_price * ratio), 10)
            total_order.quantity = round(total_order.quantity + order.quantity, 10)
            total_order.cost = round(total_order.cost + order.cost, 10)

        if not self._add_position(order):
            return True

        if len(order.open_order_ids) == 0:
            self.dones[order.symbol].append(order)

        return True

    def __process_close_order_done(self, order: o.Order, msg):
        pos = self.pos[order.symbol]
        if not pos:
            self.logging.error(f"Position is not opened. order={order}")
            return True

        self.logging.info(f'pos[{order.symbol}]={pos}')

        price = msg.get('AveragePrice')
        # TODO: 현재는 모든 주문이 채워졌을 때에만 노티를 주기 때문에 accum을 사용.
        #       나중에 모든 체결 내용을 보내주는 경우 다르게 구현 필요.
        quantity = msg.get('OrderFilledAccumQuantity')

        if quantity == pos.quantity:
            self.pos[order.symbol] = None
        elif quantity > pos.quantity:
            self.logging.error(f'Close quantity too big. position={pos}, order={order}')
            self.pos[order.symbol] = None
        else:
            self.pos[order.symbol].quantity = round(self.pos[order.symbol].quantity - quantity, 10)

        # close할 포지션의 정보 기록
        order.open_time = pos.open_time
        order.open_price = pos.open_price
        order.open_type = pos.open_type

        # close 주문 정보 업데이트
        order.close_time = datetime.now()
        order.close_price = price
        order.close_type = order.order_type

        order.quantity = quantity

        self.dones[order.symbol].append(order)
        self.logging.info(f"position after closed:{self.get_position(order.symbol)}")

        # on_all_order_done() 에서 전달하는 전체 주문 정보
        total_order = None
        for total in self.totals[order.symbol]:
            if total.order_ids == order.order_ids:
                total_order = total
        if total_order is None:
            total_order = copy.deepcopy(order)            
            self.totals[order.symbol].append(total_order)
        else:
            ratio = order.quantity / (total_order.quantity + order.quantity)

            # close할 포지션의 정보 기록
            total_order.open_time = pos.open_time
            total_order.open_price = pos.open_price
            total_order.open_type = pos.open_type

            # close 주문 정보 업데이트
            total_order.close_time = datetime.now()            
            total_order.close_price = round((total_order.close_price * (1.0 - ratio)) + (order.close_price * ratio), 10)
            total_order.close_type = order.order_type

            total_order.quantity = round(total_order.quantity + order.quantity, 10)

        return True

    def __process_order_done(self, order: o.Order, msg):
        if order.opt is enum.OrderOpt.OPEN:
            return self.__process_open_order_done(order, msg)
        elif order.opt is enum.OrderOpt.CLOSE:
            return self.__process_close_order_done(order, msg)
     
        return False

    def __process_order_done_from_msg(self, msg):
        # NOTE(vince): 웹소켓 응답이며, 여기에서는 OrderID가 하나씩만 전달된다.
        order_id = msg.get("OrderID")
        status = msg.get("OrderStatus")
        symbol = msg.get("Symbol")

        self.logging.info(f'order_id info: {order_id}')
        self.logging.info(f'open info: {self.opens[symbol]}')

        done_order = None
        remain_orders = []
        for order in self.opens[symbol]:
            self.logging.info(f'order info: {order}')
            self.logging.info(f'order_ids info: {order.order_ids}')
            self.logging.info(f'open_order_ids info: {order.open_order_ids}')
            if order_id in order.order_ids:
                order.open_order_ids.remove(order_id)
                done_order = order
                self.logging.info(f'length of order.open_order_ids: {len(order.open_order_ids)}')
            if len(order.open_order_ids) > 0:
                remain_orders.append(order)

        if done_order is None:
            # 비정상적인 상황.
            self.logging.error(f'open orders: {self.opens[symbol]}')
            self.logging.error(f'Unexpected order received. order_id={order_id}')
            return None

        self.logging.info(f"status info: {status}")
        self.logging.info(f"Remain_orders: {remain_orders}")
        self.logging.info(f"Done_orders: {done_order}")

        self.opens[symbol] = remain_orders
        self.logging.info(f"Processing order done: {done_order.to_json()}")

        if status == 'FILLED':
            self.__process_order_done(done_order, msg)
            self.logging.info(f'filled order done')
        elif status == 'CANCELED':
            # 주문 취소는 open_orders에서 제거한 것으로 끝.
            # TODO: 만약 주문 cancel을 받아서 동작하는 전략이 있다면 수정 필요.
            self.logging.info(f"self.cancel_cnts[cancel.symbol]: {self.cancel_cnts[symbol]} - 1")
            self.cancel_cnts[symbol] -= 1
            return None
        else:
            self.logging.error(f"unexpected order status: order={msg}")
            return None

        return done_order

    def __create_req_common(self):
        req = {}
        req["strategy_name"] = self.args.nickname
        req["exchange_alias"] = self.args.ex_alias
        return req

    def __get_order_url(self):
        order_config = config.order()
        url = f'http://{order_config["Url"]}:{order_config["Port"]}'
        return url

    def __update_author(self):
        req = self.__create_req_common()
        url = f"{self.__get_order_url()}/update_author"

        res = requests.post(url, json=req)
        if not res.ok:
            # 호환성을 위해, 또 critical한 설정이 아니므로 로그만 남긴다
            self.logging.info(f"Could not set author: err={res.json()}")

    def __get_account(self):
        req = self.__create_req_common()
        url = f"{self.__get_order_url()}/account"

        res = requests.get(url, json=req)
        if res.status_code != 200:
            self.logging.error(f"Failed to get account info: err={res.json()}")
            return
        data = res.json()
        return data

    def __load_positions(self):
        req = self.__create_req_common()
        url = f"{self.__get_order_url()}/positions"

        res = requests.get(url, json=req)
        if res.status_code != 200:
            raise Exception(f"Failed to load positions: err={res.json()}")

        positions = res.json()
        if not positions:
            return

        for pos in positions:
            # 거래소의 position_size는 부호가 포함되어있다.
            position_size = float(pos["pa"])

            order = o.Order(
                ex_alias=self.args.ex_alias,
                strategy_name=self.args.nickname,
                symbol=pos["s"],
                opt=enum.OrderOpt.OPEN,
                quantity=abs(position_size),
                price=float(pos["ep"]),
            )

            if position_size > 0.0:
                order.side = enum.OrderSide.BUY
            elif position_size < 0.0:
                order.side = enum.OrderSide.SELL
            else:
                self.logging.warning(f"Invalid position size: pos={pos}")
                continue

            self.logging.info(f"Position loaded: order={order.to_json()}")

            # TODO: 현재는 symbol별 포지션이 하나만 존재할 수 있다.
            if self.pos[order.symbol]:
                raise Exception(f"Position already exist: " f"pos={self.pos[order.symbol]}, order={order}")

            self.pos[order.symbol] = order

    def __load_open_orders(self):
        req = self.__create_req_common()
        url = f"{self.__get_order_url()}/open_orders"

        res = requests.get(url, json=req)
        if res.status_code != 200:
            raise Exception(f"Failed to load open orders: err={res.json()}")

        open_orders = res.json()
        if not open_orders:
            return

        for res_order in open_orders:
            price = max(float(res_order["price"]), float(res_order["stopPrice"]))
            order = o.Order(
                ex_alias = self.args.ex_alias,
                strategy_name = self.args.nickname,
                symbol = res_order['symbol'],
                opt = enum.OrderOpt.CLOSE if res_order['reduceOnly'] else enum.OrderOpt.OPEN,
                order_ids = list([res_order['orderId']]),
                open_order_ids = list([res_order['orderId']]),
                side = enum.OrderSide[res_order['side']],
                order_type = enum.OrderType[res_order['type']],
                quantity = round(float(res_order['origQty']) - float(res_order['executedQty']), 10),
                price = price,
                stop_price = price,
                working_type = enum.WorkingType[res_order['workingType']],
                reduce_only = res_order['reduceOnly'],
            )

            self.opens[order.symbol].append(order)
            self.logging.info(f"loaded open order: {order.to_json()}")

    def __send_leverage_to_exchange(self):
        if not self.args.leverage:
            self.logging.info("Use account default leverage")
            return

        url = f"{self.__get_order_url()}/leverage"

        for symbol in self.args.symbols:
            req = self.__create_req_common()
            req["symbol"] = symbol
            req["leverage"] = self.args.leverage

            res = requests.post(url, json=req)
            if res.status_code != 200:
                raise Exception(f"Failed to set leverage: err={res.json()}")

            data = res.json()
            if data["leverage"] != self.args.leverage:
                raise Exception(f"Leverage res is not valid: err={res.json()}")

        self.logging.info(f"leverage initialized at {self.args.leverage}")

    def init(self, order_done_cb, all_order_done_cb):
        self.order_done_cb = order_done_cb
        self.all_order_done_cb = all_order_done_cb

        # redis
        redis_config = config.redis()
        self.redis_client = redis.Redis(host=redis_config["Url"], port=redis_config["Port"], db=0)
        subscribe = Listener(
            self.redis_client,
            notify_order=self.on_order_done,
            target=self.args.ex_alias,
        )
        subscribe.start()

        self.__get_account()
        self.__update_author()
        self.__load_positions()
        self.__load_open_orders()
        self.__send_leverage_to_exchange()

    def on_data(self, datas):
        self.last_data = datas

    def get_funding_rate(self, symbol) -> float:
        return None
        # TODO sungmkim - CCXT or Argos_Order
        # now_ts = helper.now_ts()
        # next_ts = self.funding_rate_next_ts[symbol]
        #
        # if now_ts >= next_ts:
        #     # funding rate 갱신 시간이 지났으므로 새로운 funding rate를 요청한다.
        #     req = self.__create_req_common()
        #     req["symbol"] = symbol
        #     url = f"{self.__get_order_url()}/funding_rate"
        #
        #     res = requests.get(url, json=req)
        #     if res.status_code != 200:
        #         self.logging.error(f"Failed to get funding rate: err={res.json()}")
        #     else:
        #         data = res.json()
        #         self.funding_rate_next_ts[symbol] = data["nextFundingTime"]
        #         self.funding_rate[symbol] = float(data["lastFundingRate"])
        #
        # return self.funding_rate[symbol]

    def get_balance(self):
        from typing import Final

        EXPIRATION_MS: Final = 60000  # 필요에 따라 가감할 것

        now_ts = helper.now_ts()
        is_expired = ("expiration" not in self.account_info) or self.account_info["expiration"] < now_ts
        if not is_expired:
            return {
                "totalMarginBalance": self.account_info["totalMarginBalance"],
                "totalWalletBalance": self.account_info["totalWalletBalance"],
                "availableBalance": self.account_info["totalMarginBalance"] - self.account_info["totalInitialMargin"],
            }

        req = self.__create_req_common()
        url = f"{self.__get_order_url()}/account"

        res = requests.get(url, json=req)
        if res.status_code != 200:
            self.logging.error(f"Failed to get account info: err={res.json()}")
            return

        data = res.json()
        self.account_info = data
        # NOTE(vince): asset별 잔고는 아직 사용하지 않는다. 사용하게 되면 아래 코드를 활용할 수 있다.
        # self.account_info['assets'] = {datum['asset']: datum for datum in data['assets']}
        self.account_info["expiration"] = now_ts + EXPIRATION_MS
        return {
            "totalMarginBalance": self.account_info["totalMarginBalance"],
            "totalWalletBalance": self.account_info["totalWalletBalance"],
            "availableBalance": self.account_info["totalMarginBalance"] - self.account_info["totalInitialMargin"],
        }

    def on_order_done(self, data):
        try:
            self.logging.info(f"done: {data}")
            order = self.__process_order_done_from_msg(data)
            if order:
                self.order_done_cb(order)
                if len(order.order_ids) > 0 and len(order.open_order_ids) == 0:
                    total_order = None
                    for total in self.totals[order.symbol]:
                        if total.order_ids == order.order_ids:
                            total_order = total
                    if total_order is not None:
                        self.all_order_done_cb(total_order)

        except Exception:
            import traceback

            self.logging.error("caught in on_order_done()", exc_info=True)
            err_msg = f"[{self.args.strategy}] Error\n{traceback.format_exc()}"
            helper.send_slack(err_msg, self.args.author)

    def set_max_position_count(self, count):
        pass
