import datetime
import json
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib

from strategies.base.strategy import Strategy
from . import base as mode
from data import live as data
from order import backtest, live
from common import helper


class ApiHandler(BaseHTTPRequestHandler):
    def __init__(self, data_handler, *args, **kwargs):
        self.data_handler = data_handler
        super().__init__(*args, **kwargs)

    def _json_default(self, value):
        # logging.info('json_default method:', value)
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.isoformat()
        raise TypeError("not JSON serializable")

    def do_GET(self):
        # logging.info(self.path)
        status = "success"
        check_duration = helper.interval_in_seconds(self.data_handler.args.interval)
        if self.data_handler.last_update_ts == 0:
            status = "initializing"
        elif helper.now_ts() - self.data_handler.last_update_ts > 2 * check_duration * 1000:
            status = "websocket disconnected"
            msg = (
                f"[{self.data_handler.args.strategy}] error status. "
                f"last_updated={datetime.datetime.fromtimestamp(self.data_handler.last_update_ts/1000.0)}"
            )
            print(msg)
            helper.send_slack(msg)

        result = {"result": status}

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(result, default=self._json_default), "utf-8"))

    def log_message(self, format, *args):
        # 콘솔에 매번 로그가 찍히는 것 방지용
        pass


class LiveMode(mode.Base):
    def __init__(self, strategy_name, is_live):
        mode.Base.__init__(self, strategy_name, is_live)
        self.is_live = is_live
        self.logging.info(f'is_live: {is_live}, strategy_name: {strategy_name}')

        # 라이브 모드에서 사용할 데이터 핸들러 등록
        self.data_handler = data.LiveData(self.args)

        # 라이브 모드에서 사용할 주문 핸들러 등록
        if is_live:
            self.order_handler = live.LiveOrder(self.args)
        else:
            self.order_handler = backtest.BacktestOrder(self.args)

    def __on_data(self, datas):
        self.order_handler.on_data(datas)

        datas_wo_funding_rate = [(s, d) for s, d, f in datas]
        self.strategy.on_data(datas_wo_funding_rate)

    def __on_order_done(self, order):
        self.logging.info(f'__on_order_done: order={order}')
        self.strategy.on_order_done(order)

    def __on_all_order_done(self, order):
        self.logging.info(f'__on_all_order_done: order={order}')
        self.strategy.on_all_order_done(order)

    def run(self, variables = list()):
        self.logging.info(f'run: start,  variables: {variables}')
        self.data_handler.init(self.__on_data)
        self.order_handler.init(self.__on_order_done, self.__on_all_order_done)
        self.logging.info(f'self.args: {self.args}, self.args.strategy: {self.args.strategy}')

        strategy = importlib.import_module('strategies.' + self.args.strategy)
        self.logging.info(f'run: strategy: {strategy}')

        self.strategy: Strategy = getattr(strategy, self.args.strategy)(
            self.data_handler, self.order_handler, self.logging
        )

        self.strategy.on_start()

        handler = partial(ApiHandler, self.data_handler)
        server = HTTPServer(("", 9000), handler)

        mode_name = "live" if self.is_live else "live_paper"
        # logging.info(f'{mode_name} mode started running.')
        self.logging.info(f"{mode_name} mode started running.")
        server.serve_forever()
