from collections import defaultdict
import threading
import datetime
import time
import json

import pandas as pd
import redis

from library.binance import client
from library.binance import websockets
from library.binance import helpers
from data import base
from common.config import Config as config
from common import helper


class CandlestickWebsocketData(threading.Thread):
    def __init__(self, exchange_class, strategy, symbol, interval, callback, load_history):
        threading.Thread.__init__(self)
        self.exchange_class = exchange_class
        self.strategy = strategy
        self.symbol = symbol
        self.interval = interval
        self.callback = callback
        self.load_history = load_history
        self.history_end_ts = None
        self._keepalive_timer = None
        self._last_data = None
        self._keepalive_interval = 60  # interval 1-min

    def run(self):
        # TODO sungmkim - get from api_key.json
        self.client = client.Client("api_key", "api_secret")
        self.__start_socket()

    def _start_keepalive_timer(self):
        callback = self._keepalive_socket

        self._keepalive_timer = helpers.RepeatTimer(self._keepalive_interval, callback)
        self._keepalive_timer.setDaemon(True)
        self._keepalive_timer.start()

    def _keepalive_socket(self):
        interval = self._keepalive_interval * 1e3  # in milliseconds
        now = round(time.time() * 1e3)
        if self._last_data and (now - self._last_data["E"] > interval):
            print("Restart CandlestickWebsocketData")
            self.__restart_socket()

    def __start_socket(self):
        self.socket = websockets.BinanceSocketManager(self.client)

        if self.exchange_class == "futures":
            self.connection_key = self.socket.start_kline_futures_socket(
                self.symbol.upper(), self.__on_msg, self.interval
            )
        else:
            self.connection_key = self.socket.start_kline_socket(self.symbol.upper(), self.__on_msg, self.interval)

        self.socket.start()
        self._start_keepalive_timer()

    def __restart_socket(self):
        self._keepalive_timer.cancel()
        self.socket.stop_socket(self.connection_key)
        self.__start_socket()

    def __on_msg(self, msg):
        data = msg.get("data")
        self._last_data = data

        if data is None:
            data = msg

        if data["e"] == "error":
            msg = f"[{self.strategy}][{self.symbol}] socket error: {data}"
            print(msg)
            helper.send_slack(msg)
            self.__restart_socket()
            return

        # 최초로 도착하는 메시지의 시작 시간이 불러올 이전 기록의 종료 시간이 된다.
        if self.history_end_ts is None:
            self.history_end_ts = data["k"]["t"] - 1
            self.load_history(self.symbol, self.history_end_ts)

        # x 가 True인 경우에는 지정된 기간의 캔들스틱이 닫혔다는 뜻이므로 콜백을 호출한다.
        if data["k"]["x"] is True:
            self.callback(self.symbol, data["k"])


class LiveData(base.Base):
    def __init__(self, args):
        base.Base.__init__(self, args)
        self.ws = defaultdict(lambda: None)
        self.datas = defaultdict(lambda: None)
        self.columns = [
            "datetime",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        self.last_update_ts = 0

    def init(self, on_data):
        redis_conf = config.redis()
        self.redis = redis.StrictRedis(host=redis_conf["Url"], port=redis_conf["Port"], db=1)
        self.__load_redis_variables()
        self.__load_redis_variables_cache()

        self.on_data = on_data

        for symbol in self.args.symbols:
            self.ws[symbol] = CandlestickWebsocketData(
                self.args.ex_class,
                self.args.strategy,
                symbol,
                self.args.interval,
                self.__candlestick_callback,
                self.__load_history,
            )

            self.ws[symbol].start()

    def set_variable(self, symbol, key, value):
        self.variables[symbol][key] = value
        name = self.__get_redis_name(symbol)
        self.redis.hset(name, key, value)

        return True

    def save_variable_cache(self, key, value):
        self.variables_cache[key] = value

        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)

        name = self.__get_cache_redis_name()
        self.redis.hset(name, key, value)

        return True

    def __get_redis_name(self, symbol):
        return f"{self.args.strategy}-{self.args.ex_name}-{self.args.ex_alias}-" f"{self.args.ex_class}-{symbol}"

    def __get_cache_redis_name(self):
        return f"{self.args.strategy}-{self.args.ex_name}-{self.args.ex_alias}-" f"{self.args.ex_class}-cache"

    def __load_redis_variables(self):
        check_symbols = self.args.symbols + ["DEFAULT"]
        for symbol in check_symbols:
            redis_data = self.redis.hgetall(self.__get_redis_name(symbol))
            if not redis_data:
                continue

            # reset_variable이 True일 경우엔 기존에 저장된 값을 사용하지 않는다.
            if self.args.reset_variables:
                self.redis.hdel(self.__get_redis_name(symbol), *redis_data.keys())
                continue

            for k, v in redis_data.items():
                key = k.decode("utf-8")
                value = v.decode("utf-8")
                self.variables[symbol][key] = helper.convert_to_original(value)

            self.logging.info(f"variables loaded. s={symbol}, v={self.variables[symbol]}")

    def __load_redis_variables_cache(self):
        redis_cache = self.redis.hgetall(self.__get_cache_redis_name())
        if not redis_cache:
            return

        if self.args.reset_variables:
            self.redis.hdel(self.__get_cache_redis_name(), *redis_cache.keys())
            return

        for k, v in redis_cache.items():
            key = k.decode("utf-8")
            value = v.decode("utf-8")
            self.variables_cache[key] = helper.convert_to_original(value)

        self.logging.info("variables cache loaded.")

    def __candlestick_callback(self, symbol, data):
        ohlcv = [
            data["t"],
            float(data["o"]),
            float(data["h"]),
            float(data["l"]),
            float(data["c"]),
            float(data["v"]),
        ]
        series = self.__build_series_from_ohlcv(ohlcv)

        expected = self.datas[symbol].iloc[-1].name + datetime.timedelta(
            seconds=helper.interval_in_seconds(self.args.interval)
        )
        while expected < series.name:
            msg = f"[{self.args.strategy}][{symbol}] data missing. expected={expected}, received={series.name}"
            self.logging.info(msg)
            helper.send_slack(msg)
            if isinstance(expected, pd.Timestamp):
                int_expected = int(expected.timestamp() * 1000)
            if isinstance(series.name, datetime.datetime):
                int_series_name = int(series.name.timestamp() * 1000)

            missing_df = self.__get_history_from_exchange(symbol, int_expected, int_series_name, 10)
            df = missing_df.loc[expected, :]
            if not df.empty:
                self.logging.info(f"[{symbol}] new data={df.name}")
                # self.datas[symbol] = self.datas[symbol].append(df)
                self.datas[symbol] = pd.concat([self.datas[symbol], df.to_frame().T])

                expected = self.datas[symbol].iloc[-1].name + datetime.timedelta(
                    seconds=helper.interval_in_seconds(self.args.interval)
                )
            else:
                fail_msg = f"[{symbol}] failed to retrieve data"
                self.logging.info(fail_msg)
                helper.send_slack(fail_msg)
                break

        if expected > series.name:
            warn_msg = f"[{symbol}] unexpected data. expected={expected}, received={series.name}"
            self.logging.info(warn_msg)
            helper.send_slack(warn_msg)
            # 이 분봉에 대한 동작은 이미 완료하였으므로 callback을 건너뛴다
            return

        #self.datas[symbol] = self.datas[symbol].append(series)
        self.datas[symbol] = pd.concat([self.datas[symbol], series.to_frame().T])

        # self.logging.info(series.name)
        self.__clear_old_df(symbol)

        # 모든 symbol의 데이터가 일치하는지 확인.
        datas = []
        prev_date = None
        for s, df in self.datas.items():
            new_date = df.iloc[-1].name

            if prev_date and prev_date != new_date:
                # 다른 symbol의 데이터를 더 기다려야 하는 상황
                return

            prev_date = new_date
            datas.append((s, df, 0.0))  # 현재 live에서는 funding rate 정보를 주지 않는다.

        self.last_update_ts = helper.now_ts()
        try:
            self.on_data(datas)
        except Exception:
            import traceback

            self.logging.error("caught in on_data()", exc_info=True)
            err_msg = f"[{self.args.strategy}] Error\n{traceback.format_exc()}"
            helper.send_slack(err_msg, self.args.author)

    def __build_series_from_ohlcv(self, ohlcv):
        dt = datetime.datetime.fromtimestamp(ohlcv[0] / 1000, tz=datetime.timezone.utc)
        series = pd.Series(ohlcv, self.columns[1:])
        series.name = dt
        return series

    def __load_history(self, symbol, history_end_ts):
        history_end_time = datetime.datetime.fromtimestamp(history_end_ts / 1000, tz=datetime.timezone.utc)
        history_start = history_end_time - datetime.timedelta(days=self.args.history_days)
        today_start_time = datetime.datetime(
            history_end_time.year,
            history_end_time.month,
            history_end_time.day,
            tzinfo=datetime.timezone.utc,
        )
        today_start_ts = int(today_start_time.timestamp() * 1000)

        # 아퀴스 데이터에서 오늘 날짜 직전까지의 kline 정보를 받아온다.
        self.datas[symbol] = self._get_data(symbol, history_start, today_start_time)
        # self.logging.info(f'__get_data\n{self.datas[symbol]}')

        # 거래소에서 직접 오늘 하루치 kline 정보를 받아온다.
        kline_dataframe = self.__get_history_from_exchange(symbol, today_start_ts, history_end_ts, 1500)
        # self.logging.info(f'__get_history_from_exchange\n{kline_dataframe}')

        self.datas[symbol] = pd.concat([self.datas[symbol], kline_dataframe])
        # self.logging.info(f'concated\n{self.datas[symbol]}')

        self.logging.info(
            f"[{symbol}] data loaded. total len={len(self.datas[symbol])}\n from kline len={len(kline_dataframe)}"
        )

    def __build_kline_df(self, kline_history):
        filtered_list = [
            [
                datetime.datetime.fromtimestamp(x[0] / 1000, tz=datetime.timezone.utc),
                x[0],
                float(x[1]),
                float(x[2]),
                float(x[3]),
                float(x[4]),
                float(x[5]),
            ]
            for x in kline_history
        ]
        df = pd.DataFrame(filtered_list, columns=self.columns)
        df.set_index("datetime", inplace=True)
        return df

    def __clear_old_df(self, symbol):
        df = self.datas[symbol]
        if len(df) > self.data_length:
            drop_length = len(df) - self.data_length
            df.drop(df.index[:drop_length], inplace=True)

    def __get_history_from_exchange(self, symbol, start, end, limit):
        # TODO sungmkim - get from api_key.json
        self.client = client.Client("api_key", "api_secret")
        kline_history = None
        if self.args.ex_class == "futures":
            kline_history = self.client.futures_klines(
                symbol=symbol.upper(),
                interval=self.args.interval,
                limit=limit,
                startTime=start,
                endTime=end,
            )
        else:
            kline_history = self.client.get_klines(
                symbol=symbol.upper(),
                interval=self.args.interval,
                limit=limit,
                startTime=start,
                endTime=end,
            )

        return self.__build_kline_df(kline_history)
