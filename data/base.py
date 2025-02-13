import json
import datetime
import os
from collections import defaultdict
import requests

import pandas as pd
from abc import ABC, abstractmethod

from common import helper
from common import arg
from common import log
from common.config import Config as config


class ArquesDateTime:
    @staticmethod
    def get_timestamp(_datetime):
        timestamp = datetime.datetime.strptime(_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp = int(timestamp.timestamp()) * 1000
        return timestamp

    @staticmethod
    def convert_timestamp_from_datetime(_datetime):
        _datetime = _datetime.replace(tzinfo=datetime.timezone.utc)
        return int(_datetime.timestamp()) * 1000

    @staticmethod
    def convert_datetime_from_timestamp(_timestamp):
        _datetime = datetime.datetime.fromtimestamp(_timestamp / 1000, tz=datetime.timezone.utc)
        return _datetime

    @staticmethod
    def get_nowtime_string():
        _datetime_utc = datetime.datetime.now(tz=datetime.timezone.utc)
        log_time = _datetime_utc.isoformat(sep=" ", timespec="milliseconds").split("+")[0].replace(" ", "T") + "Z"
        # timestamp = datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        return log_time

    @staticmethod
    def get_nowtime():
        _datetime_utc = datetime.datetime.now(tz=datetime.timezone.utc)
        # log_time = _datetime_utc.isoformat(sep=' ', timespec='milliseconds').split('+')[0].replace(" ", "T") + "Z"
        # timestamp = datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        return _datetime_utc

    @staticmethod
    def convert_datetime_from_string(_datetime, format="%Y-%m-%d"):
        timestamp = datetime.datetime.strptime(_datetime, format)
        return timestamp

    @staticmethod
    def convert_string_from_datetime(_datetime, format="%Y-%m-%d"):
        time = datetime.datetime.strftime(_datetime, format)
        timesplit = time.split(".")
        try:
            if len(timesplit[1].replace("Z", "")) > 3:
                time = timesplit[0] + "." + timesplit[1][:3] + "Z"
            return time
        except Exception:
            return time


class Base(ABC):
    @abstractmethod
    def init(self, on_data):
        pass

    @abstractmethod
    def set_variable(self, symbol, key, value):
        pass

    @abstractmethod
    def save_variable_cache(self, key, value):
        pass

    def __init__(self, args: arg.Args):
        self.args = args
        self.data_length = (args.history_days) * helper.calculate_number_of_intervals_per_day(args.interval)

        # 전략에서 사용하는 변수들을 저장하는 맵: [symbol][key] = value
        self.variables = defaultdict(lambda: {})
        self.variables_cache = defaultdict(lambda: {})

        # logging
        self.logging = log.makeLogger(args.strategy)

    def _get_data(self, symbol, start, end):
        data = self.__try_load_from_cache(symbol, start, end)
        if data is None:
            # 캐시된 데이터가 없을 경우 새로 받아와서 저장한다.
            try:
                data = self.__load_from_api_server(symbol.lower(), start, end)
            except Exception:
                import traceback

                err_msg = f"[{self.args.strategy}] Error\n{traceback.format_exc()}"
                print(err_msg)
                pass

        data["datetime"] = pd.to_datetime(data["datetime"])
        data.drop_duplicates("datetime", inplace=True)
        data.set_index(keys="datetime", inplace=True)

        if self.args.fill_missing_data:
            data = self.__fill_missing_data(data)

        return data

    def _get_funding_rate(self, symbol, start, end):
        import ccxt

        # CCXT client initialization
        if not hasattr(self, 'ccxt_client'):
            self.ccxt_client = ccxt.binance(
                {
                    'rateLimit': 1200,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future'}
                })
        # Verify the symbol
        ccxt_symbol = self.__get_symbol(symbol)
        markets = self.ccxt_client.load_markets()
        if ccxt_symbol not in markets:
            raise ValueError(f"Unsupported symbol: {ccxt_symbol}")

        # Fetch funding rates
        try:
            funding_rate = self.ccxt_client.fetch_funding_rate(ccxt_symbol)
            # Create a DataFrame and format the data
            data = {
                "symbol": [funding_rate["symbol"]],
                "funding_rate": [funding_rate["fundingRate"]],
                "datetime": [pd.to_datetime(funding_rate["timestamp"], unit="ms", utc=True)],
            }
            df = pd.DataFrame(data)
            df.set_index("datetime", inplace=True)

            self.logging.info(f"({symbol}) funding rate loaded: {funding_rate['fundingRate']} at {df.index[0]}")

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self.logging.error(f"Failed to fetch funding rate for {symbol}: {str(e)}")

            # Create a fallback DataFrame with funding rate = 0
            df = pd.DataFrame(
                {
                    "symbol": [symbol],
                    "funding_rate": [0],
                    "datetime": [pd.to_datetime(start, unit="ms", utc=True)],
                }
            )
            df.set_index("datetime", inplace=True)

        return df

    def __get_json(self, file_name):
        filePath = f"{os.path.dirname(os.path.realpath(__file__))}/../{file_name}.json"
        with open(filePath) as json_file:
            json_data = json.load(json_file)
            return json_data

    def __get_cache_dir(self, symbol, start, end):
        cache_path = helper.create_directory("/data_cache")
        start_str = start.strftime("%Y-%m-%d-%H-%M-%S")
        end_str = end.strftime("%Y-%m-%d-%H-%M-%S")
        filename = (
            f"{self.args.ex_name}_{self.args.ex_class}_{symbol}_" f"{self.args.interval}_{start_str}_{end_str}.csv"
        )
        full_path = os.path.join(cache_path, filename)
        return full_path

    def __try_load_from_cache(self, symbol, start, end):
        file_path = self.__get_cache_dir(symbol, start, end)
        try:
            return pd.read_csv(file_path)
        except IOError:
            self.logging.info(f"{symbol} No cache found. Trying to download from GCP.")
            return None

    def __get_symbol(self, symbol):
        if symbol.lower() == "btcusdt":
            return "BTC/USDT"
        elif symbol.lower() == "ethusdt":
            return "ETH/USDT"
        return symbol

    def __load_from_api_server(self, symbol, start, end):
        import ccxt
        import pandas as pd
        ccxt_symbol = self.__get_symbol(symbol)

        # CCXT 클라이언트 초기화
        if not hasattr(self, 'ccxt_client'):
            self.ccxt_client = ccxt.binance({'rateLimit': 1200, 'enableRateLimit': True})

        # 심볼 유효성 확인
        markets = self.ccxt_client.load_markets()
        if ccxt_symbol not in markets:
            raise ValueError(f"Unsupported symbol: {ccxt_symbol}")

        # 시간 변환 (UTC 기준 및 초 단위)
        if isinstance(start, datetime.datetime):
            start_timestamp = int(start.timestamp() * 1000)  # 밀리초 단위로 변환
        else:
            start_timestamp = int(
                ArquesDateTime.convert_datetime_from_string(start, format="%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000
            )

        if isinstance(end, datetime.datetime):
            end_timestamp = int(end.timestamp() * 1000)
        else:
            end_timestamp = int(
                ArquesDateTime.convert_datetime_from_string(end, format="%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000
            )

        # Interval 유효성 확인
        interval_map = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "8h": "8h",
            "12h": "12h",
            "1d": "1d",
            "3d": "3d",
            "1w": "1w",
            "1M": "1M",
        }

        if self.args.interval not in interval_map:
            raise ValueError(f"Unsupported interval: {self.args.interval}")

        ccxt_interval = interval_map[self.args.interval]

        # 시간 범위 유효성 검사
        now_timestamp = int(datetime.datetime.utcnow().timestamp() * 1000)
        if start_timestamp < 0 or start_timestamp > now_timestamp:
            raise ValueError(f"Invalid start time: {start_timestamp}")

        # 데이터프레임 초기화
        data = pd.DataFrame(columns=["timestamp", "datetime", "open", "high", "low", "close", "volume"], dtype="object")

        # OHLCV 데이터 로드
        limit = 1000
        while start_timestamp < end_timestamp:
            try:
                ohlcv = self.ccxt_client.fetch_ohlcv(ccxt_symbol, timeframe=ccxt_interval, since=start_timestamp,
                                                     limit=limit)
            except ccxt.NetworkError as e:
                raise RuntimeError(f"Network error: {str(e)}")
            except ccxt.ExchangeError as e:
                raise RuntimeError(f"Exchange error: {str(e)}")

            if not ohlcv:
                break

            chunk = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            chunk["datetime"] = pd.to_datetime(chunk["timestamp"], unit="ms", utc=True)
            chunk = chunk[["timestamp", "datetime", "open", "high", "low", "close", "volume"]]

            if data.empty:
                data = chunk
            else:
                data = pd.concat([data, chunk], ignore_index=True, copy=False)

            start_timestamp = int(chunk.iloc[-1]["timestamp"]) + 1

        data.to_csv(self.__get_cache_dir(symbol, start, end), header=True, index=False)
        return data

    def __fill_missing_data(self, df):
        interval = self.args.interval
        interval = interval.replace("m", "min")
        interval = interval.replace("h", "H")
        interval = interval.replace("d", "D")
        df = df.resample(interval).asfreq()
        df["volume"].fillna(0, inplace=True)
        df["close"].fillna(method="ffill", inplace=True)
        df["open"].fillna(df["close"], inplace=True)
        df["high"].fillna(df["close"], inplace=True)
        df["low"].fillna(df["close"], inplace=True)
        return df

    def get_variable(self, symbol, key):
        return self.variables[symbol].get(key, None)

    def get_variable_cache(self, key):
        return self.variables_cache.get(key, None)

    def get_all_variables(self, symbol):
        return self.variables[symbol]
