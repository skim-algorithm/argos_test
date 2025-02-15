import json
import datetime
import os
from collections import defaultdict
import requests

import ccxt
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
        start_time = start
        end_time = end
        ms = 1000
        one_minutes: int = 60 * ms
        limit = 300
        results = pd.DataFrame(columns=["symbol", "funding_rate", "datetime"])
        while start_time < end_time:
            funding_rate_history = self.ccxt_client.fetch_funding_rate_history(
                ccxt_symbol,
                since=start_time,
                limit=limit,
                params={"endTime": end_time}
            )
            if not funding_rate_history:
                break
            data = {
                "symbol": [rate["symbol"] for rate in funding_rate_history],
                "funding_rate": [rate["fundingRate"] for rate in funding_rate_history],
                "datetime": [pd.to_datetime(rate["timestamp"], unit="ms", utc=True) for rate in funding_rate_history],
            }
            df = pd.DataFrame(data)
            results = pd.concat([results, df], ignore_index=True)
            start_time = funding_rate_history[-1]["timestamp"] + one_minutes  # Increment start time to avoid duplicates
        
        results.set_index("datetime", inplace=True)
        self.logging.info(f"({symbol}) funding rate history loaded from {start} to {end}")
        return results

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
        elif symbol.lower() == "bnbusdt":
            return "BNB/USDT"
        elif symbol.lower() == "solusdt":
            return "SOL/USDT"
        return symbol

    def __load_from_api_server(self, symbol, start, end):
        ccxt_symbol = self.__get_symbol(symbol)
        ms = 1000
        one_minutes: int = 60 * ms
        limit = 1000

        # CCXT 클라이언트 초기화
        if not hasattr(self, 'ccxt_client'):
            self.ccxt_client = ccxt.binance({
                'rateLimit': 1200,
                'enableRateLimit': True,
                'options': {'defaultType': 'future'}
            })

        # 심볼 유효성 확인
        markets = self.ccxt_client.load_markets()
        if ccxt_symbol not in markets:
            raise ValueError(f"Unsupported symbol: {ccxt_symbol}")

        # 시간 변환 (UTC 기준 및 초 단위)
        if isinstance(start, datetime.datetime):
            start_timestamp = int(start.timestamp() * ms)  # 밀리초 단위로 변환
        else:
            start_timestamp = int(
                ArquesDateTime.convert_datetime_from_string(start, format="%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * ms
            )

        if isinstance(end, datetime.datetime):
            end_timestamp = int(end.timestamp() * ms)
        else:
            end_timestamp = int(
                ArquesDateTime.convert_datetime_from_string(end, format="%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * ms
            )

        # Interval 유효성 확인
        interval_map = { "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1h", "1d": "1d", "1w": "1w", "1M": "1M" }
        if self.args.interval not in interval_map:
            raise ValueError(f"Unsupported interval: {self.args.interval}")
        ccxt_interval = interval_map[self.args.interval]

        # 시간 범위 유효성 검사
        now_timestamp = int(datetime.datetime.utcnow().timestamp() * ms)
        if start_timestamp < 0 or start_timestamp > now_timestamp:
            raise ValueError(f"Invalid start time: {start_timestamp}")

        # 데이터프레임 초기화
        data = pd.DataFrame(
			columns=["timestamp", "datetime", "open", "high", "low", "close", "volume"], 
			dtype="object"
		)
        # OHLCV 데이터 로드
        while start_timestamp <= end_timestamp:
            try:
                ohlcv = (self.ccxt_client.fetch_ohlcv
                         (ccxt_symbol,
                          timeframe=ccxt_interval,
                          since=start_timestamp,
                          limit=limit))
            except ccxt.NetworkError as err:
                raise RuntimeError(f"Network error: {str(err)}")
            except ccxt.ExchangeError as err:
                raise RuntimeError(f"Exchange error: {str(err)}")
            if not ohlcv:
                break

            chunk = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            chunk["datetime"] = pd.to_datetime(chunk["timestamp"], unit="ms", utc=True)
            chunk = chunk[["timestamp", "datetime", "open", "high", "low", "close", "volume"]]

            if data.empty:
                data = chunk
            else:
                data = pd.concat([data, chunk], ignore_index=True, copy=False)
            start_timestamp = ohlcv[-1][0] + one_minutes

            readable_start_time = datetime.datetime.fromtimestamp(start_timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
            readable_end_time = datetime.datetime.fromtimestamp(end_timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
            print(f"({symbol}) {ccxt_symbol} OHLCV data loaded from {readable_start_time} to {readable_end_time}")

        data.to_csv(self.__get_cache_dir(symbol, start, end), header=True, index=False)
        return data

    def __fill_missing_data(self, df):
        interval = self.args.interval
        interval = interval.replace("m", "min")
        interval = interval.replace("h", "H")
        interval = interval.replace("d", "D")
        df = df.resample(interval).asfreq()
        df["volume"] = df["volume"].fillna(0)
        df["close"] = df["close"].ffill()
        df["open"] = df["open"].fillna(df["close"])
        df["high"] = df["high"].fillna(df["close"])
        df["low"] = df["low"].fillna(df["close"])
        return df

    def get_variable(self, symbol, key):
        return self.variables[symbol].get(key, None)

    def get_variable_cache(self, key):
        return self.variables_cache.get(key, None)

    def get_all_variables(self, symbol):
        return self.variables[symbol]
