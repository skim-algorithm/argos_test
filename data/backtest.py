import datetime
import sys
import typing
from dataclasses import dataclass

import pandas as pd

from common import arg
from common import helper
from . import base


@dataclass
class Data:
    symbol: str
    use_count: int
    df: typing.Any
    entire_df: typing.Any
    entire_length: int
    funding_rate_df: typing.Any
    skip_count: int


class BacktestData(base.Base):
    def __init__(self, args: arg.Args):
        base.Base.__init__(self, args)
        self.data_counter = 0
        self.datas = dict()

    def _load_history(self, symbol):
        history_start = self.args.backtest.start_time - datetime.timedelta(days=self.args.history_days + 1)
        df = self._get_data(symbol, history_start, self.args.backtest.start_time)

        return df.tail(self.data_length - 1)

    def __get_next(self):
        next_datas = []
        for data in self.datas.values():
            if data.use_count >= min(data.entire_length, len(data.df) - self.data_length):
                continue

            if data.skip_count > 0:
                # skip_count가 있으면 그 만큼 데이터를 스킵한다.
                data.skip_count -= 1
                continue

            next_df = data.df[data.use_count: self.data_length + data.use_count]
            data.use_count += 1

            # funding rate가 변경될 경우 해당 내용 추가
            funding_rate = None
            data_date = next_df.iloc[-1].name
            if len(data.funding_rate_df) > 0 and data.funding_rate_df.iloc[0].name <= data_date:
                funding_rate = data.funding_rate_df.iloc[0]["funding_rate"]
                data.funding_rate_df.drop(data.funding_rate_df.head(1).index, inplace=True)

            next_datas.append((data.symbol, next_df, funding_rate))

        return next_datas

    def __load_funding_rate(self, symbol):
        start_timestamp = helper.datetime_to_timestamp(self.args.backtest.start_time)
        end_timestamp = helper.datetime_to_timestamp(self.args.backtest.end_time)
        return base.Base._get_funding_rate(self, symbol.lower(), start_timestamp, end_timestamp)

    def init(self, on_data):
        self.on_data = on_data
        min_skip_count = sys.maxsize

        for symbol in self.args.symbols:
            df = self._load_history(symbol)
            entire_df = self._get_data(symbol, self.args.backtest.start_time, self.args.backtest.end_time)
            df = pd.concat([df, entire_df])

            if len(df) < self.data_length:
                self.logging.warning(f"symbol {symbol} is skipped due to empty data")
                continue

            funding_rate_df = self.__load_funding_rate(symbol)

            actual_start_time = df.iloc[self.data_length - 1].name
            self.logging.info(f"({symbol}) data loaded from {actual_start_time}")

            data_start_time = pd.to_datetime(self.args.backtest.start_time, utc=True)

            skip_count = int(
                (actual_start_time - data_start_time).total_seconds() / helper.interval_in_seconds(self.args.interval)
            )
            print(f"skip_count = {skip_count}")
            min_skip_count = min(skip_count, min_skip_count)

            self.datas[symbol] = Data(
                symbol=symbol,
                use_count=0,
                df=df,
                entire_df=entire_df,
                entire_length=len(entire_df),
                funding_rate_df=funding_rate_df,
                skip_count=skip_count,
            )

        for k, v in self.datas.items():
            self.datas[k].skip_count -= min_skip_count

    def set_variable(self, symbol, key, value):
        self.variables[symbol][key] = value
        return True

    def save_variable_cache(self, key, value):
        self.variables_cache[key] = value
        return True

    def update_entire_df(self, symbol, col_name, values):
        self.datas[symbol].entire_df[col_name] = values

    def get_entire_df(self, symbol):
        if symbol not in self.datas:
            return None

        return self.datas[symbol].entire_df

    def run(self, on_start):
        on_start()
        while next_data := self.__get_next():
            self.on_data(next_data)
