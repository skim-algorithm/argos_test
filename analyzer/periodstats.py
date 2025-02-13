import numpy as np
from statistics import geometric_mean

from . import base


class PeriodStats(base.Base):
    def __init__(self, symbol, order_handler):
        base.Base.__init__(self, order_handler)
        self.symbol = symbol

        self.initial_value = None
        self.last_value = None
        self.start_time = None
        self.end_time = None
        self.pnl_value = 0.0
        self.returns = []
        self.positive_count = 0
        self.negative_count = 0
        self.nochange_count = 0
        self.positive_avg = 0.0
        self.negative_avg = 0.0
        self.best = float("-inf")
        self.worst = float("inf")
        self.win_rate = 0.0

    def on_data(self, df):
        self.end_time = df.iloc[-1].name
        if self.start_time is None:
            self.start_time = self.end_time

        value = 0.0
        if not self.symbol:
            value = self.order_handler.get_value()
        else:
            value = self.order_handler.get_value(self.symbol)

        if self.initial_value is None:
            self.initial_value = value

        if self.last_value is None:
            self.last_value = value

        self.pnl_value = value - self.last_value
        return_value = (value / self.last_value) - 1.0
        self.returns.append(return_value)

        self.last_value = value

    def finalize(self):
        positive_list = []
        negative_list = []

        for val in self.returns:
            if val > 0.0:
                self.positive_count += 1
                positive_list.append(val)
            elif val < 0.0:
                self.negative_count += 1
                negative_list.append(val)
            else:
                self.nochange_count += 1

            self.best = max(self.best, val)
            self.worst = min(self.worst, val)

        self.positive_avg = np.mean(positive_list) if positive_list else 0
        self.negative_avg = np.mean(negative_list) if negative_list else 0

        self.sharpe_ratio = self.calculate_sharpe_ratio()
        self.win_rate = (
            100.0 * self.positive_count / (self.positive_count + self.negative_count)
            if self.positive_count + self.negative_count > 0
            else 0
        )

    def calculate_sharpe_ratio(self):
        # Sharpe Ratio = (자산 X의 기대수익률 – 무위험 자산 수익률) / 자산 X의 기대수익률의 표준편차
        try:
            # avg = np.mean(self.returns)
            avg = geometric_mean(map(lambda x: x + 1.0, self.returns)) - 1.0
            stdev = np.std(self.returns)
        except Exception:
            avg = 0
            stdev = 0

        interval_sharpe_ratio = avg / stdev if stdev > 0 else 0
        return np.sqrt(365) * interval_sharpe_ratio

    def calculate_anuualized_geo_mean(self):
        if len(self.returns) == 0:
            return 0.0

        prect = self.last_value / self.initial_value
        return ((prect ** (365 / len(self.returns))) - 1) * 100.0

    def get_last_return_value(self):
        return self.returns[-1] if len(self.returns) > 0 else 0.0
