import time
from collections import defaultdict

from common import helper
from common import arg
from common import log

from . import drawdown, turnover, periodstats, trade, detail


class Analyzer:
    def __init__(self, args: arg.Args, order_handler):
        self.args = args
        self.order_handler = order_handler
        self.interval_in_day = self.__calculate_interval_in_day(args.interval)
        self.intervals_per_year = helper.calculate_number_of_intervals_per_year(args.interval)
        self.current_year = 0

        # 전체 analyzers 들을 추가한다.
        self.analyzers = defaultdict(dict)
        for symbol in args.symbols:
            self.analyzers[symbol]["total"] = self.__build_analyzers(symbol)

        # analyzer의 on_data를 일단위로만 실행하기 위해 마지막 실행 날짜를 기록한다.
        self.last_analyzed_day = 0

        # logging
        self.logging = log.makeLogger(args.strategy)

    def __build_analyzers(self, symbol):
        # 분석에 사용할 analyzer들을 추가한다.
        analyzers = {}
        analyzers["drawdown"] = drawdown.DrawDown(symbol, self.order_handler)
        analyzers["all_drawdown"] = drawdown.DrawDown(None, self.order_handler)
        analyzers["periodstats"] = periodstats.PeriodStats(symbol, self.order_handler)
        analyzers["all_periodstats"] = periodstats.PeriodStats(None, self.order_handler)
        analyzers["turnover"] = turnover.TurnOver(self.order_handler)
        analyzers["trade"] = trade.Trade(symbol, self.order_handler)

        # 데이터프레임 단위의 분석을 사용할 경우에만 추가.
        # 다른 analyzer의 정보를 사용하기 때문에 가장 마지막에 append 해야 한다.
        if self.args.backtest.use_analyze_per_dataframe:
            analyzers["detail"] = detail.Detail(self, symbol, self.order_handler)

        return analyzers

    def __calculate_interval_in_day(self, interval):
        """하루 단위의 데이터프레임 간격 (ex: 1시간 = 1/24)"""
        if "d" in interval:
            day = interval.split("d")[0]
            return float(day)

        if "h" in interval:
            hour = interval.split("h")[0]
            return float(hour) / 24

        if "m" in interval:
            minute = interval.split("m")[0]
            return float(minute) / 24 / 60

        self.logging.error(f"Invalid interval to calculate interval in day: {interval}")
        return 0.0

    def __get_current_year(self, datas):
        _, df = datas[0]
        return df.iloc[-1].name.year

    def __update_year_and_return_true_if_changed(self, datas):
        """현재 연도를 업데이트 하고 이전과 달라졌는지 확인한다."""
        prev_year = self.current_year
        self.current_year = self.__get_current_year(datas)
        return prev_year != self.current_year

    def on_data(self, datas):
        if self.__update_year_and_return_true_if_changed(datas):
            # 연도가 바뀌었으므로 해당 연도에 맞는 analyzers 새로 생성.
            for symbol in self.args.symbols:
                self.analyzers[symbol][self.current_year] = self.__build_analyzers(symbol)

        # analyzer의 on_data는 일단위로 실행된다.

        if self.last_analyzed_day == datas[0][1].iloc[-1].name.day:
            return
        else:
            self.last_analyzed_day = datas[0][1].iloc[-1].name.day

        for symbol, df in datas:
            for analyzer in self.analyzers[symbol]["total"].values():
                analyzer.on_data(df)

            for analyzer in self.analyzers[symbol][self.current_year].values():
                analyzer.on_data(df)

    def on_order_done(self, order):
        for analyzer in self.analyzers[order.symbol]["total"].values():
            analyzer.on_order_done(order)

        for analyzer in self.analyzers[order.symbol][self.current_year].values():
            analyzer.on_order_done(order)

    def finalize(self):
        begin_ts = time.time()
        for analyzer_group in self.analyzers.values():
            for analyzers in analyzer_group.values():
                for analyzer in analyzers.values():
                    analyzer.finalize()

        self.logging.info(f"Finalizing took {time.time() - begin_ts} seconds.")
