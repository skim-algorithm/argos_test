import time
import os
import importlib
from datetime import datetime, timedelta
import logging
from collections import defaultdict
import math

import pandas as pd

from analyzer import analyzer
from common import helper
from common import enum
from common import log
from . import base as mode
from data import backtest as data
from order import backtest as order


class BacktestMode(mode.Base):
    def __init__(self, strategy_name: str, is_simple: bool, symbols_to_override=list()):
        mode.Base.__init__(self, strategy_name, False)

        # 간단한 결과만 확인하는 심플 모드 설정
        self.is_simple = is_simple

        # 백테스트 결과를 저장할 디렉토리 생성
        self.excel_out_directory = helper.create_directory("/backtest_result")

        if symbols_to_override:
            self.args.symbols = symbols_to_override

        # 백테스트에서 사용할 데이터, 주문 핸들러
        self.data_handler = data.BacktestData(self.args)
        self.order_handler = order.BacktestOrder(self.args)

        # 백테스트 분석 모듈 로드
        self.analyzer = analyzer.Analyzer(self.args, self.order_handler)

    def __on_data(self, datas):
        # 로깅 형태를 데이터 시간 - 전략 - 메시지로 변경한다.
        log.console_handler.setFormatter(
            logging.Formatter(f"{datas[0][1].iloc[-1].name} - {self.args.strategy} - %(message)s")
        )

        # 이전에 등록한 주문에 대한 완료처리
        self.order_handler.on_data(datas)

        # 전략 실행
        datas_wo_funding_rate = [(s, d) for s, d, f in datas]
        self.strategy.on_data(datas_wo_funding_rate)

        # analyzer.on_data는 order_handler.on_data 이후에 불려야 한다.
        self.analyzer.on_data(datas_wo_funding_rate)

    def __on_order_done(self, order):
        self.strategy.on_order_done(order)
        self.analyzer.on_order_done(order)

    def run(self, variables=list()):
        self.data_handler.init(self.__on_data)
        self.order_handler.init(self.__on_order_done)

        strategy = importlib.import_module("strategies." + self.args.strategy)
        self.strategy = getattr(strategy, self.args.strategy)(self.data_handler, self.order_handler, self.logging)

        # variables 오버라이드를 수행한다. 현재는 모든 symbol + DEFAULT를 동일한 값으로 설정.
        if variables:
            symbols = self.args.symbols + ["DEFAULT"]
            for symbol in symbols:
                for name, value in variables:
                    self.data_handler.set_variable(symbol, name, value)

        begin_ts = time.time()
        # 전략 실행
        self.data_handler.run(self.strategy.on_start)
        self.logging.info(f"Backtest took {time.time() - begin_ts} seconds.")

        # 전략이 모두 실행된 후 analyzers의 finalize를 실행한다.
        self.analyzer.finalize()

        summaries = self.__build_summary(variables)
        for symbol, summary in summaries.items():
            self.logging.info(symbol)
            self.logging.info(summary)

        order_histories = self._build_order_history()
        for symbol, history in order_histories.items():
            self.logging.info(symbol)
            self.logging.info(history)

        symbols_str = ""
        detail_datas = {}

        for symbol in self.args.symbols:
            if symbols_str != "":
                symbols_str += "-"
            symbols_str += symbol

            if self.args.backtest.use_analyze_per_dataframe:
                detail_data = self.__update_and_get_entire_df(symbol)
                # logging.info(detail_data)
                detail_datas[symbol] = detail_data

        # TODO: Create detail_ALL sheet in backtest result
        # if self.args.backtest.use_analyze_per_dataframe:
        # detail_datas['ALL'] = self.__update_and_get_entire_df_ALL()

        if len(self.args.symbols) > 4:
            symbols_str = f"{len(self.args.symbols)}symbols"

        extra_filename = ""
        if summaries and variables:
            first_summary = summaries["ALL"]["total"]
            sharpe = "{:.3f}".format(first_summary["Sharpe"]).replace(".", "_")
            mdd = "{:.3f}".format(first_summary["Max Drawdown(%)"]).replace(".", "_")
            extra_filename = f"sharpe_{sharpe}_mdd_{mdd}_"

        variables_string, is_shortened = helper.variable_to_filename(variables)
        filename = (
            f"{self.args.strategy}_{self.args.ex_class}_{extra_filename}"
            f"{variables_string}"
            f'{self.args.backtest.start_time.strftime("%Y-%m-%d")}_'
            f'{self.args.backtest.end_time.strftime("%Y-%m-%d")}_'
            f"{symbols_str}_{self.args.interval}_{time.time()}.xlsx"
        )

        excel_path = os.path.join(self.excel_out_directory, filename)

        if summaries:
            begin_ts = time.time()
            if self.is_simple:
                json_file = (
                    f"{self.args.ex_class}_"
                    f'{self.args.backtest.start_time.strftime("%Y-%m-%d")}_'
                    f'{self.args.backtest.end_time.strftime("%Y-%m-%d")}_'
                    f"{self.args.interval}.json"
                )
                for symbol, summary in summaries.items():
                    json_summary = f"backtest_result/{symbol}_{self.args.strategy}_summary_{json_file}"
                    summary.to_json(json_summary, orient="columns")

                for symbol, order_history in order_histories.items():
                    json_order = f"backtest_result/{symbol}_{self.args.strategy}_order_{json_file}"
                    order_history.to_json(json_order, orient="records")
            else:
                with pd.ExcelWriter(
                    excel_path, engine="xlsxwriter"
                ) as writer:  # pylint: disable=abstract-class-instantiated
                    if is_shortened:
                        var_df = pd.DataFrame(variables, columns=["variable", "value"])
                        var_df.to_excel(writer, sheet_name="variables", index=False)

                    for symbol, summary in summaries.items():
                        summary.to_excel(writer, sheet_name=f"summary_{symbol}")

                    if order_histories:
                        total_orders = pd.concat(order_histories.values(), axis=0)
                        total_orders.sort_values(by=["Open Date"], inplace=True)
                        total_orders.to_excel(writer, sheet_name="orders")

                    for symbol, order_history in order_histories.items():
                        order_history.to_excel(writer, sheet_name=f"order_{symbol}")

                    if detail_datas:
                        is_chart_added = False
                        for symbol, _data in detail_datas.items():
                            if _data is not None:
                                _data.to_excel(writer, sheet_name=f"detail_{symbol}")

                                # Add cum PnL chart
                                if not is_chart_added:
                                    workbook = writer.book  # pylint: disable=no-member
                                    worksheet = writer.sheets["summary_ALL"]
                                    chart = workbook.add_chart({"type": "line"})
                                    chart.add_series(
                                        {
                                            "values": f"=detail_{symbol}!$O$2:$O${len(_data)+1}",
                                            "categories": f"detail_{symbol}!$A$2:$A${len(_data)+1}",
                                        }
                                    )
                                    worksheet.insert_chart(f'B{len(summaries["ALL"])+3}', chart)
                                    is_chart_added = True

            self.logging.info(f"Writing excel took {time.time() - begin_ts} seconds.")

        return summaries

    def __build_summary(self, variables):
        from typing import Dict

        begin_ts = time.time()
        returns = {}
        turnover_values = defaultdict(lambda: (0.0, 0))
        all_open_trades = defaultdict()
        start_times: Dict[str, datetime] = {}

        for symbol, analyzers in self.analyzer.analyzers.items():
            df = pd.DataFrame()
            detail_infos = {}
            sim_df = pd.DataFrame()
            sim_infos = {}
            for year, analyzer in analyzers.items():  # noqa: F402
                if not analyzer["periodstats"].start_time:
                    # 시작 시간이 늦어 한 번도 호출되지 않은 연도는 패스한다.
                    continue

                detail_infos = {}

                # 기본 정보
                initial_value = analyzer["all_periodstats"].initial_value
                last_value = analyzer["all_periodstats"].last_value
                start_times[year] = (
                    min(start_times[year], analyzer["all_periodstats"].start_time)
                    if year in start_times
                    else analyzer["all_periodstats"].start_time
                )

                if variables:
                    sim_infos["Variable"] = helper.variable_to_string(variables)
                    detail_infos["Variable"] = sim_infos["Variable"]

                sim_infos["Strategy"] = self.args.strategy
                sim_infos["Start"] = analyzer["all_periodstats"].start_time.strftime("%Y-%m-%d %H:%M:%S")
                sim_infos["End"] = analyzer["all_periodstats"].end_time.strftime("%Y-%m-%d %H:%M:%S")
                sim_infos["Duration"] = (
                    analyzer["all_periodstats"].end_time - analyzer["all_periodstats"].start_time + timedelta(days=1)
                )
                sim_infos["Start value"] = initial_value
                sim_infos["Final value"] = last_value
                sim_infos["Return(%)"] = analyzer["all_periodstats"].calculate_anuualized_geo_mean()

                # Drawdown 정보
                # detail_infos['Drawdown(%)'] = analyzer['drawdown'].drawdown
                sim_infos["Max Drawdown(%)"] = analyzer["all_drawdown"].max_drawdown
                sim_infos["Max Drawdown Period(Days)"] = analyzer["all_drawdown"].max_drawdown_period

                detail_infos["Symbol"] = symbol

                # Profit 정보
                sim_infos["WinRate(%)"] = analyzer["all_periodstats"].win_rate
                sim_infos["Positive"] = analyzer["all_periodstats"].positive_count
                sim_infos["Negative"] = analyzer["all_periodstats"].negative_count
                sim_infos["Nochange"] = analyzer["all_periodstats"].nochange_count
                sim_infos["PositiveAvg"] = analyzer["all_periodstats"].positive_avg
                sim_infos["NegativeAvg"] = analyzer["all_periodstats"].negative_avg
                sim_infos["Best"] = analyzer["all_periodstats"].best
                sim_infos["Worst"] = analyzer["all_periodstats"].worst

                # Sharpe Ratio
                sim_infos["Sharpe"] = analyzer["all_periodstats"].sharpe_ratio

                # Return/MDD
                ret = analyzer["all_periodstats"].calculate_anuualized_geo_mean()
                mdd = analyzer["all_drawdown"].max_drawdown
                sim_infos["Return/MDD"] = (ret / mdd) if mdd != 0 else 0

                # Turnover 정보
                detail_infos["Turnover(%)"] = analyzer["turnover"].turnover

                # All의 turnover를 계산하기 위해 심볼별 결과를 합산해서 저장
                tv = turnover_values[year]
                turnover_values[year] = (
                    tv[0] + analyzer["turnover"].accum_turnover,
                    max(tv[1], analyzer["turnover"].daily_turnover_count),
                )

                # Trade 정보
                detail_infos["Trade Open"] = analyzer["trade"].number_of_open_orders
                detail_infos["Trade Closed"] = analyzer["trade"].number_of_closed_orders
                detail_infos["Trade Won"] = analyzer["trade"].number_of_wins
                detail_infos["Trade Lost"] = analyzer["trade"].number_of_loses
                detail_infos["Trade Best PnL"] = analyzer["trade"].best_pnl
                detail_infos["Trade Worst PnL"] = analyzer["trade"].worst_pnl
                detail_infos["Win Rate(%)"] = analyzer["trade"].win_rate
                detail_infos["P/L Ratio"] = analyzer["trade"].pnl_ratio
                detail_infos["Sharpe"] = analyzer["periodstats"].sharpe_ratio
                detail_infos["Max Drawdown(%)"] = analyzer["drawdown"].max_drawdown
                detail_infos["Return(%)"] = analyzer["periodstats"].calculate_anuualized_geo_mean()

                # Position Period
                duration = analyzer["periodstats"].end_time - analyzer["periodstats"].start_time + timedelta(days=1)
                open_trades = analyzer["trade"].number_of_open_orders
                detail_infos["Position Period"] = (
                    (duration.total_seconds() / open_trades) / 3600 / 24 if open_trades != 0 else 0
                )

                if year in all_open_trades:
                    all_open_trades[year] += open_trades
                else:
                    all_open_trades[year] = open_trades

                # 펀딩피 정보
                detail_infos["Total Funding Fee"] = analyzer["trade"].total_funding_fee

                df[year] = detail_infos.values()
                sim_df[year] = sim_infos.values()

            if not len(df):
                continue

            if "ALL" not in returns:
                sim_df.insert(0, "type", sim_infos.keys())
                sim_df.set_index(keys="type", inplace=True)
                returns["ALL"] = sim_df

            # total_df = df.pop('total')
            # df[f'total'] = total_df
            df.insert(0, "type", detail_infos.keys())
            df.set_index(keys="type", inplace=True)

            returns[symbol] = df

        # 저장한 값으로 All의 Turnover를 계산해서 추가
        if "ALL" in returns:
            start_times = {k: v.strftime("%Y-%m-%d %H:%M:%S") for k, v in start_times.items()}
            returns["ALL"].loc["Start"] = start_times
            all_returns = returns["ALL"].loc["Return(%)"].values.tolist()
            all_sharpes = returns["ALL"].loc["Sharpe"].values.tolist()
            all_durations = returns["ALL"].loc["Duration"].values.tolist()

            all_turnovers = [100 * v[0] / v[1] for v in turnover_values.values() if v[1]]
            all_marginbps = [
                ((1 + ret / 100) ** (1 / 365) - 1) / (to / 100) * 1e4 if to != 0 else 0
                for (ret, to) in zip(all_returns, all_turnovers)
            ]
            all_fitnesses = [
                (s * math.sqrt(ret / to) if to > 0 and ret > 0 else 0)
                for (s, ret, to) in zip(all_sharpes, all_returns, all_turnovers)
            ]

            returns["ALL"].loc["TurnOver(%)"] = all_turnovers
            returns["ALL"].loc["Margin(bps)"] = all_marginbps
            returns["ALL"].loc["Fitness"] = all_fitnesses

            returns["ALL"].loc["Position Period"] = [
                (d.total_seconds() / ot) / 3600 / 24 if ot != 0 else 0
                for (d, ot) in zip(all_durations, all_open_trades.values())
            ]

        self.logging.info(f"Building summary took {time.time() - begin_ts} seconds.")
        return returns

    def __update_and_get_entire_df(self, symbol):
        detail_analyzer = self.analyzer.analyzers[symbol]["total"]["detail"]
        data = detail_analyzer.data.copy()

        if data.empty is True:
            return

        # 결과 보여줄 때 불필요한 컬럼 제거
        data.drop(columns="timestamp", inplace=True)
        data.drop(columns="open", inplace=True)
        data.drop(columns="high", inplace=True)
        data.drop(columns="low", inplace=True)
        data.drop(columns="volume", inplace=True)

        # DatetimeIndex를 String 포멧으로 변경 (타임존 정보는 엑셀에 들어갈 수 없음)
        # data['datetime'] = data.index.strftime('%Y-%m-%d %H:%M:%S')
        data["datetime"] = data.index.strftime("%Y-%m-%d")

        data.reset_index(drop=True, inplace=True)
        data.set_index("datetime", drop=True, inplace=True)

        data.rename(columns={"close": "Close Price"}, inplace=True)

        # 결과에 필요한 정보 추가
        data["Position Size"] = detail_analyzer.position_sizes
        data["Position Value"] = detail_analyzer.position_values
        # data['Cash Value'] = detail_analyzer.cash_values
        # data['Portfolio Value'] = detail_analyzer.portfolio_values
        data["PnL($)"] = detail_analyzer.pnl_values
        data["Cum PnL"] = detail_analyzer.cum_pnl_values
        data["Return(%)"] = detail_analyzer.return_values
        data["Cum Return(%)"] = detail_analyzer.cum_return_values
        data["Turnover(%)"] = detail_analyzer.turnover_values
        data["Win Rate(%)"] = detail_analyzer.win_rate_values
        data["P/L Ratio"] = detail_analyzer.pnl_ratio_values
        data["DD(%)"] = detail_analyzer.drawdown_values
        data["DD Period"] = detail_analyzer.drawn_period_values
        data["MDD(%)"] = detail_analyzer.max_drawdown_values

        portfolio = pd.Series(detail_analyzer.all_portfolio_values)
        pnl = portfolio - portfolio.shift(1)
        data["Total Cum PnL"] = list(pnl.cumsum().array)

        return data

    def build_trade_amount(self):
        order_histories = self._build_order_history()
        all_orders = pd.concat(order_histories.values(), axis=0)

        all_orders["open_pos"] = (all_orders["Open Price"] * all_orders["Size"]).abs()
        all_orders["close_pos"] = (all_orders["Close Price"] * all_orders["Size"]).abs()

        open_dates = all_orders.copy()
        open_dates["Open Date"] = pd.to_datetime(open_dates["Open Date"])
        open_dates = open_dates[["Open Date", "open_pos"]]
        open_dates = open_dates.resample("D", on="Open Date").sum().fillna(0)

        close_dates = all_orders.copy()
        close_dates["Close Date"] = pd.to_datetime(close_dates["Close Date"])
        close_dates = close_dates[["Close Date", "close_pos"]]
        close_dates = close_dates.resample("D", on="Close Date").sum().fillna(0)

        trade_amount = pd.concat([open_dates, close_dates], axis=1).fillna(0)
        trade_amount["trade_amt"] = trade_amount["open_pos"] + trade_amount["close_pos"]
        return trade_amount.fillna(0)[1:]

    # TODO: This create detail_ALL, but only works with symbol that has the same start date.
    # Need to rewrite for it to work will all cases
    def __update_and_get_entire_df_ALL(self):
        data = None
        pos_values = None
        number_of_wins = None
        number_of_closed_orders = None
        number_of_loses = None
        total_loses = None
        total_profit = None

        returns = None

        for symbol in self.args.symbols:
            detail_analyzer = self.analyzer.analyzers[symbol]["total"]["detail"]
            data = detail_analyzer.data.copy()

            if data.empty is True:
                return

            # 결과 보여줄 때 불필요한 컬럼 제거
            data.drop(columns="timestamp", inplace=True)
            data.drop(columns="open", inplace=True)
            data.drop(columns="high", inplace=True)
            data.drop(columns="low", inplace=True)
            data.drop(columns="volume", inplace=True)
            data.drop(columns="close", inplace=True)

            # DatetimeIndex를 String 포멧으로 변경 (타임존 정보는 엑셀에 들어갈 수 없음)
            # data['datetime'] = data.index.strftime('%Y-%m-%d %H:%M:%S')
            data["datetime"] = data.index.strftime("%Y-%m-%d")

            data.reset_index(drop=True, inplace=True)
            data.set_index("datetime", drop=True, inplace=True)
            data.index = pd.to_datetime(data.index)

            if returns is None:
                returns = data

            portfolio = pd.Series(detail_analyzer.all_portfolio_values)
            pnl = portfolio - portfolio.shift(1)
            cum_pnl = pnl.cumsum()
            return_pct = pnl / portfolio.shift(1) * 100
            cum_return = []

            for i in return_pct:
                if len(cum_return) == 0:
                    cum_return.append(0)
                    continue
                cr = (1 + i) * (1 + cum_return[-1]) - 1
                cum_return.append(cr)

            data["pos_val"] = list(detail_analyzer.position_values)
            if pos_values is not None:
                pos_values += data["pos_val"]
            else:
                pos_values = data["pos_val"]

            data["all_cash_values"] = detail_analyzer.all_cash_values
            returns["Cash Value"] = data["all_cash_values"]

            data["all_portfolio_values"] = detail_analyzer.all_portfolio_values
            returns["Portfolio Value"] = data["all_portfolio_values"]

            data["pnl"] = pnl.fillna(0)
            returns["PnL($)"] = data["pnl"]

            data["cum_pnl"] = cum_pnl.fillna(0)
            returns["Cum PnL"] = data["cum_pnl"]

            data["return_pct"] = return_pct.fillna(0)
            returns["Return(%)"] = data["return_pct"]

            data["cum_return"] = cum_return
            returns["Cum Return(%)"] = data["cum_return"]

            data = pd.concat([data, self.build_trade_amount()], axis=1)

            returns["Turnover(%)"] = (100 * data["trade_amt"] / returns["Portfolio Value"].shift(1)).fillna(0)

            data["number_of_wins"] = detail_analyzer.number_of_wins
            number_of_wins = (
                (number_of_wins + data["number_of_wins"]) if number_of_wins is not None else data["number_of_wins"]
            )

            data["number_of_closed_orders"] = detail_analyzer.number_of_closed_orders
            number_of_closed_orders = (
                number_of_closed_orders + data["number_of_closed_orders"]
                if number_of_closed_orders is not None
                else data["number_of_closed_orders"]
            )

            data["number_of_loses"] = detail_analyzer.number_of_loses
            number_of_loses = (
                number_of_loses + data["number_of_loses"] if number_of_loses is not None else data["number_of_loses"]
            )

            data["total_loses"] = detail_analyzer.total_loses
            total_loses = total_loses + data["total_loses"] if total_loses is not None else data["total_loses"]

            data["total_profit"] = detail_analyzer.total_profit
            total_profit = total_profit + data["total_profit"] if total_profit is not None else data["total_profit"]

        returns["Position Value"] = pos_values

        returns["Win Rate(%)"] = (number_of_wins / number_of_closed_orders * 100).fillna(0)
        returns["P/L Ratio"] = ((total_profit / number_of_wins) / (total_loses / number_of_loses).abs()).fillna(0)
        returns["DD(%)"] = (
            100
            * (returns["Portfolio Value"].cummax() - returns["Portfolio Value"])
            / returns["Portfolio Value"].cummax()
        )

        from itertools import groupby, chain
        import numpy as np

        returns["DD Period"] = list(
            chain.from_iterable(
                (np.arange(len(list(j))) + 1).tolist() if i == 1 else [0] * len(list(j))
                for i, j in groupby(returns["DD(%)"] != 0)
            )
        )
        returns["MDD(%)"] = list(returns["DD(%)"].cummax())

        returns["datetime"] = returns.index.strftime("%Y-%m-%d")
        returns.set_index("datetime", inplace=True)

        returns = returns[
            [
                "Position Value",
                "Cash Value",
                "Portfolio Value",
                "PnL($)",
                "Cum PnL",
                "Return(%)",
                "Cum Return(%)",
                "Turnover(%)",
                "Win Rate(%)",
                "P/L Ratio",
                "DD(%)",
                "DD Period",
                "MDD(%)",
            ]
        ]

        return returns

    def _build_order_history(self):
        begin_ts = time.time()
        returns = {}

        for symbol, dones in self.order_handler.dones.items():
            closed_orders = [e for e in dones if e.opt is enum.OrderOpt.CLOSE]
            columns = [
                "Num",
                "Symbol",
                "Open Date",
                "Close Date",
                "Open Type",
                "Close Type",
                "Size",
                "Open Price",
                "BUY/SELL",
                "Close Price",
                "Gross PnL",
                "Commission",
                "Funding Fee",
                "Net PnL",
                "RETURN(%)",
            ]
            data = []
            for index, order in enumerate(closed_orders):  # noqa: F402
                row = []

                row.append(index + 1)
                row.append(order.symbol)
                row.append(order.open_time.strftime("%Y-%m-%d %H:%M:%S"))
                row.append(order.close_time.strftime("%Y-%m-%d %H:%M:%S"))
                row.append(order.open_type.value)
                row.append(order.close_type.value)

                size_with_symbol = order.quantity if order.side is enum.OrderSide.SELL else order.quantity * -1
                row.append(size_with_symbol)
                row.append(order.open_price)

                position_side = "BUY" if order.side is enum.OrderSide.SELL else "SELL"
                row.append(position_side)

                row.append(order.close_price)
                row.append(order.pnl)
                row.append(order.pnl - order.pnl_w_comm)
                row.append(order.funding_fee)
                row.append(order.pnl_w_comm - order.funding_fee)
                row.append(order.returns)

                data.append(row)

            df = pd.DataFrame(data=data, columns=columns)
            df.set_index("Num", inplace=True)

            returns[symbol] = df

        self.logging.info(f"Building order history took {time.time() - begin_ts} seconds.")
        return returns
