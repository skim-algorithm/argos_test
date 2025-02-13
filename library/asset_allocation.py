import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
import helper
import numpy as np
import pandas as pd
import re
import itertools
import datetime
from datetime import datetime as dt
from timeit import timeit
from scipy.optimize import fmin_tnc
from scipy.optimize import fmin_l_bfgs_b
from scipy.optimize import minimize
import math


class AssetAllocation(object):
    def __init__(self, result_list, start_date=None):
        self.name_list = []
        self.exist_start_date = True if start_date else False
        self.start_date = dt.strptime(start_date, "%Y-%m-%d") if start_date else None
        self.start_date_list = []
        self.end_date = "2020-01-01"
        self.result_list = result_list
        self.intervals_per_year = 365
        self.data_dict = self.combine_data(self.result_list)
        self.strategy_info = {}
        helper.create_directory("/strategies/info_results/asset_allocation")

    def period_per_day(self, interval):
        factor = 365
        i = int(re.sub("[^0-9]", "", interval))
        if "h" in interval.lower():
            factor = 24 / i
        elif "m" in interval.lower():
            factor = 24 * (60 / i)
        elif "s" in interval.lower():
            factor = 24 * 60 * (60 / i)

        factor = int(factor)
        return factor

    def test_equal_weight(self, rebalance=True, to_excel=False):
        # check each strategy's performance
        default_array = np.zeros(len(self.result_list))
        for i in range(len(default_array)):
            ary = default_array.copy()
            ary[i] = 1.0
            self.analyze_combined_strategy(ary, rebalance, to_excel)

        # check former portfolio's performance
        sum_num = 1 / (len(default_array) - 1)
        ary = default_array.copy()
        for i in range(len(default_array) - 1):
            ary[i] += sum_num
        self.analyze_combined_strategy(ary, rebalance, to_excel)

        # check combined portfolio's performance
        sum_num = 1 / len(default_array)
        ary = default_array + sum_num
        self.analyze_combined_strategy(ary, rebalance, to_excel)

    def combine_data(self, result_list):
        data_dict = {}
        end_date_list = []
        separator = "_"
        for i in range(len(result_list)):
            start_date = dt.strptime(result_list[i].split("_")[3], "%Y-%m-%d")
            end_date = dt.strptime(result_list[i].split("_")[4], "%Y-%m-%d")
            self.start_date_list.append(start_date)
            end_date_list.append(end_date - datetime.timedelta(days=1))
        self.start_date_list = sorted(self.start_date_list)
        self.end_date = min(end_date_list)
        # include data
        for i in range(len(result_list)):
            raw_data = pd.read_excel(
                "../strategies/info_results/{}.xlsx".format(result_list[i]),
                index_col=0,
                sheet_name=None,
                parse_dates=True,
            )
            sheet = None
            for key in raw_data.keys():
                if "detail" in key:
                    sheet = key
                    break

            if self.exist_start_date:
                if self.start_date < max(self.start_date_list):
                    raise Exception("Input start date is before latest strategy start date")
                data = raw_data[sheet][self.start_date : self.end_date]
                divider = data["Portfolio Value"].loc[self.start_date] / 10000
                data["Portfolio Value"] /= divider
            else:
                self.start_date = min(self.start_date_list)
                data = pd.DataFrame(
                    raw_data[sheet],
                    index=pd.date_range(self.start_date, self.end_date, freq="1D"),
                )
            print(data["Portfolio Value"])
            name = separator.join(result_list[i].split("_")[:2])
            data_dict[name] = data
            self.name_list.append(name)

        return data_dict

    def analyze_combined_strategy(self, ratio, rebalance=True, to_excel=False):

        df_list = [data["Portfolio Value"] for data in self.data_dict.values()]
        df_return_list = [data["Return(%)"] for data in self.data_dict.values()]
        df = pd.concat(df_list, axis=1)
        df_return = pd.concat(df_return_list, axis=1)
        df.columns = self.name_list
        df_return.columns = self.name_list
        ratio_df = pd.DataFrame(columns=self.name_list, index=df.index)
        for index in df.index:
            df_row = np.array(df.loc[index])
            ratio_df.loc[index] = np.where(np.isnan(df_row), np.NaN, ratio)
            ratio_df.loc[index] /= ratio_df.loc[index].sum() if ratio_df.loc[index].sum(skipna=True) != 0 else 1

        # solve portfolio value with combined strategy
        pd.set_option("display.max_columns", None)

        portfolio_df = pd.DataFrame(columns=self.name_list, index=df.index)
        if not self.exist_start_date:
            for i, idx in enumerate(portfolio_df.index):
                if idx == self.start_date_list[0]:
                    portfolio_df.loc[idx] = df.loc[idx] * ratio_df.loc[idx]
                    continue
                portfolio_df.loc[idx] = portfolio_df.loc[idx - datetime.timedelta(days=1)] * (
                    1 + df_return.loc[idx] / 100
                )
                if idx in self.start_date_list:
                    total_balance = portfolio_df.loc[idx].sum(skipna=True)
                    if total_balance == 0:
                        total_balance = 10000
                    if rebalance:
                        portfolio_df.loc[idx] = total_balance * ratio_df.loc[idx]
                    else:
                        for strat in ratio_df.columns:
                            if not math.isnan(portfolio_df.loc[idx, strat]):
                                if ratio_df[strat].iloc[i]:
                                    ratio_change = (
                                        ratio_df.loc[idx, strat] / ratio_df.loc[idx - datetime.timedelta(days=1), strat]
                                    )
                                    portfolio_df.loc[idx, strat] = portfolio_df.loc[idx, strat] * ratio_change
                                else:
                                    portfolio_df.loc[idx, strat] = 0
                            else:
                                portfolio_df.loc[idx, strat] = total_balance * ratio_df.loc[idx, strat]

        else:
            portfolio_df = df * ratio_df

        combined_data = pd.DataFrame(
            np.zeros(len(portfolio_df)),
            columns=["Portfolio Value"],
            index=portfolio_df.index,
        )
        combined_data["Portfolio Value"] = portfolio_df.sum(axis=1, skipna=True)
        combined_data = combined_data[combined_data != 0].dropna()

        combined_data["Return"] = combined_data["Portfolio Value"].pct_change()
        sharpe_ratio = (
            ((combined_data["Return"] + 1).prod() ** (1 / len(combined_data)) - 1)
            / np.std(combined_data["Return"])
            * np.sqrt(self.intervals_per_year)
        )

        # solve several stats with combined strategy
        balance = combined_data["Portfolio Value"]
        balance_cummax = balance.cummax()
        mdd = ((balance_cummax - balance) / balance_cummax * 100).max()
        max_dd_period = balance[balance == balance_cummax].index.to_series().diff().max()
        returns = (
            (combined_data["Portfolio Value"].iloc[-1] / combined_data["Portfolio Value"].iloc[0])
            ** (365 / len(combined_data))
            - 1
        ) * 100.0
        sharpe_ratio, mdd, returns = (
            round(sharpe_ratio, 6),
            round(mdd, 3),
            round(returns, 3),
        )

        print(
            "Start Date: {} / End Date: {} / Ratio: {} / Sharpe: {} / Mdd(%): {} / Max_dd_Period: {} / Returns(%): {}".format(
                combined_data.index[0],
                combined_data.index[-1],
                ratio / ratio.sum(),
                sharpe_ratio,
                mdd,
                max_dd_period,
                returns,
            )
        )

        if to_excel:
            combined_data = combined_data.join(portfolio_df)
            separator = "-"
            if len(self.name_list) > 5:
                combined_data.to_excel(
                    "../strategies/info_results/asset_allocation/ratio{}&strat_num{}.xlsx".format(
                        [round(r, 2) for r in ratio], len(self.name_list)
                    ),
                    sheet_name="detail",
                )
            else:
                combined_data.to_excel(
                    "../strategies/info_results/asset_allocation/ratio{}&{}.xlsx".format(
                        [round(r, 2) for r in ratio], separator.join(self.name_list)
                    ),
                    sheet_name="detail",
                )

        return sharpe_ratio, mdd, max_dd_period, returns

    @timeit
    def find_optimal_ratio(self, mdd_limit):

        data_num = len(self.data_dict)
        pairs = itertools.product(np.arange(0, 1.01, 0.02), repeat=data_num)
        result_dict = {}
        strategy_info = []
        best_sharpe = {
            "Pair": (0, 0),
            "Sharpe": 0,
            "Mdd(%)": 0,
            "Max_dd_Period": 0,
            "Returns(%)": 0,
        }
        for pair in pairs:
            ratio = tuple(round(i, 2) for i in pair)
            if round(np.array(ratio).sum(), 2) == 1.0:
                sharpe, mdd, max_dd_period, returns = self.analyze_combined_strategy(np.array(ratio), to_excel=False)
                result = {
                    "Sharpe": sharpe,
                    "Mdd(%)": mdd,
                    "Max_dd_Period": max_dd_period,
                    "Returns(%)": returns,
                }
                result_dict[ratio] = result
                if mdd <= mdd_limit and sharpe >= best_sharpe["Sharpe"]:
                    best_sharpe["Pair"], best_sharpe["Sharpe"] = ratio, sharpe
                    best_sharpe["Mdd(%)"], best_sharpe["Max_dd_Period"] = (
                        mdd,
                        max_dd_period,
                    )
                    best_sharpe["Returns(%)"] = returns
                if np.any(1.0 in ratio):
                    strategy_info.append(result)

        strategy_info.reverse()
        index = pd.MultiIndex.from_tuples(result_dict.keys(), names=self.name_list)
        result_dataframe = pd.DataFrame(result_dict.values(), index=index)
        separator = "-"
        for name, info in zip(self.name_list, strategy_info):
            print(name, info)
        print(best_sharpe)
        result_dataframe.to_csv(
            "../strategies/info_results/asset_allocation/{}.csv".format(separator.join(self.name_list))
        )

        return best_sharpe

    def get_combined_sharpe(self, ratio, to_excel=False):

        sharpe_ratio, _, _, _ = self.analyze_combined_strategy(ratio, to_excel)

        return -sharpe_ratio

    def get_maximum_sharpe(self):
        data_num = len(self.data_dict)
        bounds = []
        for i in range(data_num):
            bounds.append((0, 1))
        optimal_ratio, nfeval, rc = fmin_l_bfgs_b(
            self.get_combined_sharpe,
            np.ones(data_num) / data_num,
            epsilon=0.01,
            approx_grad=True,
            bounds=bounds,
            maxfun=10000,
            factr=1e2,
        )
        for i in range(data_num):
            ratio = np.zeros(data_num)
            ratio[i] = 1
            print(self.name_list[i])
            self.get_combined_sharpe(ratio, to_excel=False)
        self.analyze_combined_strategy(optimal_ratio, to_excel=False)


if __name__ == "__main__":
    agent = AssetAllocation(
        [
            "junggil_003_futures_2020-01-01_2021-10-25_BTCUSDT_1m_1635137319.7298524",
            "junggil_004_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635138235.4760425",
            "junggil_005_futures_2020-01-01_2021-10-25_13symbols_15m_1635167229.073372",
            "skim_002_futures_2020-10-01_2021-10-25_15symbols_1m_1635179438.6631575",
            # 'harin_001_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635138498.073447',
            # 'skim_004_futures_2020-01-01_2021-10-25_BTCUSDT_1m_1635138567.655567',
            # 'junggil_006_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_15m_1635137585.8993316',
            # 'skim_003_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635149929.7658296',
            # 'harin_002_futures_2020-01-01_2021-10-25_ETHUSDT-XRPUSDT_1m_1635139034.1453316',
            # 'harin_003_futures_2020-01-01_2021-10-25_5symbols_1m_1635141992.23297'
        ]
    )
    agent.test_equal_weight(rebalance=False, to_excel=True)
    # agent.find_optimal_ratio(mdd_limit=20)
    # agent.get_maximum_sharpe()
