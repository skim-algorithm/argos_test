import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
import helper
import numpy as np
import pandas as pd
import re
import math
import itertools
import datetime
from datetime import datetime as dt, timedelta
from timeit import timeit
from scipy.optimize import fmin_tnc
from scipy.optimize import fmin_l_bfgs_b
from scipy.optimize import minimize


class MetaSim(object):
    def __init__(self, file_name, meta_start_date=None):
        self.meta_start_date = dt.strptime(meta_start_date, "%Y-%m-%d") if meta_start_date else None
        self.alpha_start_date = self.alpha_end_date = self.meta_end_date = None
        self.alpha_start_date_df = (
            self.alpha_return
        ) = (
            self.blank_days
        ) = self.alpha_min_days = self.alpha_weight = self.alpha_balance = self.alpha_ir = self.t_corr = pd.DataFrame()
        self.alpha_list = []
        self.cov_alpha_list = []
        self.file_name = file_name
        self.intervals_per_year = 365
        helper.create_directory("/strategies/info_results/meta_sim")
        self.combo_type = 2  # equal:0, IR:1, div_corr:2, div_corr2:3
        self.freq = 7
        self.n_days = 90  # number of days(look-back window size) used for IR/correlation calculation
        self.min_days = 60  # minimum days required to participate in meta
        self.adjust_factor = 0.05
        self.corr_threshold = 0.3
        self.num_alpha = 0
        self.meta_start_days = 1

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

    def calculate_ir(self, alpha_min_days, n_days):
        alpha_ir = pd.DataFrame(columns=self.alpha_list, index=self.alpha_return.index)
        for i, idx in enumerate(alpha_ir.index):
            for alpha in self.alpha_list:
                # zero-out alphas with os days less than min_days
                if i < int(alpha_min_days[alpha]) - 1:
                    continue
                # calculate ir with the size of available os days up to date
                if i < int(self.blank_days[alpha]) + n_days - 1:
                    if i != len(alpha_ir) - 1:
                        alpha_ir[alpha].iloc[i] = (
                            (
                                (self.alpha_return[alpha].iloc[int(self.blank_days[alpha]) : i + 1] + 1).prod()
                                ** (1 / (i - int(self.blank_days[alpha]) + 1))
                                - 1
                            )
                            / np.std(
                                self.alpha_return[alpha].iloc[int(self.blank_days[alpha]) : i + 1],
                                ddof=1,
                            )
                            * np.sqrt(self.intervals_per_year)
                        )
                    else:
                        alpha_ir[alpha].iloc[i] = (
                            (
                                (self.alpha_return[alpha].iloc[int(self.blank_days[alpha]) :] + 1).prod()
                                ** (1 / (i - int(self.blank_days[alpha]) + 1))
                                - 1
                            )
                            / np.std(
                                self.alpha_return[alpha].iloc[int(self.blank_days[alpha]) :],
                                ddof=1,
                            )
                            * np.sqrt(self.intervals_per_year)
                        )
                # calculate ir with the size of n_days os
                else:
                    if i != len(alpha_ir) - 1:
                        alpha_ir[alpha].iloc[i] = (
                            ((self.alpha_return[alpha].iloc[i - n_days + 1 : i + 1] + 1).prod() ** (1 / n_days) - 1)
                            / np.std(
                                self.alpha_return[alpha].iloc[i - n_days + 1 : i + 1],
                                ddof=1,
                            )
                            * np.sqrt(self.intervals_per_year)
                        )
                    else:
                        alpha_ir[alpha].iloc[i] = (
                            ((self.alpha_return[alpha].iloc[i - n_days + 1 :] + 1).prod() ** (1 / n_days) - 1)
                            / np.std(self.alpha_return[alpha].iloc[i - n_days + 1 :], ddof=1)
                            * np.sqrt(self.intervals_per_year)
                        )
        alpha_ir = alpha_ir.astype(dtype="float64")
        return alpha_ir

    def calculate_covariance_matrix(self, day_idx, min_days, n_days):
        trading_date = self.alpha_start_date + timedelta(days=day_idx)
        os_days = (
            pd.DataFrame((trading_date - self.alpha_start_date_df).apply(lambda d: d.days)).rename(
                columns={"start_date": "os_days"}
            )
            + 1
        )
        # filter out alphas of which os days is less than min days
        os_days = os_days[(os_days["os_days"] >= min_days)]
        self.cov_alpha_list = os_days.index
        min_os_days = int(os_days.min())
        cov_window_days = min(min_os_days, n_days)
        look_back_date = trading_date - timedelta(days=cov_window_days - 1)
        return_matrix = self.alpha_return.loc[look_back_date:trading_date, self.cov_alpha_list]

        covariance = return_matrix.corr()
        return covariance

    def AMM(self):
        alpha_weight_temp = self.alpha_return.copy()
        self.blank_days = (
            pd.DataFrame((self.alpha_start_date_df - self.alpha_start_date).apply(lambda d: d.days))
            .transpose()
            .rename(index={"start_date": "days"})
        )
        self.alpha_min_days = self.blank_days + self.min_days
        if not self.meta_start_date:
            self.meta_start_days = self.alpha_min_days.iloc[0].min()
        else:
            if self.combo_type > 0 and self.adjust_factor == 0.0:
                self.meta_start_days = max(
                    self.alpha_min_days.iloc[0].min(),
                    (self.meta_start_date - self.alpha_start_date).days,
                )
            else:
                self.meta_start_days = (self.meta_start_date - self.alpha_start_date).days

        if self.combo_type == 0:
            alpha_weight_temp = alpha_weight_temp.where(alpha_weight_temp.isna(), other=1.0)
            sum_wt = alpha_weight_temp.sum(axis=1)
            avg_wt = 1.0 / sum_wt
            self.alpha_weight = alpha_weight_temp.where(alpha_weight_temp.isna(), other=avg_wt, axis=0)
        else:
            # calculate n_day-IR of alphas
            alpha_ir = self.calculate_ir(self.alpha_min_days, self.n_days)
            self.alpha_ir = alpha_ir

            # applying quantize IR model
            if self.adjust_factor > 0.0:
                alpha_ir_quantize = alpha_ir.copy()
                for i, idx in enumerate(alpha_ir_quantize.index):
                    for alpha in self.alpha_list:
                        # assign average-discounted ir value to the alphas with os days less than min_days
                        if i < int(self.alpha_min_days[alpha]) - 1:
                            os_days = (idx - self.alpha_start_date_df[alpha]).days + 1
                            if os_days > 0:
                                alpha_ir_quantize[alpha].iloc[i] = 1.0 - self.adjust_factor / 2.0
                            continue

                        ir_raw = alpha_ir_quantize[alpha].iloc[i]
                        if ir_raw > 3.0:
                            alpha_ir_quantize[alpha].iloc[i] = 1.0 + self.adjust_factor / 2.0
                        elif ir_raw > 2.0:
                            alpha_ir_quantize[alpha].iloc[i] = 1.0 + self.adjust_factor
                        elif ir_raw > 1.0:
                            alpha_ir_quantize[alpha].iloc[i] = 1.0
                        elif ir_raw > 0.0:
                            alpha_ir_quantize[alpha].iloc[i] = 1.0 - self.adjust_factor

                alpha_ir = alpha_ir_quantize

            ir_wt = alpha_ir
            if self.combo_type == 1:
                # zero-out alphas with negative ir
                ir_wt_temp = ir_wt.mask(ir_wt <= 0.0, 0.0, axis=0)
                zero_cnt = (ir_wt_temp == 0.0).sum(axis=1)
                sum_ir = ir_wt_temp.sum(axis=1)
                ir_norm = pd.DataFrame(1.0 / sum_ir)
                for i, idx in enumerate(alpha_weight_temp.index):
                    alpha_weight_temp.iloc[i] = ir_wt_temp.iloc[i] * ir_norm[0].iloc[i] * (1 - 0.0001 * zero_cnt[i])

                alpha_weight_temp = alpha_weight_temp.mask(alpha_weight_temp <= 0.0, 0.0001, axis=0)
                # alpha_weight_temp['sum'] = alpha_weight_temp.sum(axis=1)

                self.alpha_weight = alpha_weight_temp
            else:
                # calculate n_day-t_corr of alphas
                t_corr = pd.DataFrame(columns=self.alpha_list, index=self.alpha_return.index)
                for i, idx in enumerate(alpha_ir.index):
                    os_days = (
                        pd.DataFrame((idx - self.alpha_start_date_df).apply(lambda d: d.days)).rename(
                            columns={"start_date": "os_days"}
                        )
                        + 1
                    )
                    # select alphas of which os days are between 1 and min days
                    os_days = os_days[(0 < os_days["os_days"]) & (os_days["os_days"] < self.min_days)]
                    incubation_alpha_list = os_days.index
                    # assign 0 as t_corr to the alphas with os days less than min_days
                    t_corr.loc[idx, incubation_alpha_list] = 0.0

                    if i >= self.min_days - 1:
                        # if i == 174:
                        #     a = 1

                        # calculate covariance matrix on the i-th day
                        cov_mtrx = self.calculate_covariance_matrix(i, self.min_days, self.n_days)
                        # load ir of alphas on the i-th day
                        alpha_ir_i = alpha_ir.loc[idx, self.cov_alpha_list].T

                        for alpha in self.cov_alpha_list:
                            cov_alpha = cov_mtrx[alpha]
                            cov_others = cov_alpha.drop(alpha)
                            ir_threshold = alpha_ir_i.loc[alpha]
                            ir_others = alpha_ir_i.drop(alpha)
                            correlated_alpha_ir = ir_others[cov_others >= self.corr_threshold]
                            superior_alpha_ir = correlated_alpha_ir[correlated_alpha_ir > ir_threshold]
                            equivalent_alpha_ir = correlated_alpha_ir[correlated_alpha_ir == ir_threshold]
                            t_corr[alpha].iloc[i] = (
                                cov_others.loc[superior_alpha_ir.index].sum(axis=0)
                                + cov_others.loc[equivalent_alpha_ir.index].sum(axis=0) * 0.5
                            )

                self.t_corr = t_corr
                corr_wt = 1.0 / (t_corr + 1.0)

                if self.combo_type == 3:
                    sum_corr_wt = corr_wt.sum(axis=1)
                    corr_norm = pd.DataFrame(1.0 / sum_corr_wt)

                    for i, idx in enumerate(alpha_weight_temp.index):
                        alpha_weight_temp.iloc[i] = corr_wt.iloc[i] * corr_norm[0].iloc[i]

                    # alpha_weight_temp['sum'] = alpha_weight_temp.sum(axis=1)
                    self.alpha_weight = alpha_weight_temp
                else:
                    div_corr_wt = ir_wt * corr_wt
                    # zero-out alphas with negative div_corr
                    div_corr_wt_temp = div_corr_wt.mask(div_corr_wt <= 0.0, 0.0, axis=0)
                    zero_cnt = (div_corr_wt_temp == 0.0).sum(axis=1)
                    sum_div_corr = div_corr_wt_temp.sum(axis=1)
                    div_corr_norm = pd.DataFrame(1.0 / sum_div_corr)
                    for i, idx in enumerate(alpha_weight_temp.index):
                        alpha_weight_temp.iloc[i] = (
                            div_corr_wt_temp.iloc[i] * div_corr_norm[0].iloc[i] * (1 - 0.0001 * zero_cnt[i])
                        )

                    alpha_weight_temp = alpha_weight_temp.mask(alpha_weight_temp <= 0.0, 0.0001, axis=0)
                    # alpha_weight_temp['sum'] = alpha_weight_temp.sum(axis=1)

                    self.alpha_weight = alpha_weight_temp

    def calculate_meta_balance(self):
        # load alphas' information from raw data file
        info_data = pd.read_excel(
            "../strategies/info_results/{}.xlsx".format(self.file_name),
            index_col=0,
            sheet_name="PF_Info",
            parse_dates=True,
            header=0,
        )
        self.num_alpha = len(info_data)
        self.alpha_list = info_data.index
        self.alpha_start_date_df = info_data["start_date"]
        self.alpha_start_date = min(list(info_data["start_date"]))
        self.alpha_end_date = min(list(info_data["end_date"]))
        ref_col = [1] + list(np.arange(3, 3 + self.num_alpha * 6, 6))

        # collect return data of each alpha
        return_data = pd.read_excel(
            "../strategies/info_results/{}.xlsx".format(self.file_name),
            index_col=0,
            sheet_name="IR",
            parse_dates=True,
            header=0,
            usecols=ref_col,
            skiprows=3,
            names=self.alpha_list.insert(0, "date"),
        )

        num_days = (self.alpha_end_date - self.alpha_start_date).days
        self.alpha_return = return_data[: num_days + 1]
        self.alpha_return = self.alpha_return.astype(dtype="float64")
        self.alpha_return.index = [dt.strptime(d, "%Y-%m-%d %H:%M:%S") for d in self.alpha_return.index]

        # calculate daily alpha weight in meta based on the given combo-type
        self.AMM()

        # calculate daily alpha balance in meta
        self.meta_start_date = self.alpha_start_date + timedelta(days=int(self.meta_start_days))
        self.meta_end_date = self.alpha_end_date + timedelta(days=1)
        meta_dates = pd.date_range(self.meta_start_date, self.meta_end_date)
        alpha_balance = pd.DataFrame(columns=self.alpha_list, index=meta_dates)

        for i, idx in enumerate(alpha_balance.index):
            # alpha_balance is the value at the start of trading day
            if i == 0:
                alpha_balance.iloc[i] = 10000 * self.alpha_weight.iloc[i + self.meta_start_days - 1]
                continue
            alpha_balance.iloc[i] = alpha_balance.iloc[i - 1] * (
                1 + self.alpha_return.iloc[i + self.meta_start_days - 1]
            )
            if i % self.freq == 0:
                balance_temp = alpha_balance.iloc[i].sum(skipna=True)
                alpha_balance.iloc[i] = balance_temp * self.alpha_weight.iloc[i + self.meta_start_days - 1]

        self.alpha_balance = alpha_balance
        meta_balance = pd.DataFrame(self.alpha_balance.sum(skipna=True, axis=1))

        return meta_balance

    def analyze_meta_stat(self, to_excel=False):
        if self.n_days < self.min_days:
            # show error message
            raise SystemExit("error: insufficient number of days for combo module calculation")

        meta_df = pd.DataFrame()
        meta_df["Balance"] = self.calculate_meta_balance()
        meta_df["Return"] = meta_df["Balance"].pct_change()
        meta_df["PnL"] = meta_df["Balance"].diff()
        meta_df["Cum_PnL"] = meta_df["PnL"].cumsum()
        meta_df = meta_df.mask(meta_df.isna(), 0.0, axis=0)

        # calculate meta stats
        returns = ((meta_df["Balance"].iloc[-1] / meta_df["Balance"].iloc[0]) ** (365 / len(meta_df)) - 1) * 100.0
        sharpe_ratio = (
            ((meta_df["Return"] + 1).prod() ** (1 / len(meta_df)) - 1)
            / np.std(meta_df["Return"], ddof=1)
            * np.sqrt(self.intervals_per_year)
        )
        balance = meta_df["Balance"]
        balance_cummax = balance.cummax()
        mdd = ((balance_cummax - balance) / balance_cummax * 100).max()
        max_balance_days = balance[balance == balance_cummax].index.to_series()
        max_balance_days = max_balance_days.append(balance.index[-1:].to_series())
        max_dd_period = max_balance_days.diff().max()
        max_dd_period_temp = str(max_dd_period).split(" ")[0] + " " + str(max_dd_period).split(" ")[1]

        sharpe_ratio, mdd, returns = (
            round(sharpe_ratio, 6),
            round(mdd, 3),
            round(returns, 3),
        )
        print(
            "Start Date:{} / End Date:{} / Returns(%):{} / Sharpe:{} / Mdd(%):{} / Max_dd_Period:{}".format(
                meta_df.index[0].date(),
                meta_df.index[-1].date(),
                returns,
                sharpe_ratio,
                mdd,
                max_dd_period_temp,
            )
        )

        if to_excel:
            combo_logic = None
            if self.combo_type == 0:
                combo_logic = "equal"
            elif self.combo_type == 1:
                combo_logic = "ir"
            elif self.combo_type == 2:
                combo_logic = "DivCorr"
            elif self.combo_type == 3:
                combo_logic = "DivCorr2"

            start_date_temp = str(self.meta_start_date).split(" ")[0]
            end_date_temp = str(self.meta_end_date).split(" ")[0]
            self.alpha_balance = self.alpha_balance.join(meta_df)
            meta_parameters = [
                combo_logic,
                self.freq,
                self.n_days,
                self.min_days,
                self.adjust_factor,
                self.corr_threshold,
                start_date_temp,
                end_date_temp,
                returns,
                sharpe_ratio,
                mdd,
                max_dd_period_temp,
            ]
            meta_info = pd.DataFrame(
                meta_parameters,
                columns=["Meta"],
                index=[
                    "Combo_Type",
                    "Frequency",
                    "N_Days",
                    "min_Days",
                    "Discount_Factor",
                    "Corr_Threshold",
                    "Start_Date",
                    "End_Date",
                    "Returns(%)",
                    "Sharpe",
                    "Mdd(%)",
                    "Max_dd_Period",
                ],
            )

            with pd.ExcelWriter(
                "../strategies/info_results/meta_sim/meta_result_{}_{}D_sharpe_{}_mdd_{}_{}_{}.xlsx".format(
                    combo_logic,
                    self.freq,
                    sharpe_ratio,
                    mdd,
                    start_date_temp,
                    end_date_temp,
                )
            ) as writer:
                meta_info.to_excel(writer, sheet_name="meta_stats")
                self.alpha_balance.to_excel(writer, sheet_name="meta_balance")
                self.alpha_weight.to_excel(writer, sheet_name="alpha_weight")
                self.alpha_ir.to_excel(writer, sheet_name="alpha_ir")
                self.t_corr.to_excel(writer, sheet_name="t_corr")

        return sharpe_ratio, mdd, max_dd_period, returns


if __name__ == "__main__":
    agent = MetaSim("OS_performance", "2021-09-07")  # '2021-10-18'
    agent.analyze_meta_stat(to_excel=True)
