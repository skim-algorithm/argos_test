import multiprocessing
from itertools import product
import logging
import time
import sys
import traceback
from pathlib import Path

from mode import backtest
from common import log
from common import helper
from common import arg
from data import backtest as data


# Retreive data for a symbol of strategy
class StrategyData(data.BacktestData):
    def __init__(self, strategy_name: str):
        args = arg.create_args(strategy_name, is_live=False)
        super().__init__(args)

    def get_data(self, symbol):
        self._load_history(symbol)
        self._get_data(symbol, self.args.backtest.start_time, self.args.backtest.end_time)


class Multi:
    def __init__(self, strategy_name):
        self.strategy_name = str(strategy_name)
        self.variables = list(list())
        self.variables_to_test = list()
        self.permutable_variables = list()

        args = arg.create_args(self.strategy_name, False)
        self.syms_from_config = [s.lower() for s in args.symbols]

        # 파라미터 최적화를 실행하는 동안 불필요한 로그 출력을 제한한다.
        log.min_log_level = logging.WARNING

    def run_strategy(self, variables):
        try:
            symbols_to_override = [s[1] for s in variables if s[0] == "symbols"]
            if symbols_to_override:
                symbols_to_override = [s.upper() for s in symbols_to_override[0]]

            argos = backtest.BacktestMode(self.strategy_name, False, symbols_to_override)
            return variables, argos.run(variables)
        except Exception:
            print("Exception: run_strategy. Returning empty run_strategy")
            Path(".error").mkdir(parents=True, exist_ok=True)
            import os

            with open(
                os.path.dirname(os.path.realpath(__file__)) + "/../.error/" + "multi_error.log",
                "w",
            ) as f:
                f.write("".join(traceback.format_exception(*sys.exc_info())))
            return variables, {}

    def add_variable(self, name: str, values: list):
        self.variables.append(list(product([name], values)))

    def set_variables_to_test(self):

        # Check if symbols variable were added
        if self.variables[0][0][0] == "symbols":
            # Collect all single symbols and all multi symbols
            single_syms = [sym for sym in self.variables[0] if len(sym[1]) == 1] or []
            multi_syms = [sym for sym in self.variables[0] if len(sym[1]) > 1] or []

            if single_syms == [] and multi_syms == []:
                # 테스트할 variables의 조합을 구한다.
                self.variables_to_test = list(product(*self.variables))
                return

            if len(single_syms) > 0:
                self.variables_to_test.extend(list(product(single_syms, *self.variables[1:])))

            permut_pairs = []
            if len(multi_syms) > 0:
                # Add permut var
                for s in multi_syms:
                    prod_pair = []
                    # Variables other than 'symbols' starts from index 1
                    for var in self.variables[1:]:
                        # Aggregate the variable values for permutation
                        variable_name = var[0][0]
                        variables_value = [v[1] for v in var]

                        if variable_name not in self.permutable_variables:
                            continue

                        # Permuation len depends on len of symbols
                        permut = product(variables_value, repeat=len(s[1]))

                        pairs = [(variable_name, v) for v in list(permut)]
                        prod_pair.append(pairs)
                    permut_pairs.extend(list(product([s], *prod_pair)))

                # Add nonpermut var
                var_pairs = []
                for var in self.variables[1:]:
                    variable_name = var[0][0]
                    variables_value = [v[1] for v in var]
                    if variable_name not in self.permutable_variables:
                        pairs = [(variable_name, v) for v in variables_value]
                        var_pairs.append(pairs)

                com = []
                # Combine tuples of permut and nonpermut

                # ex: (('symbols', ['btcusdt', 'xrpusdt', 'ethusdt']),
                # ('trading_interval', (720, 720, 720)), ('mi_io', 720))
                for r in product(permut_pairs, *var_pairs):
                    o = r[0] + (*r[1:],)
                    com.append(o)

                self.variables_to_test.extend(com)

        else:
            self.variables_to_test = list(product(*self.variables))

    def set_permutable_variables(self, names: list):
        self.permutable_variables.extend(names)

    def get_strategy_data(self, symbol):
        data = StrategyData(self.strategy_name)
        data.get_data(symbol)

    def get_all_data(self):
        all_syms = []
        for v in self.variables_to_test:
            for sym in v[0][1]:
                if sym not in all_syms:
                    all_syms.append(sym)
        # Download data into cache first before start strategy to solve duplicate downloads because of multiprocessing
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            pool.map(self.get_strategy_data, all_syms)

    def run(self):
        self.set_variables_to_test()

        print(f"Running parameter optimization on {self.variables_to_test}")
        print(f"Num of cases: {len(self.variables_to_test)}")

        begin_ts = time.time()
        # Prefetch all data of each symbols
        self.get_all_data()

        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.map(self.run_strategy, [list(t) for t in self.variables_to_test])

        helper.save_multi_summary(self.strategy_name, results)
        print(f"Multi process took {time.time() - begin_ts} seconds.")
