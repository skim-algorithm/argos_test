from common.multi import Multi

if __name__ == "__main__":
    """
    Adding symbol variable has 2 options:
    1. Get symbols from strategy json config file
    >>> multi.add_variable('symbols', [multi.syms_from_config])

    2. Multi symbols (with permutation)
    >>> multi.add_variable('symbols',
                   [
                      ["ethusdt"], ["ethusdt", "xrpusdt"]
                   ])

    """

    multi = Multi("skim_005_2")
    multi.add_variable("symbols", [["btcusdt", "ethusdt"]])
    multi.add_variable("long_period_mapping", [12 * 24 * 60, 7 * 24 * 60])
    multi.add_variable("short_period_mapping", [300, 3 * 24 * 60])
    multi.add_variable("stoch_mapping", [100, 300, 500])
    multi.add_variable("ti_interval", [10, 20])
    multi.run()