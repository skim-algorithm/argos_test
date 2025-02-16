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

    multi = Multi("skim_005")
    multi.add_variable("symbols", [["btcusdt"]])
    multi.add_variable("long_period", [3* 24 * 60, 4 * 24 * 60, 5 * 24 * 60])
    multi.add_variable("short_period", [300, 400, 500])
    multi.add_variable("stoch", [180, 360, 720])
    multi.run()
