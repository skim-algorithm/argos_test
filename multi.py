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

    multi = Multi("junggil_003_2")
    multi.add_variable("symbols", [["btcusdt"]])
    multi.add_variable("max_loss", [0.025, 0.01])
    multi.add_variable("band_period", [12, 7])
    multi.add_variable("recent_band_period", [7, 3])
    multi.run()