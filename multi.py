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

    multi = Multi("harin_002")
    multi.add_variable("symbols", [["ethusdt"], ["ethusdt", "xrpusdt"]])
    multi.add_variable("trading_interval", [120, 240, 360])
    multi.add_variable("diff_ratio", [1, 2])
    multi.set_permutable_variables(["trading_interval"])
    multi.run()
