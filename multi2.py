from common.multi2 import Multi2

if __name__ == "__main__":
    multi2 = Multi2("band_003_2", start_date=None, end_date=None)
    multi2.add_variable("symbols", [["btcusdt"]])
    multi2.add_variable("reset_period", [5, 10, 20])
    multi2.add_variable("band_period", [20, 15, 7])
    multi2.add_variable("recent_band_period", [3])
    multi2.run()