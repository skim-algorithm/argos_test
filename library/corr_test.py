import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
import helper
import pandas as pd
import re


def interval_to_minutes(interval):
    i = int(re.sub("[^0-9]", "", interval))
    if "d" in interval.lower():
        minutes = 24 * 60 * i
    elif "h" in interval.lower():
        minutes = 60 * i
    elif "m" in interval.lower():
        minutes = i
    elif "s" in interval.lower():
        minutes = i / 60
    else:
        raise ValueError("incorrect interval")

    return minutes


def combine_data(result_list):
    helper.create_directory("/strategies/info_results/corr_results")
    data_dict = {}
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
        data_dict["data_{}".format(i)] = raw_data[sheet]

    return data_dict


def corr_test(result_list):

    data_dict = combine_data(result_list)

    # calculate correlation with 1-day return(%)
    data_list, name_list = [], []
    separator, j = "_", 1
    for i, data in enumerate(data_dict.values()):
        d = pd.DataFrame(data["Portfolio Value"])
        if "/" not in result_list[i]:
            name = separator.join(result_list[i].split("_")[:2])
        else:
            name = "allocation{}".format(j)
            j += 1
        d[name] = d["Portfolio Value"].pct_change() * 100
        data_list.append(pd.DataFrame(d[name]))
        name_list.append(name)

    data = data_list[0]
    data = data.join(data_list[1:])
    data.dropna(inplace=True)

    pd.set_option("display.max_columns", None)
    print(data)
    corr = data.corr()
    print(corr)
    separator = "-"
    corr.to_csv("../strategies/info_results/corr_results/corr_{}.csv".format(separator.join(name_list)))


if __name__ == "__main__":
    corr_test(
        [
            "junggil_003_futures_2020-01-01_2021-10-25_BTCUSDT_1m_1635137319.7298524",
            "junggil_004_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635138235.4760425",
            "junggil_005_futures_2020-01-01_2021-10-25_13symbols_15m_1635167229.073372",
            "skim_002_futures_2020-10-01_2021-10-25_15symbols_1m_1635179438.6631575",
            "harin_001_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635138498.073447",
            "skim_004_futures_2020-01-01_2021-10-25_BTCUSDT_1m_1635138567.655567",
            "junggil_006_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_15m_1635137585.8993316",
            "skim_003_futures_2020-01-01_2021-10-25_BTCUSDT-ETHUSDT_1m_1635149929.7658296",
            "harin_002_futures_2020-01-01_2021-10-25_ETHUSDT-XRPUSDT_1m_1635139034.1453316",
            "harin_003_futures_2020-01-01_2021-10-25_5symbols_1m_1635141992.23297",
            "asset_allocation/ratio[0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.0]&junggil_003-junggil_004-junggil_005-skim_002-harin_001-skim_004-junggil_006-skim_003-harin_002-harin_003",
        ]
    )
