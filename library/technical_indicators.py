import numpy as np
import pandas as pd
import math


def rsi(data, smoothingPeriod):
    """
    calculate RSI
    """
    up = np.where(data.diff(1) > 0, data.diff(1), 0)
    dw = np.where(data.diff(1) < 0, data.diff(1) * (-1), 0)

    AU = up.ewm(span=smoothingPeriod, adjust=False, min_periods=smoothingPeriod).mean()
    AD = dw.ewm(span=smoothingPeriod, adjust=False, min_periods=smoothingPeriod).mean()
    rs = AU / abs(AD)
    RSI = 1.0 - 1.0 / (1 + rs)

    return RSI


def stochastic_oscillator(data, fastk_period=14, slowk_period=1, slowd_period=3):
    df_temp = data.copy()
    sz = len(df_temp)
    if sz < fastk_period:
        # show error message
        raise SystemExit("short of input data history")
    tempSto_K = []
    for i in range(sz):
        if i >= fastk_period - 1:
            tempUp = df_temp["Close"][i] - min(df_temp["Low"][i - fastk_period + 1 : i + 1])
            tempDown = max(df_temp["High"][i - fastk_period + 1 : i + 1]) - min(
                df_temp["Low"][i - fastk_period + 1 : i + 1]
            )
            tempSto_K.append(tempUp / tempDown)
        else:
            tempSto_K.append(0)  # initialize 0 for the period of earlier than 'fastk_period'
    df_temp["Sto_K"] = pd.Series(tempSto_K, index=df_temp.index)

    df_temp["Sto_SlowK"] = pd.Series(pd.rolling_mean(df_temp["Sto_K"], slowk_period))
    df_temp["Sto_SlowD"] = pd.Series(pd.rolling_mean(df_temp["Sto_D"], slowd_period))

    return df_temp


def macd(data, fastperiod=12, slowperiod=26, signalperiod=9):
    df_temp = data.copy()
    # sz = len(df_temp)
    df_temp["EMAFast"] = df_temp["close"].ewm(span=fastperiod, adjust=False, min_periods=fastperiod).mean()
    df_temp["EMASlow"] = df_temp["close"].ewm(span=slowperiod, adjust=False, min_periods=slowperiod).mean()
    df_temp["MACD"] = df_temp["EMAFast"] - df_temp["EMASlow"]
    df_temp["MACDSignal"] = df_temp["MACD"].ewm(span=signalperiod, adjust=False, min_periods=signalperiod).mean()
    df_temp["MACDHistogram"] = df_temp["MACD"] - df_temp["MACDSignal"]

    return df_temp


def force_index(data, smoothingPeriod=2):
    df_temp = data.copy()
    df_temp["ForceIndex_raw"] = df_temp["close"].diff() * df_temp["volume"]
    df_temp["ForceIndex"] = (
        df_temp["ForceIndex_raw"].ewm(span=smoothingPeriod, adjust=False, min_periods=smoothingPeriod).mean()
    )

    return df_temp


def divergence(prices, indicator, LookBack=5, rel=1, resolution=0.0050):
    df_temp = prices.copy()
    df_temp2 = indicator.copy()

    sz = len(df_temp)
    sz2 = len(df_temp2)
    if sz < LookBack or sz2 < 4:
        # show error message
        raise SystemExit("error: insufficient input data history")

    df_temp = df_temp.iloc[-LookBack:, :]
    df_temp2 = df_temp2.iloc[-LookBack:, :]
    max_price = float(df_temp["high"].max())
    min_price = float(df_temp["low"].min())
    current_high_price = float(df_temp["high"].iloc[-1])
    current_low_price = float(df_temp["low"].iloc[-1])
    max_ind = float(df_temp2.max())
    min_ind = float(df_temp2.min())

    if abs(float(indicator.iloc[-3])) == 0.0:
        resolution_indicator = 0.0
    else:
        if rel == 1:
            resolution_indicator = abs(float(indicator.iloc[-3])) * resolution
        else:
            eff_num_ind = math.log10(abs(float(indicator.iloc[-3])))
            mul = resolution * pow(10, 3)
            if eff_num_ind >= 0:
                resolution_indicator = mul * pow(10, int(eff_num_ind) - 2)
            else:
                resolution_indicator = mul * pow(10, int(eff_num_ind) - 3)

    if df_temp["high"].iloc[-3] == 0.0:
        resolution_price = 0.0
    else:
        if rel == 1:
            resolution_price = df_temp["high"].iloc[-3] * resolution
        else:
            eff_num_p = math.log10(df_temp["high"].iloc[-3])
            mul = resolution * pow(10, 3)
            if eff_num_p >= 0:
                resolution_price = mul * pow(10, int(eff_num_p) - 2)
            else:
                resolution_price = mul * pow(10, int(eff_num_p) - 3)

    divergence_val = 0.0
    # Bearish Divergence: movement of oscillator's maxima
    if (float(indicator.iloc[-3]) - float(indicator.iloc[-4])) > 0.0:
        if current_high_price == max_price and current_high_price - df_temp["high"].iloc[-3] > resolution_price:
            # lowering maximum
            if (
                float(indicator.iloc[-1]) < float(indicator.iloc[-3])
                and float(indicator.iloc[-3]) - float(indicator.iloc[-1]) > resolution_indicator
            ):
                # strong sell
                divergence_val = -1.0
            # double ceiling
            elif abs(float(indicator.iloc[-1]) - float(indicator.iloc[-3])) < resolution_indicator and min(
                float(indicator.iloc[-1]), float(indicator.iloc[-3])
            ) > float(indicator.iloc[-2]):
                # weak sell
                divergence_val = -0.25
        elif (
            max(current_high_price, df_temp["high"].iloc[-3]) == max_price
            and abs(current_high_price - df_temp["high"].iloc[-3]) < resolution_price
            and (df_temp["high"].iloc[-3] - df_temp["high"].iloc[-4]) > 0.0
            and min(current_high_price, df_temp["high"].iloc[-3]) > df_temp["high"].iloc[-2]
        ):
            if (
                float(indicator.iloc[-1]) < float(indicator.iloc[-3])
                and float(indicator.iloc[-3]) - float(indicator.iloc[-1]) > resolution_indicator
            ):
                # medium sell
                divergence_val = -0.5
    # Bullish Divergence: movement of oscillator's minima
    elif (float(indicator.iloc[-3]) - float(indicator.iloc[-4])) < 0.0:
        if current_low_price == min_price and df_temp["low"].iloc[-3] - current_low_price > resolution_price:
            # heightening minimum
            if (
                float(indicator.iloc[-1]) > float(indicator.iloc[-3])
                and float(indicator.iloc[-1]) - float(indicator.iloc[-3]) > resolution_indicator
            ):
                # strong buy
                divergence_val = 1.0
            # double floor
            elif abs(float(indicator.iloc[-1]) - float(indicator.iloc[-3])) < resolution_indicator and max(
                float(indicator.iloc[-1]), float(indicator.iloc[-3])
            ) < float(indicator.iloc[-2]):
                # weak buy
                divergence_val = 0.25
        elif (
            min(current_low_price, df_temp["low"].iloc[-3]) == min_price
            and abs(current_low_price - df_temp["low"].iloc[-3]) < resolution_price
            and (df_temp["low"].iloc[-3] - df_temp["low"].iloc[-4]) < 0.0
            and max(current_low_price, df_temp["low"].iloc[-3]) < df_temp["low"].iloc[-2]
        ):
            if (
                float(indicator.iloc[-1]) > float(indicator.iloc[-3])
                and float(indicator.iloc[-1]) - float(indicator.iloc[-3]) > resolution_indicator
            ):
                # medium buy
                divergence_val = 0.5

    return divergence_val


def elder_ray_index(data, smoothingPeriod=13):
    df_temp = data.copy()
    df_temp["ema"] = df_temp["close"].ewm(span=smoothingPeriod, adjust=False, min_periods=smoothingPeriod).mean()
    df_temp["bull_power"] = df_temp["high"] - df_temp["ema"]
    df_temp["bear_power"] = df_temp["low"] - df_temp["ema"]

    return df_temp


def position_by_elder_ray_index(data, smoothingPeriod=13):
    position = 0
    elder_ray_index_df = elder_ray_index(data, smoothingPeriod)
    ema_slope_t = float(elder_ray_index_df["ema"].diff().iloc[-1])
    bull_power_t = float(elder_ray_index_df["bull_power"].iloc[-1])
    bull_power_slope_t = float(elder_ray_index_df["bull_power"].diff().iloc[-1])
    bear_power_t = float(elder_ray_index_df["bear_power"].iloc[-1])
    bear_power_slope_t = float(elder_ray_index_df["bear_power"].diff().iloc[-1])

    if ema_slope_t > 0 and (bear_power_t < 0 and bear_power_slope_t > 0):
        position = 1
    elif ema_slope_t < 0 and (bull_power_t > 0 and bull_power_slope_t < 0):
        position = -1

    return position
