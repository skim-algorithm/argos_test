import numpy as np
import pandas as pd


def get_force_index(df, interval, period):
    """
    추세를 측정하는데 거래량이라는 지표를 추가, 거래량이 강할수록 추세가 크다고 가정
    """

    open = df.open.resample("{}min".format(interval), origin="start").first()
    close = df.close.resample("{}min".format(interval), origin="start").last()
    volume = df.volume.resample("{}min".format(interval), origin="start").sum()
    forex = (close - open) * volume
    forex_index = forex.ewm(span=period, adjust=False, min_periods=period).mean()

    return forex_index


def get_rsi(df, interval, period):

    close = df.close.resample("{}min".format(interval), origin="start").last()
    up = pd.DataFrame(np.where(close.diff(1) > 0, close.diff(1), 0.0))
    dw = pd.DataFrame(np.where(close.diff(1) < 0, close.diff(1) * (-1), 0.0))
    avg_gain = up.ewm(span=period, adjust=False, min_periods=period).mean()
    avg_loss = dw.ewm(span=period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 1.0 - 1.0 / (1 + rs)

    return rsi


def get_stochastic(df, interval, period1, period2):

    close = df.close.resample("{}min".format(interval), origin="start").last()
    high = df.high.resample("{}min".format(interval), origin="start").max().rolling(period1).max()
    low = df.low.resample("{}min".format(interval), origin="start").min().rolling(period1).min()
    fast_k = (close - low) / (high - low)
    fast_d = fast_k.rolling(period2).mean()
    slow_d = fast_d.rolling(period2).mean()

    return slow_d


def get_adx(df, interval, period):
    """
    adx: 추세판단(0~100사이의 값), 보통 25이상이면 추세가 어떤 방향으로든 있다고 판단
    """

    high = df.high.resample("{}min".format(interval), origin="start").max()
    low = df.low.resample("{}min".format(interval), origin="start").min()
    high_diff, low_diff = high.diff().dropna(), -low.diff().dropna()
    dm_plus = high_diff.where(high_diff > low_diff, 0)
    dm_minus = low_diff.where(low_diff > high_diff, 0)
    dm_plus = dm_plus.where(dm_plus > 0, 0)
    dm_minus = dm_minus.where(dm_minus > 0, 0)
    tr = (high - low).iloc[1:]
    index = dm_plus.index
    smoothed_dm_plus, smoothed_dm_minus, smoothed_tr = (
        pd.Series(index=index),
        pd.Series(index=index),
        pd.Series(index=index),
    )
    for i in range(period - 1, len(dm_plus)):
        if i == period - 1:
            smoothed_dm_plus[i] = dm_plus[0 : i + 1].sum()
            smoothed_dm_minus[i] = dm_minus[0 : i + 1].sum()
            smoothed_tr[i] = tr[0 : i + 1].sum()
        else:
            smoothed_dm_plus[i] = smoothed_dm_plus[i - 1] * (period - 1) / period + dm_plus[i]
            smoothed_dm_minus[i] = smoothed_dm_minus[i - 1] * (period - 1) / period + dm_minus[i]
            smoothed_tr[i] = smoothed_tr[i - 1] * (period - 1) / period + tr[i]
    smoothed_dm_plus, smoothed_dm_minus, smoothed_tr = (
        smoothed_dm_plus.dropna(),
        smoothed_dm_minus.dropna(),
        smoothed_tr.dropna(),
    )
    di_plus = 100 * smoothed_dm_plus / smoothed_tr
    di_minus = 100 * smoothed_dm_minus / smoothed_tr
    dx = 100 * abs(di_plus - di_minus) / abs(di_plus + di_minus)
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean().dropna()

    return adx, dx


def get_aroon(df, interval, period):
    """
    aroon_up: 0~100 사이의 값, 높을수록 최고가가 최근에 발생
    aroon_down: 0~100 사이의 값, 높을수록 최저가가 최근에 발생
    """

    high = df.high.resample("{}min".format(interval), origin="start").max()[-period:]
    low = df.low.resample("{}min".format(interval), origin="start").min()[-period:]
    high_index, low_index = high.argmax(), low.argmin()
    aroon_up, aroon_down = (
        high_index / (period - 1) * 100,
        low_index / (period - 1) * 100,
    )

    return aroon_up, aroon_down


def get_macd(df, interval, ma_period1, ma_period2, signal_period):

    assert ma_period1 < ma_period2
    close = df.close.resample("{}min".format(interval), origin="start").last()
    ma1 = close.ewm(span=ma_period1, adjust=False, min_periods=ma_period1).mean()
    ma2 = close.ewm(span=ma_period2, adjust=False, min_periods=ma_period2).mean()
    fast_macd = ma1 - ma2
    signal_macd = fast_macd.ewm(span=signal_period, adjust=False, min_periods=signal_period).mean()

    return fast_macd, signal_macd


def get_trend(df, interval, period):

    close = df.close.resample("{}min".format(interval), origin="start").last().iloc[-period:]
    open = df.open.resample("{}min".format(interval), origin="start").first().iloc[-period:]
    trend = 100 * abs(close[-1] - open[0]) / abs(close - open).sum()

    return trend
