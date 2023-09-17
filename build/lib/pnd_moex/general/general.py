import pandas as pd
import numpy as np
from adtk.detector import QuantileAD, PersistAD, VolatilityShiftAD
from pnd_moex.util.other import find_all_sequences
from pandas.tseries.offsets import BDay


def anomaly_detect(
    ts: pd.Series,
    quantile: bool = True,
    persist: bool = False,
    volatility: bool = False,
) -> pd.DataFrame:
    """
    Apply different methods of anomaly detection on the given time series data.

    :param ts: Time series data with a datetime/timestamp index.
    :type ts: pd.Series
    :return: DataFrame with columns representing different detection methods and the same datetime index.
    :rtype: pd.DataFrame
    """
    # Calculate shifted percentage values
    pct_lag1 = ts.pct_change()
    pct_lag2 = ts.shift(-1).pct_change()
    pct_lag3 = ts.shift(-2).pct_change()
    pct_over3 = ts.shift(-2).pct_change(periods=3)

    # Create a result DataFrame
    df = pd.DataFrame(index=ts.index)
    # Discover anomalies
    # Detect 3-day consecutive over 20% growth
    a_map = (np.array([pct_lag1, pct_lag2, pct_lag3]) > 0.2).all(axis=0)

    # Correct values for better visuals
    # The algorithm returns True for records where the 3-day growth rate is above 20%
    # To improve visualization, mark these three days as part of the pump
    ids = np.where(a_map == True)
    for id in ids[0]:
        a_map[id : id + 3] = True
    df["3over20"] = a_map

    # Apply a similar approach, but looking for growth over 80% over 3 days
    a_map = pct_over3 > 0.8
    ids = np.where(a_map == True)
    for id in ids[0]:
        a_map[id : id + 3] = True
    # tut nado sdelat' otbor
    df["80over3"] = a_map

    # Use ADTK tools to detect anomalies
    # Quantile AD
    if quantile:
        q_ad = QuantileAD(high=0.99, low=0)
        df["quantile"] = q_ad.fit_detect(pct_lag1)
    if persist:
        # Persist AD
        persist_ad = PersistAD(30, c=5.0, side="positive")
        df["persist"] = persist_ad.fit_detect(ts)
    if volatility:
        # Volatility AD
        volatility_shift_ad = VolatilityShiftAD(c=6.0, side="positive", window=30)
        df["volatility"] = volatility_shift_ad.fit_detect(ts)

    # Filling NA with False
    df.fillna(False, inplace=True)

    return df


def anomaly_news_markup_func(
    df: pd.DataFrame,
    anomaly_map: pd.Series,
    news_list: list,
    val_col: str = "CLOSE",
    mark_period: bool = True,
    na_mark: any = -1,
) -> pd.DataFrame:
    """
    Mark data in DataFrame based on anomalies and news.

    Mark -1 for anomalies and NaN values, mark 0 for normal data, and mark 1 for data with anomalies before news.

    :param df: Security data.
    :type df: pd.DataFrame
    :param anomaly_map: Anomaly data as a pd.Series.
    :type anomaly_map: pd.Series
    :param news_list: List of news dates with risk parameter restrictions.
    :type news_list: list
    :return: DataFrame with resampled and marked data.
    :rtype: pd.DataFrame
    """

    # Resample over business days
    freqed_df = df.asfreq("B")
    freqed_df["anomaly"] = anomaly_map.asfreq("B")
    freqed_df["mark"] = 0
    n, _ = freqed_df.shape
    # Detect all NaN and anomalies and mark them as -1
    a_n_list = find_all_sequences(
        freqed_df["anomaly"], lambda x: x is True
    ) + find_all_sequences(freqed_df[val_col], lambda x: np.isnan(x))
    # Cut +/- 3 days for NaN and anomalies
    a_n_list = list(
        map(
            lambda x: (
                x[0] - 3 if x[0] > 3 else 0,
                x[1] + 3 if x[1] < n - 3 else n - 1,
            ),
            a_n_list,
        )
    )
    a_n_list.sort(key=lambda x: x[0])
    idx = freqed_df.index

    # Mark anomalies and NaN
    for start_idx, end_idx in a_n_list:
        st_d = idx[start_idx]
        ed_d = idx[end_idx]
        freqed_df.loc[st_d:ed_d, "mark"] = na_mark

    # Process news
    for news_date in news_list:
        news = pd.Timestamp(news_date.date())
        previous_date = news - BDay(10)
        next_date = news + BDay(0)
        # Get previous 10 days before news
        selected_df = freqed_df.loc[previous_date:next_date]

        if any(selected_df[val_col].isna()):
            continue

        if any(selected_df["anomaly"]):
            f_a_date = selected_df.loc[selected_df["anomaly"] == True].index[0]
            if mark_period:
                st_date = f_a_date - BDay(7)
                ed_date = f_a_date + BDay(5)
                freqed_df.loc[st_date:ed_date, "mark"] = 1
            else:
                freqed_df.loc[f_a_date, "mark"] = 1
    freqed_df.drop(columns=["anomaly"], inplace=True)
    return freqed_df


# def anomaly_detect(ts: pd.Series) -> pd.DataFrame:
#     """
#     Apply different methods of anomaly detection on the given time series data.

#     :param ts: Time series data with a datetime/timestamp index.
#     :type ts: pd.Series
#     :return: DataFrame with columns representing different detection methods and the same datetime index.
#     :rtype: pd.DataFrame
#     """
#     # Calculate shifted percentage values
#     pct_lag1 = ts.pct_change()
#     pct_lag2 = ts.shift(-1).pct_change()
#     pct_lag3 = ts.shift(-2).pct_change()
#     pct_over3 = ts.shift(-2).pct_change(periods=3)

#     # Create a result DataFrame
#     df = pd.DataFrame(index=ts.index)
#     # Discover anomalies
#     # Detect 3-day consecutive over 20% growth
#     a_map = (np.array([pct_lag1, pct_lag2, pct_lag3]) > 0.2).all(axis=0)

#     # Correct values for better visuals
#     # The algorithm returns True for records where the 3-day growth rate is above 20%
#     # To improve visualization, mark these three days as part of the pump
#     ids = np.where(a_map == True)
#     for a_id in ids[0]:
#         a_map[a_id : a_id + 3] = True
#     df["3over20"] = a_map

#     # Apply a similar approach, but looking for growth over 80% over 3 days
#     a_map = pct_over3 > 0.8
#     ids = np.where(a_map == True)
#     for a_id in ids[0]:
#         a_map[a_id : a_id + 3] = True
#     # tut nado sdelat' otbor
#     df["80over3"] = a_map

#     # Use ADTK tools to detect anomalies
#     # Quantile AD
#     q_ad = QuantileAD(high=0.99, low=0)
#     df["quantile"] = q_ad.fit_detect(pct_lag1)

#     # Persist AD
#     persist_ad = PersistAD(30, c=5.0, side="positive")
#     df["persist"] = persist_ad.fit_detect(ts)

#     # Volatility AD
#     volatility_shift_ad = VolatilityShiftAD(c=6.0, side="positive", window=30)
#     df["volatility"] = volatility_shift_ad.fit_detect(ts)

#     # Filling NA with False
#     df.fillna(False, inplace=True)

#     return df
