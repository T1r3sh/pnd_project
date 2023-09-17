import pandas as pd
import numpy as np
from adtk.detector import QuantileAD, PersistAD, VolatilityShiftAD


def anomaly_detect(ts: pd.Series) -> pd.DataFrame:
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
    q_ad = QuantileAD(high=0.99, low=0)
    df["quantile"] = q_ad.fit_detect(pct_lag1)

    # Persist AD
    persist_ad = PersistAD(30, c=5.0, side="positive")
    df["persist"] = persist_ad.fit_detect(ts)

    # Volatility AD
    volatility_shift_ad = VolatilityShiftAD(c=6.0, side="positive", window=30)
    df["volatility"] = volatility_shift_ad.fit_detect(ts)

    # Filling NA with False
    df.fillna(False, inplace=True)

    return df
