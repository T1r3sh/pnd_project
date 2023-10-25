import itertools

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pnd_moex.util.other import find_all_sequences


def visual_args(period: tuple, color: str, ymax: float) -> dict:
    """
    Arguments constructor for shape-type visual elements in plotly.

    :param period: A tuple with 2 values, representing the start and end of the sequence. If the sequence length is 1, both values should be the same.
    :type period: tuple
    :param color: The color used for visuals.
    :type color: str
    :param ymax: The upper bound for visuals.
    :type ymax: float
    :return: Arguments for visuals, which are used in a dropdown menu later on.
    :rtype: dict
    """
    res = dict(
        type="rect",
        xref="x",
        yref="y",
        x0=period[0],
        y0=0,
        x1=period[1],
        y1=ymax,
        line=dict(
            color=color,
            opacity=0.8,
        ),
        fillcolor=color,
        opacity=0.3,
    )
    return res


def gather_periods_visuals(
    ts: pd.Series, periods: list, color: str, ymax: float
) -> list:
    """
    Gather all visuals for one anomaly group.

    :param ts: Time series data with a datetime/timestamp index.
    :type ts: pd.Series
    :param periods: List of tuples representing time periods or two identical dates if the period length is 0.
    :type periods: list
    :param color: Color for the plot.
    :type color: str
    :param ymax: Upper bound for visual elements.
    :type ymax: float
    :return: List of arguments for visuals.
    :rtype: list
    """
    res = []
    for period in periods:
        timestamps = tuple([ts[idx] for idx in period])
        args = visual_args(timestamps, color, ymax)
        res.append(args)
    return res


def gather_all_visuals(
    anomalies: pd.DataFrame,
    colors: list = ["#f44336", "#ff4757", "#f0c419", "#578ca9", "#19aa9c"],
    ymax: float = 500,
) -> dict:
    """
    Create a dictionary with visuals for all anomalies in the anomaly DataFrame.

    :param anomalies: Anomaly DataFrame with columns representing different methods and an index of dates.
    :type anomalies: pd.DataFrame
    :param colors: Colormap for visuals, defaults to ["#f44336", "#ff4757", "#f0c419", "#578ca9", "#19aa9c"].
    :type colors: list, optional
    :param ymax: Upper bound for visuals, defaults to 500.
    :type ymax: float, optional
    :return: A dictionary containing visuals for all methods in the anomalies DataFrame, used in a dropdown menu.
    :rtype: dict
    """
    cols = anomalies.columns
    res = {}
    for color, col in zip(colors, cols):
        res[col] = gather_periods_visuals(
            anomalies.index,
            find_all_sequences(anomalies[col], lambda x: x),
            color,
            ymax,
        )
    return res


def anomaly_plot(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    news: list = [],
    date_col: str = "TRADEDATE",
    open_col: str = "OPEN",
    high_col: str = "HIGH",
    low_col: str = "LOW",
    close_col: str = "CLOSE",
    volume_col: str = "VOLUME",
) -> None:
    """
    Anomaly plotting function.
    Creates candlestick charts with a volume line plot.
    Creates a menu to depict results of different anomaly detection methods.
    Also plots actual news if provided.

    :param df: Time series DataFrame with Date, Open, High, Low, Close, and Volume columns.
    :type df: pd.DataFrame
    :param anomalies: Anomalies DataFrame.
    :type anomalies: pd.DataFrame
    :param news: List of dates where news with new limitations occurred, defaults to [].
    :type news: list, optional
    :param date_col: Name of the date column, defaults to "TRADEDATE".
    :type date_col: str, optional
    :param open_col: Name of the open column, defaults to "OPEN".
    :type open_col: str, optional
    :param high_col: Name of the high column, defaults to "HIGH".
    :type high_col: str, optional
    :param low_col: Name of the low column, defaults to "LOW".
    :type low_col: str, optional
    :param close_col: Name of the close column, defaults to "CLOSE".
    :type close_col: str, optional
    :param volume_col: Name of the volume column, defaults to "VOLUME".
    :type volume_col: str, optional
    """
    # Creating figure
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)
    # Plotting candlesticks
    fig.add_trace(
        go.Candlestick(
            x=df[date_col],
            open=df[open_col],
            high=df[high_col],
            low=df[low_col],
            close=df[close_col],
        ),
        row=1,
        col=1,
    )
    # Plotting news
    ymax = df[high_col].max() * 1.25

    for n_t in news:
        fig.add_trace(
            go.Scatter(
                x=[n_t, n_t], y=[0, ymax], mode="lines", marker={"color": "black"}
            )
        )

    # Getting anomaly visuals
    all_anomaly_visuals = gather_all_visuals(anomalies, ymax=ymax)

    # Creating a dropdown menu with visuals for each anomaly detection type
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=list(
                    [dict(label="None", method="relayout", args=[{"shapes": []}])]
                    + [
                        dict(
                            label=name,
                            method="relayout",
                            args=[{"shapes": val}],
                        )
                        for name, val in all_anomaly_visuals.items()
                    ]
                    + [
                        dict(
                            label="All",
                            method="relayout",
                            args=[
                                {
                                    "shapes": list(
                                        itertools.chain.from_iterable(
                                            all_anomaly_visuals.values()
                                        )
                                    )
                                }
                            ],
                        ),
                    ],
                ),
                active=0,
                x=0.2,
                xanchor="left",
                y=1.27,
                yanchor="top",
            )
        ]
    )

    # Creating Volume plot
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df[volume_col],
            opacity=1,
        ),
        row=2,
        col=1,
    )
    # Updating the title and y-axes labels for each plot
    fig.update_layout(
        title_text=f"{df['SECID'].iloc[0]} Time Series ",
        yaxis1_title=f"Price, {df['currencyid'].iloc[0]}",
        yaxis1=dict(range=[0, ymax]),  # ymax - ваш верхний лимит
        yaxis2_title="Volume",
        xaxis1_rangeslider_visible=False,
    )
    # Updating width and legend
    fig.update_layout(
        width=1200,
        showlegend=False,
    )
    fig.show()


def pnd_sample_data_plot(
    pump_and_dump_df: pd.DataFrame,
    time_series_df: pd.DataFrame,
    sample_size: int = 5,
    date_column="TRADEDATE",
    open_column="OPEN",
    high_column="HIGH",
    low_column="LOW",
    close_column="CLOSE",
    currency_column="currencyid",
    token_column="SECID",
) -> None:
    """
    Visualize a sample of time series data.

    This function generates candlestick charts for a random sample of tokens from pump and dump data.

    :param pump_and_dump_df: DataFrame with pump and dump information, including token and p_date.
    :type pump_and_dump_df: pd.DataFrame
    :param time_series_df: Time series data for securities with candlestick information.
    :type time_series_df: pd.DataFrame
    :param sample_size: Number of tokens to include in the sample, defaults to 5.
    :type sample_size: int, optional
    :param date_column: Name of the date column in the time series DataFrame, defaults to "TRADEDATE".
    :type date_column: str, optional
    :param open_column: Name of the open price column in the time series DataFrame, defaults to "OPEN".
    :type open_column: str, optional
    :param high_column: Name of the high price column in the time series DataFrame, defaults to "HIGH".
    :type high_column: str, optional
    :param low_column: Name of the low price column in the time series DataFrame, defaults to "LOW".
    :type low_column: str, optional
    :param close_column: Name of the close price column in the time series DataFrame, defaults to "CLOSE".
    :type close_column: str, optional
    :param currency_column: Name of the currency column in the time series DataFrame, defaults to "currencyid".
    :type currency_column: str, optional
    :param token_column: Name of the token column in both DataFrames, defaults to "SECID".
    :type token_column: str, optional
    """
    sample_df = pump_and_dump_df.sample(sample_size)
    fig = make_subplots(
        rows=sample_size,
        cols=1,
        subplot_titles=tuple(
            f"Time Series for {token}" for token in sample_df["token"].tolist()
        ),
    )
    for idx, (_, row) in enumerate(sample_df.iterrows()):
        security_df = time_series_df[time_series_df[token_column] == row["token"]]
        fig.add_trace(
            go.Candlestick(
                x=security_df[date_column],
                open=security_df[open_column],
                high=security_df[high_column],
                low=security_df[low_column],
                close=security_df[close_column],
            ),
            row=idx + 1,
            col=1,
        )
        fig.update_xaxes(rangeslider_visible=False)
        fig.update_yaxes(title_text=f"Price, {security_df[currency_column].iloc[0]}")
        for date_ in row["p_date"]:
            fig.add_vline(x=date_, row=idx + 1, col=1)
    fig.update_layout(
        height=sample_size * 250,
        width=1200,
        showlegend=False,
        title_text=f"Time series of {sample_size} random tokens",
    )
    fig.show()
