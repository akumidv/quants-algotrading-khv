import pandas as pd
import pytest
import model
import datetime


def test_get_candle_eod_data():
    res = model.get_candle_eod_data('BINANCE', 10)
    df = res['data']
    assert isinstance(df, pd.DataFrame)
    assert 'ticker' in df.columns
    print(df)


def test_get_exchange_eod_history():
    df = model.get_exchange_eod_history('NYSE', 100, '2022-02-24')
    assert isinstance(df, pd.DataFrame)
    assert 'ticker' in df.columns
    print(df)


def test_get_api_eod_date():
    tickers_eod = model.get_api_eod_date('NYSE', datetime.date.fromisoformat('2022-02-24'))
    assert isinstance(tickers_eod, list)
    print(tickers_eod)
