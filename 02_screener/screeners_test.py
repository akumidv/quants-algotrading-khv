import pytest
import pandas as pd

import screeners

FEATHER_TEST_DATA_FILE = './df-NYSE-202.ftr'  # './df-BINANCE-202.ftr'


@pytest.fixture()
def df():
    df_data = pd.read_feather(FEATHER_TEST_DATA_FILE)
    if 'exchange' not in df_data.columns:
        df_data['exchange'] = 'NYSE'
    df_data['full_ticker'] = df_data['exchange'] + ':' + df_data["ticker"]
    return df_data


def test_scrn_sma_cross(df):
    res = screeners.scrn_sma_cross(df)
    print(res)
