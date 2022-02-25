import pandas as pd

DEF_COL_NAME = 'close'

def check_2ma_above(df):
    eod_last = df.iloc[-1]
    eod_prev = df.iloc[-2]
    return eod_prev['ma_middle'] < eod_prev['ma_long'] and eod_last['ma_middle'] > eod_last['ma_long']


def check_2ma_bellow(df):
    eod_last = df.iloc[-1]
    eod_prev = df.iloc[-2]
    return eod_prev['ma_middle'] > eod_prev['ma_long'] and eod_last['ma_middle'] < eod_last['ma_long']


def scrn_sma_cross(df, params={}):

    ma_long = params.get('ma_long', 200)
    ma_middle = params.get('ma_middle', 50)
    if ma_long is None or ma_middle is None:
        raise ValueError(f'There are not necessary parameters ma_long or ma_middle')

    df_filtered = df.groupby('full_ticker', sort=False).filter(
        lambda x: len(x) >= (ma_long + 1)).reset_index(0, drop=True) # will have minimum 2 values with ma_long after calculation

    if df_filtered.empty:
        raise ValueError(f'There are not enough days of data to calculate ma_long {ma_long} for the last two days.')

    df_filtered['ma_long'] = df_filtered.groupby('full_ticker', sort=False)[DEF_COL_NAME].rolling(ma_long).mean().reset_index(0,
                                                                                                                    drop=True)
    df_filtered['ma_middle'] = df_filtered.groupby('full_ticker', sort=False)[DEF_COL_NAME].rolling(ma_middle).mean().reset_index(0,
                                                                                                                     drop=True)
    df_with_ma = df_filtered.groupby('full_ticker', sort=False).filter(
        lambda x: not x.iloc[-2:][['ma_long', 'ma_middle']].isnull().values.any())

    if df_with_ma.empty:
        raise ValueError(f'There is does not enough data for MA calculation {ma_long} and {ma_middle} periods')

    df_signals_golden = df_with_ma.groupby('full_ticker', sort=False).filter(check_2ma_above).reset_index(0, drop=True)
    df_signals_death = df_with_ma.groupby('full_ticker', sort=False).filter(check_2ma_bellow).reset_index(0, drop=True)

    return {'golden cross': list(pd.unique(df_signals_golden['full_ticker'])),
            'dead cross': list(pd.unique(df_signals_death['full_ticker']))}
