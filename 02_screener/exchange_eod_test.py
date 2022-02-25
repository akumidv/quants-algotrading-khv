import exchange_eod as e_eod
import datetime


def test_get_candle_data():
    tickers_data = e_eod.get_entire_eod_data('NYSE', '2022-02-24')
    print(tickers_data)
    assert isinstance(tickers_data, list)
    assert isinstance(tickers_data[0], dict)
    assert 'code' in tickers_data[0]
    assert tickers_data[0]['code'] == 'A'

    # {'code': 'A', 'exchange_short_name': 'US', 'date': '2022-02-24', 'open': 123.7, 'high': 128.63, 'low': 123.06, 'close': 128.15, 'adjusted_close': 128.15, 'volume': 3249671}, {'code': 'AA', 'exchange_short_name': 'US', 'date': '2022-02-24', 'open': 75.29, 'high': 77.45, 'low': 69.85, 'close': 73.3, 'adjusted_close': 73.3, 'volume': 11506930},


def test_get_exchange_info():
    exch_info = e_eod.get_exchange_info('NYSE')
    print(exch_info)
    assert isinstance(exch_info, list)
    assert isinstance(exch_info[0], dict)
    assert 'Code' in exch_info[0]
    assert exch_info[0]['Code'] == 'A'

    # {'code': 'A', 'exchange_short_name': 'US', 'date': '2022-02-24', 'open': 123.7, 'high': 128.63, 'low': 123.06, 'close': 128.15, 'adjusted_close': 128.15, 'volume': 3249671}, {'code': 'AA', 'exchange_short_name': 'US', 'date': '2022-02-24', 'open': 75.29, 'high': 77.45, 'low': 69.85, 'close': 73.3, 'adjusted_close': 73.3, 'volume': 11506930},


def test_get_entire_eod_data():
    tickers_eod = e_eod.get_entire_eod_data('NYSE', datetime.datetime.fromisoformat('2022-02-22'))
    print(tickers_eod)
    assert isinstance(tickers_eod, list)
    assert isinstance(tickers_eod[0], dict)
    assert 'code' in tickers_eod[0]
    assert tickers_eod[0]['code'] == 'A'
