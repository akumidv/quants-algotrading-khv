import requests
import os
import traceback
import datetime

from requests import HTTPError

DEV_MODE = True if os.environ.get('PYTHON_MODE', None) == 'development' else False

API_KEY = ''
req_headers = {'X-MBX-APIKEY': os.environ.get('BINANCE_API_KEY')} if os.environ.get('BINANCE_API_KEY') else {'X-MBX-APIKEY': API_KEY} if API_KEY else {}
API_URL = 'https://api.binance.com/api/v3'
WEIGHT_LIMIT = 1200  # REQUEST_WEIGHT  http://api.binance.com/api/v3/exchangeInfo

BINANCE_COLUMNS = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volumne', 'number_of_trades', 'taker_buy_base', 'taker_buy_quote']
BINANCE_API_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volumne', 'number_of_trades', 'taker_buy_base', 'taker_buy_quote']

# import requests_cache
# requests_cache.install_cache('eod_cache', backend='sqlite', expire_after=1800, allowable_methods=('GET', 'POST'))
session = requests.Session()


def request_hanlder(url, params={}):
    if not url:
        raise ValueError('Url do not set for request API')
    params_list = [f'{key}={params[key]}' for key in params.keys()]
    query_params = '&'.join(params_list)
    request_url = f'{url}?{query_params}' if query_params else url
    resp = session.get(request_url)
    resp.raise_for_status()
    data = resp.json()
    if int(resp.headers.get('x-mbx-used-weight', '0')) > WEIGHT_LIMIT - 200 or int(resp.headers.get('x-mbx-used-weight-1m', '0')) > WEIGHT_LIMIT - 200:
            raise f"Error. Near to binance weight limits: x-mbx-used-weight {resp.headers.get('x-mbx-used-weight')}, used-weight-1m {resp.headers.get('x-mbx-used-weight-1m')}"

    return data


def get_exchange_info():
    exch_info = request_hanlder(f'{API_URL}/exchangeInfo')
    return exch_info


def get_all_tickers(exch_info, ticker_pair='USDT'):
    if ticker_pair:
        tickers = [ticker['symbol'] for ticker in exch_info['symbols'] if ticker['symbol'].endswith(ticker_pair)]
    else:
        tickers = [ticker['symbol'] for ticker in exch_info['symbols']]
    return tickers


# Candles data structure https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
def get_candle_data(symbol, start_dt, end_dt, limit = 1000, timeframe='1d'):
    if not start_dt:
        raise ValueError('Do not set date')

    start_datetime_ts = int(start_dt.timestamp()) * 1000
    end_datetime_ts = int(end_dt.timestamp()) * 1000
    res = request_hanlder(f'{API_URL}/klines', {'symbol': symbol, 'startTime': start_datetime_ts, "endTime": end_datetime_ts, "interval": timeframe, "limit": limit})
    tickers_data = []
    for data_item in res:
        ticker_data = {}
        ticker_data['ticker'] = symbol
        for idx, key in enumerate(BINANCE_API_COLUMNS):
            ticker_data[key] = int(data_item[idx]) if key in ['date', 'close_time', 'number_of_trades'] else float(data_item[idx])
        if timeframe == '1d':
            ticker_data['date'] = datetime.date.fromtimestamp(int(ticker_data['date']/1000))
        else:
            ticker_data['datetime'] = ticker_data['date']
        ticker_data['adjusted_close'] = ticker_data['close']
        tickers_data.append(ticker_data)
    return tickers_data




