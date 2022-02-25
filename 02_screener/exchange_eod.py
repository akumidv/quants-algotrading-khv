import pandas as pd
import requests
import os
import traceback

from requests import HTTPError

DEV_MODE = True if os.environ.get('PYTHON_MODE', None) == 'development' else False

API_KEY = os.environ['EOD_TOKEN']
API_URL = 'https://eodhistoricaldata.com/api'

EXCHANGE_END_TRADEDAY_UTC_HOUR = {'ASX': 7, 'NYSE': 24}
EXCHANGE_MIN_DAY_DATA = {'ASX': 1900, 'NYSE': 1000, 'CC': 500, 'US': 1000}

# import requests_cache
# requests_cache.install_cache('eod_cache', backend='sqlite', expire_after=1800, allowable_methods=('GET', 'POST'))
session = requests.Session()


def request_hanlder(url, params={}):
    if not url:
        raise ValueError('Url do not set for request API')

    params['api_token'] = API_KEY
    params['fmt'] = 'json'

    params_list = [f'{key}={params[key]}' for key in params.keys()]
    query_params = '&'.join(params_list)
    request_url = f'{url}?{query_params}'
    resp = session.get(request_url)
    resp.raise_for_status()
    data = resp.json()

    return data


def transfer_exch_code(exchange):
    if exchange == 'ASX':
        return 'AU'
    # elif exchange in ['NYSE', 'NASDAQ']:
    #     return 'US'
    return exchange


def filter_exchange(data, exchange):
    req_exch_code = transfer_exch_code(exchange)
    if req_exch_code == 'US':
        exch_data = [ticker for ticker in data if ticker and ticker.get('Exchange') and ticker['Exchange'].startswith(exchange)]
    else:
        exch_data = [ticker for ticker in data if ticker and ticker.get('Exchange') == req_exch_code]

    return exch_data


def get_exchange_info(exchange):
    if not exchange:
        raise ('Exchange code do not set')
    req_exch_code = transfer_exch_code(exchange)
    exch_info = request_hanlder(f'{API_URL}/exchange-symbol-list/{req_exch_code}')
    if exchange != 'US':
        exch_info = filter_exchange(exch_info, exchange)
    return exch_info


def get_all_tickers(exchange_info, ticker_type = None):
    if ticker_type:
        tickers = [ticker['Code'] for ticker in exchange_info if ticker['Type'] == ticker_type]
    else:
        tickers = [ticker['Code'] for ticker in exchange_info]
    return tickers


# https://eodhistoricaldata.com/financial-apis/bulk-api-eod-splits-dividends/
def get_entire_eod_data(exchange, dt=None):
    if not exchange:
        raise ValueError('Exchange code do not set')
    if not dt:
        raise ValueError('Do not set date')
    req_exch_code = transfer_exch_code(exchange)
    tickers_eod = request_hanlder(f'{API_URL}/eod-bulk-last-day/{req_exch_code}', {'date': dt}) # https://eodhistoricaldata.com/api/eod-bulk-last-day/US?fmt=json&api_token=
    return tickers_eod

