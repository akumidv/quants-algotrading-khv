import datetime
from datetime import timedelta

import pandas as pd
import json
from typing import Union
from google.cloud import firestore

import exchange_eod as exch_eod
import exchange_binance as exch_binance
from exchange_binance import BINANCE_COLUMNS
import store as st



def get_exchange_eod(exchange_code: str, days_delta: int, req_iso_date: Union[None, str] = None):
    if exchange_code in ['NYSE', 'ASX']:
        return get_exchange_eod_history(exchange_code, days_delta, req_iso_date)
    if exchange_code == 'BINANCE':
        return get_candle_eod_data(days_delta, req_iso_date)
    return None


def get_collection_name_eod(exchange):
    return f'exchange_{exchange}_EOD'


def get_candle_eod_data(exchange, days_delta, req_date=None):
    if exchange.upper() == 'BINANCE':
        exch_api = exch_binance
    else:
        raise ValueError('Unknown exchange')
    if days_delta > 1000:
        ValueError('Do not impilemented requests more than 1000 candles')
    exch_info = exch_api.get_exchange_info()
    exch_tickers = exch_api.get_all_tickers(exch_info)
    if req_date is not None:
        end_datetime = datetime.datetime.fromisoformat(req_date + 'T00:00:00+00:00')
    else:
        end_datetime = datetime.datetime.utcnow() - timedelta(days=1)
    end_datetime = end_datetime.replace(tzinfo=datetime.timezone.utc)
    end_datetime = end_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_datetime = end_datetime - timedelta(days=days_delta - 1)
    end_datetime = end_datetime - timedelta(microseconds=1)
    start_datetime = start_datetime.replace(tzinfo=datetime.timezone.utc)
    start_datetime = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f'[INFO] Get {exchange} CANDLE data from {start_datetime} til {end_datetime} when request data is {req_date}')
    all_tickers_data = []
    requests_api = 0
    for idx, ticker in enumerate(exch_tickers):
        requests_api += 1
        tickers_eod = exch_api.get_candle_data(ticker, start_datetime, end_datetime)
        all_tickers_data.extend(tickers_eod)

    df = pd.DataFrame(all_tickers_data,
                      columns=['ticker'].extend(BINANCE_COLUMNS))

    stats = {'datetime': f'{end_datetime}', 'req_api': requests_api, 'req_storage': 0,
             'tickers': 0 if not len(all_tickers_data) else len(pd.unique(df['ticker'])),
             'tf': 0 if not len(all_tickers_data) else df.groupby('ticker', sort=False).size().max(),
             'records': len(all_tickers_data)}
    print(stats)
    del all_tickers_data
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    df['exchange'] = exchange
    df.sort_values(by=['ticker', 'date'], ascending=True, ignore_index=True, inplace=True)

    return df


def get_exchange_eod_history(exchange_code, days_delta, req_date=None):
    if not exchange_code:
        raise ValueError('Do not set exchange value')
    elif not days_delta:
        raise ValueError('Do not set period')

    exch_info = exch_eod.get_exchange_info(exchange_code)
    exch_tickers = exch_eod.get_all_tickers(exch_info)

    if req_date is not None:
        start_date = datetime.date.fromisoformat(req_date)
    else:
        start_date = datetime.date.today() if exchange_code != 'CC' else datetime.date.today() - timedelta(days=1)


    print(f'[INFO] Get {exchange_code} EOD for date {start_date} when request data is {req_date}, for exchange tickers {len(exch_tickers)}')
    days_with_data = 0
    requests_api = 1  # 1 request is getting data for exchange tickers
    requests_storage = 0
    delta = 0
    all_tickers_data = []

    docs = st.store.collection(get_collection_name_eod(exchange_code)).order_by('date', direction=firestore.Query.DESCENDING).stream()
    try:
        cur_doc = next(docs)
        while cur_doc is not None and cur_doc.id > f'{start_date}':  # Search current date in storage
            cur_doc = next(docs)
    except Exception as err:
        cur_doc = None

    while days_with_data < days_delta and delta < (days_delta * 2):
        request_date = start_date - timedelta(days=delta)
        tickers_eod = []
        try:
            while cur_doc is not None and cur_doc.id > f'{request_date}':  # Search next date in storage
                cur_doc = next(docs)
        except:
            cur_doc = None
        if cur_doc is not None and cur_doc.id == f'{request_date}':
            requests_storage += 1
            tickers_store_str = cur_doc.get('tickers_data')
            if tickers_store_str is None:
                tickers_store_str = '[]'
            label_seq = cur_doc.get('tickers_data_label_seq')
            if label_seq is None:
                label_seq = st.TICKERS_DATA_LABEL_SEQ
            tickers_store = json.loads(tickers_store_str)
            del tickers_store_str
            for ticker_data_arr in tickers_store:  # Unpack values from array
                ticker_data = st.unpack_ticker_val(ticker_data_arr, label_seq)
                if ticker_data:
                    ticker_data['date'] = request_date
                    tickers_eod.append(ticker_data)
            del tickers_store
        else:
            requests_api += 1
            tickers_api_eod = get_api_eod_date(exchange_code, request_date, exch_tickers=exch_tickers,
                                               skip_if_empty=True if delta == 0 else False)  # skip save data for request day if data absent

            if delta == 0 and req_date is None and (not tickers_api_eod or len(tickers_api_eod) < exch_eod.EXCHANGE_MIN_DAY_DATA.get(exchange_code, 100)):
                break
            for ticker_api_data in tickers_api_eod:
                if ticker_api_data.get('code'):
                    ticker_data = {}
                    for key in ticker_api_data.keys():
                        if key in st.TICKERS_DATA_LABEL_SEQ:
                            ticker_data[key] = ticker_api_data[key]
                    if ticker_data:
                        ticker_data['date'] = request_date
                        ticker_data['ticker'] = ticker_api_data['code']
                        tickers_eod.append(ticker_data)
            del tickers_api_eod
        if tickers_eod and len(tickers_eod) >= exch_eod.EXCHANGE_MIN_DAY_DATA.get(exchange_code, 100):  # in some days volumes is 0, for example ASX 2021-01-26
            days_with_data += 1
            all_tickers_data.extend(tickers_eod)
        delta += 1
        if delta % 10 == 0:
            print(f'[INFO] days requested {delta}, days with data {days_with_data + 1 if tickers_eod else days_with_data}/{days_delta}, req_api {requests_api}, req_storage {requests_storage}')
    df = pd.DataFrame(all_tickers_data,
                      columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'adjusted_close'])
    stats = {'datetime': f'{start_date}', 'req_api': requests_api, 'req_storage': 0,
             'tickers': 0 if not len(all_tickers_data) else len(pd.unique(df['ticker'])),
             'tf': 0 if not len(all_tickers_data) else df.groupby('ticker', sort=False).size().max(),
             'records': len(all_tickers_data)}
    print(stats)
    del all_tickers_data
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    df['exchange'] = exchange_code
    df.sort_values(by=['ticker', 'date'], ascending=True, ignore_index=True, inplace=True)
    return df


def get_api_eod_date(exchange, request_date, exch_tickers=None, skip_if_empty=False):
    request_date_iso = f'{request_date}'
    tickers_eod = exch_eod.get_entire_eod_data(exchange, request_date_iso)
    exch_ref = st.store.collection(get_collection_name_eod(exchange))
    ticker_list = []
    eod_data = []
    if tickers_eod:
        for ticker_data in tickers_eod:
            ticker_code = ticker_data.get('code')
            if ticker_code and (exch_tickers is None or ticker_code in exch_tickers) and ticker_data.get(
                    'date') == request_date_iso:
                ticker_data = st.extend_adjusted_data(ticker_data)
                ticker_list.append(ticker_code)
                ticker_data['ticker'] = ticker_code
                ticker_data_arr = st.pack_ticker_val(ticker_data)
                eod_data.append(ticker_data_arr)
    dt = datetime.datetime.combine(request_date, datetime.datetime.min.time())
    eod_data_str = json.dumps(
        eod_data, separators=(',', ':'))  # more faster to load data 0.02 instead of 0.1 and avoid exception: Error when process: 400 too many index entries for entity firestore when save tickers_data as object with tickers as keys.
    if eod_data or not skip_if_empty:
        exch_ref.document(request_date_iso).set({'date': request_date_iso, 'datetime': dt, 'exchange': exchange,
                                                 'tickers_data_label_seq': st.TICKERS_DATA_LABEL_SEQ,
                                                 'tickers_number': len(ticker_list),
                                                 'tickers_list': ticker_list,
                                                 'tickers_data': eod_data_str})
    del eod_data
    del eod_data_str
    return tickers_eod
