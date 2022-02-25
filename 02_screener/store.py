from google.cloud import firestore
from google.oauth2.service_account import Credentials
import os
import json

GOOGLE_APPLICATION_CREDENTIALS = os.path.dirname(__file__) + '/gcloud-key.json'
with open(GOOGLE_APPLICATION_CREDENTIALS, 'r') as file:
    gcloud_key = json.load(file)
    GOOGLE_PROJECT = gcloud_key['project_id']

creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
store = firestore.Client(credentials=creds, project=GOOGLE_PROJECT)

TICKERS_DATA_LABEL_SEQ = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'adjusted_open', 'adjusted_high', 'adjusted_low', 'adjusted_close']

def pack_ticker_val(tickers_data):
    tickers_data_arr = [tickers_data.get(label) for label in TICKERS_DATA_LABEL_SEQ]
    return tickers_data_arr


def unpack_ticker_val(tickers_data_arr, label_seq=TICKERS_DATA_LABEL_SEQ):
    tickers_data = {}
    for idx, label in enumerate(label_seq):
        if idx < len(tickers_data_arr):
            tickers_data[label] = tickers_data_arr[idx]
    return tickers_data


def extend_adjusted_data(ticker_data):
    if 'adjusted_close' not in ticker_data.keys():
        return ticker_data
    diff = ticker_data['close'] - ticker_data['adjusted_close']
    if not ticker_data.get('adjusted_open'):
        ticker_data['adjusted_open'] = ticker_data['close'] - diff
    if not ticker_data.get('adjusted_high'):
        ticker_data['adjusted_high'] = ticker_data['high'] - diff
    if not ticker_data.get('adjusted_low'):
        ticker_data['adjusted_low'] = ticker_data['low'] - diff
    return ticker_data
