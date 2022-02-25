import os
import traceback
import pandas as pd
import numpy as np
import datetime
import model as md
import interface as ui
from screeners import scrn_sma_cross


SCREENERS_DEF = {
    'Cross SMA': {
        'func': scrn_sma_cross,
        'params': {
            'ma_long': 200,
            'ma_middle': 50
        }
    }
}


INTERVAL = 202

def error_handler(res_error):
    print('[ERROR]', res_error)
    tb = traceback.format_exc()
    print(tb)
    res_send = ui.send_eror(res_error)
    if res_send.get('error', True) is not None:
        print('[ERROR] Send message error:\n', res_send)
    return res_error


def service_info_handler(res):
    res_send = ui.send_info_message(res['message'])
    if res_send.get('error', True) is not None:
        print('[ERROR] Send message error:\n', res_send)
    return res


def process(exchange='NYSE', req_datetime=None):
    df = md.get_exchange_eod(exchange, INTERVAL, req_datetime)

    if df is None or df.empty:
        msg = f"There is no data at the end of the day for the *{exchange}* exchange. Skipped signal calculation."
        res_send = ui.send_info_message(msg)
        return {'error': None, 'message': msg}
    screeners_res = {}
    data_length = len(df)
    df['full_ticker'] = df['exchange'] + ':' + df["ticker"]
    uniq_tickers = pd.unique(df["ticker"])
    max_date = df["date"].unique().max()
    last_datetime = np.datetime_as_string(max_date, unit='D')

    for screener_code in SCREENERS_DEF.keys():
        print(
            f"[INFO] Exchange '{exchange}', request date {last_datetime}, screener '{screener_code}', tickers {len(uniq_tickers)}")
        res_signals = SCREENERS_DEF[screener_code]['func'](df, SCREENERS_DEF[screener_code]['params'])
        is_signals = False
        ticker_signals = {}
        for signal_name in res_signals.keys():
            res_tickers = res_signals[signal_name]
            if res_tickers:
                is_signals = True
                if len(res_signals.keys()) == 1:
                    screener_name = screener_code
                else:
                    screener_name = f"{screener_code}: {signal_name}"
                res_tickers.sort()
                # ticker_signals[screener_name] = res_tickers
                screeners_res[screener_name] = res_tickers
                ui.send_signals(res_tickers, exchange, screener_name, req_datetime)
        if not is_signals:
            service_info_handler({'error': None,
                                  'message': f"{last_datetime} Exchange *{exchange}*, hasn't signals for screener '{screener_code}',\n"
                                             f"Eod data length: {data_length}, signals len: {len(res_tickers)}\n"})
            continue

    return {'error': None, 'message': f'Signals: {screeners_res}'}



def run(request):
    """Responds to any HTTP request.
        Args:
            request (flask.Request): HTTP request object.
        Returns:
            The response text or any set of values that can be turned into a
            Response object using
            `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
        """

    try:
        request_json = request.get_json(force=True)
        if not request_json or request_json.get('exchange') is None or isinstance(request_json['exchange'], str):
            return error_handler(
                {'error': 1,
                 'message': f'Error request, do not have exchanges in request json: {request_json}'})
        res = process(request_json['exchange'], req_datetime= request_json.get('datetime', None))
    except Exception as err:
        print('[ERROR] ', err)
        tb = traceback.format_exc()
        print(tb)
        return error_handler({'error': 3, 'message': f'There server-side exception error'})
    return res

