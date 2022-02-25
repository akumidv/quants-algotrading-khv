import requests
import os


TG_TOKEN = os.environ['TG_TOKEN']
DEV_CHAT_ID = os.environ['CHAT_ID']
ADMIN_ID = os.environ['CHAT_ID']
# ADMIN_CHANNEL_ID = f'-100{ADMIN_ID}'
ADMIN_CHANNEL_ID = ADMIN_ID
PRIVATE_ID = os.environ['CHAT_ID']
# CHANNEL_ID = f'-100{PRIVATE_ID}'
CHANNEL_ID = PRIVATE_ID


def prepare_screener_signals_list(tickers_signals, exchange, screener_name):
    msgTxt = ''
    for idx, ticker in enumerate(tickers_signals):
        if idx == 0:
            msgTxt += f'- {exchange} - {ticker: <5} - {screener_name}'
        else:
            msgTxt += f'\n- {exchange} - {ticker: <5} - {screener_name}'
    return msgTxt


def prepare_screener_signals_exch(tickers_signals, msgTxt):
    for full_ticker in tickers_signals:
        msgTxt += f'\n- [{full_ticker}](https://www.tradingview.com/chart/?symbol={full_ticker})'
    return msgTxt


def send_signals(tickers_signals, exchange, screener_name, last_date, run_mode=None):
    if not tickers_signals:
        return {'error': None, 'message': 'There is none of signals', 'data': {}}
    msgText = prepare_screener_signals_exch(tickers_signals, f'{last_date} *{exchange}* - {screener_name}:')
    if msgText:
        return send_telegram_message(msgText, CHANNEL_ID if run_mode is None else (DEV_CHAT_ID if run_mode else ADMIN_CHANNEL_ID))
    else:
        return {'error': None, 'message': 'There is none of signals', 'data': {}}


def send_eror(res_error):
    try:
        if 'message' in res_error:
            msgText = f'Code {res_error["error"]}: {res_error["message"]}'
        else:
            msgText = f'{res_error}'
        msgText.replace('[', '\[').replace('`', '\`').replace('_', '\_')
        msgText = f'```\n{msgText}\n```'
        return send_telegram_message(msgText, ADMIN_CHANNEL_ID, 'Markdown')
    except Exception as err:
        print('send_error', err)
        return res_error

def send_info_message(msgText):
    try:
        return send_telegram_message(msgText, ADMIN_CHANNEL_ID, 'Markdown')
    except Exception as err:
        print('send_info_error', err)
        return {'error': 1, 'message': f'Error occurred when send info message: {msgText}.\n{err}'}

def send_telegram_message(text, chat_id = ADMIN_CHANNEL_ID, parse_mode = 'Markdown'):
    HEADERS = {
        'Accept': 'application/json',
        'user-agent': 'Premium Stock Screener bot'
    }
    try:
        resp = requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                             data={'chat_id': chat_id, 'text': text, 'disable_web_page_preview': False, 'parse_mode': parse_mode}, # HTML  MarkdownV2 https://core.telegram.org/bots/api#formatting-options
                             headers=HEADERS,
                             timeout=(5, 15))
        repl = resp.json()
        if resp.status_code != 200 or not repl or not repl['ok']:
            return {'error': resp.status_code, 'message': f'Response code {resp.status_code}, text {resp.text}'}
        return {'error': None, 'message': 'Telegram send status: ok', 'data': repl}
    except Exception as err:
        return {'error': 1, f'message': 'Error request for telegram app: {err}'}

