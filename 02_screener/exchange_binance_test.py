import exchange_binance as eb
import datetime


def test_get_candle_data():
    tickers_data = eb.get_candle_data('BTCUSDT', datetime.datetime(year=2022, month=1, day=1), datetime.datetime(year=2022, month=2, day=1))
    assert isinstance(tickers_data, list)
    assert isinstance(tickers_data[0], dict)
    assert 'ticker' in tickers_data[0]
    assert tickers_data[0]['ticker'] == 'BTCUSDT'
    print(tickers_data)
    #  [{'ticker': 'BTCUSDT', 'date': datetime.date(2022, 1, 1), 'open': 46216.93, 'high': 47954.63, 'low': 46208.37, 'close': 47722.65, 'volume': 19604.46325, 'close_time': 1641081599999, 'quote_asset_volumne': 924155159.5834866, 'number_of_trades': 714899, 'taker_buy_base': 9942.36679, 'taker_buy_quote': 468738711.7901065, 'adjusted_close': 47722.65},


def test_get_exchange_info():
    exch_info = eb.get_exchange_info()
    print(exch_info)
    assert isinstance(exch_info, list)
    assert isinstance(exch_info[0], dict)
    assert 'symbol' in exch_info[0]
    assert exch_info[0]['symbol'] == 'BTCUSDT'
    # {'symbol': 'TLMBNB', 'status': 'TRADING', 'baseAsset': 'TLM', 'baseAssetPrecision': 8, 'quoteAsset': 'BNB', 'quotePrecision': 8, 'quoteAssetPrecision': 8, 'baseCommissionPrecision': 8, 'quoteCommissionPrecision': 8, 'orderTypes': ['LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT'], 'icebergAllowed': True, 'ocoAllowed': True, 'quoteOrderQtyMarketAllowed': True, 'isSpotTradingAllowed': True, 'isMarginTradingAllowed': False, 'filters': [{'filterType': 'PRICE_FILTER', 'minPrice': '0.00000010', 'maxPrice': '1000.00000000', 'tickSize': '0.00000010'}, {'filterType': 'PERCENT_PRICE', 'multiplierUp': '5', 'multiplierDown': '0.2', 'avgPriceMins': 5}, {'filterType': 'LOT_SIZE', 'minQty': '1.00000000', 'maxQty': '92141578.00000000', 'stepSize': '1.00000000'}, {'filterType': 'MIN_NOTIONAL', 'minNotional': '0.05000000', 'applyToMarket': True, 'avgPriceMins': 5}, {'filterType': 'ICEBERG_PARTS', 'limit': 10}, {'filterType': 'MARKET_LOT_SIZE', 'minQty': '0.00000000', 'maxQty': '92141578.00000000', 'stepSize': '0.00000000'}, {'filterType': 'MAX_NUM_ORDERS', 'maxNumOrders': 200}, {'filterType': 'MAX_NUM_ALGO_ORDERS', 'maxNumAlgoOrders': 5}], 'permissions': ['SPOT']}]


def test_get_all_tickers():
    exch_info = eb.get_exchange_info()
    exch_tickers = eb.get_all_tickers(exch_info)
    print(exch_tickers)
    assert isinstance(exch_tickers, list)
    assert isinstance(exch_tickers[0], str)

