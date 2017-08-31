import urllib2
import time
import json
from pymongo import MongoClient
import sys
import dateutil.parser

import gdax

SAMPLE_RATE=5

symbol = sys.argv[1]
public_client = gdax.PublicClient()


client = MongoClient()
db = client['bitmicro']
ltc_trades = db[symbol[:3]+'_trades']


def format_trade(trade):
    '''
    Formats trade data
    '''
    formated = {}
    formated['_id'] = long(trade['trade_id'])
    formated['amount'] = float(trade['size'])
    formated['price'] = float(trade['price'])
    formated['timestamp'] = dateutil.parser.parse(trade['time']).strftime('%s')
    formated['side'] = trade['side']
    return formated


def get_json():
    '''
    Gets json from the API
    '''
    trades = public_client.get_product_trades(symbol)
    return map(format_trade, trades)
    

print 'Running...'
last_timestamp = 0
while True:
    start = time.time()
    try:
        trades = get_json()
    except Exception as e:
        print e
        sys.exc_clear()
    else:
        print trades
        for trade in trades:
            ltc_trades.update_one({'_id': trade['_id']},{'$setOnInsert': trade}, upsert=True)
        
        time_delta = time.time()-start
        if time_delta < SAMPLE_RATE:
            time.sleep(SAMPLE_RATE-time_delta)
