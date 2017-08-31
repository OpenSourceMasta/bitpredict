import urllib2
import time
import json
from pymongo import MongoClient
import sys
import gdax
from itertools import izip


SAMPLE_RATE = 5 #seconds

symbol = sys.argv[1]
public_client = gdax.PublicClient()

client = MongoClient()
db = client['bitmicro']
ltc_books = db[symbol[:3]+'_books']


def format_book(entry):
    '''
    Converts book data to float
    '''
    def format_entry(arr):
        d = {}
        d['price'] = float(arr[1])
        d['amount'] = float(arr[0])
        return d

    seq = entry.pop('sequence')
    
    formated  = {k: map(format_entry,v) for k, v in entry.items()}
    formated['_id'] = seq
    return formated


def get_json():
    '''
    Gets json from the API
    '''
    book = public_client.get_product_order_book(sys.argv[1], level=2)
    
    return format_book(book)


print 'Running...'
while True:
    start = time.time()
    try:
        book = get_json()
        print book
    except Exception as e:
        print e
        sys.exc_clear()
    else:
        book['_id'] = time.time()
        ltc_books.insert_one(book)
        time_delta = time.time()-start
        if time_delta < SAMPLE_RATE:
            print "gotta heckin sleep"
            time.sleep(SAMPLE_RATE-time_delta)
