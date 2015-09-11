import pandas as pd
import pymongo
import itertools
from sklearn.metrics import r2_score
from math import log

client = pymongo.MongoClient()
db = client['bitmicro']
predictions = db['btc_predictions']

cursor = predictions.find().limit(30000).sort('_id', pymongo.DESCENDING)
df = pd.DataFrame(list(cursor))
df = df[df.future_price != 0]
df['actual'] = (df.future_price/df.current_price).apply(log)
score = r2_score(df.actual.values, df.prediction.values)
print 'observations:', len(df)
print 'r^2:', score


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk