# CREST API in maintenance mode
# latest SDE in csv format from https://www.fuzzwork.co.uk/dump/latest/

import csv
import gzip
import time
import logging
import pandas
import requests
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession
from tqdm import tqdm


logging.basicConfig(
    filename='gethistory.log', level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
    )

def rate_limited(period, damping = 1.0):
    '''
    https://github.com/tomasbasham/ratelimit
    Prevent a method from being called
    if it was previously called before
    a time widows has elapsed.
    :param period: The time window after which method invocations can continue.
    :param damping: A factor by which to dampen the time window.
    :return function: Decorated function that will forward method invocations
    if the time window has elapsed.
    '''
    frequency = damping / float(period)
    def decorate(func):
        last_called = [0.0]
        def func_wrapper(*args, **kargs):
            elapsed = time.clock() - last_called[0]
            left_to_wait = frequency - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            last_called[0] = time.clock()
            return func(*args, **kargs)
        return func_wrapper
    return decorate

def processData(result,csvwriter):
    try:
        resp=result.result()
        if resp.status_code==200:
            orders=resp.json()
            for order in orders['items']:
                csvwriter.writerow([
                    result.typeid,
                    result.regionid,
                    order['date']+'Z',
                    order['lowPrice'],
                    order['highPrice'],
                    order['avgPrice'],
                    order['volume'],
                    order['orderCount']
                ])
            return True
        else:
            logging.warn("None 200 status. {} Returned: {}".format(result.typeid,resp.status_code))
            return False
    except requests.exceptions.ConnectionError as e:
        logging.warn(e)
        return False

@rate_limited(150)
def getData(requestsConnection,typeid,regionid):
    url='https://crest-tq.eveonline.com/market/{}/history/?type=https://crest-tq.eveonline.com/inventory/types/{}/'.format(regionid,typeid)
    future=requestsConnection.get(url)
    future.typeid=typeid
    future.regionid=regionid
    return future


if __name__ == '__main__':
    df = pandas.read_csv('invTypes.csv.bz2')
    baseitemids = df[(df.published == 1) & (df.marketGroupID != 'None')].typeID
    regionids = pandas.read_csv('mapRegions.csv.bz2').regionID

    session = FuturesSession(max_workers=70)
    session.headers.update({'UserAgent':'Fuzzwork Market Monitor'});

    with gzip.open('history-latest.csv.gz', 'w') as f:
        csvwriter = csv.writer(f, dialect='excel')
        csvwriter.writerow(['type_id', 'region_id', 'date', 'lowest', 'highest', 'average', 'volume', 'order_count'])

        for region in regionids:
            futures = []
            retryfutures = []

            for typeid in tqdm(baseitemids,desc="downloading region {}".format(region),leave=True):
                futures.append(getData(session,typeid,region))

            for result in tqdm(cf.as_completed(futures),desc='Processing',total=len(baseitemids),leave=True):
                status=processData(result,csvwriter)
                if not status:
                    retryfutures.append(getData(session,result.typeid,result.regionid))
                    logging.warn("adding {} to retry in {}".format(result.typeid,result.regionid))

            if retryfutures:
                for result in tqdm(cf.as_completed(retryfutures),desc='Processing Failures',total=len(retryfutures),leave=True):
                    status=processData(result,csvwriter)
                    if not status:
                        logging.warn("{} failed in {}".format(result.typeid,result.regionid))

