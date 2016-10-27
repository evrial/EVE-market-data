# latest SDE in csv format from https://www.fuzzwork.co.uk/dump/latest/

import csv
import time
import logging

import pandas as pd
import requests
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession
from tqdm import tqdm

logging.basicConfig(
    filename='gethistory.log', level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
    )


def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
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
                    order['date'],
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

@RateLimited(150)
def getData(requestsConnection,typeid,regionid):
    url='https://crest-tq.eveonline.com/market/{}/history/?type=https://crest-tq.eveonline.com/inventory/types/{}/'.format(regionid,typeid)
    future=requestsConnection.get(url)
    future.typeid=typeid
    future.regionid=regionid
    return future


if __name__ == '__main__':
    df = pd.read_csv('invTypes.csv')
    baseitemids = df[(df.published == 1) & (df.marketGroupID != 'None')].typeID
    regionids = pd.read_csv('mapRegions.csv').regionID

    session = FuturesSession(max_workers=10)
    session.headers.update({'UserAgent':'Fuzzwork Market Monitor'});

    with open('history.csv', 'wb') as csvfile:
        csvwriter = csv.writer(csvfile, dialect='excel')
        csvwriter.writerow(['typeID', 'regionID', 'date', 'lowPrice', 'highPrice', 'avgPrice', 'volume', 'orderCount'])

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

