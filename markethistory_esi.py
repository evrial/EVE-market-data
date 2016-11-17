"""
Download invtypes and mapregions table from latest SDE in csv format
https://www.fuzzwork.co.uk/dump/latest/
https://developers.eveonline.com/resource/resources
https://esi.tech.ccp.is/
"""
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

def get_data(session, type_id, region_id):
    url = 'https://esi.tech.ccp.is/latest/markets/{}/history/'.format(region_id)
    payload = {'datasource': 'tranquility', 'type_id': type_id}
    future = session.get(url, params=payload, timeout=30)
    future.type_id = type_id
    future.region_id = region_id
    return future

def process_data(result):
    try:
        r = result.result()
        r.raise_for_status()
        if r.status_code == requests.codes.ok:
            for day in r.json():
                csvwriter.writerow([
                    result.type_id,
                    result.region_id,
                    day['date'],
                    day['lowest'],
                    day['highest'],
                    day['average'],
                    day['volume'],
                    day['order_count']
                ])
            return True
    except Exception as e:
        logging.warn(e)
        return False


if __name__ == '__main__':
    session = FuturesSession(max_workers=50)
    session.headers.update({'UserAgent': 'Fuzzwork Market Monitor'});

    with gzip.open('latest-history.csv.gz', 'w') as f:
        csvwriter = csv.writer(f, dialect='excel')
        csvwriter.writerow(['type_id', 'region_id', 'date', 'lowest', 'highest', 'average', 'volume', 'order_count'])

        regions = pandas.read_csv('mapRegions.csv.bz2', usecols=['regionID'])
        types = pandas.read_csv('invTypes.csv.bz2', usecols=['typeID', 'published', 'marketGroupID'])
        baseitems = types[(types.published == 1) & (types.marketGroupID != 'None')]

        for region_id in regions.regionID:
            futures = []
            retryfutures = []

            for type_id in tqdm(baseitems.typeID, desc="Downloading region {}".format(region_id),leave=True):
                futures.append(get_data(session, type_id, region_id))

            for result in tqdm(cf.as_completed(futures),desc='Processing',total=len(baseitems),leave=True):
                status = process_data(result)
                if not status:
                    retryfutures.append(get_data(session, result.type_id, result.region_id))
                    logging.warn("adding {} to retry in {}".format(result.type_id,result.region_id))

            if retryfutures:
                for result in tqdm(cf.as_completed(retryfutures),desc='Processing Failures',total=len(retryfutures),leave=True):
                    status = process_data(result)
                    if not status:
                        logging.error("{} failed in {}".format(result.type_id,result.region_id))
