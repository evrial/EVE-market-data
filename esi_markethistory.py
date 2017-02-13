"""
https://esi.tech.ccp.is/
https://developers.eveonline.com/resource/resources
Download CSV tables from the latest Static Data Export:
https://www.fuzzwork.co.uk/dump/latest/
https://stackoverflow.com/questions/40417503/applying-retry-on-grequests-in-python
"""
import csv
import gzip
import logging
import pandas as pd
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging
from tqdm import tqdm


logger = logging.getLogger(__name__)
handler = SentryHandler('https://d4e61a4be1164e709412a0e826daa3d8:a37e2bc8875349dbbc801d69a976197d@sentry.io/136766')
setup_logging(handler)

def get_data(session, type_id, region_id):
    url = 'https://esi.tech.ccp.is/latest/markets/{region_id}/history/'.format(
        region_id=region_id)
    params = {'type_id': type_id, 'datasource': 'tranquility'}
    future = session.get(url, params=params, timeout=30)
    future.type_id = type_id
    future.region_id = region_id
    return future

def process_data(result):
    try:
        r = result.result()
        r.raise_for_status()
        assert r.status_code == 200
        for day in r.json():
            day['type_id'] = result.type_id
            day['region_id'] = result.region_id
            writer.writerow(day)
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    session = FuturesSession(max_workers=50)
    session.headers.update({'UserAgent': 'Fuzzwork Market Monitor'})

    retries = Retry(total=50, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504],
        raise_on_redirect=True, raise_on_status=True)
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_maxsize=50))
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_maxsize=50))

    regions = pd.read_csv('mapRegions.csv.bz2', usecols=['regionID'])
    types = pd.read_csv('invTypes.csv.bz2', usecols=['typeID', 'published', 'marketGroupID'])
    types = types[(types.published == 1) & (types.marketGroupID != 'None')]

    with gzip.open('history-latest.csv.gz', 'w') as csvfile:
        fieldnames = ['type_id', 'region_id', 'date', 'lowest', 'highest',
                      'average', 'volume', 'order_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for region_id in regions['regionID']:
            futures = (get_data(session, type_id, region_id) for type_id in types['typeID'])

            for result in tqdm(as_completed(futures),
                    desc='Downloading region {}'.format(region_id),
                    total=len(types), leave=True):
                process_data(result)
