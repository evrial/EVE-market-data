"""
https://esi.tech.ccp.is/
https://developers.eveonline.com/resource/resources
Download 2 CSV tables from the latest Static Data Export:
https://www.fuzzwork.co.uk/dump/latest/mapRegions.csv.bz2
https://www.fuzzwork.co.uk/dump/latest/invTypes.csv.bz2
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
from tqdm import tnrange, tqdm_notebook


logger = logging.getLogger(__name__)
handler = SentryHandler('https://d4e61a4be1164e709412a0e826daa3d8:a37e2bc8875349dbbc801d69a976197d@sentry.io/136766')
setup_logging(handler)

def get_data(session, type_id, region_id):
    url = 'https://esi.tech.ccp.is/latest/markets/{}/history/'.format(region_id)
    params = {'type_id': type_id, 'datasource': 'tranquility'}
    future = session.get(url, params=params, timeout=30)
    future.type_id = type_id
    future.region_id = region_id
    return future

def process_data(resp):
    try:
        r = resp.result()
        r.raise_for_status()
        assert r.status_code == 200
        for day in r.json():
            day.update({'type_id': resp.type_id, 'region_id': resp.region_id})
            writer.writerow(day)
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    session = FuturesSession(max_workers=50)
    session.headers.update({'UserAgent': 'Fuzzwork Market Monitor'})
    # https://stackoverflow.com/questions/40417503/applying-retry-on-grequests-in-python
    retries = Retry(total=50, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504],
        raise_on_redirect=True, raise_on_status=True)
    session.mount('http://', HTTPAdapter(max_retries=retries, pool_maxsize=50))
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_maxsize=50))

    regions = pd.read_csv('data/mapRegions.csv.bz2')
    types = pd.read_csv('data/invTypes.csv.bz2', usecols=['typeID', 'published', 'marketGroupID'])
    types = types.query('published == 1 and marketGroupID != "None"')

    with gzip.open('data/history-latest.csv.gz', 'wt') as csvfile:
        fieldnames = ['type_id', 'region_id', 'date', 'lowest', 'highest',
                      'average', 'volume', 'order_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _, row in tqdm_notebook(regions.iterrows(), total=len(regions), desc='Total'):
            futures = (get_data(session, type_id, row['regionID'])
                for type_id in types['typeID'])

            for resp in tqdm_notebook(as_completed(futures), total=len(types), leave=False,
                    unit='req', desc='{}'.format(row['regionName'])):
                process_data(resp)
