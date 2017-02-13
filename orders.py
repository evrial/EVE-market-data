import logging
import sys
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession

logging.basicConfig(
    stream=sys.stderr, level=logging.INFO,
    format='%(relativeCreated)s %(message)s')

route_url = 'https://esi.tech.ccp.is/latest/markets/{region_id}/orders/'
region_id = 10000002

session = FuturesSession(max_workers=20)
futures = []

total = 0

logging.info('Sending requests to ESI API')
for n in range(1, 35):
    future = session.get(
        route_url.format(region_id=region_id),
        params={'order_type': 'all', 'datasource': 'tranquility', 'page': n})
    futures.append(future)

logging.info('Requests sent; awaiting responses')

for future in as_completed(futures, timeout=30):
    res = future.result()
    print(future)
    print(len(res.json()))
    total += len(res.json())

logging.info('Process complete')
print('Total {} orders'.format(total))
