import logging, sys

import concurrent.futures as cf
from requests_futures.sessions import FuturesSession

TEST_URL = 'https://crest-tq.eveonline.com/market/10000002/orders/all/?page='

logging.basicConfig(
    stream=sys.stderr, level=logging.INFO,
    format='%(relativeCreated)s %(message)s',
    )

session = FuturesSession(max_workers=20)
futures = []

logging.info('Sending requests to CREST API')
for n in range(10):
    try_url = TEST_URL + str(n)
    future = session.get(try_url)
    futures.append(future)

logging.info('Requests sent; awaiting responses')

for future in cf.as_completed(futures, timeout=15):
    res = future.result()
    print(future)
    if 'next' in res.json():
        print(res.json()['next'])

logging.info('Process complete')