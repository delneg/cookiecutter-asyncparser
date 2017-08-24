import aiohttp
import asyncio
import aiosocks
from aiosocks.connector import ProxyConnector, ProxyClientRequest
from bs4 import BeautifulSoup
import logging
from logging.handlers import RotatingFileHandler
import sys
from constants import *
from utilities import user_agent
from bucket import AsyncLeakyBucket

logger = logging.getLogger()

log_format = logging.Formatter(LOGGER_FORMAT)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(log_format)
logger.addHandler(ch)

fh = RotatingFileHandler(LOGFILE_NAME, maxBytes=LOGFILE_MAX_BYTES, backupCount=LOGFILE_BACKUP_COUNT)
fh.setFormatter(log_format)
logger.addHandler(fh)
# Bucket - limits REQUESTS/PER_TIME, i.e. 5 requests per 10 seconds (implements rate-limiting)
bucket = AsyncLeakyBucket(REQUESTS, PER_TIME)


async def process(data):
    # process received HTML
    soup = BeautifulSoup(data, 'html.parser')
    title = soup.title.text
    return title


async def fetch(session, some_data):
    async with bucket:
        url = 'https://example.com/' + some_data
        try:
            async with session.get(url,
                                   # proxy='socks5://127.0.0.1:9050', - allows you to use http or https or socks proxies
                                   headers=user_agent(), # - change your user agent to one of the most popular
                                   ) as response:
                text = await response.text()
                processed = await process(text)
                if processed:
                    print(f"{some_data} - {processed}")
                    # do some more stuff over the processed data
        except Exception as e:
            logger.error(f"Exception for {some_data} - {str(e)}")


async def main():
    tasks = []
    conn = ProxyConnector(remote_resolve=True)

    async with aiohttp.ClientSession(connector=conn, request_class=ProxyClientRequest) as session:
        for i in range(1, 5):
            task = asyncio.ensure_future(fetch(session, str(i)))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
