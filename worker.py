import aiohttp
import asyncio
import logging


class Worker(object):
    worker_id: int
    crawl_manager: 'CrawlManager'
    _logger: logging.Logger

    def __init__(self, crawl_manager: 'CrawlManager', worker_id: int):
        self.crawl_manager = crawl_manager
        self.worker_id = worker_id
        self._logger = logging.getLogger(self.__class__.__name__)

    async def process_work(self, work):
        item_tasks = []
        responses = []

        async with aiohttp.ClientSession() as session:
            for item in work:
                # Fetching is the bottleneck. Once we have a result,
                # start postprocessing immediately, and continue to the
                # next item
                self._logger.debug(f"Downloading {item}")

                try:
                    result = await self.crawl_manager.scraper.download(session, item)
                    await result.text()
                    responses.append(result)
                except aiohttp.ClientConnectionError as e:
                    self._logger.info(f"Exception while downloading {item}: {e}")
                    # TODO: report failure
                    continue

                if result.status != 200:
                    self._logger.info(f"Unexpected status code while downloading {item}: {result.status}")
                    continue

                for func in self.crawl_manager.scraper.processing_callbacks:
                    item_tasks.append(asyncio.ensure_future(func(self.crawl_manager, item, result)))

            try:
                await asyncio.gather(*item_tasks)
            except Exception as e:
                self._logger.info(f"Exception while performing postprocessing tasks: {e}")

            for resp in responses:
                await resp.release()


    async def run(self):
        while not await self.crawl_manager.crawl_done():
            work = await self.crawl_manager.checkout_work(self.worker_id)

            if not work:
                # We didn't get anything, but the crawl isn't done.
                # Other workers are probably still running, which could
                # result in new items being added soon. Try again in a bit
                await asyncio.sleep(30)
                continue

            await self.process_work(work)

            await self.crawl_manager.mark_work_finished(self.worker_id, work)

