from concurrent.futures import ThreadPoolExecutor
from typing import Sequence, Iterable, Callable, Any
from threading import Thread

import asyncio
import logging

from worker import Worker


ProcessingCallback = Callable[['CrawlManager', str, Any], None]


class CrawlManager(object):
    name: str
    processing_callbacks: Sequence[ProcessingCallback]
    logger: logging.Logger

    def __init__(
            self,
            name: str,
            num_workers: int,
            tracker: 'ItemTracker',
            scraper: 'Scraper',
            saver: 'ItemSaver',
    ):
        self._logger = logging.getLogger(f'CrawlManager[{name}]')

        self.name = name
        self.num_workers = num_workers
        self.tracker = tracker
        self.tracker.crawl_manager = self
        self.saver = saver
        self.saver.crawl_manager = self
        self.scraper = scraper

    async def initialize_tracker_with_items(self, items: Iterable[str], explored: Iterable[str]):
        await self.tracker.async_init()
        await self.tracker.add_items(items)
        await self.tracker.mark_explored(explored)

    async def create_worker(self):
        worker_id = await self.tracker.get_worker_id()
        return Worker(self, worker_id)

    async def has_work(self):
        return await self.tracker.has_work()

    async def checkout_work(self, worker_id, n=1000):
        """
        Attempt to atomically check out `n` items to the given `worker_id`.

        NOTE: This may not return exactly `n` items back to the worker.
        """
        return await self.tracker.checkout_work(worker_id, n)

    async def mark_work_finished(self, worker_id, work):
        return await self.tracker.mark_work_finished(worker_id, work)

    async def add_new_items(self, items: Iterable[str]):
        return await self.tracker.add_items(items)

    async def save(self, item, response):
        return await self.saver.save(item, response)

    async def crawl_done(self):
        return await self.tracker.crawl_done()

    async def crawl(self):
        workers = [await self.create_worker() for _ in range(self.num_workers)]

        await asyncio.gather(*[worker.run() for worker in workers])
        self._logger.info("Crawl done!")

        await self.tracker.shutdown()
        await self.saver.close()

