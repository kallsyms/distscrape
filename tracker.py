from typing import Iterable, Set, Mapping

import aioredis
import asyncio
import collections
import logging

from util import grouper


class ItemTracker(object):
    _logger: logging.Logger
    crawl_manager: 'CrawlManager'

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        # self.crawl_manager is set by the CrawlManager itself when it is initialized

    async def async_init(self):
        """
        Perform any late initialization asynchronously
        """
        raise NotImplementedError()

    async def add_items(self, items: Iterable[str]):
        """
        Add `items` to the set of all known items, and add any items
        not previously known to the set of unexplored items.
        """
        raise NotImplementedError()

    async def mark_explored(self, items: Iterable[str]):
        """
        Mark `items` as explored.
        """
        raise NotImplementedError()

    async def get_worker_id(self):
        """
        Create a new, unique worker id used for tracking what each
        worker is currently assigned.
        """
        raise NotImplementedError()

    async def crawl_done(self) -> bool:
        """
        Determine if the crawl is entirely finished.
        """
        raise NotImplementedError()

    async def checkout_work(self, worker_id: int, n: int) -> Set[str]:
        """
        Checkout approximately `n` items to the given `worker_id`,
        returning a set of all work the worker should do.
        """
        raise NotImplementedError()

    async def mark_work_finished(self, worker_id: int, work: Set[str]):
        """
        Mark that the given `worker_id` has finished items in `work`.
        """
        raise NotImplementedError()

    async def shutdown(self):
        """
        Method called when the Tracker is shutting down.
        Useful to close sockets, files, etc.
        """
        raise NotImplementedError()


class InMemoryTracker(ItemTracker):
    all_items: Set[str]
    unexplored_items: Set[str]
    last_worker_id: int
    last_id_lock: asyncio.Lock
    assigned_work: Mapping[int, Set[str]]

    def __init__(self):
        super().__init__()
        self.all_items = set()
        self.unexplored_items = set()
        self.last_worker_id = 0
        self.last_id_lock = asyncio.Lock()
        self.assigned_work = collections.defaultdict(set)

    async def async_init(self):
        pass

    async def add_items(self, items):
        new_items = items - self.all_items
        self.all_items.update(new_items)
        self.unexplored_items.update(new_items)

    async def mark_explored(self, items):
        self.unexplored_items.difference_update(items)

    async def get_worker_id(self):
        async with self.last_id_lock:
            self.last_worker_id += 1
            return self.last_worker_id

    async def crawl_done(self):
        return len(self.unexplored_items) == 0 and all(len(assigned) == 0 for assigned in self.assigned_work.values())

    async def checkout_work(self, worker_id, n):
        for _ in range(n):
            if len(self.unexplored_items) == 0:
                break

            self.assigned_work[worker_id].add(self.unexplored_items.pop())

        return self.assigned_work[worker_id]

    async def mark_work_finished(self, worker_id, work):
        assert work.issubset(self.assigned_work[worker_id])
        self.assigned_work[worker_id].difference_update(work)

    async def shutdown(self):
        pass


class RedisTracker(ItemTracker):
    _redis_address: str
    crawl_manager: 'CrawlManager'

    def __init__(self, redis_address: str):
        super().__init__()
        self._redis_address = redis_address

    async def async_init(self):
        self._redis = await aioredis.create_redis_pool(self._redis_address, minsize=1, maxsize=4)
        await self.clear()

    async def clear(self):
        self._logger.info(f"Clearing all items in {self._items_key}")
        await self._redis.delete(self._items_key)

        self._logger.info(f"Clearing unexplored items in {self._unexplored_key}")
        await self._redis.delete(self._unexplored_key)

        self._logger.info(f"Resetting worker id counter in {self._worker_id_key}")
        await self._redis.set(self._worker_id_key, 0)

        self._logger.info(f"Resetting temporary id counter in {self._temp_id_key}")
        await self._redis.set(self._temp_id_key, 0)

        for worker_k in await self._redis.keys(self._checked_out_work_key('*')):
            self._logger.info(f"Clearing items checked out by worker in {worker_k}")
            await self._redis.delete(worker_k)

    def _keyname(self, elem: str):
        """
        Return a key name for `elem` unique to this crawl.
        """
        return f"{self.crawl_manager.name}_{elem}"

    @property
    def _items_key(self):
        """
        The set key used to store all known items.
        """
        return self._keyname('all_items')

    @property
    def _unexplored_key(self):
        """
        The set key used to store all unexplored items.
        """
        return self._keyname('unexplored')

    @property
    def _worker_id_key(self):
        """
        The counter key used to generate worker IDs.
        """
        return self._keyname('worker_id')

    @property
    def _temp_id_key(self):
        """
        The counter key used to generate temporary set names.
        """
        return self._keyname('temp_id')

    def _checked_out_work_key(self, worker_id):
        """
        The set key used to store all items checked out to the given
        `worker_id`.
        """
        return self._keyname(f'checked_out_{worker_id}')

    async def add_items(self, items):
        temp_id = await self._redis.incr(self._temp_id_key)
        temp_key = self._keyname(f'temp_{temp_id}')

        # Load all items we're adding into a temp table
        for some_items in grouper(1000, items):
            await self._redis.sadd(temp_key, *some_items)

        # Find the new items (i.e. items not already in all_items)
        await self._redis.sdiffstore(temp_key, temp_key, self._items_key)

        # Save new items into all_items and unexplored
        await self._redis.sunionstore(self._items_key, self._items_key, temp_key)
        await self._redis.sunionstore(self._unexplored_key, self._unexplored_key, temp_key)

        # And clean up after ourselves
        await self._redis.delete(temp_key)

    async def mark_explored(self, items):
        for some_items in grouper(1000, items):
            await self._redis.srem(self._unexplored_key, *some_items)

    async def get_worker_id(self):
        return await self._redis.incr(self._worker_id_key)

    async def crawl_done(self):
        unexplored_len = await self._redis.scard(self._unexplored_key)
        if unexplored_len > 0:
            return False

        for worker_k in await self._redis.keys(self._checked_out_work_key('*')):
            checked_out = await self._redis.scard(worker_k)
            if checked_out > 0:
                return False

        return True

    async def checkout_work(self, worker_id, n):
        worker_work_set = self._checked_out_work_key(worker_id)

        items = await self._redis.srandmember(self._unexplored_key, n)
        for item in items:
            await self._redis.smove(self._unexplored_key, worker_work_set, item)

        return {item.decode('utf-8') for item in await self._redis.smembers(worker_work_set)}

    async def mark_work_finished(self, worker_id, work):
        worker_work_set = self._checked_out_work_key(worker_id)
        checked_out_work = {item.decode('utf-8') for item in await self._redis.smembers(worker_work_set)}

        assert work.issubset(checked_out_work)

        await self._redis.srem(worker_work_set, *checked_out_work)

    async def shutdown(self):
        self._redis.close()
        await self._redis.wait_closed()
