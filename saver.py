from functools import partial
from io import BytesIO
from threading import Thread
from typing import Iterable, Set, Mapping, Union, Callable

import aiofiles
import aioredis
import asyncio
import logging
import os
import tarfile


class ItemSaver(object):
    _logger: logging.Logger
    crawl_manager: 'CrawlManager'

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        # self.crawl_manager is set by the CrawlManager itself when it is initialized

    async def save(self, item, response):
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()


class NullItemSaver(ItemSaver):
    """
    An item saver that doesn't actually save anything.
    Useful for just crawling for IDs.
    """
    async def save(self, item, response):
        pass

    async def close(self):
        pass


class RedisItemSaver(ItemSaver):
    """
    An item saver that stores the response text into redis.
    """
    _redis_address: str
    crawl_manager: 'CrawlManager'

    def __init__(self, redis_address: str):
        super().__init__()
        self._redis_address = redis_address
        asyncio.get_event_loop().run_until_complete(self.async_init())

    async def async_init(self):
        self._redis = await aioredis.create_redis_pool(self._redis_address, minsize=1, maxsize=4)

    def _keyname(self, item: str):
        return f"{self.crawl_manager.name}_item_{item}"

    async def save(self, item, response):
        content = await response.read()
        self._redis.set(self._keyname(item), content)

    async def close(self):
        self._redis.close()
        await self._redis.wait_closed()


class FileItemSaver(ItemSaver):
    """
    An ItemSaver that saves responses to files on disk.
    """
    file_path_fmt: Union[str, Callable[[str], str]]

    def __init__(self, file_path_fmt='{0}'):
        super().__init__()
        self.file_path_fmt = file_path_fmt

    async def save(self, item, response):
        content = await response.read()

        if callable(self.file_path_fmt):
            file_path = self.file_path_fmt(item)
        else:
            file_path = self.file_path_fmt.format(item)

        dirs = os.path.dirname(file_path)

        if dirs:
            try:
                if not os.path.exists(dirs):
                    os.makedirs(dirs)
            except OSError:
                pass

        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(content)

    async def close(self):
        pass


class TarItemSaver(ItemSaver):
    """
    An ItemSaver that saves items into a tar(.gz|.bz2) file
    """
    tar_path: str
    file_path_fmt: Union[str, Callable[[str], str]]

    def __init__(self, tar_path, file_path_fmt='{0}'):
        super().__init__()
        self.tar_path = tar_path
        self.file_path_fmt = file_path_fmt

        compression = ''
        if self.tar_path.endswith('gz'):
            compression = ':gz'
        elif self.tar_path.endswith('bz2'):
            compression = ':bz2'

        self._tarfile = tarfile.open(self.tar_path, 'w'+compression)

    def _do_save(self, item: str, response, content: bytes):
        bio = BytesIO()
        bio.write(content)
        bio.seek(0)

        if callable(self.file_path_fmt):
            file_path = self.file_path_fmt(item)
        else:
            file_path = self.file_path_fmt.format(item)

        tinfo = tarfile.TarInfo(name=file_path)
        tinfo.size = len(content)
        self._tarfile.addfile(tarinfo=tinfo, fileobj=bio)

    async def save(self, item, response):
        content = await response.read()

        await asyncio.get_event_loop().run_in_executor(None, partial(self._do_save, item, response, content))

    async def close(self):
        self._tarfile.close()

