import re


class Scraper(object):
    async def download(self, session, item):
        """
        Overridable method to download the given item.
        """
        raise NotImplementedError()

    async def save(self, crawl_manager, item, response):
        """
        Method to save the item.
        N.B. This is handled by the scraper (and is a scraper
        processing callback) because some crawls may not want
        to actually save a response (e.g. ID discovery).
        """
        await crawl_manager.save(item, response)


class FakeResponse(object):
    """
    Fake aiohttp response object for NullScraper/tests
    """
    async def text(self):
        return ""

    @property
    def status(self):
        return 200

    async def release(self):
        pass


class NullScraper(Scraper):
    async def download(self, session, item):
        return FakeResponse()

    @property
    def processing_callbacks(self):
        return []


class SimpleScraper(Scraper):
    def __init__(self, link_regex: str):
        self._regex = re.compile(link_regex)

    async def download(self, session, item):
        return await session.get(item)

    async def add_new_links(self, crawl_manager, item, response):
        text = await response.text()
        await crawl_manager.add_new_items(self._regex.findall(text))

    @property
    def processing_callbacks(self):
        return [
            self.add_new_links,
            self.save,
        ]


class IDScraper(Scraper):
    def __init__(self, download_url_fmt: str, id_regex: str):
        self.download_url_fmt = download_url_fmt
        self._regex = re.compile(id_regex)

    async def download(self, session, item):
        return await session.get(self.download_url_fmt.format(item))

    async def add_new_ids(self, crawl_manager, item, response):
        text = await response.text()
        await crawl_manager.add_new_items(self._regex.findall(text))

    @property
    def processing_callbacks(self):
        return [
            self.add_new_ids,
            self.save,
        ]

