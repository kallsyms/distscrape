#!/usr/bin/env python3
import asyncio
import logging
import os

from crawl_manager import CrawlManager
from saver import RedisItemSaver, FileItemSaver, TarItemSaver
from scraper import IDScraper, NullScraper
from tracker import InMemoryTracker, RedisTracker


async def main(cm):
    await cm.initialize_tracker_with_items(
        (line.strip() for line in open('test_data/youtube/known.txt', 'r')),
        (line.strip() for line in open('test_data/youtube/explored.txt', 'r')),
    )
    await cm.crawl()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    tracker = InMemoryTracker()
    # tracker = RedisTracker('redis://localhost')

    scraper = IDScraper('https://www.youtube.com/annotations_invideo?video_id={}', r'v=([a-zA-Z0-9_-]{11})')

    saver = TarItemSaver(tar_path='annotations.tar.gz', file_path_fmt='{0}.xml')
    # saver = FileItemSaver(file_path_fmt=lambda fn: os.path.join('annotations', fn[:2], fn) + '.xml')
    # saver = RedisItemSaver('redis://localhost')

    cm = CrawlManager(
        name='test_youtube_annotations',
        num_workers=1,
        tracker=tracker,
        scraper=scraper,
        saver=saver,
    )

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main(cm))
    finally:
        loop.close()
