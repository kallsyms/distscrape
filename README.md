# distscrape

## What?

Distscrape is a distributed scraping framework written in Python 3 built for large, distributed site scrapes/crawls.
It heavily uses asyncio (and asyncio modules) to be as quick as possible, while still ensuring no data is lost.


## Why?

I wrote distscrape originally to crawl YouTube for video annotations, where will be removed in early 2019.
I needed a system that could be easily pre-initalized with hundreds of millions of IDs to crawl, which standalone
scrapy could not handle. Something like [scrapy-redis](https://github.com/rmax/scrapy-redis) could have helped, however
I already wasn't a fan of scrapy's pipelining architecture, so I decided to write my own.


## How?

See [ARCHITECTURE.md](./ARCHITECTURE.md)


## Getting Started

The provided [test_yt_crawl.py](./test_yt_crawl.py) shows how the various component implementations can be pieced together.


## TODO
See [TODO.md](./TODO.md)
