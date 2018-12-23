# Distscrape Architecture

Distscrape is primarily built towards three primary goals:
* Be as fast as possible
    * Nearly everything is asynchronous, allowing it to basically go as fast as the connection between the scraper and the target will let it.
* Guarantee no loss of data
    * All work is "checked out" from the [Tracker](#Tracker) so that even if a worker crashes, the URLs/IDs it was working on can easily be requeued.
    * All Tracker operations are atomic, so no two workers can be working on the same item, and no items can accidentally be dropped.
* Remain flexible for all kinds of scraping and crawling workloads
    * Discrete component separation allows for everything from site mirroring, to tasks like ID discovery simply by changing out the implementations used (see below).


## Components

Distscrape has 4 main components:

* Scraper
* Tracker
* Saver
* Crawl manager

Each of these is explained in detail below:


### Scraper

Scrapers implement the logic that does the actual scraping.
This includes downloading content, and searching it for more links, IDs, etc. to crawl.

#### Provided implementations
* `SimpleScraper`: Scrapes each page for links matching the provided `link_regex` and adds all results back into the work queue.
* `IDScraper`: Scrapes each page for IDs matching the provided `id_regex`, and downloads items by GETing the specified `download_url_fmt`.


### Tracker

The tracker tracks what all of the workers are doing - it is the central authority for what is going on at any given time.
It gives workers their IDs, leases work out to the workers, adds new items the workers find, and ensure no URLs/IDs are lost along the way.

#### Provided implementations
* `InMemoryTracker`: A simple tracker that uses Python `set`s to track items. Useful for smaller scrapes.
* `RedisTracker`: A tracker backed by a redis server. Useful for larger scrapes, or scrapes where many machines are required (e.g. due to restrictions per-IP)


### Saver

Savers, as you may expect, save the content that is downloaded by the scrapers.

#### Provided implementations
* `FileItemSaver`: Saves results to files on disk based on a configurable file name scheme
* `RedisItemSaver`: Saves results into a redis server based on a configurable key scheme
* `TarItemSaver`: Saves results into a tar(.gz|.bz) archive


### Crawl manager

The crawl manager is responsible for coordinating all of the other parts of distscrape.
It contains the main crawling loop, and proxies requests from one part of the scraper to another.
