import asyncio
import itertools


def grouper(n, iterable):
    """
    https://stackoverflow.com/a/8991553/8135152
    """
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

