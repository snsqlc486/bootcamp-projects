import asyncio
import aiohttp
import pandas as pd
import time
import json
from pathlib import Path
from urllib.parse import quote
from datetime import datetime
import html
import re
import argparse

GRAPHQL_URL = "https://caching.graphql.imdb.com/"
OPERATION_NAME = "TitleReviewsRefine"
PERSISTED_QUERY_HASH = (
    "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87"
)

MAX_CALLS_PER_SECOND = 2  # 초당 요청 수
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3

REVIEWS_PER_REQUEST = 25  # 한 요청당 리뷰 개수

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

CHECKPOINT_FILE = "imdb_rating_checkpoint.json"

# ==========================
# Rate Limiter
# ==========================


class RateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.updated_at = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 1

            self.tokens -= 1


rate_limiter = RateLimiter(MAX_CALLS_PER_SECOND)