"""
IMDb 수집기 공통 설정 모듈

IMDb에서 리뷰와 평점을 수집할 때 필요한 공통 설정을 담고 있습니다.
review_collector.py와 rating_collector.py가 이 파일을 import하여 사용합니다.

주요 설정:
- GraphQL API 엔드포인트 (리뷰 수집용)
- 속도 제한(Rate Limiter): IMDb 서버에 과도한 요청을 보내지 않도록 제어
- 재시도 횟수, 타임아웃 등 HTTP 요청 설정
"""

import asyncio       # 비동기 프로그래밍 (여러 요청을 동시에 처리)
import aiohttp       # 비동기 HTTP 요청 라이브러리
import pandas as pd
import time
import json
from pathlib import Path
from urllib.parse import quote  # URL에 특수문자를 안전하게 인코딩
from datetime import datetime
import html          # HTML 엔티티 디코딩 (&amp; → & 등)
import re
import argparse

# ============================================================
# GraphQL API 설정
# ============================================================
# IMDb의 리뷰 데이터는 GraphQL API를 통해 제공됩니다.
# GraphQL은 REST API와 달리 필요한 데이터만 정확히 요청할 수 있는 API 방식입니다.
GRAPHQL_URL = "https://caching.graphql.imdb.com/"
OPERATION_NAME = "TitleReviewsRefine"
# Persisted Query: 미리 등록된 쿼리의 해시값 (매번 전체 쿼리를 보내는 대신 해시만 전송)
PERSISTED_QUERY_HASH = (
    "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87"
)

# ============================================================
# 요청 속도 및 재시도 설정
# ============================================================
MAX_CALLS_PER_SECOND = 2   # 초당 최대 요청 수 (너무 빠르면 IMDb에서 차단됨)
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)  # 전체 30초, 연결 10초 타임아웃
MAX_RETRIES = 3             # 실패 시 최대 재시도 횟수

REVIEWS_PER_REQUEST = 25   # 한 번의 GraphQL 요청으로 가져올 리뷰 개수

# 브라우저처럼 보이기 위한 User-Agent 헤더
# 봇 차단을 피하기 위해 실제 Chrome 브라우저의 User-Agent를 사용
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 평점 수집 중단 시 재시작을 위한 체크포인트 파일
CHECKPOINT_FILE = "imdb_rating_checkpoint.json"


# ============================================================
# Rate Limiter (속도 제한기)
# ============================================================

class RateLimiter:
    """
    비동기 환경에서 초당 요청 수를 제한하는 클래스입니다.

    '토큰 버킷' 알고리즘을 사용합니다:
    - 시간이 지날수록 토큰이 채워지고 (rate만큼)
    - 요청을 보낼 때마다 토큰 1개를 소비합니다
    - 토큰이 없으면 생길 때까지 대기합니다

    이 방식으로 서버에 과도한 요청을 보내지 않으면서도
    가능한 한 빠르게 데이터를 수집할 수 있습니다.
    """

    def __init__(self, rate: float):
        """
        Args:
            rate (float): 초당 허용 요청 수 (예: 2.0 → 초당 2회)
        """
        self.rate = rate
        self.tokens = rate             # 현재 보유 토큰 수
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()     # 동시 접근 방지 (비동기 락)

    async def acquire(self):
        """
        요청 허가를 기다립니다.
        토큰이 있으면 즉시 반환, 없으면 토큰이 생길 때까지 대기합니다.
        """
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            # 시간이 지난 만큼 토큰 보충 (rate만큼씩)
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.updated_at = now

            if self.tokens < 1:
                # 토큰 부족 → 1개 생길 때까지 대기
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 1

            self.tokens -= 1  # 토큰 1개 소비


# 전역 Rate Limiter 인스턴스 (모든 수집기가 공유)
rate_limiter = RateLimiter(MAX_CALLS_PER_SECOND)
