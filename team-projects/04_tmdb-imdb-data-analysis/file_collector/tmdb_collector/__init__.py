"""
TMDB 데이터 수집 패키지

TMDB(The Movie Database) API를 사용하여 영화와 드라마 데이터를 수집합니다.

모듈 구성:
- id_collector: 특정 기간 내 영화/드라마 ID 목록 수집
- movie_collector: 영화 상세 정보 수집 (장르, 출연진, OTT 제공사 등)
- tv_collector: TV 드라마 상세 정보 수집 (시리즌/시즌 구조)

사용 예시:
    from file_collector.tmdb_collector import collect_movie_ids, collect_tv_ids

    # ID 수집
    movie_ids = collect_movie_ids("2020-01-01", "2023-12-31", months=1)
    tv_ids = collect_tv_ids("2020-01-01", "2023-12-31", months=1)
"""

__version__ = "1.0.0"

from .id_collector import *

# 영화 수집
from .movie_collector import *

# TV 드라마 수집
from .tv_collector import *
