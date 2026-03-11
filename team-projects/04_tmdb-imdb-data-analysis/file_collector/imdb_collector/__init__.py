"""
IMDb 데이터 수집 패키지

IMDb에서 두 가지 방식으로 데이터를 수집합니다.
- GraphQL API: 사용자 리뷰 텍스트 수집 (review_collector)
- HTML 스크래핑: 평점 및 투표 수 수집 (rating_collector)

사용 예시:
    from file_collector.imdb_collector import collect_imdb_reviews, collect_imdb_ratings

    # 리뷰 수집
    reviews = collect_imdb_reviews(imdb_ids, output_path="files/reviews.parquet")

    # 평점 수집
    ratings = collect_imdb_ratings(imdb_ids, output_path="files/ratings.parquet")
"""

VERSION = "1.0.0"

from .review_collector import collect_imdb_reviews
from .rating_collector import collect_imdb_ratings
__all__ = ["collect_imdb_reviews", "collect_imdb_ratings"]
