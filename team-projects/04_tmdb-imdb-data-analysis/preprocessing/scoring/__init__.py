"""
scoring 패키지

IMDb 평점과 리뷰 감성 점수를 결합하여 흥행 점수(hit_score)를 계산합니다.

hit_score 공식:
    hit_score = ((scaled_rating + sentiment_score) / 2) × scaled_num_votes × 100

    - scaled_rating: IMDb 평점을 0~1로 정규화
    - sentiment_score: 리뷰 긍정 비율 (0~1)
    - scaled_num_votes: 투표 수를 로그 변환 후 0~1로 정규화

사용 예시:
    from preprocessing.scoring import calculate_score
    result = calculate_score(df_ratings, df_sentiments)
"""

__version__ = "1.0.0"

from .calculator import *
