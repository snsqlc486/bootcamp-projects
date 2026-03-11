"""
히트 점수(Hit Score) 계산 모듈

영화/드라마의 '흥행 성공도'를 나타내는 종합 점수를 계산합니다.

히트 점수 계산 공식:
    hit_score = ((평점점수 + 감성점수) / 2) × 투표수점수 × 100

각 요소의 의미:
- 평점점수: TMDB와 IMDb 평점을 투표 수로 가중평균한 뒤 0~1로 정규화
- 감성점수: 리뷰의 긍정/부정 감성을 유용성(helpful ratio)으로 가중평균
- 투표수점수: 로그 스케일 적용 후 0~1로 정규화 (대중적 인지도 반영)

투표 수를 곱하는 이유:
  평점이 높아도 투표 수가 적으면 신뢰도가 낮습니다.
  투표 수를 곱함으로써 인지도와 품질을 함께 반영합니다.
"""

import numpy as np
import pandas as pd


# ============================================================
# 감성 점수 계산
# ============================================================

def compute_weighted_sentiment_score(data):
    """
    영화/드라마별로 리뷰 감성 점수의 가중평균을 계산합니다.

    사용 파일: review_score.parquet (감성 분석 결과)

    단순 평균이 아닌 가중평균을 사용하는 이유:
    - 유용하다는 평가(helpful votes)를 많이 받은 리뷰가 더 신뢰할 수 있으므로
    - helpful_ratio가 높은 리뷰에 더 높은 가중치를 부여합니다

    Args:
        data (pd.DataFrame): review_id, imdb_id, sentiment_score, helpful_ratio 컬럼 필요

    Returns:
        pd.DataFrame: imdb_id, sentiment_score 컬럼을 가진 데이터프레임
    """
    return data.groupby('imdb_id').apply(calculate_sentiment_score_group).reset_index().rename(columns={0: 'sentiment_score'})


def calculate_sentiment_score_group(group):
    """
    하나의 영화/드라마(group)에 대한 가중평균 감성 점수를 계산합니다.

    공식: Σ(감성점수 × 유용성비율) / Σ(유용성비율)

    예시:
        리뷰1: 감성=0.9, 유용성=0.8 → 기여도 = 0.9 × 0.8 = 0.72
        리뷰2: 감성=0.3, 유용성=0.2 → 기여도 = 0.3 × 0.2 = 0.06
        가중평균 = (0.72 + 0.06) / (0.8 + 0.2) = 0.78

    Args:
        group (pd.DataFrame): 동일한 imdb_id를 가진 리뷰 그룹

    Returns:
        float: 가중평균 감성 점수 (0~1, 1에 가까울수록 긍정적)
    """
    # 분자: 감성점수 × 유용성비율의 합
    numerator = (group['sentiment_score'] * group['helpful_ratio']).sum()

    # 분모: 유용성비율의 합 (가중치 총합)
    denominator = group['helpful_ratio'].sum()

    # 가중치가 모두 0이면(유용성 정보 없음) 단순 점수 0 반환
    if denominator == 0:
        return 0

    return numerator / denominator


# ============================================================
# 평점 정규화
# ============================================================

def scaler(column, min_val, max_val):
    """
    값을 0~1 범위로 정규화(Min-Max Scaling)합니다.

    공식: (x - min) / (max - min)
    clip(0, 1)로 범위를 벗어나는 값은 0 또는 1로 고정합니다.

    Args:
        column (pd.Series): 정규화할 숫자 시리즈
        min_val (float): 최솟값 기준
        max_val (float): 최댓값 기준

    Returns:
        pd.Series: 0~1로 정규화된 시리즈
    """
    return ((column - min_val) / (max_val - min_val)).clip(0, 1)


def calculate_ratings(data):
    """
    TMDB와 IMDb 평점을 합산하여 정규화된 평점 정보를 계산합니다.

    사용 파일: 00_main (전처리된 메인 데이터)

    투표 수 가중평균:
        통합 평점 = (TMDB평점 × TMDB투표수 + IMDb평점 × IMDb투표수) / 총투표수

    정규화:
        - 투표 수: 로그 변환 후 1~17 범위로 정규화 (로그를 쓰는 이유: 100만 투표와 100 투표의 차이가 너무 크기 때문)
        - 평점: 1~10 범위로 정규화

    Args:
        data (pd.DataFrame): imdb_id, tmdb_rating, tmdb_rating_count, imdb_rating, imdb_rating_count 컬럼 필요

    Returns:
        pd.DataFrame: imdb_id, scaled_num_votes, scaled_rating 컬럼을 가진 데이터프레임
    """
    # TMDB와 IMDb 투표 수 합산
    num_votes = data['imdb_rating_count'] + data['tmdb_rating_count']

    # 투표 수 가중평균으로 통합 평점 계산
    rating = ((data['tmdb_rating'] * data['tmdb_rating_count']) + (data['imdb_rating'] * data['imdb_rating_count'])) / num_votes

    # 투표 수: 로그 변환 후 정규화 (범위를 전체보다 20% 넓게 잡아 극단값 처리)
    scaled_num_votes = scaler(np.log(num_votes), min_val=1, max_val=17)

    # 평점: 최솟값 1, 최댓값 10으로 정규화
    scaled_rating = scaler(rating, min_val=1, max_val=10)

    return pd.DataFrame({
        'imdb_id': data['imdb_id'],
        'scaled_num_votes': scaled_num_votes,
        'scaled_rating': scaled_rating
    })


# ============================================================
# 최종 히트 점수 계산
# ============================================================

def calculate_score(data):
    """
    정규화된 평점과 감성 점수로 최종 히트 점수를 계산합니다.

    사용 파일: 감성 점수 + 평점 정보 합산 파일

    공식:
        hit_score = ((scaled_rating + sentiment_score) / 2) × scaled_num_votes × 100

    - 평점과 감성을 동등하게 반영 (각 50%)
    - 투표 수를 곱하여 인지도 반영 (인기 없는 고평점 작품 억제)
    - 100을 곱하여 가독성 향상

    Args:
        data (pd.DataFrame): imdb_id, scaled_rating, sentiment_score, scaled_num_votes 컬럼 필요

    Returns:
        pd.DataFrame: imdb_id, hit_score 컬럼을 가진 데이터프레임
    """
    hit_score = ((data['scaled_rating'] + data['sentiment_score']) / 2) * data['scaled_num_votes'] * 100
    return pd.DataFrame({
        'imdb_id': data['imdb_id'],
        'hit_score': hit_score
    })
