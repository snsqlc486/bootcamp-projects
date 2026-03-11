import numpy as np
import pandas as pd

# 리뷰 감성점수의 가중평균 계산기.
## 사용파일 : review_score.parquet
def compute_weighted_sentiment_score(data):
    return data.groupby('imdb_id').apply(calculate_sentiment_score_group).reset_index().rename(columns={0:'sentiment_score'})

def calculate_sentiment_score_group(group):
    """
    각 그룹(imdb_id)에 대해 가중 평균을 계산하는 함수
    """
    # 1. 분자 계산: sentiment_score * helpful_ratio의 합
    numerator = (group['sentiment_score'] * group['helpful_ratio']).sum()

    # 2. 분모 계산: helpful_ratio의 합 (가중치 총합)
    denominator = group['helpful_ratio'].sum()

    # 3. 가중 평균
    if denominator == 0:
        return 0

    return numerator / denominator

# 평점
## 사용파일 : 00_main
### 스케일러
def scaler(column, min_val, max_val):
    return ((column - min_val) / (max_val - min_val)).clip(0, 1)

### 계산기
def calculate_ratings(data):
    num_votes=data['imdb_rating_count'] + data['tmdb_rating_count']
    rating=((data['tmdb_rating'] * data['tmdb_rating_count']) + (data['imdb_rating'] * data['imdb_rating_count'])) / num_votes

    # 고정값 스케일러로 스케일링
    ## 최소, 최대값은 전체 범위보다 20% 더 넓은 범위로 설정
    scaled_num_votes = scaler(np.log(num_votes), min_val = 1, max_val = 17)
    ## 점수는 정의상 최소 최대로 설정
    scaled_rating = scaler(rating, min_val = 1, max_val = 10)

    # 점수용 데이터 프레임 반환
    return pd.DataFrame({
        'imdb_id':data['imdb_id'],
        'scaled_num_votes': scaled_num_votes,
        'scaled_rating': scaled_rating
    })

# 전체 점수 계산기
## 사용 파일 감정점수와 평점 정보 평합 파일
def calculate_score(data):
    hit_score = ((data['scaled_rating'] + data['sentiment_score']) / 2) * data['scaled_num_votes'] * 100
    return  pd.DataFrame({
        'imdb_id':data['imdb_id'],
        'hit_score': hit_score
    })