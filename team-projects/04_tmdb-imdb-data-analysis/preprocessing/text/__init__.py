"""
text 패키지

수집된 원시 데이터를 분석에 적합한 형태로 전처리합니다.

주요 처리:
- 데이터 필터링: 투표 수 30개 미만, 상영 시간 45분 미만/300분 초과 제거
- 언어 필터링: 상위 10개 언어 작품만 유지
- 테이블 정규화: 장르, 언어, OTT 제공사 등 다중값 컬럼을 1행 1값 구조로 분리
- 플랫폼명 통일: 'Netflix basic with Ads' → 'Netflix' 등 이름 통합
- 리뷰 전처리: HTML 제거, 최소 길이 필터링

사용 예시:
    from preprocessing.text import DataPreprocessor
    processor = DataPreprocessor(movie_df, drama_df)
    processor.preprocess()
    processor.save(output_dir="files/preprocessed/")
"""

__version__ = '1.0.0'

from .main_processor import *
from .review_processor import *
