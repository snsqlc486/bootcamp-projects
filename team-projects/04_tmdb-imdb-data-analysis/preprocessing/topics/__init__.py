"""
topics 패키지

BERTopic을 사용하여 영화/드라마/리뷰 텍스트에서 토픽(주제)을 자동으로 추출합니다.

BERTopic 파이프라인:
  텍스트 → BERT 임베딩 → UMAP 차원 축소 → HDBSCAN 클러스터링 → c-TF-IDF 키워드 추출

주요 모듈:
- topic_modeler: TopicModeler 클래스 (BERTopic 학습, 이상치 병합, 결과 저장)
- utils:         토픽 후처리 (클러스터링, 요약, UMAP 2D 시각화 데이터 생성)

사용 예시:
    from preprocessing.topics import TopicModeler
    modeler = TopicModeler(data=df, type_name='movie')
    modeler.fit_transform()
    modeler.save_results(save_point='movie')
"""

__version__ = "1.0.0"

from .utils import *
from .topic_modeler import *
