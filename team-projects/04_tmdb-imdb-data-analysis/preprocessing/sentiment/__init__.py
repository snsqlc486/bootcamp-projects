"""
sentiment 패키지

BERT 기반 모델로 IMDb 리뷰 텍스트의 감성을 분석합니다.

사용 모델: distilbert-base-uncased-finetuned-sst-2-english
  - DistilBERT를 SST-2(영화 리뷰 감성 분류) 데이터셋으로 파인튜닝한 모델
  - 각 리뷰를 'positive' 또는 'negative'로 분류하고 긍정 확률(0~1)을 반환

특징:
  - 청크 단위 처리로 대용량 데이터 지원
  - 중단 후 이어서 처리 가능 (Resume 기능)

사용 예시:
    from preprocessing.sentiment import SentimentAnalyzer
    analyzer = SentimentAnalyzer()
    analyzer.analyze(df, output_dir="files/sentiment/")
    result = analyzer.merge_chunks(output_dir="files/sentiment/")
"""

__version__ = "1.0.0"

from .analyzer import *
