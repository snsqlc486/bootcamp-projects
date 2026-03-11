"""
vectorization 패키지

텍스트 데이터를 수치 벡터로 변환하는 패키지입니다.
두 가지 방식을 지원합니다.

TF-IDF 벡터화 (OverviewTfidfVectorizer):
  단어 빈도 기반. 각 단어를 독립적인 피처로 취급합니다.
  빠르고 해석 가능하지만, 단어의 문맥적 의미는 반영하지 못합니다.
  → 흥행/비흥행 구분 키워드 추출에 활용

BERT 임베딩 (BERTVectorizer):
  의미 기반. 문맥을 이해하여 비슷한 의미의 문장을 가까운 벡터로 표현합니다.
  → BERTopic 토픽 모델링 입력으로 활용

사용 예시:
    from preprocessing.vectorization import OverviewTfidfVectorizer, load_files

    df = load_files(main_file_path="files/movies.parquet")
    vectorizer = OverviewTfidfVectorizer()
    vectorizer.fit(df)
    vectorizer.extract_keywords(n_keywords=10)
"""

__version__ = "1.0.0"

from .tfidf_vectorizer import OverviewTfidfVectorizer
from .loader import load_files
