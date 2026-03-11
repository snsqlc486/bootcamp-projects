"""
preprocessing 패키지

수집된 원시 데이터를 분석 가능한 형태로 가공하는 패키지입니다.

하위 패키지:
- text:          텍스트 전처리 (컬럼 정규화, 테이블 분리, 언어 필터링)
- scoring:       흥행 점수 계산 (평점 + 감성 점수 → hit_score)
- sentiment:     BERT 기반 리뷰 감성 분석 (긍정/부정 분류)
- vectorization: TF-IDF / BERT 임베딩 벡터화
- topics:        BERTopic 기반 토픽 모델링
"""
