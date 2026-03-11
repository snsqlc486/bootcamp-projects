"""
토픽 모델링 공통 설정 모듈

이 파일은 BERTopic 기반 토픽 모델링에 필요한 라이브러리와 설정을 담고 있습니다.
다른 모듈(topic_modeler.py, utils.py)이 이 파일을 import하여 공통 설정을 사용합니다.

BERTopic 파이프라인:
  텍스트 → [BERT 임베딩] → [UMAP 차원 축소] → [HDBSCAN 클러스터링] → [c-TF-IDF 키워드 추출]
  1. BERT 임베딩: 각 문서를 의미 벡터로 변환
  2. UMAP: 고차원 벡터를 2~10차원으로 압축 (비슷한 의미 → 가까운 위치)
  3. HDBSCAN: 밀도가 높은 영역을 하나의 클러스터(토픽)로 묶음
  4. c-TF-IDF: 각 클러스터를 대표하는 핵심 단어 추출
"""

import pandas as pd
import numpy as np
import os
from bertopic import BERTopic                          # 토픽 모델링 메인 라이브러리
from umap import UMAP                                  # 차원 축소 (Uniform Manifold Approximation)
from hdbscan import HDBSCAN                            # 밀도 기반 클러스터링
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS  # 단어 빈도 계산
from sentence_transformers import SentenceTransformer  # BERT 임베딩 모델
from dotenv import load_dotenv                         # .env 파일 환경변수 로드
from sklearn.cluster import AgglomerativeClustering    # 계층적 클러스터링 (토픽 그룹화용)

import warnings
warnings.filterwarnings('ignore')       # 불필요한 경고 메시지 숨김
os.environ['MallocStackLogging'] = '0'  # macOS 메모리 로그 비활성화

# 환경변수에서 파일 경로 읽기
load_dotenv()
OUTPUT_DIR = os.getenv("BERT_OUTPUT_DIR")      # 토픽 모델 결과 저장 디렉토리
DRAMA_FILE_PATH = os.getenv("DRAMA_FILE_PATH") # 드라마 데이터 경로
MOVIE_FILE_PATH = os.getenv("MOVIE_FILE_PATH") # 영화 데이터 경로
HIT_FILE_PATH = os.getenv("HIT_FILE_PATH")     # 흥행 점수 데이터 경로
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 전역 임베딩 모델 (topic_modeler.py와 공유)
embedding_model = SentenceTransformer('Qwen/Qwen3-Embedding-0.6B')