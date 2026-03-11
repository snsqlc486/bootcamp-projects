"""
벡터화 모듈 공통 설정

TF-IDF 벡터화와 BERT 임베딩 생성에 필요한 라이브러리와
기본 파일 경로를 설정합니다.

TF-IDF란?
  텍스트에서 각 단어의 중요도를 숫자로 표현하는 방법입니다.
  자주 나오는 단어보다 특정 문서에서만 자주 나오는 단어에 높은 점수를 줍니다.

BERT 임베딩이란?
  문장 전체의 의미를 고차원 숫자 벡터로 표현하는 딥러닝 기반 방법입니다.
  단어 순서와 문맥을 이해하므로 TF-IDF보다 더 풍부한 표현이 가능합니다.
"""

import re          # 정규표현식 (텍스트 정제에 사용)
import spacy       # NLP 라이브러리 (표제어 추출, 품사 분석)
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS  # TF-IDF
import torch       # 딥러닝 프레임워크 (GPU 감지에 사용)
from tqdm import tqdm
from sentence_transformers import SentenceTransformer  # 문장 임베딩 모델

# ============================================================
# 기본 파일 경로
# ============================================================
# 히트 점수 파일: 각 영화/드라마의 최종 히트 점수가 저장된 파일
HIT_FILE_PATH = 'files/final_files/00_hit_score.parquet'

# 메인 데이터 파일: 영화 기본 정보 (제목, 줄거리 등)
MAIN_FILE_PATH = 'files/final_files/movie/00_movie_main.parquet'
