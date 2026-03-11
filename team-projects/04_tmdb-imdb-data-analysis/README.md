# TMDB-IMDb 데이터 분석 프로젝트

스파르타 내일배움캠프 6조의 **TMDB-IMDb 데이터 기반 영화 및 TV 시리즈 트렌드 분석** 프로젝트입니다.

TMDB와 IMDb에서 영화/드라마 데이터를 수집하고, 텍스트 분석·감성 분석·토픽 모델링·통계 검정·예측 모델링까지 수행하는 **엔드투엔드 데이터 분석 파이프라인**입니다.

---

## 프로젝트 흐름 요약

```
데이터 수집 → 전처리 → 벡터화/임베딩 → 토픽 모델링 → 감성 분석 → 점수 산출 → 통계 분석 → 예측 모델
```

1. **데이터 수집** — TMDB API와 IMDb GraphQL에서 영화/드라마 메타데이터, 평점, 리뷰를 수집합니다.
2. **전처리** — HTML 태그, URL, 광고 등을 제거하고 텍스트를 정리합니다.
3. **벡터화** — TF-IDF와 BERT 임베딩으로 텍스트를 숫자 벡터로 변환합니다.
4. **토픽 모델링** — BERTopic을 사용해 줄거리와 리뷰에서 주요 토픽을 추출합니다.
5. **감성 분석** — BERT 모델로 리뷰의 긍정/부정을 분류합니다.
6. **히트 점수 산출** — 평점, 투표수, 감성 점수를 가중 결합하여 "히트 점수"를 계산합니다.
7. **통계 분석** — 카이제곱 검정, Kruskal-Wallis 검정 등으로 가설을 검증합니다.
8. **예측 모델** — 히트/비히트 콘텐츠를 예측하는 모델을 학습합니다.

---

## 폴더 구조

```
OTT-TEAM6/
│
├── 01_데이터 수집 및 전처리.ipynb    # 데이터 수집과 전처리 실행
├── 02_TF_IDF.ipynb                  # 줄거리 TF-IDF 분석
├── 03_임베딩.ipynb                  # BERT 임베딩 생성
├── 04_BERTopic.ipynb                # 줄거리 토픽 모델링
├── 05_리뷰_BERTopic.ipynb           # 리뷰 토픽 모델링
├── 06_리뷰_TF_IDF.ipynb             # 리뷰 TF-IDF 분석
├── 07_통계.ipynb                    # 통계 검정 (카이제곱, Kruskal-Wallis)
├── 08_예측모델.ipynb                # 히트/비히트 예측 모델
│
├── file_collector/                  # 데이터 수집 모듈
│   ├── tmdb_collector/              #   TMDB API 수집기
│   │   ├── config.py                #     API 설정, 재시도 로직
│   │   ├── id_collector.py          #     영화/드라마 ID 탐색
│   │   ├── movie_collector.py       #     영화 상세 정보 수집
│   │   └── tv_collector.py          #     드라마 상세 정보 수집
│   └── imdb_collector/              #   IMDb 수집기
│       ├── config.py                #     GraphQL 설정, 속도 제한
│       ├── review_collector.py      #     리뷰 수집
│       └── rating_collector.py      #     평점 수집
│
├── preprocessing/                   # 데이터 전처리 모듈
│   ├── text/                        #   텍스트 정제
│   │   ├── main_processor.py        #     메인 전처리 파이프라인
│   │   ├── review_processor.py      #     리뷰 텍스트 클리닝
│   │   └── utils.py                 #     유틸리티 함수
│   ├── vectorization/               #   텍스트 벡터화
│   │   ├── tfidf_vectorizer.py      #     TF-IDF 벡터화
│   │   ├── bert_vectorizer.py       #     BERT 임베딩 생성
│   │   └── loader.py                #     데이터 로더
│   ├── sentiment/                   #   감성 분석
│   │   └── analyzer.py              #     BERT 감성 분류기
│   ├── topics/                      #   토픽 모델링
│   │   ├── topic_modeler.py         #     BERTopic 모델링
│   │   └── config.py                #     모델 설정 (UMAP, HDBSCAN)
│   └── scoring/                     #   점수 산출
│       └── calculator.py            #     히트 점수 계산
│
└── statistical_analysis/            # 통계 분석 모듈
    ├── assumptions.py               #   정규성/등분산성 검정
    ├── chi_square.py                #   카이제곱 독립성 검정
    └── kruskal.py                   #   Kruskal-Wallis H 검정
```

---

## 각 모듈 상세 설명

### file_collector — 데이터 수집

외부 API에서 원본 데이터를 가져오는 모듈입니다.

| 파일 | 하는 일 |
|------|---------|
| `tmdb_collector/id_collector.py` | TMDB Discover API로 날짜 범위 내 영화/드라마 ID를 검색합니다 |
| `tmdb_collector/movie_collector.py` | 영화 제목, 장르, 출연진, 키워드, OTT 제공사, 수익 등 상세 정보를 수집합니다 |
| `tmdb_collector/tv_collector.py` | 드라마 시즌, 에피소드, 방송사, 출연진 정보를 수집합니다 |
| `imdb_collector/review_collector.py` | IMDb GraphQL API로 사용자 리뷰를 수집합니다 |
| `imdb_collector/rating_collector.py` | IMDb 평점 데이터를 비동기로 수집합니다 |

### preprocessing — 전처리

수집한 데이터를 분석 가능한 형태로 가공하는 모듈입니다.

- **text/** — HTML 태그, URL, 특수문자 제거 / 영어 비율 필터링 / 장르·OTT 제공사 정규화
- **vectorization/** — TF-IDF (불용어 제거 + 표제어 추출) / Sentence Transformers 임베딩
- **sentiment/** — BERT 기반 긍정/부정 감성 분류 (배치 처리, GPU 자동 감지)
- **topics/** — BERTopic (UMAP 차원축소 + HDBSCAN 클러스터링)으로 토픽 추출
- **scoring/** — 감성 점수 + 정규화된 평점 + 투표수를 가중 결합하여 "히트 점수" 산출

### statistical_analysis — 통계 분석

가설 검증을 위한 통계 검정 모듈입니다.

- **assumptions.py** — Shapiro-Wilk 정규성 검정, Levene 등분산성 검정
- **chi_square.py** — 카이제곱 독립성 검정 + Cramér's V 효과 크기
- **kruskal.py** — Kruskal-Wallis H 검정 (비모수 그룹 비교)

---

## 사용 기술

| 분류 | 기술 |
|------|------|
| 데이터 수집 | `requests`, `aiohttp`, TMDB API, IMDb GraphQL |
| 데이터 처리 | `pandas`, `numpy` |
| 텍스트 분석 | `spacy`, `scikit-learn` (TF-IDF) |
| 임베딩 | `sentence-transformers` (Qwen3-Embedding-0.6B) |
| 감성 분석 | `transformers` (BERT) |
| 토픽 모델링 | `bertopic`, `umap-learn`, `hdbscan` |
| 통계 검정 | `scipy.stats` |
| 딥러닝 | `torch` |
| 시각화 | `matplotlib`, `seaborn` |

---

## 실행 방법

### 1. 환경 설정

```bash
# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 필요 패키지 설치
pip install pandas numpy scikit-learn scipy torch transformers sentence-transformers
pip install bertopic umap-learn hdbscan spacy requests aiohttp python-dotenv tqdm
pip install matplotlib seaborn
python -m spacy download en_core_web_sm
```

### 2. API 키 설정

프로젝트 루트에 `.env` 파일을 생성합니다:

```
TMDB_API_KEY=your_tmdb_api_key_here
```

TMDB API 키는 [TMDB 개발자 페이지](https://developer.themoviedb.org/)에서 무료로 발급받을 수 있습니다.

### 3. 노트북 순서대로 실행

```
01 → 02 → 03 → 04 → 05 → 06 → 07 → 08
```

각 노트북은 이전 단계의 출력 파일을 입력으로 사용하므로 **순서대로 실행**해야 합니다.

---

## 데이터 형식

- 수집된 데이터는 **Parquet** 형식으로 `files/` 디렉토리에 저장됩니다.
- 최종 결과물: `files/final_files/00_hit_score.parquet` (히트 점수가 포함된 최종 데이터)
