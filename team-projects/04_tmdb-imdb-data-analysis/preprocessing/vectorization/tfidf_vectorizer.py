"""
TF-IDF 기반 줄거리 벡터화 모듈

TF-IDF(Term Frequency-Inverse Document Frequency)란?
  단어가 문서 내에서 얼마나 중요한지를 수치로 표현하는 방법입니다.
  - TF(단어 빈도): 특정 단어가 해당 문서에서 얼마나 자주 나오는가
  - IDF(역문서 빈도): 그 단어가 전체 문서 중 얼마나 희귀한가 (흔한 단어는 중요도 낮춤)
  예: "murder"라는 단어가 한 줄거리에만 집중적으로 등장하면 높은 점수

spaCy 표제어 추출(lemmatization)이란?
  "running" → "run", "murders" → "murder"처럼 단어를 기본형으로 바꾸는 작업입니다.
  이렇게 하면 같은 의미의 단어를 하나로 통합해 분석 정확도가 올라갑니다.
"""

from .config import *

class OverviewTfidfVectorizer():
    """
    영화/드라마 줄거리(overview)를 TF-IDF로 벡터화하는 클래스.

    주요 기능:
    - 불용어(stopwords) 제거: 분석에 도움이 안 되는 일반 단어 제거
    - spaCy 표제어 추출: 단어를 기본형으로 통일
    - TF-IDF 행렬 생성: 각 문서를 숫자 벡터로 변환
    - 키워드 추출: 각 작품의 핵심 단어 상위 N개 추출
    - 델타 키워드: 흥행작과 비흥행작을 구분 짓는 차별적 단어 추출

    사용 예시:
        vectorizer = OverviewTfidfVectorizer()
        vectorizer.fit(df, overview_column='overview')  # TF-IDF 학습
        vectorizer.extract_keywords(n_keywords=10)      # 키워드 추출
        pos_df, neg_df = vectorizer.extract_delta_keywords()  # 흥행/비흥행 키워드
    """

    def __init__(
            self,
            max_features=5000,
            ngram_range=(1, 2),
            min_df=3,
            max_df=0.8,
            use_lemma=True
        ):

        self.use_lemma = use_lemma
        self.stopwords = self._get_base_stopwords()

        # TF-IDF 벡터라이저 설정
        # max_features: 최대 단어 수 (가장 중요한 5000개만 사용)
        # ngram_range=(1,2): 단일 단어 + 2단어 조합 모두 분석 ("action", "action hero" 등)
        # min_df=3: 최소 3개 문서에 등장한 단어만 포함 (희귀 오타/고유명사 제거)
        # max_df=0.8: 전체 80% 이상 문서에 등장하는 단어는 제외 (너무 흔한 단어 제거)
        # token_pattern: 알파벳 소문자 3글자 이상인 단어만 인식
        self.vectorizer = TfidfVectorizer(
            max_features=max_features,
            ngram_range=ngram_range,
            min_df=min_df,
            max_df=max_df,
            stop_words=list(self.stopwords),
            lowercase=True,
            token_pattern=r"(?u)\b[a-z]{3,}\b"
        )
        self.feature_names = None   # 학습 후 단어 목록 저장
        self.tfidf_matrix = None    # 학습 후 TF-IDF 행렬 저장 (문서 수 × 단어 수)
        self.df = None              # 원본 데이터 저장

        if self.use_lemma:
            print("spaCy 모델 로딩 중...")
            # 'en_core_web_sm' 모델이 설치되어 있어야 합니다.
            # disable=['parser', 'ner']: 문장 분석/개체명 인식은 불필요하므로 비활성화 (속도 향상)
            self.nlp = spacy.load('en_core_web_sm',
                                  disable=['parser', 'ner'])
            print("완료!")
        else:
            self.nlp = None

    # 전처리
    ## 불용어 불러오기
    def _get_base_stopwords(self):
        base_stopwords = set(ENGLISH_STOP_WORDS)
        costom_stopwords = {
            # 지명, 인명, 기관 (데이터에서 튀는 특정 단어들)
            "york", "america", "england", "mexico", "colombia", "john", "jimmy", "city",
            "televisa", "remake", "manchester", "orleans", "isle", "san", "santa", "saint",
            'india', 'indian', 'british', 'london',
            
            # 너무 일반적인 명사
            'man',

            # 제작/형식 관련 (줄거리 외적인 단어)
            "produced", "production", "television", "tv", "series", "show", "episode", "season",
            "drama", "documentary", "feature", "theatrical", "cinematic", "adaptation", "sequel",
            "trilogy", "installment", "cast", "footage", "debut", "screen", "archive", "filmmaker",
            'story',

            'aka', 'anthology', 'biopic', 'chapter', 'character', 'cinema', 'director',
            'document', 'film', 'movie', 'narrate', 'narrative', 'original', 'plot', 'portray',
            'producer', 'protagonist', 'retelling', 'scene', 'script'

            # 숫자 및 의미 없는 토큰
            "one", "two", "three", "iii", "ii", "iv", "vi", "vii", "viii", "ix", "la", "las", "los",
            "del", "jr", "st", "dr", "mr", "mrs",

            # 동작/상태 (너무 흔해서 변별력 없음)
            'find', 'make', 'take', 'get', 'go', 'come', 'set', 'help', 'know', 'see', 'want',
            'begin', 'start', 'happen', 'appear', 'stay', 'seem', 'put', 'keep', 'let', 'become',
            'try', 'look', 'happen', 'proceed', 'continue', 'soon',

            'ability', 'able', 'accept', 'acceptance', 'accord', 'actual', 'add', 'adjust', 'admit',
            'allow', 'apparent', 'apparently', 'appearance', 'apply', 'approach', 'arise', 'assume',
            'attend', 'available', 'away', 'background', 'basis', 'become', 'begin',
            'beginning', 'behave', 'behavior', 'belong', 'bring', 'cause', 'change', 'choose', 'come',
            'complete', 'consider', 'consist', 'contain',

            # 시간/단위/장소 (배경 정보일 뿐 서사 특징이 아닌 것)
            'life', 'world', 'time', 'day', 'year', 'night', 'way', 'place', 'home', 'thing',
            'people', 'person', 'group', 'name', 'end', 'area', 'event', 'incident', 'situation',
            'period', 'aspect', 'matter', 'condition', 'month', 'week', 'hour', 'minute',
            'today', 'tonight', 'tomorrow', 'yesterday', 'morning', 'afternoon', 'evening',

            # 일반 형용사 (긍정/부정의 서사적 의미가 약한 것들)
            'young', 'old', 'new', 'good', 'bad', 'great', 'small', 'large', 'big', 'little',
            'long', 'high', 'various', 'different', 'several', 'numerous', 'entire', 'whole',
            'normal', 'ordinary', 'typical', 'regular', 'simple', 'basic',

            # 강조 부사 (TF-IDF에서 큰 비중을 차지하지만 정보량은 적음)
            'clearly', 'obviously', 'certainly', 'definitely', 'completely', 'totally', 'entirely',
            'extremely', 'highly', 'quite', 'rather', 'fairly', 'pretty', 'somewhat', 'almost',
            'mostly', 'mainly', 'actually', 'really', 'truly', 'simply', 'merely', 'basically',
            'essentially', 'naturally', 'especially', 'particularly'
        }

        return base_stopwords.union(costom_stopwords)
    ## 오버뷰 정제
    def _clean_overview(self, text):
        """
        줄거리 텍스트를 TF-IDF 분석에 적합하게 정제합니다.

        처리 순서:
        1. 소문자 변환
        2. HTML 태그 제거 (<br>, <p> 등)
        3. 영문자/숫자/공백/하이픈 외 문자 제거
        4. 단독 숫자 제거
        5. 연속 하이픈 정리
        6. 여백 정리
        7. (use_lemma=True인 경우) spaCy 표제어 추출: "runs" → "run"
        """
        if pd.isna(text) or len(text) < 1:
            return ""
        text = text.lower()
        text = re.sub(r'<[^>]+>', '', text)              # HTML 태그 제거
        text = re.sub(r'[^a-zA-Z0-9\s-]', ' ', text)    # 특수문자 → 공백
        text = re.sub(r'\b\d+\b', '', text)              # 단독 숫자 제거
        text = re.sub(r'-+', '-', text)                  # 연속 하이픈 정리
        text = re.sub(r'\s+', ' ', text)                 # 연속 공백 정리
        text = text.strip()

        if self.use_lemma and self.nlp:
            doc = self.nlp(text)
            # 표제어(lemma) 추출: 3글자 미만 토큰은 의미가 적으므로 제외
            lemmas = [token.lemma_ for token in doc if len(token.text) >= 3]
            return ' '.join(lemmas)
        else:
            return text

    # 데이터 입력 및 벡터화
    def fit(self, df, overview_column='overview'):
        """
        줄거리 데이터로 TF-IDF 모델을 학습합니다.

        처리 단계:
        1. 모든 줄거리 텍스트를 _clean_overview()로 정제
        2. TF-IDF 행렬 생성: 각 문서가 숫자 벡터로 변환됨
           - 결과: (문서 수) × (단어 수) 크기의 희소 행렬(sparse matrix)
        3. 학습된 단어 목록(feature_names) 저장

        Args:
            df (pd.DataFrame): 줄거리 컬럼이 포함된 데이터
            overview_column (str): 줄거리 컬럼명 (기본값: 'overview')
        """
        self.df = df.copy()

        # 1. 줄거리 정제: 5000개마다 진행 상황 출력
        overview_clean = []
        for i, text in enumerate(self.df[overview_column]):
            overview_clean.append(self._clean_overview(text))
            if (i + 1) % 5000 == 0:
                print(f"   진행: {i+1}/{len(self.df)}")

        self.df['overview_clean'] = overview_clean

        # 2. TF-IDF 학습: 정제된 텍스트 목록으로 벡터라이저 학습 및 변환
        # fit_transform = 학습(fit) + 변환(transform)을 한 번에 수행
        corpus = self.df['overview_clean'].tolist()
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)
        self.feature_names = self.vectorizer.get_feature_names_out()  # 학습된 단어 목록

    # 키워드 추출
    def extract_keywords(self, n_keywords=10):
        """
        각 문서(작품)에서 TF-IDF 점수가 높은 상위 키워드를 추출합니다.

        TF-IDF 점수가 높은 단어 = 해당 작품의 줄거리에서 두드러지면서
        다른 작품에는 잘 안 나오는 특징적인 단어

        결과는 self.df['keywords'] 컬럼에 리스트 형태로 저장됩니다.
        예: ["serial killer", "detective", "investigation", ...]

        Args:
            n_keywords (int): 추출할 키워드 수 (기본값: 10)
        """
        if self.tfidf_matrix is None:
            raise ValueError("먼저 fit()을 실행하여 모델을 학습시켜야 합니다.")
        all_keywords = []

        for i in range(self.tfidf_matrix.shape[0]):
            # i번째 문서의 TF-IDF 벡터를 밀집 배열(dense array)로 변환
            row_vector = self.tfidf_matrix[i].toarray()[0]

            # 점수 내림차순 정렬 후 상위 n개 인덱스 선택
            # np.argsort: 오름차순 인덱스 반환, [::-1]: 역순(내림차순), [:n_keywords]: 상위 n개
            top_indices = np.argsort(row_vector)[::-1][:n_keywords]

            # 인덱스를 실제 단어로 변환 (점수가 0인 단어는 제외)
            keywords = [self.feature_names[idx] for idx in top_indices if row_vector[idx] > 0]
            all_keywords.append(keywords)

        # 데이터프레임에 리스트 형태로 저장
        self.df['keywords'] = all_keywords

    # 흥행작 비흥행작 키워드 도출
    def extract_delta_keywords(self, n_top=30):
        """
        흥행작과 비흥행작 사이에서 차별적으로 나타나는 키워드를 추출합니다.

        원리:
        - 흥행작(hit_label=1) 그룹의 평균 TF-IDF 벡터 계산
        - 비흥행작(nonhit_label=1) 그룹의 평균 TF-IDF 벡터 계산
        - 두 벡터의 차이(delta) = 흥행 특징 단어 vs 비흥행 특징 단어

        예시:
        - delta가 큰 양수 단어: 흥행작에 많이 등장하는 키워드 (예: "heist", "superhero")
        - delta가 큰 음수 단어: 비흥행작에 많이 등장하는 키워드 (예: "village", "boring")

        Args:
            n_top (int): 각 그룹에서 추출할 키워드 수 (기본값: 30)

        Returns:
            tuple: (pos_df, neg_df)
                pos_df: 흥행작 특징 키워드 (keyword, delta_score, tfidf_score)
                neg_df: 비흥행작 특징 키워드 (keyword, delta_score, tfidf_score)
        """
        if self.tfidf_matrix is None:
            raise ValueError("먼저 fit()을 실행하여 모델을 학습시켜야 합니다.")

        # 흥행작/비흥행작 행 인덱스 추출
        top_indices = self.df[self.df['hit_label'] == 1].index.tolist()
        bottom_indices = self.df[self.df['nonhit_label'] == 1].index.tolist()

        # 1. 각 그룹의 평균 TF-IDF 벡터 계산
        # CSR Matrix(희소 행렬)의 행 슬라이싱: 해당 인덱스의 행만 추출
        # .mean(axis=0): 행 방향 평균 → 단어별 평균 점수
        # .A1: 행렬을 1차원 배열로 변환
        top_mean = self.tfidf_matrix[top_indices].mean(axis=0).A1
        bottom_mean = self.tfidf_matrix[bottom_indices].mean(axis=0).A1

        # 2. 델타 스코어 계산: 흥행작 평균 - 비흥행작 평균
        # 양수 → 흥행작에 더 많이 등장하는 단어
        # 음수 → 비흥행작에 더 많이 등장하는 단어
        delta = top_mean - bottom_mean

        # 3. 상위 키워드 인덱스 추출
        pos_indices = np.argsort(delta)[::-1][:n_top]  # 내림차순: 흥행 특징 단어
        neg_indices = np.argsort(delta)[:n_top]         # 오름차순: 비흥행 특징 단어

        # 4. 결과를 DataFrame으로 정리
        pos_df = pd.DataFrame({
            'keyword': self.feature_names[pos_indices],
            'delta_score': delta[pos_indices],
            'tfidf_score': top_mean[pos_indices],
        })

        neg_df = pd.DataFrame({
            'keyword': self.feature_names[neg_indices],
            'delta_score': delta[neg_indices],
            'tfidf_score': bottom_mean[neg_indices],
        })

        return pos_df, neg_df