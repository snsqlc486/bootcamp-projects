from .config import *

class OverviewTfidfVectorizer():
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

        self.vectorizer = TfidfVectorizer(
            max_features=max_features,
            ngram_range=ngram_range,
            min_df=min_df,
            max_df=max_df,
            stop_words=list(self.stopwords),
            lowercase=True,
            token_pattern=r"(?u)\b[a-z]{3,}\b"
        )
        self.feature_names = None
        self.tfidf_matrix = None
        self.df = None

        if self.use_lemma:
            print("spaCy 모델 로딩 중...")
            # 'en_core_web_sm' 모델이 설치되어 있어야 합니다.
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
        """줄거리 텍스트 정제"""
        if pd.isna(text) or len(text) < 1:
            return ""
        text = text.lower()
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'[^a-zA-Z0-9\s-]', ' ', text)
        text = re.sub(r'\b\d+\b', '', text)
        text = re.sub(r'-+', '-', text)
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        if self.use_lemma and self.nlp:
            doc = self.nlp(text)
            lemmas = [token.lemma_ for token in doc if len(token.text) >= 3]
            return ' '.join(lemmas)
        else:
            return text

    # 데이터 입력 및 벡터화
    def fit(self, df, overview_column='overview'):

        self.df = df.copy()

        # 1. 줄거리 정제
        overview_clean = []
        for i, text in enumerate(self.df[overview_column]):
            overview_clean.append(self._clean_overview(text))
            if (i + 1) % 5000 == 0:
                print(f"   진행: {i+1}/{len(self.df)}")

        self.df['overview_clean'] = overview_clean

        # 2. TF-IDF 학습
        corpus = self.df['overview_clean'].tolist()
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)
        self.feature_names = self.vectorizer.get_feature_names_out()

    # 키워드 추출
    def extract_keywords(self, n_keywords= 10):
        if self.tfidf_matrix is None:
            raise ValueError("먼저 fit()을 실행하여 모델을 학습시켜야 합니다.")
        all_keywords = []

        for i in range(self.tfidf_matrix.shape[0]):
            # i번째 행의 벡터 추출
            row_vector = self.tfidf_matrix[i].toarray()[0]

            # 점수가 0보다 큰 인덱스 중 상위 n개 정렬
            top_indices = np.argsort(row_vector)[::-1][:n_keywords]

            # 실제 단어로 변환 (점수가 0인 경우는 제외)
            keywords = [self.feature_names[idx] for idx in top_indices if row_vector[idx] > 0]
            all_keywords.append(keywords)

        # 데이터프레임에 리스트 형태로 저장
        self.df['keywords'] = all_keywords

    # 흥행작 비흥행작 키워드 도출
    ## 상위 영화 인덱스 필터링
    ## 하위 영화 인덱스 필터링
    ## 델타스코어를 통한 하위, 상위 키워드 분류
    def extract_delta_keywords(self, n_top=30):
        """
        두 그룹(성공 vs 실패) 간의 TF-IDF 평균 차이를 계산하여 차별적 키워드 추출
        """

        if self.tfidf_matrix is None:
            raise ValueError("먼저 fit()을 실행하여 모델을 학습시켜야 합니다.")

        top_indices = self.df[self.df['hit_label'] == 1].index.tolist()
        bottom_indices = self.df[self.df['nonhit_label'] == 1].index.tolist()

        # 1. 각 그룹의 평균 TF-IDF 벡터 계산 (CSR Matrix 행 슬라이싱 활용)
        # ⚠️ indices는 리스트나 넘파이 배열 형태여야 함
        top_mean = self.tfidf_matrix[top_indices].mean(axis=0).A1
        bottom_mean = self.tfidf_matrix[bottom_indices].mean(axis=0).A1

        # 2. 델타 스코어 계산 (성공 - 실패)
        delta = top_mean - bottom_mean

        # 3. 양수(성공 특징)와 음수(실패 특징)에서 상위 키워드 인덱스 추출
        pos_indices = np.argsort(delta)[::-1][:n_top]
        neg_indices = np.argsort(delta)[:n_top]

        # 4. 결과 정리
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