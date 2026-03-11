"""
영화/드라마 메인 데이터 전처리 모듈

수집된 원본 데이터를 분석에 적합한 형태로 가공합니다.

처리 과정:
1. 품질 필터링 (투표 수 30 이상, 필수 컬럼 존재 등)
2. 메인 테이블 구성 (분석에 필요한 핵심 컬럼만 선택)
3. 정규화 테이블 생성 (장르, 국가, OTT 제공사를 별도 테이블로 분리)
4. parquet 파일로 저장

정규화 테이블이 필요한 이유:
  영화 하나가 여러 장르나 OTT 플랫폼에 속할 수 있습니다.
  이를 단순히 "Action, Drama" 문자열로 저장하면 분석이 어렵습니다.
  "1행 = 1값" 형태로 분리하면 통계 분석과 집계가 쉬워집니다.
"""

from .utils import *


class DataPreprocessor:
    """
    영화 또는 드라마 데이터를 전처리하는 클래스입니다.

    사용 예시:
        preprocessor = DataPreprocessor(type='movie')
        preprocessor.preprocess(raw_df)
        preprocessor.save('files/final_files/movie/')
    """

    def __init__(self, type):
        """
        Args:
            type (str): 'movie'(영화) 또는 'tv'(드라마)
        """
        if type not in ['movie', 'tv']:
            raise TypeError('Type must be either "movie" or "tv"')
        else:
            self.type = type

        # 전처리 결과가 저장될 속성들 (preprocess() 실행 후 채워짐)
        self.main_data = None        # 핵심 메인 테이블
        self.roi_data = None         # 투자수익률 데이터 (영화만)
        self.countries_data = None   # 제작 국가 정규화 테이블
        self.genre_data = None       # 장르 정규화 테이블
        self.providers_data = None   # OTT 제공사 정규화 테이블

    def preprocess(self, data):
        """
        전체 데이터 전처리 파이프라인을 실행합니다.

        단계:
        1. 공통 필터링 (투표 수, 필수 컬럼 NaN 제거)
        2. 타입별 필터링 (영화: 런타임, TV: 에피소드 런타임)
        3. 메인 데이터 구성
        4. ROI 데이터 구성 (영화만)
        5. 정규화 테이블 생성

        Args:
            data (pd.DataFrame): 수집된 원본 데이터
        """
        # ===== 1. 공통 품질 필터링 =====
        # 투표 수 30 미만은 신뢰도가 낮으므로 제외
        data = data[data['vote_count'] >= 30].copy()

        # 컬럼명 통일 (TMDB의 vote_average/vote_count → tmdb_rating/tmdb_rating_count)
        data = data.rename(
            columns={'vote_average': 'tmdb_rating', 'vote_count': 'tmdb_rating_count'}
        ).copy()

        # 분석에 필수적인 컬럼에 결측값이 있으면 해당 행 제거
        data = data.dropna(
            subset=[
                'imdb_id', 'genres', 'overview',
                'poster_path', 'tmdb_rating', 'tmdb_rating_count',
                'imdb_rating', 'imdb_rating_count'
            ]
        )

        # ===== 2. 타입별 추가 필터링 =====
        if self.type == 'movie':
            # 단편(45분 이하)과 초장편(5시간 초과)은 분석 대상 제외
            data = data[(data['runtime'] > 45) & (data['runtime'] <= 300)].copy()
            data['release_date'] = pd.to_datetime(data['release_date'])
        else:
            # TV: 에피소드 런타임이 "30, 45" 같은 문자열로 저장되어 있어서 평균 계산
            data['episode_run_time_average'] = data['episode_run_time'].apply(
                lambda x: sum(map(int, str(x).split(','))) / len(str(x).split(','))
            )
            data['first_air_date'] = pd.to_datetime(data['first_air_date'])

        # ===== 3~5. 각 서브 테이블 생성 =====
        self.main_data = self._main_data_processing(data)

        if self.type == 'movie':
            # ROI(투자수익률) 계산용: 예산과 수익이 모두 0이 아닌 경우만
            self.roi_data = data.loc[
                (data['budget'] != 0) & (data['revenue'] != 0),
                ['id', 'imdb_id', 'budget', 'revenue']
            ].dropna().copy()
        else:
            self.roi_data = None

        self.countries_data, self.genre_data, self.providers_data = self._process_normalized_tables(data)

    def _main_data_processing(self, data):
        """
        분석에 필요한 핵심 컬럼만 선택하여 메인 테이블을 구성합니다.

        상위 10개 언어 외의 언어는 'xx'로 통합합니다.
        (소수 언어를 개별 분석하기엔 데이터가 부족하므로)

        Args:
            data (pd.DataFrame): 필터링된 원본 데이터

        Returns:
            pd.DataFrame: 핵심 컬럼만 포함된 메인 테이블
        """
        if self.type == 'movie':
            main_data = data[[
                'id', 'imdb_id', 'title', 'original_language',
                'overview', 'release_date', 'runtime', 'genres',
                'keywords', 'poster_path', 'tmdb_rating', 'tmdb_rating_count',
                'imdb_rating', 'imdb_rating_count'
            ]].copy()
            # 상위 10개 언어 외는 'xx'로 통합
            main_data = self._filter_top_10(main_data, 'original_language', 'xx')
        else:
            main_data = data[[
                'id', 'imdb_id', 'title', 'original_language',
                'overview', 'first_air_date', 'episode_run_time_average', 'genres',
                'keyword', 'poster_path', 'tmdb_rating', 'tmdb_rating_count',
                'imdb_rating', 'imdb_rating_count'
            ]].copy()
            main_data = self._filter_top_10(main_data, 'original_language', 'xx')

        return main_data

    def _process_normalized_tables(self, data):
        """
        국가, 장르, OTT 제공사를 1행=1값 형태의 정규화 테이블로 변환합니다.

        예: 영화 tt001이 Action, Drama 장르를 가지면
            → (tt001, Action), (tt001, Drama) 두 행으로 분리

        Args:
            data (pd.DataFrame): 필터링된 원본 데이터

        Returns:
            tuple: (countries_data, genre_data, providers_data) 세 개의 정규화 테이블
        """
        data_for_parsing = data.copy()

        # 영화와 TV의 국가 컬럼명이 다름
        if self.type == 'movie':
            parsing_col = ['production_countries', 'genre_ids']
            country_col = 'production_countries'
        else:
            parsing_col = ['origin_country', 'genre_ids']
            country_col = 'origin_country'

        # 쉼표 구분 문자열 → 리스트 변환 ("Action, Drama" → ["Action", "Drama"])
        parsing_columns(data_for_parsing, parsing_col)

        # OTT 제공사: JSON 딕셔너리 → 국가 구분 없이 플랫폼 리스트로 변환
        data_for_parsing['providers_flatrate'] = optimized_provider_parse(
            data_for_parsing, "providers_flatrate"
        )
        parsing_col.append('providers_flatrate')

        # 각 컬럼을 1행=1값 정규화 테이블로 변환
        normalized_tables = table_normalization(data_for_parsing, parsing_col)

        countries_data = normalized_tables[0]
        genre_ids = normalized_tables[1]
        providers_data = normalized_tables[2]

        # 국가 데이터 정제
        countries_data = countries_data.dropna()
        countries_data[country_col] = countries_data[country_col].str.lower()
        countries_data = self._filter_top_10(countries_data, country_col, 'other')
        countries_data = countries_data.drop_duplicates()

        # 장르 ID → 장르 이름 매핑
        genre_ids['genre_ids'] = genre_ids['genre_ids'].astype('int')
        genre_ids.rename(columns={'genre_ids': 'genre_id'}, inplace=True)

        genres_df = get_genre_mapping(self.type)

        if not genres_df.empty:
            # genre_id를 기준으로 장르 이름 합치기
            genre_data = pd.merge(genre_ids, genres_df, on='genre_id', how='left')
        else:
            genre_data = genre_ids

        # OTT 제공사 이름 정제 및 통일
        providers_data = self._preprocess_providers(providers_data)
        providers_data = self._filter_top_10(
            providers_data,
            'providers_flatrate',
            'other'
        )
        providers_data = providers_data.drop_duplicates()

        return countries_data, genre_data, providers_data

    def save(self, output_dir):
        """
        전처리 결과를 parquet 파일로 저장합니다.

        저장 파일:
        - 00_{type}_main.parquet     : 메인 테이블
        - 01_{type}_roi.parquet      : ROI 데이터 (영화만)
        - 02_{type}_countries.parquet: 국가 정규화 테이블
        - 03_{type}_genres.parquet   : 장르 정규화 테이블
        - 04_{type}_providers.parquet: OTT 제공사 정규화 테이블

        Args:
            output_dir (str): 저장할 디렉토리 경로
        """
        import os

        os.makedirs(output_dir, exist_ok=True)

        if self.main_data is not None:
            path = os.path.join(output_dir, f"00_{self.type}_main.parquet")
            self.main_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        if self.roi_data is not None:
            path = os.path.join(output_dir, f"01_{self.type}_roi.parquet")
            self.roi_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        if self.countries_data is not None:
            country_name = 'production_countries' if self.type == 'movie' else 'origin_countries'
            path = os.path.join(output_dir, f"02_{self.type}_{country_name}.parquet")
            self.countries_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        if self.genre_data is not None:
            path = os.path.join(output_dir, f"03_{self.type}_genres.parquet")
            self.genre_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        if self.providers_data is not None:
            path = os.path.join(output_dir, f"04_{self.type}_providers.parquet")
            self.providers_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

    @staticmethod
    def _filter_top_10(data, column, fill_text):
        """
        특정 컬럼에서 빈도 상위 10개 값 외 나머지를 fill_text로 치환합니다.

        소수 카테고리를 개별 분석하기엔 데이터가 부족하므로
        상위 10개만 유지하고 나머지는 하나의 범주로 묶습니다.

        예: 언어 컬럼에서 상위 10개 언어 외는 모두 'xx'로 변경

        Args:
            data (pd.DataFrame): 원본 데이터
            column (str): 처리할 컬럼명
            fill_text (str): 상위 10개 외 값을 대체할 텍스트

        Returns:
            pd.DataFrame: 처리된 데이터프레임
        """
        data = data.copy()

        top_10 = data[column].value_counts()[:10].index
        data.loc[
            ~data[column].isin(top_10),
            column
        ] = fill_text

        return data

    @staticmethod
    def _preprocess_providers(data):
        """
        OTT 플랫폼 이름을 정제하고 통일합니다.

        수집된 플랫폼 이름에는 여러 변형이 존재합니다.
        예: "Netflix Standard with Ads", "Netflix Kids" → 모두 "netflix"

        처리 단계:
        1. 소문자 변환 및 공백 제거
        2. 불필요한 접미사 제거 ("amazon channel" 등)
        3. '+' 기호를 'plus'로 통일
        4. 플랫폼별 이름 통일 매핑 적용

        Args:
            data (pd.DataFrame): providers_flatrate 컬럼이 있는 데이터

        Returns:
            pd.DataFrame: 정제된 플랫폼 테이블
        """
        data = data.copy()

        # 소문자 변환 및 앞뒤 공백 제거
        data['providers_flatrate'] = (
            data['providers_flatrate'].str.lower().str.strip()
        )

        # 특정 접미사 제거 (채널 구분을 위한 부가 설명 제거)
        replacements = {
            ' amazon channel': '',
            ' apple tv channel': '',
            ' amzon channel': '',
            ' on u-next': '',
            ' roku premium channel': ''
        }

        for old, new in replacements.items():
            data['providers_flatrate'] = (
                data['providers_flatrate'].str.replace(old, new, regex=False)
            )

        # "disney+" → "disney plus" ('+' 기호 통일)
        data['providers_flatrate'] = (
            data['providers_flatrate'].str.replace('+', ' plus', regex=False)
        )

        data['providers_flatrate'] = data['providers_flatrate'].str.strip()
        providers_flatrate = data.drop_duplicates()

        # 플랫폼 이름 통일 매핑 (다양한 변형명 → 표준명)
        platform_mapping = {
            # Paramount+
            'paramount plus premium': 'paramount plus',
            'paramount plus basic with ads': 'paramount plus',
            'paramount plus mtv': 'paramount plus',
            'paramount plus with showtime': 'paramount plus',
            'paramount plus originals': 'paramount plus',

            # Netflix
            'netflix standard with ads': 'netflix',
            'netflix kids': 'netflix',

            # Movistar
            'movistar plus plus ficción total': 'movistar plus',
            'movistar plus plus': 'movistar plus',
            'movistartv': 'movistar',
            'movistar plus': 'movistar',

            # Amazon Prime
            'amazon prime video with ads': 'amazon prime video',

            # Peacock
            'peacock premium plus': 'peacock plus',

            # YouTube
            'youtube tv': 'youtube',
            'youtube premium': 'youtube',

            # StudioCanal
            'studiocanal presents allstars': 'studiocanal presents',
            'studiocanal presents moviecult': 'studiocanal presents',

            # TV 2
            'tv 2 play': 'tv 2',

            # BBC
            'bbc kids': 'bbc',
            'bbc america': 'bbc',
            'bbc iplayer': 'bbc',
            'bbc player': 'bbc',

            # Discovery
            'discovery  plus': 'discovery plus',

            # AMC
            'amc plus': 'amc',
            'amc channels': 'amc',

            # Lionsgate
            'lionsgate pluss': 'lionsgate',
            'lionsgate play': 'lionsgate',

            # Vix
            'vix gratis': 'vix',
            'vix premium': 'vix',

            # Atres
            'atresplayer': 'atres player',

            # Now TV
            'now tv cinema': 'now tv',

            # Netzkino
            'netzkino select': 'netzkino',

            # Filmtastic
            'filmtastic bei canal plus': 'filmtastic',

            # Starz
            'starzplay': 'starz',

            # Hallmark
            'hallmark tv': 'hallmark',
            'hallmark plus': 'hallmark',

            # Arrow
            'arrow video': 'arrow',

            # Acorn TV
            'acorn tv apple tv': 'acorn tv',
            'acorntv': 'acorn tv',

            # Sky
            'sky go': 'sky',
            'sky x': 'sky',
            'sky store': 'sky',

            # RTL
            'rtl plus max': 'rtl plus',

            # Stingray
            'qello concerts by stingray': 'stingray',
            'stingray all good vibes': 'stingray',
            'stingray classica': 'stingray',
            'stingray karaoke': 'stingray',

            # MGM
            'mgm plus': 'mgm',
        }

        providers_flatrate['providers_flatrate'] = (
            providers_flatrate['providers_flatrate'].replace(platform_mapping)
        )
        providers_flatrate = providers_flatrate.drop_duplicates()

        return providers_flatrate
