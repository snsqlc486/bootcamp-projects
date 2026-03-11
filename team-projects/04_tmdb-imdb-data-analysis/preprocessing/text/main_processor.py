from .utils import *

class DataPreprocessor:
    def __init__(
            self,
            type
        ):

        if type not in ['movie', 'tv']:
            raise TypeError('Type must be either "movie" or "tv"')
        else:
            self.type = type

        self.main_data = None
        self.roi_data = None
        self.countries_data = None
        self.genre_data = None
        self.providers_data = None

    def preprocess(self, data):
        """
        전체 데이터 파싱 함수
        """

        # 공용 데이터 정제
        data = data[data['vote_count'] >= 30].copy()

        data = data.rename(
            columns={'vote_average': 'tmdb_rating', 'vote_count': 'tmdb_rating_count'}
        ).copy()

        data = data.dropna(
            subset=[
                'imdb_id', 'genres', 'overview',
                'poster_path', 'tmdb_rating', 'tmdb_rating_count',
                'imdb_rating', 'imdb_rating_count'
            ]
        )

        if self.type == 'movie':
            data = data[(data['runtime'] > 45) & (data['runtime'] <= 300)].copy()
            data['release_date'] = pd.to_datetime(data['release_date'])
        else:
            data['episode_run_time_average'] = data['episode_run_time'].apply(
                lambda x: sum(map(int, str(x).split(','))) / len(str(x).split(','))
            )
            data['first_air_date'] = pd.to_datetime(data['first_air_date'])

        # 메인 데이터 정제
        self.main_data = self._main_data_processing(data)

        # ROI 데이터 정제
        if self.type == 'movie':
            self.roi_data = data.loc[
                (data['budget'] != 0) & (data['revenue'] != 0),
                ['id', 'imdb_id', 'budget', 'revenue']
            ].dropna().copy()
        else:
            self.roi_data = None

        # 파싱 및 정규화
        self.countries_data, self.genre_data, self.providers_data = self._process_normalized_tables(data)

    def _main_data_processing(self, data):
        """
        메인데이터 처리 함수
        """

        if self.type == 'movie':
            main_data = data[[
                'id', 'imdb_id', 'title', 'original_language',
                'overview', 'release_date', 'runtime', 'genres',
                'keywords', 'poster_path', 'tmdb_rating', 'tmdb_rating_count',
                'imdb_rating', 'imdb_rating_count'
            ]].copy()
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
        정규화 테이블 생성 및 전처리

        Parameters:
        -----------
        data : pd.DataFrame
            원본 데이터
        """
        # 복사본 생성
        data_for_parsing = data.copy()

        # ============================================
        # 1. 파싱할 컬럼 정의
        # ============================================
        if self.type == 'movie':
            parsing_col = ['production_countries', 'genre_ids']
            country_col = 'production_countries'
        else:
            parsing_col = ['origin_country', 'genre_ids']
            country_col = 'origin_country'

        # 쉼표 구분 파싱
        parsing_columns(data_for_parsing, parsing_col)

        # providers_flatrate 파싱
        data_for_parsing['providers_flatrate'] = optimized_provider_parse(
            data_for_parsing, "providers_flatrate"
        )
        parsing_col.append('providers_flatrate')

        normalized_tables = table_normalization(data_for_parsing, parsing_col)

        countries_data = normalized_tables[0]
        genre_ids = normalized_tables[1]
        providers_data = normalized_tables[2]

        countries_data = countries_data.dropna()
        countries_data[country_col] = countries_data[country_col].str.lower()
        countries_data = self._filter_top_10(countries_data, country_col, 'other')
        countries_data = countries_data.drop_duplicates()

        genre_ids['genre_ids'] = genre_ids['genre_ids'].astype('int')
        genre_ids.rename(columns={'genre_ids': 'genre_id'}, inplace=True)

        genres_df = get_genre_mapping(self.type)

        if not genres_df.empty:
            genre_data = pd.merge(genre_ids, genres_df, on='genre_id', how='left')
        else:
            genre_data = genre_ids

        providers_data = self._preprocess_providers(providers_data)
        providers_data = self._filter_top_10(
            providers_data,
            'providers_flatrate',
            'other'
        )
        providers_data = providers_data.drop_duplicates()

        return countries_data, genre_data, providers_data


    def save(self, output_dir):
        import os

        os.makedirs(output_dir, exist_ok=True)

        # 메인 데이터
        if self.main_data is not None:
            path = os.path.join(output_dir, f"00_{self.type}_main.parquet")
            self.main_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        # ROI 데이터 (영화만)
        if self.roi_data is not None:
            path = os.path.join(output_dir, f"01_{self.type}_roi.parquet")
            self.roi_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        # 국가 데이터
        if self.countries_data is not None:
            country_name = 'production_countries' if self.type == 'movie' else 'origin_countries'
            path = os.path.join(output_dir, f"02_{self.type}_{country_name}.parquet")
            self.countries_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        # 장르 데이터
        if self.genre_data is not None:
            path = os.path.join(output_dir, f"03_{self.type}_genres.parquet")
            self.genre_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

        # Provider 데이터
        if self.providers_data is not None:
            path = os.path.join(output_dir, f"04_{self.type}_providers.parquet")
            self.providers_data.to_parquet(path, index=False)
            print(f"  ✓ {path}")

    @staticmethod
    def _filter_top_10(data, column, fill_text):
        """
        상위 10 외 다른 값을 지정된 텍스트로 변경하는 함수
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
        OTT 플랫폼 데이터 전처리

        Parameters:
        -----------
        providers_flatrate : pd.DataFrame
            플랫폼 정규화 테이블

        Returns:
        --------
        pd.DataFrame
            전처리된 플랫폼 테이블
        """
        data = data.copy()

        # 소문자 변환 및 공백 제거
        data['providers_flatrate'] = (
            data['providers_flatrate'].str.lower().str.strip()
        )

        # 불필요한 문자열 일괄 제거
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

        # '+' → ' plus' 변환
        data['providers_flatrate'] = (
            data['providers_flatrate'].str.replace('+', ' plus', regex=False)
        )

        # 공백 재정리 및 중복 제거
        data['providers_flatrate'] = data['providers_flatrate'].str.strip()
        providers_flatrate = data.drop_duplicates()

        # 플랫폼 이름 통일 (딕셔너리 방식)
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
