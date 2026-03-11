"""
리뷰 데이터 전처리 클래스

주요 기능:
1. 텍스트 노이즈 제거 (공백, 반복 문자, URL/광고 등)
2. 영어 비율 필터링
3. author_rating 전처리
4. helpful 관련 컬럼 전처리
5. submission_date 전처리
"""
from .utils import *

class ReviewPreprocessor:
    """
    리뷰 데이터 전처리 클래스

    영화/드라마 리뷰 데이터의 텍스트 정제 및 메타데이터 전처리
    """

    def __init__(
            self,
            text_column='review_text',
            english_threshold=0.9,
            truncate_len=None
    ):
        """
        Parameters:
        -----------
        text_column : str
            리뷰 텍스트가 담긴 컬럼명
        english_threshold : float
            영어 비율 임계값 (0~1)
        truncate_len : int or None
            텍스트 최대 길이 (None이면 자르지 않음)
        """
        self.text_column = text_column
        self.english_threshold = english_threshold
        self.truncate_len = truncate_len

        self.cleaned_data = None
        self.noise_mask = None

    # ============================================
    # 텍스트 노이즈 마스크 함수들
    # ============================================

    @staticmethod
    def _create_blank_mask(df, col):
        """공백만 있는 리뷰"""
        return df[col].str.strip().eq("")

    @staticmethod
    def _mask_no_alnum(df, col):
        """알파벳/숫자가 없는 리뷰"""
        return ~df[col].str.contains(r"[A-Za-z0-9]", na=False)

    @staticmethod
    def _mask_numeric_only(df, col):
        """숫자만 있는 리뷰"""
        cleaned = df[col].str.replace(r"[ \n\./]", "", regex=True)
        return cleaned.str.isdigit() & cleaned.ne("")

    @staticmethod
    def _mask_repeated_char(df, col):
        """반복 문자 리뷰 (예: aaaaa, !!!!!)"""
        s = df[col].astype(str)
        cleaned = s.str.replace(r"\s+", "", regex=True)
        cleaned = cleaned.str.replace(r"[^\w]", "", regex=True)
        return cleaned.str.fullmatch(r"(.)\1{3,}", na=False)

    @staticmethod
    def _mask_url_or_ad(df, col):
        """URL 또는 광고가 포함된 리뷰"""
        s = df[col].str.lower()
        has_url = s.str.contains(r"(https?://|www\.)", na=False)

        ad_keywords = [
            r"subscribe", r"follow me", r"follow us",
            r"my channel", r"visit my", r"visit our",
            r"check out my", r"check out our",
            r"free streaming", r"free download",
            r"download full movie", r"click here",
        ]
        pattern_ad = "|".join(ad_keywords)
        has_ad = s.str.contains(pattern_ad, na=False)

        return has_url | has_ad

    @staticmethod
    def _mask_low_english_ratio(df, col, threshold):
        """영어 비율이 낮은 리뷰"""
        s = df[col]
        total_len = s.str.replace(r"\s+", "", regex=True).str.len()
        eng_cnt = s.str.count(r"[A-Za-z]")
        ratio = eng_cnt / total_len.replace(0, np.nan)
        ratio = ratio.fillna(0)
        return ratio < threshold

    @staticmethod
    def _mask_repeated_word(df, col):
        """단어 반복 리뷰 (예: good good good good)"""
        return df[col].str.fullmatch(r"(\b\w+\b)( \1){4,}", na=False)

    def _build_noise_mask(self, df):
        """모든 노이즈 마스크 조합"""
        col = self.text_column

        m1 = self._create_blank_mask(df, col)
        m2 = self._mask_no_alnum(df, col)
        m3 = self._mask_numeric_only(df, col)
        m4 = self._mask_repeated_char(df, col)
        m5 = self._mask_url_or_ad(df, col)
        m6 = self._mask_low_english_ratio(df, col, self.english_threshold)
        m7 = self._mask_repeated_word(df, col)

        mask = np.logical_or.reduce([m1, m2, m3, m4, m5, m6, m7])
        return pd.Series(mask, index=df.index)

    # ============================================
    # 전처리 메인 함수들
    # ============================================

    def clean_text(self, df):
        """
        텍스트 노이즈 제거

        Parameters:
        -----------
        df : pd.DataFrame
            원본 리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            텍스트 정제된 데이터
        """
        df_proc = df.copy()

        print(f"▶ 텍스트 노이즈 제거 시작: {len(df_proc):,}개 행")

        # 노이즈 마스크 생성
        self.noise_mask = self._build_noise_mask(df_proc)

        # 노이즈 제거
        df_clean = df_proc[~self.noise_mask].copy()

        removed_count = self.noise_mask.sum()
        print(f"  - 제거된 행: {removed_count:,}개")
        print(f"  - 남은 행: {len(df_clean):,}개")

        # 텍스트 길이 제한 (선택적)
        if self.truncate_len is not None:
            df_clean[self.text_column] = df_clean[self.text_column].str.slice(
                0, self.truncate_len
            )

        self.cleaned_data = df_clean
        return df_clean

    @staticmethod
    def process_review_title(df):
        """
        review_title 전처리

        Parameters:
        -----------
        df : pd.DataFrame
            리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            전처리된 데이터
        """
        df = df.copy()

        df = df.dropna(subset=['review_title'])

        # 공백만 있는 경우 제거
        blank_mask = df['review_title'].str.strip().eq("")

        df = df[~blank_mask]

        return df

    @staticmethod
    def process_author_rating(df):
        """
        author_rating 전처리

        Parameters:
        -----------
        df : pd.DataFrame
            리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            전처리된 데이터
        """
        df = df.copy()
        # 결측치 플래그 생성
        df['author_rating_missing'] = df['author_rating'].isna().astype('Int8')

        # 결측치를 0으로 채우기
        df['author_rating'] = df['author_rating'].fillna(0).astype('float32')

        return df

    @staticmethod
    def process_helpful_columns(df):
        """
        helpful 관련 컬럼 전처리

        Parameters:
        -----------
        df : pd.DataFrame
            리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            전처리된 데이터
        """
        df = df.copy()

        help_cols = ['helpful_up_votes', 'helpful_down_votes']

        # 숫자형으로 변환
        for col in help_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['helpful_total'] = df['helpful_up_votes'] + df['helpful_down_votes']

        df['helpful_ratio'] = np.where(
            df['helpful_total'] > 0,
            df['helpful_up_votes'] / df['helpful_total'],
            0
        )

        return df

    @staticmethod
    def process_submission_date(df):
        """
        submission_date 전처리

        Parameters:
        -----------
        df : pd.DataFrame
            리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            전처리된 데이터
        """
        df = df.copy()

        # 날짜 타입 변환
        df['submission_date'] = pd.to_datetime(df['submission_date'], errors='coerce')

        # 결측치 확인
        n_missing = df['submission_date'].isna().sum()

        if n_missing > 0:
            # 결측치 제거
            df = df.dropna(subset=['submission_date'])

        return df

    # ============================================
    # 전체 파이프라인
    # ============================================

    def preprocess(self, df):
        """
        전체 리뷰 전처리 파이프라인

        Parameters:
        -----------
        df : pd.DataFrame
            원본 리뷰 데이터

        Returns:
        --------
        pd.DataFrame
            전처리 완료된 데이터
        """

        # 1. 텍스트 노이즈 제거
        df = self.clean_text(df)
        print()

        # 2. review_title 전처리
        if 'review_title' in df.columns:
            df = self.process_review_title(df)
            print()

        # 3. author_rating 전처리
        if 'author_rating' in df.columns:
            df = self.process_author_rating(df)
            print()

        # 4. helpful 컬럼 전처리
        df = self.process_helpful_columns(df)
        print()

        # 5. submission_date 전처리
        if 'submission_date' in df.columns:
            df = self.process_submission_date(df)
            print()

        self.cleaned_data = df

        return df

    def save(self, output_path):
        """
        전처리 결과 저장

        Parameters:
        -----------
        output_path : str
            출력 파일 경로 (.parquet)
        """
        if self.cleaned_data is None:
            raise ValueError("전처리된 데이터가 없습니다. preprocess()를 먼저 실행하세요.")

        self.cleaned_data.to_parquet(output_path, index=False)