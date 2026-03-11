"""
카이제곱 독립성 검정 모듈

두 범주형 변수 사이에 통계적으로 유의미한 관계가 있는지 검정합니다.

카이제곱 검정이란?
  예: "장르(Action/Drama/...)"와 "흥행 여부(hit/non-hit)" 사이에
  관련성이 있는지 통계적으로 확인하는 방법입니다.

  - 귀무가설(H0): 두 변수는 독립적이다 (관계 없음)
  - 대립가설(H1): 두 변수는 독립적이지 않다 (관계 있음)
  - p-value < 0.05이면 귀무가설을 기각 → 관계 있음

Cramér's V (크래머 V)란?
  카이제곱 검정은 "관계가 있냐 없냐"만 알려줍니다.
  Cramér's V는 그 관계가 얼마나 강한지(효과 크기)를 0~1로 표현합니다.
  - 0에 가까울수록: 관계 약함
  - 1에 가까울수록: 관계 강함
"""

from .config import *


def chi_square_test(df, x_column, y_column, alpha=0.05):
    """
    두 범주형 변수 간의 독립성을 카이제곱 검정으로 확인합니다.

    처리 단계:
    1. 교차표(crosstab) 생성: 두 변수의 빈도를 행/열로 정리
    2. 카이제곱 통계량 계산
    3. Cramér's V로 효과 크기 계산
    4. 결과 해석 및 출력

    사용 예시:
        result = chi_square_test(df, x_column='genre', y_column='is_hit')
        # → 장르와 흥행 여부가 통계적으로 독립인지 검정

    Parameters:
    -----------
    df : pd.DataFrame
        분석할 데이터
    x_column : str
        독립변수 컬럼명 (예: 'genre', 'provider')
    y_column : str
        종속변수 컬럼명 (예: 'is_hit', 'hit_label')
    alpha : float
        유의수준 (기본값 0.05 = 5%)

    Returns:
    --------
    dict : 검정 결과
        - chi2: 카이제곱 통계량
        - p_value: p값
        - dof: 자유도
        - cramers_v: 효과 크기 (0~1)
        - significant: 유의미한지 여부 (bool)
        - crosstab: 교차표
    """
    import pandas as pd
    import numpy as np
    from scipy import stats

    # 1. 교차표 생성 (행: x_column, 열: y_column의 빈도 행렬)
    df_crosstab = pd.crosstab(df[x_column], df[y_column])

    # 2. 카이제곱 독립성 검정
    # expected: 두 변수가 독립일 때의 기대 빈도
    chi2, pval, dof, expected = stats.chi2_contingency(df_crosstab)

    # 3. Cramér's V 계산 (효과 크기)
    # 공식: V = sqrt(χ² / (n × min(행수-1, 열수-1)))
    n = len(df)
    k = min(df_crosstab.shape[0] - 1, df_crosstab.shape[1] - 1)
    cramers_v = np.sqrt(chi2 / (n * k))

    # 4. 효과 크기 해석 (테이블 크기에 따라 기준이 다름)
    if k == 1:
        # 2×2 테이블 기준
        if cramers_v < 0.1:
            effect_interpretation = "무시 가능 (negligible)"
        elif cramers_v < 0.3:
            effect_interpretation = "작음 (small)"
        elif cramers_v < 0.5:
            effect_interpretation = "중간 (medium)"
        else:
            effect_interpretation = "큼 (large)"
    else:
        # 더 큰 테이블 기준 (Cohen, 1988)
        if cramers_v < 0.07:
            effect_interpretation = "무시 가능 (negligible)"
        elif cramers_v < 0.21:
            effect_interpretation = "작음 (small)"
        elif cramers_v < 0.35:
            effect_interpretation = "중간 (medium)"
        else:
            effect_interpretation = "큼 (large)"

    # 5. 결과 출력
    print("=" * 60)
    print("Chi-squared 독립성 검정 결과")
    print("=" * 60)
    print(f"독립변수: {x_column}")
    print(f"종속변수: {y_column}")
    print(f"샘플 크기: {n:,}")
    print(f"교차표 크기: {df_crosstab.shape[0]} × {df_crosstab.shape[1]}")
    print("-" * 60)
    print(f"Chi-squared 통계량: {chi2:.4f}")
    print(f"자유도 (dof): {dof}")
    print(f"p-value: {pval:.6f} {'(< 0.001)' if pval < 0.001 else ''}")
    print(f"유의수준 (α): {alpha}")
    print("-" * 60)
    print(f"Cramér's V: {cramers_v:.4f}")
    print(f"효과 크기 해석: {effect_interpretation}")
    print("-" * 60)

    if pval < alpha:
        print(f"✅ 결과: 통계적으로 유의함 (p < {alpha})")
        print(f"   → {x_column}과(와) {y_column}은(는) 독립적이지 않음")
    else:
        print(f"❌ 결과: 통계적으로 유의하지 않음 (p ≥ {alpha})")
        print(f"   → {x_column}과(와) {y_column}은(는) 독립적임")

    print("=" * 60 + "\n")

    # 6. 결과 반환
    return {
        'chi2': chi2,
        'p_value': pval,
        'dof': dof,
        'cramers_v': cramers_v,
        'effect_size': cramers_v,
        'effect_interpretation': effect_interpretation,
        'significant': pval < alpha,
        'alpha': alpha,
        'crosstab': df_crosstab,
        'expected': expected
    }


def create_heatmap(data, x_columns, y_columns, size=[18, 6]):
    """
    두 범주형 변수의 비율 교차표를 히트맵으로 시각화합니다.

    각 셀은 해당 카테고리 내에서의 비율(0: 비히트 비율, 1: 히트 비율)을 나타냅니다.

    Args:
        data (pd.DataFrame): 원본 데이터
        x_columns (str): x축 컬럼명
        y_columns (str): y축 컬럼명 (0, 1 값을 가져야 함)
        size (list): 그래프 크기 [가로, 세로]
    """
    cross_df = pd.crosstab(data[x_columns], data[y_columns])

    cross_r_df = cross_df.copy()

    # 각 카테고리 내 비율로 변환 (절대 빈도 → 상대 비율)
    cross_r_df[0] = cross_df[0] / (cross_df[0] + cross_df[1])
    cross_r_df[1] = cross_df[1] / (cross_df[0] + cross_df[1])

    plt.figure(figsize=(size[0], size[1]))
    sns.heatmap(cross_r_df.transpose(), cmap="YlGnBu")
    plt.show()


def create_crosstab(data, x_columns, y_columns):
    """
    두 범주형 변수의 비율 교차표를 생성합니다.

    절대 빈도 대신 각 카테고리 내 비율로 변환하여 반환합니다.
    히트맵 없이 숫자 테이블만 필요할 때 사용합니다.

    Args:
        data (pd.DataFrame): 원본 데이터
        x_columns (str): 행 컬럼명
        y_columns (str): 열 컬럼명 (0, 1 값을 가져야 함)

    Returns:
        pd.DataFrame: 비율 교차표
    """
    cross_df = pd.crosstab(data[x_columns], data[y_columns])

    cross_r_df = cross_df.copy()

    # 각 카테고리 내 비율로 변환
    cross_r_df[0] = cross_df[0] / (cross_df[0] + cross_df[1])
    cross_r_df[1] = cross_df[1] / (cross_df[0] + cross_df[1])

    return cross_r_df
