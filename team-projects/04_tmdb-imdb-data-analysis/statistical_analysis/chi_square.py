from .config import *

def chi_square_test(df, x_column, y_column, alpha=0.05):
    """
    Chi-squared 독립성 검정 + Cramér's V 효과 크기

    Parameters:
    -----------
    df : DataFrame
    x_column : str
        독립변수 컬럼명
    y_column : str
        종속변수 컬럼명
    alpha : float
        유의수준 (기본값 0.05)

    Returns:
    --------
    dict : 검정 결과
    """
    import pandas as pd
    import numpy as np
    from scipy import stats

    # 1. 교차표 생성
    df_crosstab = pd.crosstab(df[x_column], df[y_column])

    # 2. Chi-squared 검정
    chi2, pval, dof, expected = stats.chi2_contingency(df_crosstab)

    # 3. Cramér's V 계산 (효과 크기)
    n = len(df)
    k = min(df_crosstab.shape[0] - 1, df_crosstab.shape[1] - 1)
    cramers_v = np.sqrt(chi2 / (n * k))

    # 4. 효과 크기 해석
    if k == 1:
        # 2x2 테이블
        if cramers_v < 0.1:
            effect_interpretation = "무시 가능 (negligible)"
        elif cramers_v < 0.3:
            effect_interpretation = "작음 (small)"
        elif cramers_v < 0.5:
            effect_interpretation = "중간 (medium)"
        else:
            effect_interpretation = "큼 (large)"
    else:
        # 더 큰 테이블
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
        'effect_size': cramers_v,  # 효과 크기 = Cramér's V
        'effect_interpretation': effect_interpretation,
        'significant': pval < alpha,
        'alpha': alpha,
        'crosstab': df_crosstab,
        'expected': expected
    }

def create_heatmap(data, x_columns, y_columns, size=[18, 6]):
    cross_df = pd.crosstab(data[x_columns], data[y_columns])

    cross_r_df = cross_df.copy()

    cross_r_df[0] = cross_df[0] / (cross_df[0] + cross_df[1])
    cross_r_df[1] = cross_df[1] / (cross_df[0] + cross_df[1])

    plt.figure(figsize=(size[0],size[1]))
    sns.heatmap(cross_r_df.transpose(), cmap="YlGnBu")
    plt.show()

def create_crosstab(data, x_columns, y_columns):
    cross_df = pd.crosstab(data[x_columns], data[y_columns])

    cross_r_df = cross_df.copy()

    cross_r_df[0] = cross_df[0] / (cross_df[0] + cross_df[1])
    cross_r_df[1] = cross_df[1] / (cross_df[0] + cross_df[1])

    return cross_r_df