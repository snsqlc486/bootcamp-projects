"""
통계 검정 전제조건 확인 모듈

ANOVA와 같은 모수적 검정(parametric test)을 사용하기 전에
데이터가 해당 검정의 전제조건을 만족하는지 확인합니다.

두 가지 전제조건:
1. 정규성(Normality): 각 그룹의 데이터가 정규분포를 따르는가?
   → Shapiro-Wilk 검정 사용

2. 등분산성(Homogeneity of Variance): 모든 그룹의 분산이 같은가?
   → Levene 검정 사용

전제조건을 만족하지 않으면?
   → 비모수 검정(non-parametric test)을 사용해야 합니다.
   → 이 프로젝트에서는 Kruskal-Wallis 검정(kruskal.py)을 사용합니다.
"""

from .config import *


def levene_test(data, columns, target_col='hit_score'):
    """
    Levene 검정으로 등분산성을 확인합니다.

    등분산성이란?
    - 여러 그룹의 분산(데이터 퍼짐 정도)이 동일한지 테스트합니다.
    - 예: '장르별 히트 점수' 분석 시, 액션 장르와 드라마 장르의 분산이 같은가?

    해석:
    - p-value >= 0.05: 등분산성 만족 (그룹 간 분산 차이 없음)
    - p-value < 0.05: 등분산성 불만족 (그룹 간 분산에 유의미한 차이 있음)

    Args:
        data (pd.DataFrame): 분석할 데이터
        columns (str): 그룹을 나누는 기준 컬럼명 (예: 'genre')
        target_col (str): 비교할 수치 컬럼명 (기본값: 'hit_score')

    Returns:
        bool: True = 등분산성 만족, False = 불만족
    """
    cats = list(data[columns].unique())
    groups = []

    # 각 카테고리(그룹)별 hit_score 데이터 수집
    for cat in cats:
        groups.append(
            data[data[columns] == cat][target_col].dropna()
        )

    # Levene 검정 실행 (*groups는 각 그룹을 개별 인자로 전달)
    stat, p_value = stats.levene(*groups)

    # p-value가 0.05 이상이면 등분산성 만족
    return p_value >= 0.05


def normality_test(data, columns, target_col='hit_score', sample_size=5000):
    """
    Shapiro-Wilk 검정으로 각 그룹의 정규성을 확인합니다.

    정규성이란?
    - 데이터가 정규분포(종 모양 분포)를 따르는지 테스트합니다.
    - ANOVA, t-검정 등 모수적 방법의 기본 전제입니다.

    해석:
    - 모든 그룹에서 p-value >= 0.05: 정규성 만족
    - 하나라도 p-value < 0.05: 정규성 불만족

    Shapiro-Wilk 검정의 한계:
    - 샘플이 5,000개를 초과하면 매우 작은 차이도 유의미하게 검출됨
    - 따라서 샘플이 클 경우 5,000개를 무작위 추출하여 검정

    Args:
        data (pd.DataFrame): 분석할 데이터
        columns (str): 그룹을 나누는 기준 컬럼명
        target_col (str): 비교할 수치 컬럼명 (기본값: 'hit_score')
        sample_size (int): 최대 샘플 수 (기본값: 5000)

    Returns:
        bool: True = 모든 그룹 정규성 만족, False = 하나라도 불만족
    """
    cats = list(data[columns].unique())

    for cat in cats:
        group_data = data[data[columns] == cat][target_col].dropna()

        # 샘플이 너무 크면 일부만 무작위 추출 (검정 민감도 조절)
        if len(group_data) > sample_size:
            group_data = group_data.sample(n=sample_size, random_state=42)

        # 3개 미만은 통계 검정 불가
        if len(group_data) < 3:
            print(f"경고: {cat} 그룹의 샘플 크기가 너무 작음 ({len(group_data)})")
            continue

        # Shapiro-Wilk 검정
        stat, p_value = stats.shapiro(group_data)

        if p_value < 0.05:
            # 하나의 그룹이라도 정규성 불만족이면 False 반환
            return False

    return True


def pre_test(data, columns, target_col='hit_score'):
    """
    통계 검정 전에 등분산성과 정규성을 한꺼번에 확인합니다.

    두 조건 모두 만족 → ANOVA 등 모수적 검정 사용 가능
    하나라도 불만족 → Kruskal-Wallis 등 비모수 검정 사용

    Args:
        data (pd.DataFrame): 분석할 데이터
        columns (str): 그룹을 나누는 기준 컬럼명
        target_col (str): 비교할 수치 컬럼명 (기본값: 'hit_score')
    """
    # 등분산성 확인
    levene_result = levene_test(data, columns, target_col)

    if levene_result:
        print("등분산성을 만족함")
    else:
        print("등분산성을 만족하지 않음")

    # 정규성 확인
    shapiro_result = normality_test(data, columns, target_col)

    if shapiro_result:
        print("정규성을 만족함")
    else:
        print("정규성을 만족하지 않음")
