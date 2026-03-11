"""
Kruskal-Wallis H 검정 모듈

세 개 이상의 그룹 간 중앙값 차이가 통계적으로 유의미한지 검정합니다.

Kruskal-Wallis 검정이란?
  ANOVA(분산분석)의 비모수(non-parametric) 대안입니다.
  데이터가 정규분포를 따르지 않아도 사용할 수 있습니다.

  예: "OTT 플랫폼(Netflix, Disney+, Hulu,...)"에 따라
  히트 점수의 중앙값이 다른지 검정합니다.

  - 귀무가설(H0): 모든 그룹의 중앙값이 같다
  - 대립가설(H1): 적어도 하나의 그룹 중앙값이 다르다

Post-hoc(사후) 검정이란?
  Kruskal-Wallis가 "어떤 그룹이 다른지" 알려주지 않습니다.
  Mann-Whitney U 검정으로 그룹 쌍을 비교하여 어떤 그룹이 다른지 찾습니다.

Bonferroni 보정이란?
  여러 번 검정하면 우연히 유의미한 결과가 나올 확률이 증가합니다.
  비교 횟수만큼 유의수준을 나눠서(α/n) 오류를 보정합니다.
"""

from .config import *


def kruskal_test(data, column, target_col='hit_score', alpha=0.05, post_hoc=True):
    """
    Kruskal-Wallis H 검정, 효과 크기 계산, 사후 검정을 수행합니다.

    처리 단계:
    1. 그룹별 데이터 수집 및 기술 통계
    2. Kruskal-Wallis H 검정
    3. 효과 크기 계산 (Epsilon-squared)
    4. 결론 출력
    5. 유의미한 경우 Post-hoc 검정 (Mann-Whitney U + Bonferroni)

    사용 예시:
        result = kruskal_test(df, column='provider', target_col='hit_score')
        # → OTT 플랫폼에 따라 히트 점수 중앙값이 다른지 검정

    Parameters:
    -----------
    data : pd.DataFrame
        분석할 데이터
    column : str
        그룹 변수 컬럼명 (범주형, 예: 'genre', 'provider')
    target_col : str
        비교할 수치 컬럼명 (기본값: 'hit_score')
    alpha : float
        유의수준 (기본값: 0.05)
    post_hoc : bool
        사후 검정 수행 여부 (기본값: True)

    Returns:
    --------
    dict : 검정 결과
        - H: H 통계량
        - p_value: p값
        - epsilon_squared: 효과 크기
        - significant: 유의미한지 여부
        - group_stats: 그룹별 기술 통계
        - post_hoc: 사후 검정 결과 (리스트)
    """
    print("=" * 60)
    print(f"Kruskal-Wallis H 검정: {column} → {target_col}")
    print("=" * 60)

    # 1. 그룹별 데이터 수집
    cats = list(data[column].unique())
    cats = [c for c in cats if pd.notna(c)]  # NaN 그룹 제외

    groups = []      # 각 그룹의 target_col 값 리스트
    group_stats = [] # 각 그룹의 기술 통계

    for cat in cats:
        group_data = data[data[column] == cat][target_col].dropna()

        if len(group_data) == 0:
            print(f"⚠️  경고: '{cat}' 그룹이 비어있음 (제외)")
            continue

        groups.append(group_data)
        group_stats.append({
            'category': cat,
            'n': len(group_data),
            'median': group_data.median(),
            'mean': group_data.mean(),
            'std': group_data.std(),
            'min': group_data.min(),
            'max': group_data.max()
        })

    # 그룹이 2개 미만이면 검정 불가
    k = len(groups)
    if k < 2:
        print(f"❌ 오류: 그룹이 {k}개뿐 (최소 2개 필요)")
        return None

    # 2. 기술 통계 출력
    print(f"\n그룹 수: {k}")
    print(f"전체 샘플: {sum(len(g) for g in groups):,}\n")

    stats_df = pd.DataFrame(group_stats)
    print(stats_df.to_string(index=False))

    # 3. Kruskal-Wallis H 검정
    # *groups: 각 그룹의 데이터를 개별 인자로 전달
    H, p_value = stats.kruskal(*groups)

    # 4. 효과 크기 계산
    N = sum(len(group) for group in groups)  # 전체 샘플 수

    # Epsilon-squared: 전체 분산 중 그룹 간 차이가 설명하는 비율
    # 0~1 범위, 클수록 그룹 간 차이가 큼
    epsilon_squared = (H - k + 1) / (N - k)

    # Eta-squared: 또 다른 효과 크기 지표 (참고용)
    eta_squared = H / (N - 1)

    # 효과 크기 해석 기준 (Tomczak & Tomczak, 2014)
    if epsilon_squared < 0.01:
        effect_interpretation = "무시 가능 (negligible)"
    elif epsilon_squared < 0.06:
        effect_interpretation = "작음 (small)"
    elif epsilon_squared < 0.14:
        effect_interpretation = "중간 (medium)"
    else:
        effect_interpretation = "큼 (large)"

    print("\n" + "-" * 60)
    print("검정 결과:")
    print("-" * 60)
    print(f"H 통계량: {H:.4f}")
    print(f"p-value: {p_value:.6f}")
    print(f"자유도: {k - 1}")
    print(f"유의수준: {alpha}")
    print("-" * 60)
    print(f"Epsilon-squared: {epsilon_squared:.4f}")
    print(f"효과 크기 해석: {effect_interpretation}")
    print("-" * 60)

    # 5. 최종 결론
    if p_value < alpha:
        print(f"✅ 결과: 통계적으로 유의함 (p < {alpha})")
        print(f"   → {column}에 따른 {target_col}의 중앙값 차이가 존재함")
    else:
        print(f"❌ 결과: 통계적으로 유의하지 않음 (p ≥ {alpha})")
        print(f"   → {column}에 따른 {target_col}의 중앙값 차이가 존재하지 않음")

    print("=" * 60)

    # 6. Post-hoc 검정: 어떤 그룹 쌍이 다른지 확인
    # 조건: 전체 검정이 유의미하고, 그룹이 3개 이상일 때만 수행
    post_hoc_results = None

    if p_value < alpha and post_hoc and k > 2:
        print("\n" + "=" * 60)
        print("Post-hoc 검정 (Pairwise Mann-Whitney U)")
        print("=" * 60)

        # Bonferroni 보정: 비교 횟수만큼 유의수준을 낮춤
        # 예: 5개 그룹 → 10번 비교 → α = 0.05/10 = 0.005
        n_comparisons = k * (k - 1) / 2
        alpha_corrected = alpha / n_comparisons

        print(f"비교 횟수: {int(n_comparisons)}")
        print(f"보정된 α: {alpha_corrected:.4f} (Bonferroni)\n")

        post_hoc_results = []

        # 모든 그룹 쌍 조합에 대해 Mann-Whitney U 검정
        for i, j in combinations(range(k), 2):
            group1 = groups[i]
            group2 = groups[j]
            cat1 = cats[i]
            cat2 = cats[j]

            # Mann-Whitney U 검정: 두 그룹의 순위 분포가 같은지 검정
            u_stat, u_pval = stats.mannwhitneyu(
                group1, group2, alternative='two-sided'
            )

            # 두 그룹의 중앙값 차이
            median_diff = group1.median() - group2.median()

            significant = "✅" if u_pval < alpha_corrected else "  "

            print(f"{significant} {cat1} vs {cat2}:")
            print(f"     p-value: {u_pval:.6f}, "
                  f"중앙값 차이: {median_diff:+.2f}")

            post_hoc_results.append({
                'group1': cat1,
                'group2': cat2,
                'u_statistic': u_stat,
                'p_value': u_pval,
                'median_diff': median_diff,
                'significant': u_pval < alpha_corrected
            })

        print("\n" + "=" * 60)

    # 7. 결과 반환
    return {
        'H': H,
        'p_value': p_value,
        'dof': k - 1,
        'epsilon_squared': epsilon_squared,
        'eta_squared': eta_squared,
        'effect_interpretation': effect_interpretation,
        'significant': p_value < alpha,
        'groups': cats,
        'group_stats': stats_df,
        'post_hoc': post_hoc_results
    }
