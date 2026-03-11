from .config import *

def kruskal_test(data, column, target_col='hit_score', alpha=0.05, post_hoc=True):
    """
    Kruskal-Wallis H 검정 + 효과 크기 + Post-hoc 검정

    Parameters:
    -----------
    data : DataFrame
    column : str
        그룹 변수 (범주형)
    target_col : str
        종속 변수 (연속형)
    alpha : float
        유의수준
    post_hoc : bool
        사후 검정 수행 여부

    Returns:
    --------
    dict : 검정 결과
    """

    print("=" * 60)
    print(f"Kruskal-Wallis H 검정: {column} → {target_col}")
    print("=" * 60)

    # 1. 그룹별 데이터 수집
    cats = list(data[column].unique())
    cats = [c for c in cats if pd.notna(c)]  # NaN 제거

    groups = []
    group_stats = []

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

    # 그룹 수 확인
    k = len(groups)
    if k < 2:
        print(f"❌ 오류: 그룹이 {k}개뿐 (최소 2개 필요)")
        return None

    # 2. 기술 통계
    print(f"\n그룹 수: {k}")
    print(f"전체 샘플: {sum(len(g) for g in groups):,}\n")

    stats_df = pd.DataFrame(group_stats)
    print(stats_df.to_string(index=False))

    # 3. Kruskal-Wallis H 검정
    H, p_value = stats.kruskal(*groups)

    # 4. 효과 크기 계산
    N = sum(len(group) for group in groups)

    # Epsilon-squared (표준 공식)
    epsilon_squared = (H - k + 1) / (N - k)

    # Eta-squared (대안)
    eta_squared = H / (N - 1)

    # 효과 크기 해석
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

    # 5. 결론
    if p_value < alpha:
        print(f"✅ 결과: 통계적으로 유의함 (p < {alpha})")
        print(f"   → {column}에 따른 {target_col}의 중앙값 차이가 존재함")
    else:
        print(f"❌ 결과: 통계적으로 유의하지 않음 (p ≥ {alpha})")
        print(f"   → {column}에 따른 {target_col}의 중앙값 차이가 존재하지 않음")

    print("=" * 60)

    # 6. Post-hoc 검정 (Mann-Whitney U + Bonferroni 보정)
    post_hoc_results = None

    if p_value < alpha and post_hoc and k > 2:
        print("\n" + "=" * 60)
        print("Post-hoc 검정 (Pairwise Mann-Whitney U)")
        print("=" * 60)

        # Bonferroni 보정
        n_comparisons = k * (k - 1) / 2
        alpha_corrected = alpha / n_comparisons

        print(f"비교 횟수: {int(n_comparisons)}")
        print(f"보정된 α: {alpha_corrected:.4f} (Bonferroni)\n")

        post_hoc_results = []

        for i, j in combinations(range(k), 2):
            group1 = groups[i]
            group2 = groups[j]
            cat1 = cats[i]
            cat2 = cats[j]

            # Mann-Whitney U 검정
            u_stat, u_pval = stats.mannwhitneyu(
                group1, group2, alternative='two-sided'
            )

            # 중앙값 차이
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

    # 7. 반환
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