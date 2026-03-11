from .config import *

def levene_test(data, columns, target_col='hit_score'):
    cats = list(data[columns].unique())
    groups = []

    for cat in cats:
        groups.append(
            data[data[columns] == cat][target_col].dropna()
        )

    stat, p_value = stats.levene(
        *groups
    )

    return p_value >= 0.05

def normality_test(data, columns, target_col='hit_score', sample_size=5000):
    cats = list(data[columns].unique())

    for cat in cats:
        group_data = data[data[columns] == cat][target_col].dropna()

        # 샘플이 너무 크면 일부만 추출
        if len(group_data) > sample_size:
            group_data = group_data.sample(n=sample_size, random_state=42)

        # 샘플이 너무 작으면 테스트 불가
        if len(group_data) < 3:
            print(f"경고: {cat} 그룹의 샘플 크기가 너무 작음 ({len(group_data)})")
            continue

        stat, p_value = stats.shapiro(group_data)
        if p_value < 0.05:
            return False

    return True

def pre_test(data, columns, target_col='hit_score'):
    levene_result = levene_test(data, columns, target_col)

    if levene_result:
        print("등분산성을 만족함")
    else:
        print("등분산성을 만족하지 않음")

    shapiro_result = normality_test(data, columns, target_col)

    if shapiro_result:
        print("정규성을 만족함")
    else:
        print("정규성을 만족하지 않음")