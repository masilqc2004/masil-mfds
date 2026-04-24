"""
검색 엔진 — 제조사 + 재질 기준으로 실적 매칭
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


def get_manufacturer_candidates(master: pd.DataFrame, 업소명: str) -> pd.DataFrame:
    """
    제조사명 정확 일치로 마스터에서 후보 조회.
    동명이인 다수 가능 → 모두 반환.
    """
    if master.empty or not 업소명:
        return pd.DataFrame()
    cand = master[master["업소명"] == 업소명].copy()
    return cand.reset_index(drop=True)


def get_all_materials_for_manufacturer(performance: pd.DataFrame, 업소명: str) -> list:
    """이 제조사의 실적에서 등장하는 모든 재질 목록 (중복 제거, 정렬)"""
    if performance.empty:
        return []
    sub = performance[performance["해외업소"] == 업소명]
    if sub.empty:
        return []
    materials = sorted(sub["세부품목_품목(유형)"].dropna().unique().tolist())
    return [m for m in materials if m]


def search_performance(
    performance: pd.DataFrame,
    업소명: str,
    selected_materials: list,
    valid_from: Optional[datetime] = None,
    valid_to: Optional[datetime] = None,
    years_window: int = 5,
    name_kr_keyword: str = "",
    name_en_keyword: str = "",
) -> dict:
    """
    제조사 + (재질 | 제품명) 으로 실적 검색.

    - selected_materials 가 비어있으면 재질 매칭 없이 제품명 검색만 수행
    - name_kr_keyword / name_en_keyword : 대소문자 무시 부분 문자열 포함 검색
    - 재질이 비어있을 때는 매칭된 모든 결과를 1차로 표시

    Returns:
        {
            "exact_match": DataFrame (1차: 재질 조합 완전 동일),
            "partial_match": DataFrame (2차: 1차 제외, 재질 1개 이상 포함),
            "summary": {...}
        }
    """
    empty_result = {
        "exact_match": pd.DataFrame(),
        "partial_match": pd.DataFrame(),
        "summary": {"exact_rcno_count": 0, "partial_rcno_count": 0, "by_material": {}},
    }
    if performance.empty or not 업소명:
        return empty_result

    # 해당 제조사의 모든 행
    sub = performance[performance["해외업소"] == 업소명].copy()
    if sub.empty:
        return empty_result

    # 제품명 필터링 (제조사 범위 안에서, rcno 단위로 판단)
    name_kr_keyword = (name_kr_keyword or "").strip()
    name_en_keyword = (name_en_keyword or "").strip()

    if name_kr_keyword or name_en_keyword:
        name_mask = pd.Series(True, index=sub.index)
        if name_kr_keyword:
            name_mask &= sub["제품명(한글)"].str.contains(
                name_kr_keyword, case=False, na=False, regex=False
            )
        if name_en_keyword:
            name_mask &= sub["제품명(영문)"].str.contains(
                name_en_keyword, case=False, na=False, regex=False
            )
        matching_rcnos = sub.loc[name_mask, "rcno"].unique()
        sub = sub[sub["rcno"].isin(matching_rcnos)].copy()
        if sub.empty:
            return empty_result

    # 검색 조건이 하나도 없으면 빈 결과 (제조사만으로는 검색 불가)
    if not selected_materials and not name_kr_keyword and not name_en_keyword:
        return empty_result

    # rcno별 재질 집합 만들기
    rcno_materials = sub.groupby("rcno")["세부품목_품목(유형)"].apply(
        lambda x: frozenset(m for m in x if m)
    )

    # 재질 매칭
    if selected_materials:
        selected_set = frozenset(selected_materials)
        # 1차: 완전 일치
        exact_rcnos = rcno_materials[rcno_materials == selected_set].index.tolist()
        # 2차: 1개 이상 포함 (1차 제외)
        has_any = rcno_materials.apply(lambda s: bool(s & selected_set))
        partial_rcnos = rcno_materials[has_any].index.difference(exact_rcnos).tolist()
    else:
        # 재질 없이 제품명만으로 검색 → 모든 매칭 rcno를 1차로 취급
        exact_rcnos = rcno_materials.index.tolist()
        partial_rcnos = []

    # 결과 DataFrame
    exact_df = sub[sub["rcno"].isin(exact_rcnos)].copy()
    partial_df = sub[sub["rcno"].isin(partial_rcnos)].copy()

    # 유효기간 플래그 (등록일 ≤ 신고일 ≤ 만료일)
    def add_validity_flag(df):
        if df.empty:
            df["유효기간내"] = pd.Series(dtype=bool)
            df["5년이내"] = pd.Series(dtype=bool)
            return df
        s = df["신고필증발급일자"]
        valid = pd.Series(True, index=df.index)
        if valid_from is not None:
            valid &= s >= valid_from
        if valid_to is not None:
            valid &= s <= valid_to
        df["유효기간내"] = valid
        # 5년 이내 (오늘 기준)
        cutoff_5y = datetime.now() - timedelta(days=365 * years_window)
        df["5년이내"] = s >= cutoff_5y
        return df

    exact_df = add_validity_flag(exact_df)
    partial_df = add_validity_flag(partial_df)

    # 재질별 요약 (전체 rcno 기준 — 1차+2차 합쳐서)
    all_rcnos = exact_rcnos + partial_rcnos
    all_sub = sub[sub["rcno"].isin(all_rcnos)]

    by_material = {}
    cutoff_5y = datetime.now() - timedelta(days=365 * years_window)
    for mat in selected_materials:
        mat_rows = all_sub[all_sub["세부품목_품목(유형)"] == mat]
        # 유효기간 적용
        if valid_from is not None:
            mat_rows = mat_rows[mat_rows["신고필증발급일자"] >= valid_from]
        if valid_to is not None:
            mat_rows = mat_rows[mat_rows["신고필증발급일자"] <= valid_to]

        if mat_rows.empty:
            by_material[mat] = {
                "rcno_count": 0,
                "latest": None,
                "earliest": None,
                "within_5y": False,
            }
        else:
            latest = mat_rows["신고필증발급일자"].max()
            earliest = mat_rows["신고필증발급일자"].min()
            by_material[mat] = {
                "rcno_count": mat_rows["rcno"].nunique(),
                "latest": latest,
                "earliest": earliest,
                "within_5y": latest >= cutoff_5y if pd.notna(latest) else False,
            }

    return {
        "exact_match": exact_df,
        "partial_match": partial_df,
        "summary": {
            "exact_rcno_count": len(exact_rcnos),
            "partial_rcno_count": len(partial_rcnos),
            "by_material": by_material,
        },
    }


def format_result_for_display(df: pd.DataFrame, selected_materials: list) -> pd.DataFrame:
    """
    검색 결과를 rcno 단위로 집계하여 표시용 DataFrame 생성.
    한 rcno당 한 행, 재질은 콤마 결합.
    """
    if df.empty:
        return pd.DataFrame()

    selected_set = set(selected_materials)

    grouped = df.groupby("rcno").agg(
        제품명_한글=("제품명(한글)", "first"),
        제품명_영문=("제품명(영문)", "first"),
        제조국=("제조국", "first"),
        신고일자=("신고필증발급일자", "first"),
        전체재질=("세부품목_품목(유형)", lambda x: ", ".join(sorted(set(m for m in x if m)))),
        유효기간내=("유효기간내", "first"),
        _5년이내=("5년이내", "first"),
    ).reset_index()

    # 일치 재질 / 누락 재질 표시
    def match_info(row):
        materials = set(row["전체재질"].split(", ")) if row["전체재질"] else set()
        matched = sorted(materials & selected_set)
        extra = sorted(materials - selected_set)
        return pd.Series({
            "일치재질": ", ".join(matched),
            "추가재질": ", ".join(extra),
        })

    info = grouped.apply(match_info, axis=1)
    grouped = pd.concat([grouped, info], axis=1)

    # 정렬: 5년이내 먼저, 그 안에서 최신 순
    grouped = grouped.sort_values(
        ["_5년이내", "신고일자"], ascending=[False, False]
    ).reset_index(drop=True)

    return grouped
