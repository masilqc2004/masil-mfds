"""
식약처 기구·용기·포장 실적 조회 시스템
Streamlit 웹앱 - 검역대행 마실
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from data_loader import sync_from_drive, load_performance, load_master
from search_engine import (
    get_manufacturer_candidates,
    get_all_materials_for_manufacturer,
    search_performance,
    format_result_for_display,
)

# ─── 페이지 설정 ──────────────────────────────────────────
st.set_page_config(
    page_title="기구·용기·포장 실적 조회 | 검역대행 마실",
    page_icon="🔍",
    layout="wide",
)


# ─── 데이터 로딩 (1일 1회 캐시) ────────────────────────────
@st.cache_resource(ttl=86400, show_spinner="데이터 로드 중... (최초 실행 시 1~2분 소요)")
def get_data(force: bool = False):
    sync_info = sync_from_drive(force=force)
    perf = load_performance(
        xlsx_paths=sync_info["performance_files"],
        parquet_paths=sync_info.get("performance_fixed_files", []),
    )
    master = load_master(sync_info["master_file"])
    return perf, master, sync_info


# ─── 사이드바 ────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🏢 검역대행 마실")
    st.caption("기구·용기·포장 실적 조회 시스템")

    st.markdown("---")
    st.markdown("### ⚙️ 시스템")

    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("### 📖 사용 안내")
    st.markdown(
        """
1. **제조사명**을 식약처에 등록된 그대로 입력  
   (띄어쓰기·구두점 정확히)
2. **재질** 또는 **제품명** 입력 (둘 중 하나 이상)
3. 결과 확인
   - **1차**: 재질 조합 완전 일치
   - **2차**: 일부 재질만 매칭

⚠️ 색상 정보는 식약처 공개 데이터에 없어 사용자 직접 판단 필요
        """
    )

# ─── 데이터 로드 ─────────────────────────────────────────
try:
    performance, master, sync_info = get_data()
except Exception as e:
    st.error(f"데이터 로드 실패: {e}")
    st.stop()

if performance.empty or master.empty:
    st.warning("실적 데이터 또는 제조사 마스터가 비어있습니다.")
    st.stop()

# ─── 헤더 (회사 브랜딩) ──────────────────────────────────
st.markdown(
    """
    <div style="padding: 15px 20px; background: linear-gradient(90deg, #1e3a5f, #2b5876); border-radius: 8px; margin-bottom: 20px;">
        <h1 style="color: white; margin: 0; font-size: 26px;">🔍 기구·용기·포장 실적 조회</h1>
        <p style="color: #c8d6e5; margin: 5px 0 0 0; font-size: 14px;">검역대행 마실 | 식약처 공개 실적 데이터 기반</p>
    </div>
    """,
    unsafe_allow_html=True,
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("등록 제조사", f"{master['업소명'].nunique():,}")
col2.metric("실적 신고건수", f"{performance['rcno'].nunique():,}")
col3.metric("실적 데이터 행", f"{len(performance):,}")
date_min = performance["신고필증발급일자"].min()
date_max = performance["신고필증발급일자"].max()
if pd.notna(date_min) and pd.notna(date_max):
    col4.metric("데이터 기간", f"{date_min:%Y.%m} ~ {date_max:%Y.%m}")

st.caption(f"마지막 동기화: {sync_info.get('last_sync', '-')}")
st.markdown("---")


# ─── 입력 영역 ──────────────────────────────────────────
st.subheader("🏭 1단계 — 제조사 입력")

input_col, btn_col = st.columns([5, 1])
with input_col:
    업소명_input = st.text_input(
        "제조사명 (식약처 등록 그대로 정확히 입력)",
        placeholder="예: HEBEI ML GLASSWARE CO.,LTD",
        key="manufacturer_input",
    )
with btn_col:
    st.markdown("<br>", unsafe_allow_html=True)
    search_clicked = st.button("🔎 조회", use_container_width=True, type="primary")


# ─── 제조사 후보 표시 ─────────────────────────────────────
if not 업소명_input.strip():
    st.info("👆 제조사명을 입력하고 조회 버튼을 눌러주세요.")
    st.stop()

업소명 = 업소명_input.strip()
candidates = get_manufacturer_candidates(master, 업소명)

if candidates.empty:
    st.error(f"⚠️ 등록된 제조사를 찾을 수 없습니다: **{업소명}**")
    st.markdown(
        "**확인 사항:**\n"
        "- 식약처 등록명과 띄어쓰기·구두점·대소문자가 정확히 일치해야 합니다\n"
        "- 식약처 [수입식품정보마루](https://impfood.mfds.go.kr/)에서 정확한 명칭을 확인하세요"
    )
    st.stop()

# "후보" 삭제 (요청사항 #1)
st.markdown(f"### ✅ 제조사 ({len(candidates)}건)")

if len(candidates) > 1:
    st.warning(
        f"⚠️ 동일한 업소명을 가진 제조사가 **{len(candidates)}곳** 존재합니다. "
        "주소·등록일을 확인하여 본인이 수입할 제조사가 어느 것인지 판단해주세요."
    )

today = pd.Timestamp.now().normalize()

# 후보 표시 (요청사항 #2: 업소명 표시, 영업종류·비고 제거)
for idx, row in candidates.iterrows():
    expired = pd.notna(row["만료일"]) and row["만료일"] < today
    needs_renewal = "갱신필요" in str(row.get("비고", ""))

    status_badges = []
    if expired:
        status_badges.append("🔴 만료")
    if needs_renewal:
        status_badges.append("🟡 갱신필요")
    if not expired and not needs_renewal:
        status_badges.append("🟢 유효")

    label = (
        f"{' '.join(status_badges)} | "
        f"**{row['업소명']}** | "
        f"**{row['업소코드']}** | {row['국가']} | "
        f"등록 {row['등록일']:%Y-%m-%d} ~ 만료 {row['만료일']:%Y-%m-%d}"
    )
    with st.expander(label, expanded=(len(candidates) == 1)):
        st.markdown(f"**업소명**: {row['업소명']}")
        st.markdown(f"**업소코드**: {row['업소코드']}")
        st.markdown(f"**국가**: {row['국가']}")
        st.markdown(f"**주소**: {row['주소']}")
        st.markdown(f"**등록일**: {row['등록일']:%Y-%m-%d}")
        st.markdown(f"**만료일**: {row['만료일']:%Y-%m-%d}")

# 사용자가 어느 후보를 사용할지 선택
if len(candidates) > 1:
    selected_candidate_idx = st.radio(
        "**어느 제조사로 조회하시겠습니까?**",
        options=list(range(len(candidates))),
        format_func=lambda i: f"{candidates.iloc[i]['업소코드']} ({candidates.iloc[i]['국가']}, "
                              f"{candidates.iloc[i]['등록일']:%Y-%m-%d} ~ {candidates.iloc[i]['만료일']:%Y-%m-%d})",
        key="candidate_radio",
    )
    selected_row = candidates.iloc[selected_candidate_idx]
else:
    selected_row = candidates.iloc[0]

valid_from = selected_row["등록일"] if pd.notna(selected_row["등록일"]) else None
valid_to = selected_row["만료일"] if pd.notna(selected_row["만료일"]) else None

if pd.notna(selected_row["만료일"]) and selected_row["만료일"] < today:
    st.error(f"🔴 이 제조사는 **{selected_row['만료일']:%Y-%m-%d}**에 만료되었습니다. 새로운 제품 수입이 불가능합니다.")
if "갱신필요" in str(selected_row.get("비고", "")):
    st.warning("🟡 이 제조사는 **갱신필요** 상태입니다. 갱신 전까지 신규 수입이 불가능할 수 있습니다.")

st.markdown("---")

# ─── 재질 + 제품명 입력 (요청사항 #4) ─────────────────────
st.subheader("🧱 2단계 — 재질 및/또는 제품명 입력")
st.caption("재질·제품명 중 **최소 하나 이상** 입력해주세요. 둘 다 입력하면 두 조건을 모두 만족하는 결과만 나옵니다.")

available_materials = get_all_materials_for_manufacturer(performance, 업소명)

if not available_materials:
    st.warning(
        f"이 제조사 (**{업소명}**) 의 실적이 데이터에 없습니다. "
        "최근 신규 등록된 제조사이거나 아직 수입 실적이 없을 수 있습니다."
    )
    st.stop()

# 재질
st.markdown("**재질 선택** (선택사항)")
st.caption(f"이 제조사의 과거 실적에서 등장한 재질: {len(available_materials)}종")

material_mode = st.radio(
    "재질 입력 방식",
    ["📋 이 제조사의 기존 재질에서 선택", "✍️ 전체 재질 목록에서 선택 (처음 등록하는 재질 포함)"],
    horizontal=True,
    label_visibility="collapsed",
)

if material_mode == "📋 이 제조사의 기존 재질에서 선택":
    selected_materials = st.multiselect(
        "재질",
        options=available_materials,
        placeholder="재질을 선택하세요... (선택 안 해도 됩니다)",
        label_visibility="collapsed",
    )
else:
    all_materials = sorted(performance["세부품목_품목(유형)"].dropna().unique().tolist())
    all_materials = [m for m in all_materials if m]
    selected_materials = st.multiselect(
        "재질",
        options=all_materials,
        placeholder="재질을 검색/선택하세요... (선택 안 해도 됩니다)",
        label_visibility="collapsed",
    )

# 제품명 (요청사항 #4)
st.markdown("**제품명 검색** (선택사항 — 입력한 단어가 포함되어 있으면 검색됨)")
name_col1, name_col2 = st.columns(2)
with name_col1:
    name_kr_keyword = st.text_input(
        "한글 제품명",
        placeholder="예: 주방용품",
        key="name_kr",
    )
with name_col2:
    name_en_keyword = st.text_input(
        "영문 제품명",
        placeholder="예: tea pot",
        key="name_en",
    )

# 최소 하나의 조건 필요
if not selected_materials and not name_kr_keyword.strip() and not name_en_keyword.strip():
    st.info("👆 재질 또는 제품명 중 **최소 하나**를 입력해주세요.")
    st.stop()

st.markdown("---")

# ─── 검색 실행 ───────────────────────────────────────────
st.subheader("📊 3단계 — 검색 결과")

search_conditions = []
if selected_materials:
    search_conditions.append(f"재질: {', '.join(selected_materials)}")
if name_kr_keyword.strip():
    search_conditions.append(f"한글 제품명: '{name_kr_keyword.strip()}'")
if name_en_keyword.strip():
    search_conditions.append(f"영문 제품명: '{name_en_keyword.strip()}'")
st.caption("**검색 조건**: " + " · ".join(search_conditions))

with st.spinner("검색 중..."):
    result = search_performance(
        performance=performance,
        업소명=업소명,
        selected_materials=selected_materials,
        valid_from=valid_from,
        valid_to=valid_to,
        years_window=5,
        name_kr_keyword=name_kr_keyword,
        name_en_keyword=name_en_keyword,
    )

# ─── 재질별 요약 ─────────────────────────────────────────
if selected_materials:
    st.markdown("#### 🎯 재질별 실적 현황 (제조사 유효기간 내)")

    summary_rows = []
    for mat, info in result["summary"]["by_material"].items():
        if info["rcno_count"] > 0:
            status = "✅ 5년 이내" if info["within_5y"] else "⚠️ 5년 초과"
            latest = f"{info['latest']:%Y-%m-%d}" if info["latest"] else "-"
        else:
            status = "❌ 실적 없음"
            latest = "-"
        summary_rows.append({
            "재질": mat,
            "실적 건수(rcno)": info["rcno_count"],
            "최근 실적일": latest,
            "상태": status,
        })

    summary_df = pd.DataFrame(summary_rows)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    all_have_5y = all(info["within_5y"] for info in result["summary"]["by_material"].values())
    none_have = all(info["rcno_count"] == 0 for info in result["summary"]["by_material"].values())

    if none_have:
        st.error("❌ 선택한 모든 재질에 대해 이 제조사의 유효기간 내 실적이 **없습니다**.")
    elif all_have_5y:
        st.success("✅ 선택한 **모든 재질**에 대해 5년 이내 실적이 존재합니다. 정밀검사 면제 가능성이 높습니다. (단, 색상 일치 여부는 별도 확인 필요)")
    else:
        st.warning("⚠️ 일부 재질만 5년 이내 실적이 있습니다. 누락된 재질은 정밀검사가 필요할 수 있습니다.")

    st.markdown("---")
else:
    summary_df = pd.DataFrame()

# ─── 1차 결과 ────────────────────────────────────────────
exact_count = result["summary"]["exact_rcno_count"]

if selected_materials:
    st.markdown(f"#### 🥇 1차 — 재질 조합 완전 일치 ({exact_count}건)")
    st.caption("선택하신 재질 조합과 **완전히 동일한** 실적입니다. 같은 제품일 가능성이 매우 높습니다.")
else:
    st.markdown(f"#### 🔎 제품명 검색 결과 ({exact_count}건)")
    st.caption("제품명 조건을 만족하는 모든 실적입니다.")

if exact_count == 0:
    st.info("일치하는 실적이 없습니다.")
else:
    exact_display = format_result_for_display(result["exact_match"], selected_materials)

    for _, row in exact_display.iterrows():
        within_5y = row["_5년이내"]
        valid = row["유효기간내"]

        if not valid:
            container_style = "background-color: #f0f0f0; padding: 10px; border-radius: 5px; opacity: 0.6;"
            warning = "⚠️ 제조사 유효기간 외 실적"
        elif not within_5y:
            container_style = "background-color: #fff8e1; padding: 10px; border-radius: 5px;"
            warning = "⚠️ 5년 초과 (실적 인정 불가)"
        else:
            container_style = "background-color: #e8f5e9; padding: 10px; border-radius: 5px;"
            warning = "✅ 5년 이내 유효 실적"

        st.markdown(
            f"""
<div style="{container_style}">
<b>rcno {row['rcno']}</b> · {row['신고일자']:%Y-%m-%d} · {row['제조국']} · {warning}<br>
<b>제품명(한글)</b>: {row['제품명_한글']}<br>
<b>제품명(영문)</b>: {row['제품명_영문']}<br>
<b>재질</b>: {row['전체재질']}
</div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("")

# ─── 2차 결과 ────────────────────────────────────────────
partial_count = result["summary"]["partial_rcno_count"]

if selected_materials:
    st.markdown(f"#### 🥈 2차 — 재질 부분 일치 ({partial_count}건)")
    st.caption("선택하신 재질 중 **일부만 포함**된 실적입니다. 각 재질별 실적 확보 용도로 활용할 수 있습니다.")

    if partial_count == 0:
        st.info("부분 일치하는 추가 실적이 없습니다.")
    else:
        partial_display = format_result_for_display(result["partial_match"], selected_materials)

        PAGE_SIZE = 50
        total = len(partial_display)
        if total > PAGE_SIZE:
            max_page = (total - 1) // PAGE_SIZE + 1
            page = st.number_input(f"페이지 (1~{max_page})", min_value=1, max_value=max_page, value=1)
            start = (page - 1) * PAGE_SIZE
            end = start + PAGE_SIZE
            partial_display_page = partial_display.iloc[start:end]
            st.caption(f"전체 {total}건 중 {start+1}~{min(end, total)} 표시")
        else:
            partial_display_page = partial_display

        # 요청사항 #3: 영문 제품명도 포함
        display_df = partial_display_page.copy()
        display_df["신고일자"] = display_df["신고일자"].dt.strftime("%Y-%m-%d")
        display_df["상태"] = display_df.apply(
            lambda r: "🔴 기간외" if not r["유효기간내"]
            else ("🟡 5년초과" if not r["_5년이내"] else "🟢 유효"),
            axis=1,
        )
        display_df = display_df[[
            "상태", "rcno", "신고일자", "제품명_한글", "제품명_영문",
            "일치재질", "추가재질", "제조국"
        ]].rename(columns={
            "제품명_한글": "제품명(한글)",
            "제품명_영문": "제품명(영문)",
            "일치재질": "일치 재질",
            "추가재질": "추가 재질",
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# ─── 다운로드 ────────────────────────────────────────────
st.markdown("---")
st.markdown("#### 💾 결과 다운로드")

if exact_count > 0 or partial_count > 0:
    import io
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if exact_count > 0:
            sheet_name = "1차_완전일치" if selected_materials else "검색결과"
            format_result_for_display(result["exact_match"], selected_materials).to_excel(
                writer, sheet_name=sheet_name, index=False
            )
        if partial_count > 0:
            format_result_for_display(result["partial_match"], selected_materials).to_excel(
                writer, sheet_name="2차_부분일치", index=False
            )
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="재질별_요약", index=False)
    buffer.seek(0)

    st.download_button(
        label="📥 검색 결과 엑셀 다운로드",
        data=buffer,
        file_name=f"실적조회_{업소명[:30]}_{datetime.now():%Y%m%d_%H%M}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ─── 푸터 ────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    """
<div style="margin-top: 30px; padding: 20px; background-color: #f8f9fa; border-left: 4px solid #dc3545; border-radius: 4px; font-size: 13px; color: #495057;">
<b>⚖️ 저작권 및 이용 안내</b><br><br>
본 시스템은 <b>검역대행 마실</b>이 자체 개발한 실적 조회 도구입니다.<br>
본 프로그램의 <b>무단 복제·배포·상업적 이용</b> 시 <b>저작권법 및 관련 법령에 따라 민·형사상 책임</b>을 물을 수 있습니다.<br>
또한 본 조회 결과는 참고용이며, 실제 수입 통관 시에는 반드시 관할 관청의 공식 판단을 확인하시기 바랍니다.<br><br>
© 2026 검역대행 마실 (Masil). All rights reserved.
</div>
    """,
    unsafe_allow_html=True,
)
