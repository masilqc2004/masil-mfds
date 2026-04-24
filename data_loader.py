"""
데이터 로더 — 구글 드라이브에서 실적/제조사 마스터 엑셀 다운로드 및 캐싱
"""
import os
import io
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_META = CACHE_DIR / "_meta.json"

# 파일명 패턴 (실적: '기구' 포함, 마스터: '해외제조업소' 포함)
PERFORMANCE_NAME_HINT = "기구"
MASTER_NAME_HINT = "해외제조업소"


def _get_config():
    """
    설정값 (폴더 ID, 서비스 계정 정보) 로드.
    우선순위: Streamlit secrets → 환경변수 → 빈 값
    """
    perf_id = ""
    master_id = ""
    sa_info = None
    key_file_path = None

    # 1. Streamlit secrets 에서 읽기
    try:
        import streamlit as st
        if "MFDS_PERFORMANCE_FOLDER_ID" in st.secrets:
            perf_id = st.secrets["MFDS_PERFORMANCE_FOLDER_ID"]
        if "MFDS_MASTER_FOLDER_ID" in st.secrets:
            master_id = st.secrets["MFDS_MASTER_FOLDER_ID"]
        # 서비스 계정 정보 읽기 (3가지 방식 지원)
        if "gcp_service_account" in st.secrets:
            # 방식 A: toml 안에 전체 JSON 내용을 [gcp_service_account] 섹션으로
            sa_info = dict(st.secrets["gcp_service_account"])
        elif "GCP_KEY_FILE" in st.secrets:
            # 방식 B: 로컬 JSON 파일 경로
            key_file_path = st.secrets["GCP_KEY_FILE"]
    except Exception:
        pass

    # 2. 환경변수에서 읽기 (보완)
    if not perf_id:
        perf_id = os.environ.get("MFDS_PERFORMANCE_FOLDER_ID", "")
    if not master_id:
        master_id = os.environ.get("MFDS_MASTER_FOLDER_ID", "")
    if sa_info is None and key_file_path is None:
        if os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
            sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
        elif os.environ.get("GCP_KEY_FILE"):
            key_file_path = os.environ["GCP_KEY_FILE"]

    # 3. 키 파일 경로가 지정되었다면 파일 읽어서 sa_info 로 변환
    if sa_info is None and key_file_path:
        key_path = Path(key_file_path)
        if not key_path.is_absolute():
            key_path = Path(__file__).parent / key_path
        if key_path.exists():
            with open(key_path, "r", encoding="utf-8") as f:
                sa_info = json.load(f)

    return {
        "performance_folder_id": perf_id,
        "master_folder_id": master_id,
        "service_account_info": sa_info,
    }


def _get_drive_service():
    """서비스 계정으로 구글 드라이브 클라이언트 생성"""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    cfg = _get_config()
    sa_info = cfg["service_account_info"]
    if sa_info is None:
        raise RuntimeError(
            "서비스 계정 정보가 없습니다. secrets.toml 에 [gcp_service_account] 섹션 또는 "
            "GCP_KEY_FILE 경로를 설정하거나, 환경변수 GCP_SERVICE_ACCOUNT_JSON 을 설정하세요."
        )

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_files_in_folder(service, folder_id: str, name_hint: str = ""):
    """폴더 내 엑셀 파일 목록 조회"""
    q = f"'{folder_id}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
    if name_hint:
        q += f" and name contains '{name_hint}'"
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, modifiedTime, size)",
            pageToken=page_token,
            pageSize=1000,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _download_file(service, file_id: str, dest: Path):
    """구글 드라이브 파일 다운로드"""
    from googleapiclient.http import MediaIoBaseDownload
    request = service.files().get_media(fileId=file_id)
    with open(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _load_meta():
    if CACHE_META.exists():
        try:
            return json.loads(CACHE_META.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_meta(meta: dict):
    CACHE_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_cache_fresh(file_id: str, modified_time: str, meta: dict) -> bool:
    """캐시가 구글 드라이브의 최신 버전과 동일한지 확인"""
    cached = meta.get(file_id, {})
    return cached.get("modifiedTime") == modified_time


def sync_from_drive(force: bool = False) -> dict:
    """
    구글 드라이브와 캐시 동기화.
    Returns:
        {"performance_files": [...], "master_file": ..., "downloaded": int}
    """
    cfg = _get_config()
    perf_folder = cfg["performance_folder_id"]
    master_folder = cfg["master_folder_id"]

    if not perf_folder or not master_folder:
        # 폴더 ID가 없으면 로컬 캐시만 반환 (개발/테스트 모드)
        return _local_only()

    service = _get_drive_service()
    meta = _load_meta()
    downloaded = 0

    # 실적 파일들
    perf_files = _list_files_in_folder(service, perf_folder, PERFORMANCE_NAME_HINT)
    perf_paths = []
    for f in perf_files:
        local_path = CACHE_DIR / f["name"]
        if force or not local_path.exists() or not _is_cache_fresh(f["id"], f["modifiedTime"], meta):
            _download_file(service, f["id"], local_path)
            meta[f["id"]] = {"name": f["name"], "modifiedTime": f["modifiedTime"]}
            downloaded += 1
        perf_paths.append(local_path)

    # 마스터 파일 (가장 최신만)
    master_files = _list_files_in_folder(service, master_folder, MASTER_NAME_HINT)
    master_path = None
    if master_files:
        master_files.sort(key=lambda x: x["modifiedTime"], reverse=True)
        latest = master_files[0]
        local_path = CACHE_DIR / latest["name"]
        if force or not local_path.exists() or not _is_cache_fresh(latest["id"], latest["modifiedTime"], meta):
            _download_file(service, latest["id"], local_path)
            meta[latest["id"]] = {"name": latest["name"], "modifiedTime": latest["modifiedTime"]}
            downloaded += 1
        master_path = local_path

    meta["_last_sync"] = datetime.now().isoformat()
    _save_meta(meta)

    return {
        "performance_files": perf_paths,
        "master_file": master_path,
        "downloaded": downloaded,
        "last_sync": meta["_last_sync"],
    }


def _local_only() -> dict:
    """구글 드라이브 설정 없이 로컬 캐시 디렉터리만 사용 (개발/테스트용)"""
    perf_files = sorted([p for p in CACHE_DIR.glob("*.xlsx") if MASTER_NAME_HINT not in p.name])
    master_files = sorted([p for p in CACHE_DIR.glob(f"*{MASTER_NAME_HINT}*.xlsx")], reverse=True)
    return {
        "performance_files": perf_files,
        "master_file": master_files[0] if master_files else None,
        "downloaded": 0,
        "last_sync": "local-only mode",
    }


def load_performance(paths: list) -> pd.DataFrame:
    """실적 엑셀 여러 개를 합쳐 하나의 DataFrame으로"""
    if not paths:
        return pd.DataFrame()
    dfs = []
    for p in paths:
        df = pd.read_excel(p, dtype={"rcno": str})
        dfs.append(df)
    out = pd.concat(dfs, ignore_index=True)
    # 신고일자를 datetime으로
    out["신고필증발급일자"] = pd.to_datetime(out["신고필증발급일자"], errors="coerce")
    # 결측치 정리
    for col in ["해외업소", "세부품목_품목(유형)", "제품명(한글)", "제품명(영문)"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def load_master(path) -> pd.DataFrame:
    """제조사 마스터 엑셀 로드"""
    if path is None:
        return pd.DataFrame()
    df = pd.read_excel(path)
    df["등록일"] = pd.to_datetime(df["등록일"], errors="coerce")
    df["만료일"] = pd.to_datetime(df["만료일"], errors="coerce")
    for col in ["업소명", "업소코드", "국가", "주소", "비고"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    return df
