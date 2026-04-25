"""
데이터 로더 — 구글 드라이브에서 실적/제조사 마스터 다운로드 및 캐싱

지원 폴더:
- 실적 고정 폴더 (.parquet 파일들): 2022~2025 등 안 변하는 데이터
- 실적 갱신 폴더 (.xlsx 파일들): 2026 등 계속 갱신되는 데이터
- 제조사 마스터 폴더 (.xlsx)
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

# 파일명 패턴
PERFORMANCE_NAME_HINT = "기구"  # 또는 'mfds_'
MASTER_NAME_HINT = "해외제조업소"


def _get_config():
    """
    설정값 로드.
    우선순위: Streamlit secrets → 환경변수 → 빈 값
    """
    perf_id = ""
    perf_fixed_id = ""  # 새로 추가: 고정 데이터 폴더
    master_id = ""
    sa_info = None
    key_file_path = None

    # 1. Streamlit secrets 에서 읽기
    try:
        import streamlit as st
        if "MFDS_PERFORMANCE_FOLDER_ID" in st.secrets:
            perf_id = st.secrets["MFDS_PERFORMANCE_FOLDER_ID"]
        if "MFDS_PERFORMANCE_FIXED_FOLDER_ID" in st.secrets:
            perf_fixed_id = st.secrets["MFDS_PERFORMANCE_FIXED_FOLDER_ID"]
        if "MFDS_MASTER_FOLDER_ID" in st.secrets:
            master_id = st.secrets["MFDS_MASTER_FOLDER_ID"]
        if "gcp_service_account" in st.secrets:
            sa_info = dict(st.secrets["gcp_service_account"])
        elif "GCP_KEY_FILE" in st.secrets:
            key_file_path = st.secrets["GCP_KEY_FILE"]
    except Exception:
        pass

    # 2. 환경변수에서 읽기 (보완)
    if not perf_id:
        perf_id = os.environ.get("MFDS_PERFORMANCE_FOLDER_ID", "")
    if not perf_fixed_id:
        perf_fixed_id = os.environ.get("MFDS_PERFORMANCE_FIXED_FOLDER_ID", "")
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
        "performance_fixed_folder_id": perf_fixed_id,
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
            "서비스 계정 정보가 없습니다. secrets.toml 설정을 확인하세요."
        )

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_files_in_folder(service, folder_id: str, name_hint: str = ""):
    """폴더 내 파일 목록 조회"""
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


def _sync_folder(service, folder_id: str, name_hint: str, meta: dict, force: bool = False):
    """한 폴더의 파일들을 동기화. (path 리스트, 다운로드 수) 반환"""
    files = _list_files_in_folder(service, folder_id, name_hint)
    paths = []
    downloaded = 0
    for f in files:
        local_path = CACHE_DIR / f["name"]
        if force or not local_path.exists() or not _is_cache_fresh(f["id"], f["modifiedTime"], meta):
            _download_file(service, f["id"], local_path)
            meta[f["id"]] = {"name": f["name"], "modifiedTime": f["modifiedTime"]}
            downloaded += 1
        paths.append(local_path)
    return paths, downloaded


def sync_from_drive(force: bool = False) -> dict:
    """
    구글 드라이브와 캐시 동기화.
    Returns:
        {
            "performance_files": [...],         # 갱신 폴더(xlsx)
            "performance_fixed_files": [...],   # 고정 폴더(parquet)
            "master_file": ...,
            "downloaded": int,
            "last_sync": str
        }
    """
    cfg = _get_config()
    perf_folder = cfg["performance_folder_id"]
    perf_fixed_folder = cfg["performance_fixed_folder_id"]
    master_folder = cfg["master_folder_id"]

    # 폴더 ID 모두 없으면 로컬 모드
    if not perf_folder and not perf_fixed_folder and not master_folder:
        return _local_only()

    service = _get_drive_service()
    meta = _load_meta()
    downloaded = 0

    # 갱신 폴더 (xlsx)
    perf_paths = []
    if perf_folder:
        perf_paths, n = _sync_folder(service, perf_folder, PERFORMANCE_NAME_HINT, meta, force)
        downloaded += n

    # 고정 폴더 (parquet)
    perf_fixed_paths = []
    if perf_fixed_folder:
        # parquet 파일은 이름 힌트 없이 모두 가져옴
        perf_fixed_paths, n = _sync_folder(service, perf_fixed_folder, "", meta, force)
        # parquet 파일만 필터
        perf_fixed_paths = [p for p in perf_fixed_paths if p.suffix.lower() == ".parquet"]
        downloaded += n

    # 마스터 파일 (가장 최신만)
    master_path = None
    if master_folder:
        master_files = _list_files_in_folder(service, master_folder, MASTER_NAME_HINT)
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
        "performance_fixed_files": perf_fixed_paths,
        "master_file": master_path,
        "downloaded": downloaded,
        "last_sync": meta["_last_sync"],
    }


def _local_only() -> dict:
    """로컬 캐시만 사용 (개발/테스트용)"""
    perf_files = sorted([p for p in CACHE_DIR.glob("*.xlsx") if MASTER_NAME_HINT not in p.name])
    perf_fixed_files = sorted([p for p in CACHE_DIR.glob("*.parquet")])
    master_files = sorted([p for p in CACHE_DIR.glob(f"*{MASTER_NAME_HINT}*.xlsx")], reverse=True)
    return {
        "performance_files": perf_files,
        "performance_fixed_files": perf_fixed_files,
        "master_file": master_files[0] if master_files else None,
        "downloaded": 0,
        "last_sync": "local-only mode",
    }


def load_performance(xlsx_paths: list, parquet_paths: list = None) -> pd.DataFrame:
    """
    실적 데이터 로드. xlsx와 parquet 모두 지원.
    parquet 파일이 우선 (빠르게 로드), xlsx는 갱신 데이터용.
    """
    parquet_paths = parquet_paths or []
    if not xlsx_paths and not parquet_paths:
        return pd.DataFrame()

    dfs = []

    # Parquet 먼저 (빠름)
    for p in parquet_paths:
        df = pd.read_parquet(p, engine="pyarrow")
        # rcno를 string으로 보장
        if "rcno" in df.columns:
            df["rcno"] = df["rcno"].astype(str)
        dfs.append(df)

    # Xlsx (느림)
    for p in xlsx_paths:
        df = pd.read_excel(p, dtype={"rcno": str})
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)

    # 신고일자를 datetime으로
    if "신고필증발급일자" in out.columns:
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
