# 식약처 기구·용기·포장 실적 조회 시스템

식약처에 신고된 기구·용기·포장 수입 실적을 제조사·재질 기준으로 조회하여
정밀검사 면제 가능성을 판단할 수 있는 Streamlit 웹앱입니다.

## 핵심 매칭 로직

- **제조사**: 식약처 등록명 정확 일치 (띄어쓰기·구두점 포함)
- **재질**:
  - **1차**: rcno의 재질 조합이 사용자 선택과 완전 동일 (같은 제품 가능성 높음)
  - **2차**: 1차 제외, 사용자 선택 재질 중 1개 이상 포함
- **유효기간**: 제조사의 등록일·만료일 범위 내 실적만 유효 처리, 기간 외는 회색 표시
- **5년 룰**: 5년 초과된 실적은 별도 표시 (정밀검사 면제 효력 만료)
- **동명이인**: 동일 업소명을 가진 다수 제조사 모두 표시 후 사용자 선택

## 디렉터리 구조

```
mfds_app/
├── app.py              # Streamlit 메인 앱
├── data_loader.py      # 구글 드라이브 동기화 + 엑셀 로드
├── search_engine.py    # 검색 매칭 로직
├── test_search.py      # 검색 엔진 단위 테스트
├── requirements.txt
├── cache/              # 다운로드된 엑셀 캐시 (gitignore)
└── README.md
```

## 로컬 실행 (개발/테스트)

```bash
pip install -r requirements.txt
# cache/ 디렉터리에 엑셀 파일 직접 복사 (구글 드라이브 설정 없이 테스트)
mkdir -p cache
cp /경로/실적엑셀들/*.xlsx cache/
cp /경로/해외제조업소_*.xlsx cache/

streamlit run app.py
```

브라우저에서 자동으로 http://localhost:8501 열림.

## 구글 드라이브 연동 설정

### 1. 서비스 계정 준비

GCP 콘솔 → IAM & Admin → Service Accounts:

1. 서비스 계정 생성
2. JSON 키 다운로드
3. Google Drive API 활성화
4. 실적/마스터 폴더에 서비스 계정 이메일 **뷰어 권한** 공유

### 2. 폴더 구조

구글 드라이브에 두 개의 폴더 생성:

- **실적 폴더**: 연도별 엑셀 (`mfds_기구용기포장_2021.xlsx`, ..., `_2026.xlsx`)
- **마스터 폴더**: `해외제조업소_YYYYMMDD.xlsx` (가장 최신만 자동 사용)

각 폴더의 ID 확인 (URL의 `https://drive.google.com/drive/folders/<여기>`)

### 3. 환경변수 또는 Streamlit secrets 설정

#### 로컬 실행 (.env 또는 export):

```bash
export MFDS_PERFORMANCE_FOLDER_ID="1AbCdEf..."
export MFDS_MASTER_FOLDER_ID="1XyZ..."
export GCP_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
```

#### Streamlit Cloud 배포: `.streamlit/secrets.toml`

```toml
MFDS_PERFORMANCE_FOLDER_ID = "1AbCdEf..."
MFDS_MASTER_FOLDER_ID = "1XyZ..."

[gcp_service_account]
type = "service_account"
project_id = "..."
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "..."
client_id = "..."
# ... (서비스 계정 JSON의 모든 필드)
```

## 캐싱 동작

- 앱 시작 시 또는 24시간 경과 후 구글 드라이브 동기화 (`@st.cache_resource(ttl=86400)`)
- 파일의 `modifiedTime`이 캐시와 다를 때만 다시 다운로드
- 사이드바의 **"🔄 데이터 새로고침"** 버튼으로 즉시 강제 동기화 가능

## 데이터 보안 모델

- 원본 실적 데이터는 서버에만 존재 (사용자에게 일괄 노출되지 않음)
- 사용자는 검색 결과로 데이터 일부만 확인 가능
- 추후 비밀번호/사용자 인증 추가 예정 (현재는 누구나 접속)

## 배포 옵션

| 플랫폼 | 비용 | 특징 |
|--------|------|------|
| Streamlit Community Cloud | 무료 | GitHub 연동, public, secrets 지원 |
| Render | 무료/유료 | 더 많은 리소스, 커스텀 도메인 |
| Hugging Face Spaces | 무료 | private 가능 |
| 자체 서버 (Docker) | 서버비 | 완전 통제 |

## 추후 확장 계획 (별도 프로그램)

- 크롤러 (`mfds_crawler_v3_asc.py`)에 다음 기능 추가:
  1. 구글 드라이브에서 마지막 rcno 자동 조회
  2. 그 rcno부터 이어서 크롤
  3. 완료 시 구글 드라이브에 자동 업로드
