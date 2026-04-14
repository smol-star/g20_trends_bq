import os
import json
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# 용량 한도 설정 (정상 모드 / 세이프 모드)
# ─────────────────────────────────────────────
CAP_EVENTS_NORMAL  = 5.0    # GB — 속보 쿼리 정상 캡
CAP_GKG_NORMAL     = 50.0   # GB — GKG 쿼리 정상 캡
CAP_EVENTS_SAFE    = 2.5    # GB — 세이프 모드 속보 캡 (정상의 절반)
CAP_GKG_SAFE       = 25.0   # GB — 세이프 모드 GKG 캡 (정상의 절반)
MONTHLY_SAFE_LIMIT = 950.0  # GB — 이 값 초과 시 세이프 모드 자동 진입

USAGE_FILE = "bq_usage_tracker.json"

G20_CODES = {
    'US': 'United States', 'CH': 'China', 'GM': 'Germany', 'JA': 'Japan',
    'IN': 'India', 'UK': 'United Kingdom', 'FR': 'France', 'IT': 'Italy',
    'BR': 'Brazil', 'CA': 'Canada', 'RS': 'Russia', 'MX': 'Mexico',
    'AS': 'Australia', 'KS': 'South Korea', 'ID': 'Indonesia', 'TU': 'Turkey',
    'SA': 'Saudi Arabia', 'AR': 'Argentina', 'SF': 'South Africa'
}

# ─────────────────────────────────────────────
# 누적 사용량 추적 유틸리티
# ─────────────────────────────────────────────
def load_usage():
    """월간 누적 스캔량 파일 로드. 새 달이면 자동 리셋."""
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    if os.path.exists(USAGE_FILE):
        try:
            with open(USAGE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("month") == current_month:
                return data
        except Exception:
            pass
    # 새 달이거나 파일 손상 시 초기화
    return {"month": current_month, "total_gb": 0.0, "safe_mode": False}

def save_usage(usage: dict):
    """누적 사용량 파일 저장."""
    try:
        with open(USAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(usage, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[UsageTracker] 저장 실패: {e}")

def add_usage_and_check(gb_scanned: float) -> dict:
    """스캔량 추가 후 세이프 모드 여부 판단하여 usage 반환."""
    usage = load_usage()
    usage["total_gb"] = round(usage.get("total_gb", 0.0) + gb_scanned, 3)

    if usage["total_gb"] >= MONTHLY_SAFE_LIMIT:
        if not usage.get("safe_mode"):
            print(f"[⚠️ 세이프 모드 진입] 월간 누적 {usage['total_gb']:.1f} GB가 {MONTHLY_SAFE_LIMIT} GB를 초과했습니다.")
            print(f"  → 모든 쿼리 캡을 절반으로 자동 하향 조정합니다.")
        usage["safe_mode"] = True
    else:
        usage["safe_mode"] = False

    save_usage(usage)
    return usage

def get_caps() -> tuple[float, float]:
    """현재 세이프 모드 여부에 따라 (이벤트 캡, GKG 캡) 반환."""
    usage = load_usage()
    is_safe = usage.get("safe_mode", False) or usage.get("total_gb", 0.0) >= MONTHLY_SAFE_LIMIT
    if is_safe:
        return CAP_EVENTS_SAFE, CAP_GKG_SAFE
    return CAP_EVENTS_NORMAL, CAP_GKG_NORMAL

def print_usage_status():
    """현재 사용량 상태를 콘솔에 출력."""
    usage = load_usage()
    cap_events, cap_gkg = get_caps()
    mode_label = "🔴 세이프 모드" if usage.get("safe_mode") else "🟢 정상 모드"
    print(f"[BQ UsageTracker] {usage['month']} 누적: {usage['total_gb']:.2f} GB / {MONTHLY_SAFE_LIMIT} GB | {mode_label}")
    print(f"  → 현재 캡: 속보 {cap_events} GB / GKG {cap_gkg} GB")

# ─────────────────────────────────────────────
# BigQuery 클라이언트
# ─────────────────────────────────────────────
def get_bq_client():
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise ValueError("Google Cloud 인증 키가 설정되지 않았습니다. .env 를 확인하세요.")
    return bigquery.Client()

# ─────────────────────────────────────────────
# 속보 쿼리 (events_partitioned) — DATE 파티션 명시로 스캔량 최소화
# ─────────────────────────────────────────────
def get_g20_trends_query(limit=150):
    """
    BigQuery GDELT 속보 쿼리 (최적화 버전).
    - _PARTITIONTIME을 오늘/어제 DATE로 명시 → 불필요한 파티션 전체 스캔 차단.
    - LIMIT를 파라미터화하여 캡 초과 시 축소 재시도 가능.
    """
    return f"""
    SELECT
        GLOBALEVENTID,
        ActionGeo_CountryCode,
        NumMentions,
        NumSources,
        GoldsteinScale,
        AvgTone,
        SOURCEURL
    FROM `gdelt-bq.gdeltv2.events_partitioned`
    WHERE (
        _PARTITIONTIME = TIMESTAMP(CURRENT_DATE())
        OR _PARTITIONTIME = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
        AND ActionGeo_CountryCode IN ('US','CH','GM','JA','IN','UK','FR','IT','BR','CA','RS','MX','AS','KS','ID','TU','SA','AR','SF')
    ORDER BY NumMentions DESC, NumSources DESC
    LIMIT {limit}
    """

def verify_and_fetch_data():
    print_usage_status()
    cap_events, _ = get_caps()
    client = get_bq_client()

    # 1차(LIMIT 150) → 캡 초과 시 2차(LIMIT 75)로 자동 재시도
    for attempt, limit in enumerate([150, 75], start=1):
        query = get_g20_trends_query(limit=limit)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            dry_run_job = client.query(query, job_config=job_config)
        except GoogleAPIError as e:
            raise RuntimeError(f"빅쿼리 접속/문법/권한 오류: {e}")

        gb_processed = dry_run_job.total_bytes_processed / (1024 ** 3)
        print(f"[속보 Dry Run #{attempt}] 예상 스캔: {gb_processed:.3f} GB  (캡: {cap_events} GB, LIMIT: {limit})")

        if gb_processed <= cap_events:
            print(f"용량 확인 완료. 실제 쿼리 실행 (LIMIT {limit})...")
            query_job = client.query(query, bigquery.QueryJobConfig())
            df = query_job.to_dataframe()
            add_usage_and_check(gb_processed)
            return df

        print(f"[경고] 시도 #{attempt}: {gb_processed:.3f} GB > 캡 {cap_events} GB — {'재시도' if attempt < 2 else '차단'}")

    print("[안전장치] 모든 시도가 캡을 초과했습니다. 이번 회차는 건너뜁니다. 기존 데이터 유지.")
    return None



# ─────────────────────────────────────────────
# GKG 쿼리 (gkg_partitioned)
# ─────────────────────────────────────────────
def get_gkg_trends_query():
    """
    GKG 트렌드 쿼리 (최적화 버전).
    - _PARTITIONTIME >= INTERVAL N HOUR 방식은 파티션 매칭 불가 버그 존재.
    - 오늘부터 7일 전까지의 파티션을 날짜별로 명시하여 정확히 읽음.
    """
    return """
    SELECT 
        GKGRECORDID,
        V2Themes,
        DocumentIdentifier,
        V2Locations
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
      AND _PARTITIONTIME < TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
      AND REGEXP_CONTAINS(V2Themes, r'CULTURE|LIFESTYLE|TOURISM|FASHION')
      AND REGEXP_CONTAINS(V2Locations, r'#(US|CH|GM|JA|IN|UK|FR|IT|BR|CA|RS|MX|AS|KS|ID|TU|SA|AR|SF)#')
    """

def verify_and_fetch_gkg_data():
    print_usage_status()
    _, cap_gkg = get_caps()

    client = get_bq_client()
    query = get_gkg_trends_query()

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        dry_run_job = client.query(query, job_config=job_config)
    except GoogleAPIError as e:
        print(f"빅쿼리 접속/문법/권한 오류가 발생했습니다: {e}")
        return "ERROR_API"

    gb_processed = dry_run_job.total_bytes_processed / (1024 ** 3)
    print(f"[GKG Dry Run] 예상 스캔 용량: {gb_processed:.3f} GB  (현재 캡: {cap_gkg} GB)")

    if gb_processed > cap_gkg:
        print(f"[안전장치] {gb_processed:.2f} GB > 캡 {cap_gkg} GB → GKG 수집 차단.")
        return "ERROR_SIZE"

    print("용량 확인 완료. GKG 실제 쿼리를 실행합니다...")
    query_job = client.query(query, bigquery.QueryJobConfig())
    df = query_job.to_dataframe()
    add_usage_and_check(gb_processed)
    return df
