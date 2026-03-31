import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from dotenv import load_dotenv

load_dotenv()

G20_CODES = {
    'US': 'United States', 'CH': 'China', 'GM': 'Germany', 'JA': 'Japan',
    'IN': 'India', 'UK': 'United Kingdom', 'FR': 'France', 'IT': 'Italy',
    'BR': 'Brazil', 'CA': 'Canada', 'RS': 'Russia', 'MX': 'Mexico',
    'AS': 'Australia', 'KS': 'South Korea', 'ID': 'Indonesia', 'TU': 'Turkey',
    'SA': 'Saudi Arabia', 'AR': 'Argentina', 'SF': 'South Africa'
}

def get_bq_client():
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise ValueError("Google Cloud 인증 키가 설정되지 않았습니다. .env 를 확인하세요.")
    return bigquery.Client()

def get_g20_trends_query():
    # events 테이블과 gkg 테이블을 조합하여 사용자 요구사항 충족
    # 이벤트 테이블에서 국가 필터링 및 지표 확보 후, GKG 뷰와 조인 (혹은 독립적 수행)
    # 비용 절감을 위해 24시간 파티션 필터 엄격 적용
    return """
    SELECT 
        GLOBALEVENTID,
        ActionGeo_CountryCode,
        NumMentions,
        NumSources,
        GoldsteinScale,
        AvgTone,
        SOURCEURL
    FROM `gdelt-bq.gdeltv2.events`
    WHERE SQLDATE >= CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS INT64)
        AND ActionGeo_CountryCode IN ('US', 'CH', 'GM', 'JA', 'IN', 'UK', 'FR', 'IT', 'BR', 'CA', 'RS', 'MX', 'AS', 'KS', 'ID', 'TU', 'SA', 'AR', 'SF')
    ORDER BY NumMentions DESC, NumSources DESC
    LIMIT 300
    """

def verify_and_fetch_data():
    client = get_bq_client()
    query = get_g20_trends_query()
    
    # 1. 예상 스캔 용량 먼저 계산 (Dry Run)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        dry_run_job = client.query(query, job_config=job_config)
    except GoogleAPIError as e:
        raise RuntimeError(f"빅쿼리 접속/문법/권한 오류가 발생했습니다: {e}")
        
    bytes_processed = dry_run_job.total_bytes_processed
    gb_processed = bytes_processed / (1024 ** 3)
    
    print(f"[비용 최적화] 예상 쿼리 스캔 용량: {gb_processed:.3f} GB")
    
    # 2. 20GB 초과 시 안전 장치 발동
    if gb_processed > 20.0:
        raise MemoryError(f"안전 장치 발동: 스캔 용량이 20GB를 초과합니다. (예상: {gb_processed:.2f} GB) 실행을 중단합니다.")
        
    # 3. 실제 쿼리 실행
    print("용량 확인 완료. 실제 쿼리를 실행합니다...")
    real_job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=real_job_config)
    
    return query_job.to_dataframe()
