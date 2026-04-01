import bq_engine
import ai_processor
from data_manager import save_current_data
from datetime import datetime, timezone, timedelta
import pandas as pd

def calculate_impact_score(row):
    # 전파력(언급수), 공신력(매체수), 영향력(골드스타인) 복합 점수
    # Goldstein Scale은 -10 ~ +10 이므로 절대값으로 영향력 볼륨 측정
    mentions = row.get('NumMentions', 0)
    sources = row.get('NumSources', 0)
    goldstein = abs(row.get('GoldsteinScale', 0))
    
    # 가중치 (임의 조정 가능)
    return (mentions * 0.5) + (sources * 2.0) + (goldstein * 5.0)

def fetch_and_process():
    print(f"[{datetime.now()}] GDELT BigQuery 수집 시작...")
    
    df = bq_engine.verify_and_fetch_data()
    if df is None or df.empty:
        print("수집된 데이터가 없습니다.")
        return
        
    print(f"총 {len(df)}건의 병합된 글로벌 이벤트 데이터 확보 성공.")
    
    # 1. 통합 스코어 산출
    df['ImpactScore'] = df.apply(calculate_impact_score, axis=1)
    
    # 2. 국가별 정리 준비
    g20_mapping = bq_engine.G20_CODES
    result_data = {}
    
    # AI 분석용 국가별 큐 (배치 처리용)
    country_batch_queue = {}
    
    for code, country_name in g20_mapping.items():
        country_df = df[df['ActionGeo_CountryCode'] == code]
        if country_df.empty:
            continue
            
        # 국가별 상위 5건 이슈 추출
        country_df = country_df.sort_values(by='ImpactScore', ascending=False).head(5)
        
        trends_list = []
        for idx, row in country_df.iterrows():
            trends_list.append({
                "record_id": row['GLOBALEVENTID'],
                "url": row['SOURCEURL'],
                "title": row.get('title', ''), # BigQuery 쿼리 결과에 따라 다름
                "mentions": row['NumMentions'],
                "sources": row['NumSources'],
                "goldstein": row['GoldsteinScale'],
                "tone": float(row['AvgTone']) if pd.notna(row['AvgTone']) else 0.0,
                "score": float(row['ImpactScore'])
            })
            
        if trends_list:
            kst = timezone(timedelta(hours=9))
            result_data[country_name] = {
                "gdp_rank": list(g20_mapping.values()).index(country_name) + 1,
                "spike_score": sum([t['score'] for t in trends_list]),
                "trends": trends_list,
                "last_updated": datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S KST")
            }
            # AI 배치 큐에 추가
            country_batch_queue[country_name] = trends_list

    # 3. AI 배치 요약 실행 (5~7개 국가씩 분할 호출)
    all_countries = list(country_batch_queue.keys())
    batch_size = 6
    
    print(f"총 {len(all_countries)}개국 이슈에 대해 Gemini AI 배치 분석 요청 중 (배치 크기: {batch_size})...")
    
    for i in range(0, len(all_countries), batch_size):
        batch_keys = all_countries[i : i + batch_size]
        batch_payload = {k: country_batch_queue[k] for k in batch_keys}
        
        # AI 호출
        ai_results = ai_processor.summarize_g20_batch(batch_payload)
        
        # 결과 매핑
        for country, ai_data in ai_results.items():
            if country in result_data:
                # 해당 국가의 모든 트렌드에 동일한(혹은 통합된) 요약 적용
                # (또는 AI가 개별 이슈별로 준 경우 r-id 매칭도 가능하지만, 
                # 현재 summarize_g20_batch는 국가 단위 통합 요약을 권장함)
                for t in result_data[country]['trends']:
                    t['keyword'] = ai_data.get('headline', '주요 글로벌 이슈 식별불가')
                    t['hook'] = ai_data.get('hook', '현재 수집된 글로벌 뉴스를 분석 중인 이슈입니다.')
                    t['script'] = ai_data.get('script', '이 이슈에 대한 쇼츠 대본 초안을 준비 중입니다.')

    # 4. 국가별 이슈 스파이크 점수에 따라 최종 정렬
    sorted_countries = sorted(result_data.items(), key=lambda x: (-x[1]['spike_score'], x[1]['gdp_rank']))
    
    final_dict = {}
    for i, (c, info) in enumerate(sorted_countries):
        info["current_rank"] = i + 1
        final_dict[c] = info
        
    save_current_data(final_dict)
    print("성공적으로 데이터를 수집하고 AI를 통해 요약 저장했습니다!")

if __name__ == "__main__":
    try:
        fetch_and_process()
    except Exception as e:
        import traceback
        import json
        from data_manager import save_current_data
        from datetime import datetime, timezone, timedelta

        kst = timezone(timedelta(hours=9))
        now_str = datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S KST")
        
        print("=" * 60)
        print(f"[CRITICAL ERROR] 파이프라인 예외 발생. Fallback 데이터를 저장합니다.")
        traceback.print_exc()
        print("=" * 60)

        fallback_data = {
            "_pipeline_error": True,
            "Global Summary": {
                "gdp_rank": 1,
                "current_rank": 1,
                "spike_score": 0.0,
                "last_updated": now_str,
                "trends": [
                    {
                        "record_id": 0,
                        "url": "",
                        "mentions": 0,
                        "sources": 0,
                        "goldstein": 0.0,
                        "tone": 0.0,
                        "score": 0.0,
                        "keyword": "갱신 일시 지연",
                        "hook": f"데이터 수집 중 문제가 발생하여 이번 회차({now_str})는 갱신이 지연되었습니다. 다음 정각에 자동으로 재시도합니다.",
                        "script": f"[시스템 알림] {str(e)[:200]}"
                    }
                ]
            }
        }
        save_current_data(fallback_data)
        print("Fallback 데이터 저장 완료. 파이프라인이 정상 종료(exit 0) 처리됩니다.")
