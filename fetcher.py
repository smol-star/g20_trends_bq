import bq_engine
import ai_processor
from data_manager import save_current_data
from datetime import datetime, timezone, timedelta
import pandas as pd
import re

def extract_url_keywords(url):
    url = str(url).split('?')[0] # Remove query params
    words = re.findall(r'[a-zA-Z]+', url.lower())
    stopwords = {'http', 'https', 'www', 'com', 'org', 'net', 'news', 'article', 'html', 'story', 'post', 'world', 'the', 'and', 'for', 'with', 'index'}
    return set([w for w in words if w not in stopwords and len(w) > 3])

def calculate_jaccard(set1, set2):
    if not set1 or not set2:
        return 0.0
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union > 0 else 0.0

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
    if df is None:
        print("[안전장치 발동] 스캔 용량 초과로 이번 회차 수집을 건너뜁니다. 기존 데이터를 유지합니다.")
        return  # 기존 current_trends.json 유지 (save 호출 없음)
    if df.empty:
        print("BigQuery 응답이 비어 있습니다. 기존 데이터를 유지합니다.")
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
            
        country_df = country_df.sort_values(by='ImpactScore', ascending=False)
        clusters = []
        
        for idx, row in country_df.iterrows():
            url = row['SOURCEURL']
            score = float(row['ImpactScore'])
            kw_set = extract_url_keywords(url)
            
            matched = False
            for cluster in clusters:
                if calculate_jaccard(cluster['kw_set'], kw_set) > 0.45:
                    cluster['score'] += score
                    cluster['items'].append(row)
                    matched = True
                    break
            
            if not matched:
                clusters.append({
                    'kw_set': kw_set,
                    'score': score,
                    'items': [row],
                    'representative': row
                })
        
        # 국가별 상위 5건 클러스터 추출
        clusters = sorted(clusters, key=lambda x: x['score'], reverse=True)[:5]
        
        trends_list = []
        for c in clusters:
            row = c['representative']
            related_urls = [str(item['SOURCEURL']) for item in c['items'] if str(item['SOURCEURL']) != str(row['SOURCEURL'])]
            
            trends_list.append({
                "record_id": str(row['GLOBALEVENTID']),
                "url": row['SOURCEURL'],
                "related_urls": list(set(related_urls))[:3],
                "title": "",  # BigQuery 쿼리에 title 컬럼 없음. URL로 AI 분석.
                "mentions": int(row['NumMentions']),
                "sources": int(row['NumSources']),
                "goldstein": float(row['GoldsteinScale']),
                "tone": float(row['AvgTone']) if pd.notna(row['AvgTone']) else 0.0,
                "score": float(c['score'])
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
        
        # 결과 매핑 (정확 일치 → 부분 일치 fuzzy 순서)
        for ai_country, ai_data in ai_results.items():
            # 정확 일치
            matched = ai_country if ai_country in result_data else None
            # Fuzzy 매칭 (AI가 "Korea" 혹은 "UK" 등 축약형 반환 시 대응)
            if not matched:
                for real in result_data:
                    if ai_country.lower() in real.lower() or real.lower() in ai_country.lower():
                        matched = real
                        break
            if not matched:
                print(f"  [경고] AI 국가명 '{ai_country}' 매칭 실패. 건너뜁니다.")
                continue
                
            if not isinstance(ai_data, list):
                print(f"  [경고] AI 데이터가 배열 형식이 아닙니다(국가: '{ai_country}'). 건너뜁니다.")
                continue
            
            ai_data_dict = {str(item.get("id")): item for item in ai_data if "id" in item}
            
            for t in result_data[matched]['trends']:
                t_id = str(t['record_id'])
                if t_id in ai_data_dict:
                    ai_item = ai_data_dict[t_id]
                    t['keyword'] = ai_item.get('headline', '주요 글로벌 이슈 식별불가')
                    t['hook'] = ai_item.get('hook', '현재 수집된 글로벌 뉴스를 분석 중인 이슈입니다.')
                    t['script'] = ai_item.get('script', '이 이슈에 대한 쇼츠 대본 초안을 준비 중입니다.')
                    t['sentiment'] = ai_item.get('sentiment', 'neutral')

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
