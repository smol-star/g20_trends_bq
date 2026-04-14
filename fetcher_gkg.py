import pandas as pd
import json
import os
import re
from datetime import datetime, timezone, timedelta
import bq_engine
import ai_processor

def process_gkg():
    print(f"[{datetime.now()}] GKG 트렌드 (라이프스타일) 데이터 수집 시작...")
    df = bq_engine.verify_and_fetch_gkg_data()
    
    if isinstance(df, str) and df == "ERROR_SIZE":
        fallback_data = [{
            "status": "error", 
            "title": "데이터 스캔 용량 초과", 
            "summary": "이번 주 트렌드 데이터가 35GB를 초과하여 안전을 위해 수집이 일시 차단되었습니다."
        }]
        with open("lifestyle_trends.json", "w", encoding="utf-8") as f:
            json.dump(fallback_data, f, ensure_ascii=False, indent=2)
        print("Fallback 데이터를 생성하고 종료합니다.")
        return
        
    if isinstance(df, str) or df is None or df.empty:
        print("수집된 데이터가 없습니다.")
        return
        
    print(f"총 {len(df)}건의 GKG 데이터 수집 성공. 파싱 시작...")
    
    lifestyle_data = []
    lifestyle_themes_to_ai = {}
    seen_lifestyle_urls = set()
    
    for idx, row in df.iterrows():
        rid = row['GKGRECORDID']
        url = str(row['DocumentIdentifier'])
        themes = str(row['V2Themes'])
        
        # 라이프스타일 필터 (CULTURE 단독, LIFESTYLE, TOURISM, FASHION 등 포함)
        if re.search(r'\bCULTURE\b|LIFESTYLE|TOURISM|FASHION', themes):
            if url != 'None' and url not in seen_lifestyle_urls and len(lifestyle_themes_to_ai) < 15:
                lifestyle_themes_to_ai[rid] = {'url': url, 'themes': themes}
                seen_lifestyle_urls.add(url)
                
    print(f"라이프스타일 {len(lifestyle_themes_to_ai)}건 AI 요약 요청...")
    
    lifestyle_ai_results = ai_processor.summarize_gkg_trends(lifestyle_themes_to_ai, category="lifestyle")
    
    # 라이프스타일 데이터 구성
    for rid, info in lifestyle_themes_to_ai.items():
        ai_data = lifestyle_ai_results.get(str(rid), {})
        lifestyle_data.append({
            "id": rid,
            "url": info['url'],
            "keyword": ai_data.get('headline', '일반 라이프스타일 트렌드'),
            "hook": ai_data.get('hook', '새로운 라이프스타일 트렌드가 관찰되었습니다.'),
            "script": ai_data.get('script', '생성된 대본이 없습니다.')
        })
        
    final_payload = [{"status": "success", "data": lifestyle_data}]
    with open("lifestyle_trends.json", "w", encoding="utf-8") as f:
        json.dump(final_payload, f, ensure_ascii=False, indent=2)
        
    # 아카이브 저장
    try:
        kst = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst)
        date_str = now_kst.strftime("%Y-%m-%d")
        hour_str = now_kst.strftime("%H")
        
        snapshot_dir = os.path.join("hourly_archive_lifestyle", date_str)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        snapshot_file = os.path.join(snapshot_dir, f"{hour_str}.json")
        with open(snapshot_file, "w", encoding="utf-8") as f:
            json.dump(final_payload, f, ensure_ascii=False, indent=2)
        print(f"[Archive] 라이프스타일 스냅샷 저장 완료: {snapshot_file}")
    except Exception as e:
        print(f"[Archive] 라이프스타일 스냅샷 저장 실패: {e}")

    print("GKG 데이터 로직 완료 및 파일 저장 성공!")

if __name__ == "__main__":
    process_gkg()
