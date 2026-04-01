import pandas as pd
import json
import os
import re
from datetime import datetime
import bq_engine
import ai_processor

MACRO_12_GAMES = {
    "Market_Leader": ["genshin", "star rail", "zenless"],
    "Community_Buzz": ["blue archive", "nikke", "uma musume"],
    "Genre_Benchmark": ["wuthering waves", "arknights", "project sekai", "sekai"],
    "Global_Trend": ["fate/grand order", "fgo", "love and deepspace", "dokkan battle"]
}

def check_macro_game(text):
    text_lower = text.lower()
    for tag, games in MACRO_12_GAMES.items():
        for g in games:
            if g in text_lower:
                return tag, g
    return None, None

def process_gkg():
    print(f"[{datetime.now()}] GKG 트렌드 데이터 수집 시작...")
    df = bq_engine.verify_and_fetch_gkg_data()
    
    if isinstance(df, str) and df == "ERROR_SIZE":
        fallback_data = [{
            "status": "error", 
            "title": "데이터 스캔 용량 초과", 
            "summary": "이번 주 트렌드 데이터가 35GB를 초과하여 안전을 위해 수집이 일시 차단되었습니다."
        }]
        with open("lifestyle_trends.json", "w", encoding="utf-8") as f:
            json.dump(fallback_data, f, ensure_ascii=False, indent=2)
        with open("subculture_trends.json", "w", encoding="utf-8") as f:
            json.dump(fallback_data, f, ensure_ascii=False, indent=2)
        print("Fallback 데이터를 생성하고 종료합니다.")
        return
        
    if isinstance(df, str) or df is None or df.empty:
        print("수집된 데이터가 없습니다.")
        return
        
    print(f"총 {len(df)}건의 GKG 데이터 수집 성공. 파싱 시작...")
    
    lifestyle_data = []
    subculture_data = []
    
    lifestyle_themes_to_ai = {}
    subculture_themes_to_ai = {}
    
    # 중복 URL/토픽 방지
    seen_lifestyle_urls = set()
    seen_subculture_urls = set()
    
    for idx, row in df.iterrows():
        rid = row['GKGRECORDID']
        url = str(row['DocumentIdentifier'])
        themes = str(row['V2Themes'])
        
        # 라이프스타일 필터
        if re.search(r'CULTURE(?![_A-Z])|LIFESTYLE|TOURISM|FASHION', themes):
            if url != 'None' and url not in seen_lifestyle_urls and len(lifestyle_themes_to_ai) < 15:
                lifestyle_themes_to_ai[rid] = {'url': url, 'themes': themes}
                seen_lifestyle_urls.add(url)
                
        # 서브컬처 필터
        if re.search(r'CULTURE_ANIME|CULTURE_MANGA|ENTERTAINMENT_VIDEO_GAMES', themes):
            if url != 'None' and url not in seen_subculture_urls and len(subculture_themes_to_ai) < 25:
                # 서브카테고리 판별
                sub_cats = []
                if 'CULTURE_ANIME' in themes: sub_cats.append("Anime")
                if 'CULTURE_MANGA' in themes: sub_cats.append("Manga")
                if 'ENTERTAINMENT_VIDEO_GAMES' in themes: sub_cats.append("Video Games")
                
                # 가챠 게임 확인
                is_gacha = False
                macro_tag = None
                matched_game = None
                
                url_theme_text = url + " " + themes
                if re.search(r'gacha|genshin|star rail|zenless|blue archive|nikke|uma musume|wuthering waves|arknights|project sekai|fate/grand order|fgo|love and deepspace|dokkan battle', url_theme_text, re.IGNORECASE):
                    sub_cats.append("Gacha Game")
                    is_gacha = True
                    macro_tag, matched_game = check_macro_game(url_theme_text)
                
                subculture_themes_to_ai[rid] = {
                    'url': url, 
                    'themes': themes,
                    'sub_cats': sub_cats,
                    'is_gacha': is_gacha,
                    'macro_tag': macro_tag,
                    'matched_game': matched_game
                }
                seen_subculture_urls.add(url)
                
    print(f"라이프스타일 {len(lifestyle_themes_to_ai)}건, 서브컬처 {len(subculture_themes_to_ai)}건 AI 요약 요청...")
    
    lifestyle_ai_results = ai_processor.summarize_gkg_trends(lifestyle_themes_to_ai, category="lifestyle")
    subculture_ai_results = ai_processor.summarize_gkg_trends(subculture_themes_to_ai, category="subculture")
    
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
        
    # 서브컬처 (시장 독점/가중치 로직)
    gacha_count = sum(1 for v in subculture_themes_to_ai.values() if v['is_gacha'])
    macro_count = sum(1 for v in subculture_themes_to_ai.values() if v['macro_tag'])
    market_monopoly = (macro_count / gacha_count >= 0.7) if gacha_count > 0 else False
    
    for rid, info in subculture_themes_to_ai.items():
        ai_data = subculture_ai_results.get(str(rid), {})
        subculture_data.append({
            "id": rid,
            "url": info['url'],
            "sub_categories": info['sub_cats'],
            "is_gacha": info['is_gacha'],
            "macro_tag": info['macro_tag'],
            "matched_game": info['matched_game'],
            "market_monopoly": market_monopoly,
            "impact": "High" if info['macro_tag'] else "Normal",
            "keyword": ai_data.get('headline', '주요 서브컬처 이슈'),
            "hook": ai_data.get('hook', '최신 서브컬처 동향입니다.'),
            "script": ai_data.get('script', '생성된 대본이 없습니다.')
        })
        
    with open("lifestyle_trends.json", "w", encoding="utf-8") as f:
        json.dump([{"status": "success", "data": lifestyle_data}], f, ensure_ascii=False, indent=2)
        
    with open("subculture_trends.json", "w", encoding="utf-8") as f:
        json.dump([{"status": "success", "data": subculture_data}], f, ensure_ascii=False, indent=2)
        
    print("GKG 데이터 로직 완료 및 파일 분리 저장 성공!")

if __name__ == "__main__":
    process_gkg()
