import os
import json
import re
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

_client = None

def get_client():
    global _client
    if _client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print("[AI Error] GEMINI_API_KEY가 환경 변수에 없습니다.")
            return None
        _client = genai.Client(api_key=api_key)
    return _client

def get_available_model():
    """2026년 4월 표준: Gemini 3.1 시리즈 위주로 모델 탐색"""
    client = get_client()
    if not client: return "gemini-3.1-flash-lite"
    
    try:
        models = [m.name for m in client.models.list()]
        print(f"   [AI] BQ 프로젝트 가용 모델: {models}")
        
        preferences = [
            'gemini-3.1-flash-lite', 
            'gemini-3.1-flash',
            '3.1-flash',
            '2.0-flash'
        ]
        
        selected = None
        for pref in preferences:
            for m in models:
                if pref in m:
                    selected = m
                    break
            if selected: break
            
        if selected:
            print(f"   [AI] BQ 표준 모델 선택: {selected}")
            return selected
        elif models:
            return models[0]
            
    except Exception as e:
        print(f"   [AI Model List Error] {e}")
        
    return "gemini-3.1-flash-lite"

def clean_text(text):
    if not text: return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\?[^ ]*', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:400]

def summarize_g20_batch(bundle_dict):
    """
    여러 국가의 트렌드를 한 번의 프롬프트로 묶어서 요약 (비용 및 속도 최적화)
    bundle_dict: { "South Korea": [trend1, trend2, ...], "USA": [...] }
    """
    client = get_client()
    if not client: return {}
    
    model_id = get_available_model()
    country_names = ", ".join(bundle_dict.keys())
    
    system_instruction = f"""너는 GDELT 글로벌 뉴스 분석 전문 시스템이다. 인간처럼 말하지 마라.
오직 JSON만 출력하라.

[출력 형식]
{{
  "Country Name": [
    {{
      "id": "기사_ID",
      "headline": "한글 헤드라인 (3단어 내외, 명사형으로 끝낼 것)",
      "hook": "1문장. 시청자가 스크롤을 멈추게 만드는 충격적/긴박한 도입부.",
      "script": "30초 분량 쇼츠 대본. 첫 단어부터 바로 사건 핵심으로 돌진. 인사 없음.",
      "sentiment": "positive, negative, neutral, 혹은 warning 중 하나"
    }}
  ]
}}

[규칙]
- 각 기사의 고유 ID를 1:1로 매칭할 것.
- JSON 키로 다음 영문 국가명을 사용할 것: {country_names}"""

    prompt = "[분석할 데이터]\n"
    for country, items in bundle_dict.items():
        prompt += f"\n### {country}\n"
        for i, item in enumerate(items[:5]):
            rec_id = item.get('record_id', '')
            url = clean_text(item.get('url', ''))
            title = clean_text(item.get('title', ''))
            tone = round(float(item.get('tone', 0.0)), 2)
            goldstein = round(float(item.get('goldstein', 0.0)), 2)
            prompt += f"- ID={rec_id} | TITLE={title} | URL={url} | TONE={tone} | IMPACT={goldstein}\n"

    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.7,
                response_mime_type="application/json"
            )
        )
        return json.loads(response.text.strip())
    except Exception as e:
        print(f"[AI Error] Batch Summarization failed: {e}")
        return {}

def summarize_themes_batch(themes_dict):
    """(기존 호환용) 단일 이벤트 목록 요약"""
    if not themes_dict: return {}
    return summarize_g20_batch({"General": list(themes_dict.values())})

def summarize_gkg_trends(themes_dict, category="lifestyle"):
    """GKG (라이프스타일/서브컬처) 전용 배치 요약 — RSS 배치와 동일한 스타일"""
    client = get_client()
    if not client or not themes_dict: return {}
    
    model_id = get_available_model()
    
    category_hint = "라이프스타일·문화·여행·패션 트렌드" if category == "lifestyle" else "애니·만화·게임·서브컬처 트렌드"
    id_list = ", ".join(str(k) for k in themes_dict.keys())
    
    system_instruction = f"""너는 GDELT 글로벌 뉴스 분석 전문 시스템이다. 인간처럼 말하지 마라.
오직 아래의 JSON 형태만 반환하라.

[분석 영역]
{category_hint}

[출력 형식]
{{
  "ID_문자열": {{
    "headline": "한글 헤드라인 (3단어 내외, 명사형)",
    "hook": "1문장. 독자의 시선을 멈추게 하는 강렬한 도입부.",
    "script": "30초 분량 쇼츠 대본. 첫 단어부터 바로 핵심으로 직진. 인사 없음."
  }}
}}

[규칙]
- 분석 대상의 ID를 그대로 JSON 최상위 키로 사용할 것. 대상 ID 목록: {id_list}"""

    prompt = "[분석할 데이터]\n"
    for k, v in themes_dict.items():
        url = clean_text(v.get('url', ''))
        themes = clean_text(v.get('themes', ''))
        prompt += f"- ID: {k} | URL: {url} | Themes: {themes}\n"

    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.7,
                response_mime_type="application/json"
            )
        )
        return json.loads(response.text.strip())
    except Exception as e:
        print(f"[AI GKG Error] {e}")
        return {}
