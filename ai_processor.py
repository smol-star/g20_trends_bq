import os
import google.generativeai as genai
from dotenv import load_dotenv
import json
import re

load_dotenv()

def init_gemini():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("[AI] GEMINI_API_KEY가 환경변수에 없습니다. GitHub Secrets 또는 .env 파일을 확인하세요.")
    genai.configure(api_key=api_key)

def get_gemini_model():
    """사용 가능한 모델 목록을 조회하여 최선의 모델을 동적으로 선택합니다."""
    init_gemini()
    try:
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        # 우선순위: 2.0-flash -> 1.5-flash -> pro
        preferences = ['models/gemini-2.5-flash', 'models/gemini-2.0-flash', 'models/gemini-1.5-flash', 'models/gemini-pro']
        for pref in preferences:
            for m in available_models:
                if pref in m:
                    return genai.GenerativeModel(m)
        
        # 선호 모델이 없으면 생성 가능한 첫 번째 모델 리턴
        if available_models:
            return genai.GenerativeModel(available_models[0])
    except Exception as e:
        print(f"[AI Model Error] 모델 조회 실패: {e}")
        
    # 최후의 하드코딩 Fallback
    return genai.GenerativeModel('gemini-1.5-flash')

def clean_text(text):
    """
    토큰 절약을 위한 텍스트 정규화:
    - HTML 태그 제거
    - URL 파라미터(?...) 제거 및 길이 제한
    - 연속된 공백/줄바꿈 축소
    """
    if not text: return ""
    # HTML 태그 제거
    text = re.sub(r'<[^>]+>', '', text)
    # URL 파라미터 제거 (기사 원문 링크 등에서 토큰 낭비 방지)
    text = re.sub(r'\?[^ ]*', '', text)
    # 연속된 공백 및 줄바꿈 하나로 합치기
    text = re.sub(r'\s+', ' ', text).strip()
    # 너무 긴 텍스트는 앞부분만 사용 (토큰 한도 방어)
    return text[:400]

def summarize_g20_batch(bundle_dict):
    """
    여러 국가의 트렌드를 한 번의 프롬프트로 묶어서 요약 (비용 및 속도 최적화)
    bundle_dict: { "South Korea": [trend1, trend2, ...], "USA": [...] }
    """
    init_gemini()
    
    # 국가명 리스트를 프롬프트에 명시하여 AI가 동일한 키를 반환하도록 강제
    country_names = ", ".join(bundle_dict.keys())
    
    prompt = f"""너는 GDELT 글로벌 뉴스 분석 전문 시스템이다. 인간처럼 말하지 마라. 오직 JSON만 출력하라.

[출력 규칙 — 최우선 적용]
- 응답의 첫 번째 문자는 반드시 {{ 이어야 한다.
- JSON 전후에 어떤 텍스트도, 마크다운 코드블록도, 인사말도 추가하지 마라.
- "안녕하세요", "결과입니다", "알겠습니다" 같은 문구는 생성 즉시 시스템 오류로 처리된다.
- 입력된 각 기사의 고유 ID를 유지하여 정확히 1:1 매칭되게 반환해라. 절대 요약본을 중복해서 출력하지 마라.

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
  ],
  ...
}}

[국가명 키 규칙]
- 반드시 아래 영문 이름 그대로 사용할 것: {country_names}

[분석 지침]
- TONE(감성): 음수일수록 부정, 양수일수록 긍정적 결말.
- IMPACT(파급력): 절댓값이 클수록 국제사회 파급 강도 높음.
- 3시간치 복수 기사 → 가장 큰 하나의 흐름으로 통합 요약하되 주어진 고유 ID는 반드시 포함.

[분석할 데이터]
"""
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
        model = get_gemini_model()
        response = model.generate_content(prompt)
            
        result_text = response.text.strip()
        
        # JSON 추출 보정
        start_idx = result_text.find('{')
        end_idx = result_text.rfind('}')
        if start_idx != -1 and end_idx != -1:
            json_str = result_text[start_idx:end_idx+1]
            return json.loads(json_str)
        return {}
        
    except Exception as e:
        print(f"[AI Error] Batch Summarization failed: {e}")
        return {}

def summarize_themes_batch(themes_dict):
    """(기존 호환용) 단일 이벤트 목록 요약"""
    init_gemini()
    # 로직은 위와 유사하되 단일 리스트 처리용으로 간소화
    # ... (필요시 리팩토링하여 summarize_g20_batch를 재사용하거나 유지)
    # 현재는 fetcher.py 수정을 통해 summarize_g20_batch를 주력으로 사용할 예정입니다.
    return summarize_g20_batch({"General": list(themes_dict.values())})

def summarize_gkg_trends(themes_dict, category="lifestyle"):
    """GKG (라이프스타일/서브컬처) 전용 배치 요약 — RSS 배치와 동일한 스타일"""
    init_gemini()

    if not themes_dict: return {}

    # 카테고리 힌트 (페르소나 없이, 분석 맥락만 한 줄로)
    category_hint = "라이프스타일·문화·여행·패션 트렌드" if category == "lifestyle" else "애니·만화·게임·서브컬처 트렌드"
    id_list = ", ".join(str(k) for k in themes_dict.keys())

    prompt = f"""너는 GDELT 글로벌 뉴스 분석 전문 시스템이다. 인간처럼 말하지 마라. 오직 JSON만 출력하라.

[출력 규칙 — 최우선 적용]
- 응답의 첫 번째 문자는 반드시 {{ 이어야 한다.
- JSON 전후에 어떤 텍스트도, 마크다운 코드블록도, 인사말도 추가하지 마라.
- "안녕하세요", "결과입니다", "알겠습니다" 같은 문구는 절대 출력하지 마라.
- 아래 ID 목록을 JSON 키로 그대로 사용할 것: {id_list}

[분석 영역]
{category_hint}

[출력 형식]
{{
  "ID_문자열": {{
    "headline": "한글 헤드라인 (3단어 내외, 명사형)",
    "hook": "1문장. 독자의 시선을 멈추게 하는 강렬한 도입부.",
    "script": "30초 분량 쇼츠 대본. 첫 단어부터 바로 핵심으로 직진. 인사 없음."
  }},
  ...
}}

[분석할 데이터]
"""
    for k, v in themes_dict.items():
        url = clean_text(v.get('url', ''))
        themes = clean_text(v.get('themes', ''))
        prompt += f"- ID: {k} | URL: {url} | Themes: {themes}\n"

    try:
        model = get_gemini_model()
        response = model.generate_content(prompt)

        result_text = response.text.strip()
        start = result_text.find('{')
        end = result_text.rfind('}')
        if start != -1 and end != -1:
            return json.loads(result_text[start:end+1])
        return {}
    except Exception as e:
        print(f"[AI GKG Error] {e}")
        return {}
