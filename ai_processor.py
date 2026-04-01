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

[출력 형식]
{{
  "Country Name": {{
    "headline": "한글 헤드라인 (3단어 내외, 명사형으로 끝낼 것)",
    "hook": "1문장. 시청자가 스크롤을 멈추게 만드는 충격적/긴박한 도입부.",
    "script": "30초 분량 쇼츠 대본. 첫 단어부터 바로 사건 핵심으로 돌진. 인사 없음."
  }},
  ...
}}

[국가명 키 규칙]
- 반드시 아래 영문 이름 그대로 사용할 것: {country_names}

[분석 지침]
- TONE(감성): 음수일수록 부정, 양수일수록 긍정적 결말.
- IMPACT(파급력): 절댓값이 클수록 국제사회 파급 강도 높음.
- 3시간치 복수 기사 → '가장 큰 하나의 흐름'으로 통합 요약.

[분석할 데이터]
"""
    for country, items in bundle_dict.items():
        prompt += f"\n### {country}\n"
        for i, item in enumerate(items[:5]):
            url = clean_text(item.get('url', ''))
            title = clean_text(item.get('title', ''))
            tone = round(float(item.get('tone', 0.0)), 2)
            goldstein = round(float(item.get('goldstein', 0.0)), 2)
            prompt += f"{i+1}. TITLE={title} | URL={url} | TONE={tone} | IMPACT={goldstein}\n"

    try:
        # 모델 선택 로직 (사용자 피드백 반영: 3 Flash -> 2.5 Flash 순서)
        try:
            model = genai.GenerativeModel('gemini-3-flash')
            response = model.generate_content(prompt)
        except Exception:
            model = genai.GenerativeModel('gemini-2.5-flash')
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
    """GKG (라이프스타일/서브컬처) 전용 배치 요약"""
    init_gemini()
    
    if not themes_dict: return {}

    persona_name = "트렌드 매거진 에디터" if category == "lifestyle" else "서브컬처 전문 리뷰어"
    extra_rule = "가챠 게임(원신, 스타레일 등) 포착 시 시장 파급력과 매출 영향력을 심층 기술해라." if category != "lifestyle" else "세련된 도심지 라이프스타일의 변화에 주목해라."

    prompt = f"""
너는 {persona_name}로서 글로벌 트렌드를 분석한다.
다음 입력 데이터를 보고 각 ID별로 분석 결과를 JSON 객체로 반환해라.

[출력 형식]
{{
  "ID_STRING": {{
    "headline": "한글 헤드라인 (3단어 내외)",
    "hook": "독자의 시선을 끄는 1줄 문장",
    "script": "전문적인 리딩용 대본 (30초 분량)"
  }},
  ...
}}

[절대 규칙]
1. 인사말 금지: "알겠습니다", "요약 결과입니다" 등 어떤 부약 설명도 없이 오직 JSON만 출력할 것.
2. 강력한 시작: script는 인사 없이 바로 핵심 훅(Hook)으로 시작할 것.
3. {extra_rule}
4. 한국어로 작성할 것.

[입력 데이터]
"""
    for k, v in themes_dict.items():
        url = clean_text(v.get('url', ''))
        themes = clean_text(v.get('themes', ''))
        prompt += f"- ID: {k} | URL: {url} | Themes: {themes}\n"

    try:
        try:
            model = genai.GenerativeModel('gemini-3-flash')
            response = model.generate_content(prompt)
        except Exception:
            model = genai.GenerativeModel('gemini-1.5-flash') # 2.5가 없을 경우 대비 실제 모델명 사용 권장
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
