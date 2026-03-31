import os
import google.generativeai as genai
from dotenv import load_dotenv
import json

load_dotenv()

def init_gemini():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Gemini API 키가 없습니다. .env 파일을 확인해주세요.")
    genai.configure(api_key=api_key)

def summarize_themes_batch(themes_dict):
    """
    한 번에 여러 테마 코드를 묶어서 Gemini에게 번역/요약을 요청합니다 (속도 최적화 및 Rate Limit 방어)
    themes_dict: { id: "TAX_..., ENV_...", ... }
    """
    init_gemini()

    prompt = """다음은 GDELT 글로벌 이슈들의 뉴스 URL과 파급력/톤(분위기) 분석 수치 모음입니다.
이 이벤트들의 목록을 보고, 각 이벤트별로 다음의 3가지를 반드시 포함하는 명확한 JSON 형태의 배열(Array)로 답변해주세요.
다른 인사이트나 인사말은 절대로 출력하지 말고 **오직 순수한 JSON 형식**으로만 대답할 것!

예시 형식:
```json
[
  {
    "id": "12345",
    "headline": "아르헨티나 물가 폭등",
    "hook": "전 세계적인 비난 속에 아르헨티나가 최악의 경제 비상사태에 직면했습니다.",
    "script": "안녕하세요 글로벌 트렌드입니다. 아르헨티나 물가 폭등 소식입니다. 현재 전 세계적인 비난 속에... (약 30초 분량의 쇼츠 대본 멘트 작성)"
  }
]
```

조건:
- headline: URL을 분석하여, 이 사건이 무엇인지 유추해 자연스럽고 간결한 '구체적인 한글 뉴스 헤드라인' 3단어 내외로 요약.
- hook: 사건의 톤(분위기: -10 ~ +10)과 파급력(Goldstein: -10 ~ 10) 수치를 적극 반영해 시청자의 이목을 끄는 1~2줄 요약 문장.
- script: 유튜브 쇼츠 아나운서 리딩용 대본 초안 (30초 분량, 3~4문장).

입력 데이터:
"""
    
    for k, v in themes_dict.items():
        url = str(v.get('url', ''))[:300]
        tone = v.get('tone', 0.0)
        goldstein = v.get('goldstein', 0.0)
        prompt += f"- ID: {k} | URL: {url} | 톤: {tone} | 파급력: {goldstein}\n"
        
    try:
        try:
            model = genai.GenerativeModel('gemini-3-flash')
            response = model.generate_content(prompt)
        except Exception as e_3:
            print(f"gemini-3-flash 오류 또는 한도 초과: {e_3}")
            print("Fallback: gemini-2.5-flash 모델로 시도합니다.")
            model = genai.GenerativeModel('gemini-2.5-flash')
            response = model.generate_content(prompt)
            
        result = response.text.strip()
        
        try:
            # JSON 배열 텍스트만 찾아서 강력하게 추출 (정규식/문자열 슬라이싱 방식)
            start_idx = result.find('[')
            end_idx = result.rfind(']')
            if start_idx != -1 and end_idx != -1:
                result = result[start_idx:end_idx+1]
            else:
                # 대괄호가 없다면 예비로 마크다운 찌꺼기 제거
                result = result.replace("```json", "").replace("```", "").strip()
                    
            parsed_result = {}
            json_data = json.loads(result)
            for item in json_data:
                parsed_result[str(item['id'])] = {
                    "headline": item.get("headline", "식별 불가"),
                    "hook": item.get("hook", "분석 실패"),
                    "script": item.get("script", "대본 생성 실패")
                }
            
            return parsed_result
        except json.JSONDecodeError as je:
            print(je)
            print(f"==============================")
            print(f"[JSON 파싱 에러 발생] Gemini 원본 응답 내용:\n{response.text}")
            print(f"==============================")
            return {}
    except Exception as e:
        import traceback
        print(f"==============================")
        print(f"Gemini API 통신/기타 오류: {e}")
        traceback.print_exc()
        print(f"==============================")
        return {}
