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
여기서 너의 역할은 '핵심만을 빠르고 간결하게 전하는 전문 아나운서'입니다.

이 이벤트들의 목록을 보고, 각 이벤트별로 다음의 3가지를 반드시 포함하는 명확한 JSON 형태의 배열(Array)로 답변해주세요.
다른 인사이트나 인사말은 절대로 출력하지 말고 **오직 순수한 JSON 형식**으로만 대답할 것!

예시 형식:
```json
[
  {
    "id": "12345",
    "headline": "아르헨티나 물가 폭등",
    "hook": "전 세계적인 비난 속에 아르헨티나가 최악의 경제 비상사태에 직면했습니다.",
    "script": "아르헨티나에 물가 폭등 사태가 터졌습니다! 현재 전 세계적인 비난 속에... (약 30초 분량의 쇼츠 대본 멘트 작성)"
  }
]
```

조건 (필독!):
1. 자아 부여: 너는 핵심만을 빠르고 간결하게 전하는 쇼츠(Shorts) 전문 아나운서다.
2. 인사말 절대 금지: script 작성 시 '안녕하세요', '글로벌 트렌드입니다' 같은 쓸데없는 소리는 단 한 마디도 쓰지 마라. 1초도 낭비하지 말고 바로 사건의 핵심으로 돌진해라.
3. 링크 심층 분석: 단순히 제목만 번역하지 말고, 주소창(URL)의 단어 조합을 보고 '무슨 일이 터졌는지' 엄청난 상상력과 논리를 발휘해 아주 구체적이고 자극적으로 묘사해라.
4. headline: URL을 상상력으로 분석하여 사건이 무엇인지 유추한 자연스럽고 간결한 '구체적인 한글 뉴스 헤드라인' 3단어 내외.
5. hook: 사건의 톤(분위기: -10 ~ +10)과 파급력(Goldstein: -10 ~ 10) 수치를 적극 반영해 시청자의 스크롤을 멈추게 하는 1~2줄 요약 문장.
6. script: 유튜브 쇼츠 아나운서 리딩용 속도감 있는 대본 초안 (30초 분량, 3~4문장).

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

def summarize_gkg_trends(themes_dict, category="lifestyle"):
    init_gemini()
    
    if category == "lifestyle":
        persona = "너는 세련되고 정보 전달력이 뛰어난 '트렌드 매거진 에디터'다."
        extra_instruction = ""
    else:  # subculture
        persona = "너는 해당 분야에 대한 깊은 지식을 가진 매우 열정적인 '전문 리뷰어'다."
        extra_instruction = "만약 데이터에 핵심 가챠 게임(Macro-12: 원신, 붕괴 스타레일 등)이 포함되어 있다면, 해당 정보의 매크로 영향력을 '상(High)'으로 간주하고, 이것이 라이프스타일 등 시장 전반의 다른 서브컬처나 매출에 미치는 파급력(Cannibalization 등)을 종합적으로 기술해라."

    prompt = f"""다음은 GDELT GKG 라이프스타일 및 서브컬처 트렌드 데이터 모음입니다.
{persona}

이 이벤트들의 목록을 보고, 각 이벤트별로 다음의 3가지를 반드시 포함하는 명확한 JSON 형태의 배열(Array)로 답변해주세요.
다른 인사이트나 인사말은 절대로 출력하지 말고 **오직 순수한 JSON 형식**으로만 대답할 것!

예시 형식:
```json
[
  {{
    "id": "12345",
    "headline": "블루 아카이브 3주년 달성",
    "hook": "전 세계 서브컬처 팬덤을 휩쓴 푸른 청춘의 이야기, 마침내 3주년을 맞이했습니다.",
    "script": "단숨에 시장의 판도를 바꾼 초대형 업데이트가 떴습니다! 이번 3주년 이벤트는... (약 30초 분량 쇼츠/리뷰 대본)"
  }}
]
```

조건 (필독!):
1. 자아 부여: {persona}
2. 인사말 절대 금지: script 작성 시 '안녕하세요', '글로벌 트렌드입니다' 같은 쓸데없는 소리는 단 한 마디도 쓰지 마라. 1초도 낭비하지 말고 바로 시청자의 시선을 끄는 강력한 훅(Hook)으로 대본을 시작해라.
3. 링크 및 테마 심층 분석: URL과 주어진 테마 키워드들을 연결하여 어떤 트렌드가 떠오르고 있는지 아주 구체적이고 매력적으로 묘사해라.
4. headline: URL/테마를 상상력으로 분석해 사건이 무엇인지 유추한 자연스럽고 간결한 '구체적인 한글 뉴스 헤드라인' 3단어 내외.
5. hook: 시청자의 스크롤을 멈추게 하는 1~2줄 요약 문장.
6. script: 유튜브 쇼츠 또는 릴스 리뷰어 리딩용 속도감 있는 대본 초안 (30초 분량, 3~4문장).
{extra_instruction}

입력 데이터:
"""
    for k, v in themes_dict.items():
        url = str(v.get('url', ''))[:300]
        themes = str(v.get('themes', ''))[:200]
        prompt += f"- ID: {k} | URL: {url} | 테마/키워드: {themes}\n"
        
    try:
        try:
            model = genai.GenerativeModel('gemini-3-flash')
            response = model.generate_content(prompt)
        except Exception as e_3:
            print(f"gemini-3-flash 오류: {e_3}. Fallback: gemini-2.5-flash")
            model = genai.GenerativeModel('gemini-2.5-flash')
            response = model.generate_content(prompt)
            
        result = response.text.strip()
        
        try:
            start_idx = result.find('[')
            end_idx = result.rfind(']')
            if start_idx != -1 and end_idx != -1:
                result = result[start_idx:end_idx+1]
            else:
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
            print(f"[JSON 파싱 에러] Gemini 원본 응답:\n{{response.text}}")
            return {}
    except Exception as e:
        print(f"Gemini API 통신/기타 오류: {e}")
        return {}
