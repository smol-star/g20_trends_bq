import os
import google.generativeai as genai
from dotenv import load_dotenv

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
    # Cost & speed effective model
    model = genai.GenerativeModel('gemini-1.5-flash-8b')
    
    prompt = "다음은 GDELT 이벤트 아이디와 관련된 뉴스 URL(또는 식별자) 모음입니다. 이 주소 텍스트를 분석해 이 사건이 어떤 사건인지 유추하여 일반인이 알기 쉬운 자연스럽고 간결한 '뉴스 헤드라인 키워드(한국어, 3단어 내외)'로 바꿔서 반환해주세요.\n"
    prompt += "결과는 반드시 '아이디: 키워드' 형식으로 한 줄씩 출력해주세요.\n\n"
    
    for k, v in themes_dict.items():
        # URL 텍스트를 전달
        short_v = v[:300] if isinstance(v, str) else str(v)
        prompt += f"{k}: {short_v}\n"
        
    try:
        response = model.generate_content(prompt)
        result = response.text.strip()
        
        parsed_result = {}
        for line in result.split('\n'):
            if ':' in line:
                idx, kw = line.split(':', 1)
                parsed_result[idx.strip()] = kw.strip().replace('"', '')
        
        return parsed_result
    except Exception as e:
        print(f"Gemini API 일괄 처리 오류: {e}")
        return {}
