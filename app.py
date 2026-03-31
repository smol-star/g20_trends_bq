import streamlit as st
import pandas as pd
from data_manager import load_current_data, ARCHIVE_DIR
import os
import json

st.set_page_config(page_title="G20 실시간 글로벌 뉴스 분석 (BigQuery 엔진)", layout="wide")

FLAG_CODES = {
    'United States': 'us', 'China': 'cn', 'Germany': 'de', 'Japan': 'jp',
    'India': 'in', 'United Kingdom': 'gb', 'France': 'fr', 'Italy': 'it',
    'Brazil': 'br', 'Canada': 'ca', 'Russia': 'ru', 'Mexico': 'mx',
    'Australia': 'au', 'South Korea': 'kr', 'Indonesia': 'id', 'Turkey': 'tr',
    'Saudi Arabia': 'sa', 'Argentina': 'ar', 'South Africa': 'za'
}

def render_dashboard(data, kst_now):
    st.divider()
    for country, info in data.items():
        score = info.get("spike_score", 0.0)
        rank = info.get("gdp_rank", 99)
        current_rank = info.get("current_rank", 99)
        
        flag_code = FLAG_CODES.get(country, 'kr')
        
        st.markdown(f'''
            <div style="display: flex; align-items: center; margin-top: 20px; margin-bottom: 5px;">
                <span style="font-size: 1.2em; font-weight: bold; margin-right: 12px; color: #555;">{current_rank}위</span>
                <img src="https://flagcdn.com/w40/{flag_code}.png" width="36" style="border: 1px solid #e0e0e0; border-radius: 4px; margin-right: 12px; box-shadow: 0px 2px 4px rgba(0,0,0,0.1);">
                <h3 style="margin: 0; padding: 0;">{country} <span style="font-size: 0.7em; font-weight: normal; color: #888;">(엔진 평가 점수: {score:.1f}p)</span></h3>
            </div>
        ''', unsafe_allow_html=True)
        
        with st.expander(f"👉 {country} AI 분석 핵심 뉴스와 지수 열기", expanded=False):
            st.caption(f"🔄 수집 및 분석 갱신 시간: {info.get('last_updated', 'N/A')} &nbsp;|&nbsp; ⏰ 기록 시간: {kst_now}")
            
            trends = info.get("trends", [])
            if not trends:
                st.write("GDELT 데이터셋에서 최근 24시간 내 수집된 주요 이슈가 없습니다.")
            
            for idx, t in enumerate(trends):
                keyword = t.get('keyword', '이슈명 분석 실패')
                mentions = t.get('mentions', 0)
                sources = t.get('sources', 0)
                goldstein = t.get('goldstein', 0.0)
                tone = t.get('tone', 0.0)
                
                # 톤(Tone) 뱃지 색상
                if tone > 0:
                    tone_badge = f"<span style='color: #4CAF50;'>긍정 (Tone: +{tone:.1f})</span>"
                elif tone < 0:
                    tone_badge = f"<span style='color: #F44336;'>부정 (Tone: {tone:.1f})</span>"
                else:
                    tone_badge = f"<span style='color: #9E9E9E;'>중립 (Tone: 0.0)</span>"
                    
                st.markdown(f"<p style='font-size: 1.2em; font-weight: bold; margin: 6px 0 2px 0; color: #E91E63;'>{idx+1}. 📢 {keyword}</p>", unsafe_allow_html=True)
                
                st.markdown(f"**전파력(언급횟수)**: {mentions}회 &nbsp;|&nbsp; **공신력(보도매체 수)**: {sources}곳 &nbsp;|&nbsp; **국제사회 영향력(Goldstein)**: {goldstein} &nbsp;|&nbsp; **분위기**: {tone_badge}", unsafe_allow_html=True)
                
                if idx < len(trends) - 1:
                    st.markdown("<div style='border-top: 1px dashed #e0e0e0; margin: 8px 0;'></div>", unsafe_allow_html=True)

page = st.sidebar.radio("메뉴", ["실시간 AI 심층 브리핑", "과거 기록 보기"])

if page == "실시간 AI 심층 브리핑":
    st.title("🌐 G20 빅쿼리 & AI 심층 트렌드 분석기")
    st.markdown("전세계 최대 글로벌 이벤트 DB인 **GDELT 2.0 (Google BigQuery)**를 통해 최근 24시간 내 수십만 건의 기사를 기반으로 각국의 가장 큰 이슈들을 추출한 뒤, **Gemini AI**를 통해 쉬운 키워드로 번역/요약한 대시보드입니다.")
    st.markdown("💡 **순위 기준**: 국제사회 파급력(Goldstein) 및 다수 매체 동시보도(NumSources), 누적 언급량(Mentions) 복합점수제")
    
    data = load_current_data()
    if not data:
        st.info("데이터를 수집 중이거나 백그라운드 스크립트가 아직 실행되지 않았습니다. 1시간마다 업데이트됩니다.")
    else:
        render_dashboard(data, "최신 (실시간 렌더링)")
else:
    st.title("📜 BQ-AI 과거 데이터 기록소")
    st.markdown("1시간 주기로 데이터가 보존된 과거 기록을 열람할 수 있습니다. (hourly_archive 폴더 기준)")
    st.info("현재 UI 버전에서는 Streamlit이 백그라운드 스냅샷을 곧바로 보여줄 수 있도록 개발 중이며, 향후 기능이 확장될 예정입니다.")
