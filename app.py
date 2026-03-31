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
            <div style="display: flex; align-items: center; margin-top: 30px; margin-bottom: 10px;">
                <span style="font-size: 1.5em; font-weight: bold; margin-right: 12px; color: #1a73e8;">{current_rank}위</span>
                <img src="https://flagcdn.com/w40/{flag_code}.png" width="36" style="border: 1px solid #e0e0e0; border-radius: 4px; margin-right: 12px; box-shadow: 0px 2px 4px rgba(0,0,0,0.1);">
                <h3 style="margin: 0; padding: 0;">{country} <span style="font-size: 0.7em; font-weight: normal; color: #888;">(Spike: {score:.1f}p)</span></h3>
            </div>
        ''', unsafe_allow_html=True)
        
        trends = info.get("trends", [])
        if not trends:
            st.write("GDELT 데이터셋에서 최근 24시간 내 수집된 주요 이슈가 없습니다.")
            continue
            
        for idx, t in enumerate(trends):
            keyword = t.get('keyword', '이슈명 분석 실패')
            hook = t.get('hook', '요약 훅이 생성되지 않았습니다.')
            script = t.get('script', '쇼츠 대본이 생성되지 않았습니다.')
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
                
            # 바깥쪽에 바로 읽을 수 있는 헤드라인과 훅 노출
            st.markdown(f"<h4 style='margin-bottom: 5px; color: #d32f2f;'>🚨 {keyword}</h4>", unsafe_allow_html=True)
            st.markdown(f"<div style='background-color: #f8f9fa; border-left: 4px solid #1a73e8; padding: 10px; margin-bottom: 10px; font-size: 1.1em;'>💡 <b>{hook}</b></div>", unsafe_allow_html=True)
            
            # 안쪽에 구체적인 대본과 수치 숨김
            with st.expander(f"👉 🎤 리딩용 대본 및 원본 지표 열람"):
                st.markdown("#### 🎙️ AI 제안 쇼츠 대본 (약 30초)")
                st.info(script)
                
                st.divider()
                st.markdown(f"📊 **GDELT 원본 스탯** : 전파력(언급) {mentions}회 &nbsp;|&nbsp; 공신력(매체수) {sources}곳 &nbsp;|&nbsp; 파급력(Goldstein) {goldstein} &nbsp;|&nbsp; 언론 분위기 지수 {tone_badge}", unsafe_allow_html=True)
                
                url = t.get('url', '')
                if url:
                    st.markdown(f"🔗 **대표 보도 기사 원본**: <a href='{url}' target='_blank'>링크 이동하기</a> (안전상의 이유로 1개만 우선 제공)", unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
        st.caption(f"🔄 최신 동기화 완료 시간 (BigQuery): {info.get('last_updated', 'N/A')}")
        st.markdown("<hr style='border: 1px dotted #ccc;'>", unsafe_allow_html=True)

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
    st.markdown("매시간 수집되어 보존된 과거 기록 스냅샷을 열람할 수 있습니다.")
    
    archive_dir = "hourly_archive"
    if not os.path.exists(archive_dir):
        st.warning("🗂️ 아직 수집되어 저장된 과거 기록이 없습니다. 데이터가 쌓일 때까지 기다려 주세요.")
    else:
        dates = sorted(os.listdir(archive_dir), reverse=True)
        if not dates:
            st.warning("🗂️ 아직 수집되어 저장된 시간별 기록이 없습니다.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                selected_date = st.selectbox("📅 보관 날짜 선택 (KST 기준)", dates)
            
            date_dir = os.path.join(archive_dir, selected_date)
            # JSON 파일만 필터링 후 시간(숫자) 기준 내림차순 정렬
            hour_files = [f for f in os.listdir(date_dir) if f.endswith('.json')]
            hours = sorted([f.replace('.json', '') for f in hour_files], reverse=True)
            
            with col2:
                if not hours:
                    st.selectbox("⏰ 시간 선택", ["기록 없음"])
                else:
                    selected_hour = st.selectbox("⏰ 수집 시간 선택", hours, format_func=lambda x: f"{x}시 스냅샷")
            
            if hours:
                st.markdown("<br>", unsafe_allow_html=True)
                file_path = os.path.join(date_dir, f"{selected_hour}.json")
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        archive_data = json.load(f)
                    st.success(f"✅ {selected_date} {selected_hour}시에 저장된 역사적 글로벌 트렌드 기록입니다.")
                    render_dashboard(archive_data, f"{selected_date} {selected_hour}시 (과거 아카이브)")
                except Exception as e:
                    st.error(f"기록 파일을 읽는데 실패했습니다: {e}")
