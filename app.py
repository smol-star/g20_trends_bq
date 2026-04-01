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
    if data:
        first_country = list(data.values())[0]
        last_updated = first_country.get('last_updated', '알 수 없음')
        st.markdown(f"<div style='text-align: right; color: #2c3e50; font-size: 1.2em; font-weight: bold; background: #ecf0f1; padding: 10px; border-radius: 8px; margin-bottom: 20px;'>🔄 마지막 업데이트: {last_updated}</div>", unsafe_allow_html=True)
    
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
                
            # 감정 분석에 따른 뱃지 및 테두리 색상
            sentiment = t.get('sentiment', 'neutral').lower()
            if sentiment == 'positive':
                border_color, bg_color, emoji = '#4CAF50', '#F1F8E9', '✅'
            elif sentiment == 'negative':
                border_color, bg_color, emoji = '#e53935', '#FFEBEE', '🚨'
            elif sentiment == 'warning':
                border_color, bg_color, emoji = '#FF9800', '#FFF3E0', '⚠️'
            else:
                border_color, bg_color, emoji = '#1a73e8', '#f8f9fa', 'ℹ️'
                
            # 바깥쪽: 강력한 헤드라인과 훅 노출
            st.markdown(f"<h3 style='margin-bottom: 8px; color: #2C3E50;'>{emoji} {keyword}</h3>", unsafe_allow_html=True)
            st.markdown(f"<div style='background-color: {bg_color}; border-left: 6px solid {border_color}; padding: 15px; margin-bottom: 8px; font-size: 1.3em; font-weight: 800; color: #111; border-radius: 4px; line-height: 1.4;'>💡 {hook}</div>", unsafe_allow_html=True)
            
            # 안쪽: 대본, 상세 지표, 관련 기사 링크 등 숨김 처리
            with st.expander(f"👉 🎤 관련 원문 기사 리스트 및 리딩용 대본 보기"):
                st.markdown(f"<div style='font-size: 1.05em; line-height: 1.6; color: #333; padding: 15px; background: #fff; border: 1px solid #ddd; border-radius: 8px; font-family: sans-serif;'><h4 style='margin-top:0;'>🎙️ AI 제안 쇼츠 대본</h4>{script}</div>", unsafe_allow_html=True)
                
                st.divider()
                st.markdown(f"📊 **GDELT 원본 스탯** : 전파력(언급) {mentions}회 &nbsp;|&nbsp; 공신력(매체수) {sources}곳 &nbsp;|&nbsp; 파급력(Goldstein) {goldstein} &nbsp;|&nbsp; 언론 분위기 지수 {tone_badge}", unsafe_allow_html=True)
                
                url = t.get('url', '')
                related_urls = t.get('related_urls', [])
                
                if url:
                    st.markdown(f"🔗 **[대표 원본 기사 보러가기]({url})**")
                if related_urls:
                    st.markdown("🔗 **관련 원본 기사 묶음:**")
                    for r_url in related_urls:
                        st.markdown(f"- [{r_url}]({r_url})")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
        st.caption(f"🔄 최신 동기화 완료 시간 (BigQuery): {info.get('last_updated', 'N/A')}")
def render_lifestyle(trends):
    st.markdown("<br><h3>🌿 스페셜 매거진: 라이프스타일 포커스</h3>", unsafe_allow_html=True)
    if not trends:
        st.write("최근 수집된 관련 트렌드 데이터가 없습니다.")
        return
        
    for t in trends:
        st.markdown(f"<h4 style='color: #2E7D32;'>✨ {t.get('keyword', '')}</h4>", unsafe_allow_html=True)
        st.markdown(f"<div style='border-left: 4px solid #4CAF50; padding: 10px; background-color: #F1F8E9; margin-bottom: 10px;'>💡 <b>{t.get('hook', '')}</b></div>", unsafe_allow_html=True)
        with st.expander("👉 트렌드 매거진 에디터 대본 보기"):
            st.info(t.get('script', ''))
            st.markdown(f"[🔗 관련 보도/스레드 원문 이동]({t.get('url', '#')})")
        st.divider()

def render_subculture(trends):
    st.markdown("<br><h3>🎮 서브컬처 & 코어 게임 리뷰</h3>", unsafe_allow_html=True)
    
    if trends and trends[0].get('market_monopoly'):
        st.warning("🚨 [Macro Tracker] 상위 12개 핵심 게임 라인업이 현재 전체 서브컬처 트래픽의 70% 이상을 장악하며 시장 내 간섭 현상(Cannibalization)을 주도하고 있습니다.")
        
    for t in trends:
        badges = []
        if 'Gacha Game' in t.get('sub_categories', []):
            badges.append("🎲 가챠(Gacha)")
        if 'Anime' in t.get('sub_categories', []):
            badges.append("📺 애니(Anime)")
            
        badge_html = " ".join([f"<span style='background-color: #607D8B; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em;'>{b}</span>" for b in badges])
        
        # Macro 게임의 경우 특별 뱃지 적용
        if t.get('macro_tag'):
            color = "#E91E63" if t.get('macro_tag') == "Market_Leader" else "#9C27B0"
            macro_badge = f"<span style='background-color: {color}; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; margin-left: 8px;'>#{t.get('macro_tag')} ({t.get('matched_game')})</span>"
            badge_html += macro_badge
            
        st.markdown(f"<h4 style='color: #E91E63;'>🔥 {t.get('keyword', '')} {badge_html}</h4>", unsafe_allow_html=True)
        st.markdown(f"<div style='border-left: 4px solid #E91E63; padding: 10px; background-color: #FCE4EC; margin-bottom: 10px;'>💡 <b>{t.get('hook', '')}</b></div>", unsafe_allow_html=True)
        with st.expander("👉 전문 리뷰어 심층 분석 리딩 대본"):
            st.info(t.get('script', ''))
            st.write(f"⚠️ 매크로 영향력 설정: **{t.get('impact', 'Normal')}**")
            url = t.get('url', '#')
            if url != "None":
                st.markdown(f"[🔗 관련 출처 및 원문 링크]({url})")
        st.divider()

page = st.sidebar.radio("메뉴", ["실시간 AI 심층 브리핑", "과거 기록 보기"])

if page == "실시간 AI 심층 브리핑":
    st.title("🌐 G20 빅쿼리 & AI 심층 트렌드 분석기")
    st.markdown("전세계 최대 글로벌 이벤트 DB인 **GDELT 2.0 (Google BigQuery)**를 통해 **최근 3시간** 내 수십만 건의 기사를 기반으로 각국의 가장 큰 이슈들을 추출한 뒤, **Gemini AI**를 통해 쉬운 키워드로 번역/요약한 대시보드입니다.")
    st.markdown("💡 **순위 기준**: 국제사회 파급력(Goldstein) 및 다수 매체 동시보도(NumSources), 누적 언급량(Mentions) 복합점수제")
    
    tab_news, tab_life, tab_sub = st.tabs(["[1] ⚡ 실시간 속보", "[2] 🌿 라이프스타일 뷰", "[3] 🎮 서브컬처 & 애니"])
    
    with tab_news:
        data = load_current_data()
        if not data:
            st.info("데이터를 수집 중이거나 백그라운드 스크립트가 아직 실행되지 않았습니다.")
        else:
            render_dashboard(data, "최신 (실시간 렌더링)")
            
    with tab_life:
        try:
            with open("lifestyle_trends.json", "r", encoding="utf-8") as f:
                life_data = json.load(f)[0]
                if life_data.get("status") == "error":
                    st.error(f"🚨 {life_data.get('title')}")
                    st.warning(life_data.get("summary"))
                else:
                    render_lifestyle(life_data.get("data", []))
        except FileNotFoundError:
            st.info("💡 라이프스타일 트렌드 데이터는 주 2회(월, 목 오전 7시 13분 KST) 정기 업데이트됩니다. 아직 데이터가 수집되지 않았습니다.")
            
    with tab_sub:
        try:
            with open("subculture_trends.json", "r", encoding="utf-8") as f:
                sub_data = json.load(f)[0]
                if sub_data.get("status") == "error":
                    st.error(f"🚨 {sub_data.get('title')}")
                    st.warning(sub_data.get("summary"))
                else:
                    render_subculture(sub_data.get("data", []))
        except FileNotFoundError:
            st.info("💡 서브컬처 및 시장 파급력 데이터는 주 2회(월, 목 오전 7시 13분 KST) 정기 업데이트됩니다. 아직 데이터가 수집되지 않았습니다.")
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
