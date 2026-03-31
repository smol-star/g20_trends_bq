import json
import os
from datetime import datetime, timezone, timedelta

DATA_FILE = "current_trends.json"
ARCHIVE_DIR = "archive"

def load_current_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_current_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    # 시간별 로컬 스냅샷 저장
    try:
        kst = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst)
        history_dir = os.path.join("hourly_archive", now_kst.strftime("%Y-%m-%d"))
        os.makedirs(history_dir, exist_ok=True)
        snapshot_file = os.path.join(history_dir, f"{now_kst.strftime('%H')}.json")
        with open(snapshot_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"시간별 아카이브 저장 실패: {e}")
