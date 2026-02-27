import os
import time
import json
import hashlib
import requests
import jwt  # from PyJWT
import pandas as pd
import streamlit as st
from collections import defaultdict

# ----------------------------
# Page Config
# ----------------------------
st.set_page_config(page_title="PassKit 資源回收站", page_icon="♻️", layout="wide")
st.title("♻️ PassKit 資源回收指派系統")
st.caption("自動識別重複 ID、建立持久化回收池，並分配給缺額會員。")

# ----------------------------
# Session State 初始化 (持久化存儲)
# ----------------------------
if "persistent_recycle_pool" not in st.session_state:
    st.session_state.persistent_recycle_pool = []  # 跨搜尋的「彈藥庫」

if "search_results" not in st.session_state:
    st.session_state.search_results = {"all_rows": [], "missing": [], "search_done": False}

# ----------------------------
# Config helpers
# ----------------------------
def get_config(key: str, default: str | None = None) -> str | None:
    val = st.secrets.get(key) if hasattr(st, "secrets") else None
    if val is None:
        val = os.environ.get(key, default)
    if val is None:
        return None
    return str(val).replace("\\n", "\n").strip()

PK_API_KEY = get_config("PK_API_KEY")
PK_API_SECRET = get_config("PK_API_SECRET")
PK_API_PREFIX = get_config("PK_API_PREFIX", "https://api.pub1.passkit.io")
PROGRAM_ID = get_config("PROGRAM_ID")

missing_cfg = [k for k, v in {
    "PK_API_KEY": PK_API_KEY,
    "PK_API_SECRET": PK_API_SECRET,
    "PK_API_PREFIX": PK_API_PREFIX,
    "PROGRAM_ID": PROGRAM_ID
}.items() if not v]

if missing_cfg:
    st.error(f"❌ 缺少設定：{', '.join(missing_cfg)}")
    st.stop()

# ----------------------------
# API Functions (核心函式，不可省略)
# ----------------------------
def make_jwt_for_body(body_text: str) -> str:
    now = int(time.time())
    payload = {"uid": PK_API_KEY, "iat": now, "exp": now + 600}
    if body_text:
        payload["signature"] = hashlib.sha256(body_text.encode("utf-8")).hexdigest()
    token = jwt.encode(payload, PK_API_SECRET, algorithm="HS256")
    return token.decode("utf-8") if isinstance(token, bytes) else token

def post_list_members(filters_payload: dict) -> list[dict]:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)
    headers = {"Authorization": make_jwt_for_body(body_text), "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, data=body_text, timeout=30)
    if not resp.ok: raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
    text = resp.text.strip()
    if not text: return []
    items = []
    lines = [ln for ln in text.split("\n") if ln.strip()]
    for ln in lines:
        try: items.append(json.loads(ln))
        except: items = [json.loads(text)]; break
    return items

def search_by_display_name(name: str, max_hits: int, operator: str) -> list[dict]:
    filters = {
        "limit": min(max_hits, 1000),
        "offset": 0,
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [{"filterField": "displayName", "filterValue": name, "filterOperator": operator}]
        }]
    }
    items = post_list_members(filters)
    rows = []
    for item in items:
        member = item.get("result") or item.get("member") or item
        person = member.get("person") or {}
        d_name = (person.get("displayName") or "").strip()
        m_id = (member.get("id") or "").strip()
        if d_name and m_id:
            rows.append({"搜尋姓名": name, "displayName": d_name, "memberId": m_id})
    return rows[:max_hits]

def update_member_display_name(member_id: str, new_name: str) -> bool:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    payload = {"id": member_id, "person": {"displayName": new_name}}
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = {"Authorization": make_jwt_for_body(body_text), "Content-Type": "application/json"}
    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    return resp.ok

# ----------------------------
# UI 與 邏輯控制
# ----------------------------
with st.sidebar:
    st.header("⚙️ 管理面板")
    st.metric("📦 暫存池剩餘 ID", len(st.session_state.persistent_recycle_pool))
    if st.button("🗑️ 清空所有暫存 ID"):
        st.session_state.persistent_recycle_pool = []
        st.rerun()

with st.form("search_form"):
    input_text = st.text_area("會員名單 (每行一個姓名)", height=150, placeholder="MEIHUA LEE\nTI SU")
    colA, colB = st.columns(2)
    max_hits = colA.number_input("同名最多抓取筆數", 1, 150, 5)
    operator = colB.selectbox("比對方式", ["eq", "like"])
    submitted = st.form_submit_button("🔍 執行資源盤點")

if submitted:
    names = [n.strip() for n in (input_text or "").splitlines() if n.strip()]
    if not names: st.warning("請輸入姓名"); st.stop()

    all_rows, missing = [], []
    prog = st.progress(0)
    status_txt = st.empty()
    
    for i, name in enumerate(names):
        status_txt.text(f"查詢中 ({i+1}/{len(names)}): {name}")
        try:
            rows = search_by_display_name(name, max_hits=int(max_hits), operator=operator)
            if rows: all_rows.extend(rows)
            else: missing.append(name)
        except Exception as e:
            st.error(f"查詢出錯: {name} -> {e}")
        prog.progress((i + 1) / len(names))

    # --- 核心邏輯：驗證實體 ID 唯一性 ---
    unique_records = []
    seen_ids = set()
    for r in all_rows:
        if r["memberId"] not in seen_ids:
            unique_records.append(r)
            seen_ids.add(r["memberId"])

    member_groups = defaultdict(list)
    for r in unique_records:
        member_groups[r["搜尋姓名"]].append(r["memberId"])

    new_recycle_ids = []
    for ids in member_groups.values():
        if len(ids) > 1:
            new_recycle_ids.extend(ids[:-1]) # 僅回收重複出的 ID，保留最後一個

    # 合併入持久化彈藥庫 (確保 ID 不重複存入)
    updated_pool = set(st.session_state.persistent_recycle_pool)
    updated_pool.update(new_recycle_ids)
    st.session_state.persistent_recycle_pool = list(updated_pool)

    st.session_state.search_results = {"all_rows": all_rows, "missing": missing, "search_done": True}
    st.rerun()

# ----------------------------
# 顯示結果與執行指派
# ----------------------------
res = st.session_state.search_results
if res["search_done"]:
    st.success(f"盤點完成！本次命中 {len(res['all_rows'])} 筆，缺額 {len(res['missing'])} 人。")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 命中資料明細")
        st.dataframe(pd.DataFrame(res["all_rows"]), use_container_width=True)
    with col2:
        st.subheader("❓ 缺額名單")
        st.write(", ".join(res["missing"]) if res["missing"] else "無缺額")

    st.markdown("---")
    st.subheader("🚀 回收池指派作業")
    
    pool = st.session_state.persistent_recycle_pool
    missing_list = res["missing"]
    
    st.info(f"當前彈藥庫可用：**{len(pool)}** 個 ID | 本次待指派：**{len(missing_list)}** 人")

    if pool and missing_list:
        pair_count = min(len(pool), len(missing_list))
        preview = [{"回收 ID": pool[i], "分配給": missing_list[i]} for i in range(pair_count)]
        
        with st.expander("👀 查看即將執行的配對預覽"):
            st.table(preview)

        if st.button(f"⚡ 立即執行 {pair_count} 筆指派並扣除庫存"):
            success_ids = []
            assign_prog = st.progress(0)
            assign_status = st.empty()

            for i in range(pair_count):
                m_id, m_name = pool[i], missing_list[i]
                assign_status.text(f"處理中: {m_id} -> {m_name}")
                if update_member_display_name(m_id, m_name):
                    success_ids.append(m_id)
                assign_prog.progress((i + 1) / pair_count)

            # 消耗庫存
            st.session_state.persistent_recycle_pool = [x for x in pool if x not in success_ids]
            # 更新本次缺額名單 (移除已成功的)
            st.session_state.search_results["missing"] = missing_list[len(success_ids):]
            
            st.success(f"完成！成功回收指派 {len(success_ids)} 筆資料。")
            st.rerun()
    else:
        st.warning("回收池無 ID 可用 或 沒有缺額需要指派。")
