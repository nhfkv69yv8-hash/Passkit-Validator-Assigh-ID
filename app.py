import os
import time
import json
import hashlib
import requests
import jwt  # from PyJWT
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from collections import defaultdict
import re
import datetime

# ----------------------------
# Page Config
# ----------------------------
st.set_page_config(page_title="PassKit 資源回收站 V2", page_icon="♻️", layout="wide")
st.title("♻️ PassKit 資源回收指派系統 (最新保留版)")

# 回收門檻（以 UTC+0 today 計）
months_label = st.selectbox("回收時間門檻（creationDate + cardIssueDate 都需超過）", ["三個月", "四個月", "五個月"], index=0)
MONTHS_MAP = {"三個月": 3, "四個月": 4, "五個月": 5}
months_threshold = MONTHS_MAP.get(months_label, 3)
require_modified_old = st.checkbox("同時要求 modified 也超過門檻（可選）", value=False)
st.caption("自動移除輸入重複姓名、保留最新 PassKit ID、跨次暫存回收資源。")

# ----------------------------
# Session State 初始化
# ----------------------------
if "persistent_recycle_pool" not in st.session_state:
    st.session_state.persistent_recycle_pool = []

if "persistent_missing_people" not in st.session_state:
    # 會累積「尚未指派到 Passkit ID」的人名清單，直到你手動清空或完成指派後移除
    st.session_state.persistent_missing_people = []

if "search_results" not in st.session_state:
    st.session_state.search_results = {"all_rows": [], "missing": [], "search_done": False}

# ----------------------------
# Config & API Helpers (核心函式)
# ----------------------------
def get_config(key: str, default: str | None = None) -> str | None:
    val = st.secrets.get(key) if hasattr(st, "secrets") else None
    if val is None: val = os.environ.get(key, default)
    return str(val).replace("\\n", "\n").strip() if val else None

PK_API_KEY = get_config("PK_API_KEY")
PK_API_SECRET = get_config("PK_API_SECRET")
PK_API_PREFIX = get_config("PK_API_PREFIX", "https://api.pub1.passkit.io")
PROGRAM_ID = get_config("PROGRAM_ID")

def make_jwt_for_body(body_text: str) -> str:
    now = int(time.time())
    payload = {"uid": PK_API_KEY, "iat": now, "exp": now + 600}
    if body_text:
        payload["signature"] = hashlib.sha256(body_text.encode("utf-8")).hexdigest()
    token = jwt.encode(payload, PK_API_SECRET, algorithm="HS256")
    return token.decode("utf-8") if isinstance(token, bytes) else token

# ----------------------------
# Date helpers (UTC)
# ----------------------------
def _parse_any_date(s: str):
    """Parse various date formats used in meta/fields.
    Returns a datetime.date or None.
    """
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None

    if re.fullmatch(r"\d{6}", s):
        try:
            return datetime.datetime.strptime(s, "%d%m%y").date()
        except Exception:
            pass

    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except Exception:
            pass

    try:
        ss = s.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(ss).date()
    except Exception:
        return None


def _utc_today() -> datetime.date:
    return datetime.datetime.now(datetime.timezone.utc).date()


def _cutoff_date_months_ago(months: int) -> datetime.date:
    try:
        from dateutil.relativedelta import relativedelta
        return (_utc_today() - relativedelta(months=months))
    except Exception:
        return (_utc_today() - datetime.timedelta(days=30 * months))


def _is_older_than_months(d: datetime.date | None, months: int) -> bool:
    if d is None:
        return False
    return d <= _cutoff_date_months_ago(months)

def post_list_members(filters_payload: dict) -> list[dict]:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)
    headers = {"Authorization": make_jwt_for_body(body_text), "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, data=body_text, timeout=30)
    if not resp.ok: return []
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
        "filterGroups": [{"condition": "AND", "fieldFilters": [{"filterField": "displayName", "filterValue": name, "filterOperator": operator}]}]
    }
    items = post_list_members(filters)
    rows = []
    for item in items:
        member = item.get("result") or item.get("member") or item
        person = member.get("person") or {}
        d_name = (person.get("displayName") or "").strip()
        m_id = (member.get("id") or "").strip()
        if d_name and m_id:
            rows.append({"搜尋姓名": name, "displayName": d_name, "memberId": m_id, "created": member.get("created",""), "modified": member.get("modified",""), "meta_creationDate": (member.get("metaData", {}) or {}).get("creationDate",""), "meta_cardIssueDate": (member.get("metaData", {}) or {}).get("cardIssueDate","")})
    return rows # 這裡回傳完整列表，稍後再依順序處理

def update_member_display_name(member_id: str, new_name: str) -> bool:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    payload = {"id": member_id, "person": {"displayName": new_name}}
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = {"Authorization": make_jwt_for_body(body_text), "Content-Type": "application/json"}
    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    return resp.ok

# ----------------------------
# UI 控制面板
# ----------------------------
with st.sidebar:
    st.header("⚙️ 資源管理")
    st.metric("📦 可用回收 ID 庫存", len(st.session_state.persistent_recycle_pool))

    # 一鍵複製「尚未指派」的 Passkit ID（回收庫存剩餘）
    with st.expander('📋 未指派 Passkit ID（可一鍵複製）', expanded=False):
        pool_ids = list(st.session_state.persistent_recycle_pool)
        if pool_ids:
            pool_text = '\n'.join(pool_ids)
            st.caption('點下方按鈕即可把全部剩餘 ID 複製到剪貼簿（每行一個）。')
            html_block = """
                <div style='display:flex; gap:10px; align-items:center;'>
                  <button id='copyPoolBtn' style='padding:8px 10px; border-radius:10px; border:1px solid #d1d5db; background:#fff; font-weight:600; cursor:pointer;'>📋 一鍵複製</button>
                  <span id='copyPoolMsg' style='font-size:12px; color:#6b7280;'></span>
                </div>
                <script>
                  (function(){
                    const TEXT = __TEXT__;
                    const btn = document.getElementById('copyPoolBtn');
                    const msg = document.getElementById('copyPoolMsg');
                    if(!btn) return;
                    btn.addEventListener('click', async () => {
                      try{
                        await navigator.clipboard.writeText(TEXT);
                        msg.textContent='已複製';
                        setTimeout(()=>msg.textContent='', 1200);
                      }catch(e){
                        msg.textContent='複製失敗（瀏覽器限制）';
                        setTimeout(()=>msg.textContent='', 2000);
                      }
                    });
                  })();
                </script>
            """
            components.html(
                html_block.replace('__TEXT__', json.dumps(pool_text)),
                height=62,
            )
            st.text_area('剩餘 ID（檢視用）', pool_text, height=140)
        else:
            st.info('目前沒有剩餘未指派 ID。')
    if st.button("🗑️ 清空所有 ID 庫存"):
        st.session_state.persistent_recycle_pool = []
        st.rerun()

with st.form("search_form"):
    input_text = st.text_area("會員搜尋名單 (每行一個姓名)", height=150)
    colA, colB = st.columns(2)
    max_hits = colA.number_input("同名最多抓取筆數", 1, 150, 5)
    operator = colB.selectbox("比對方式", ["eq", "like"])
    submitted = st.form_submit_button("🔍 開始搜尋並過濾重複名單")

# ----------------------------
# 搜尋邏輯
# ----------------------------
if submitted:
    # --- 修正功能 1: 搜尋名單去重 ---
    raw_names = [n.strip() for n in (input_text or "").splitlines() if n.strip()]
    names = list(dict.fromkeys(raw_names)) # 保留順序的去重
    
    if len(raw_names) != len(names):
        st.info(f"💡 名單已自動去重：原始筆數 {len(raw_names)} 筆 -> 實際搜尋 {len(names)} 筆。")

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

    # --- 修正功能 2: 依序回收 (保留最新 ID) ---
    # 先根據 memberId 進行全域去重 (防止 TI SU 問題)
    unique_records = []
    seen_ids = set()
    for r in all_rows:
        if r["memberId"] not in seen_ids:
            unique_records.append(r)
            seen_ids.add(r["memberId"])

    # 按照搜尋姓名分組，保留最後一筆
        member_groups = defaultdict(list)
    for r in unique_records:
        member_groups[r["搜尋姓名"]].append(r)

    def _eligible_for_recycle(rec: dict) -> bool:
        c = _parse_any_date(rec.get("meta_creationDate"))
        i = _parse_any_date(rec.get("meta_cardIssueDate"))
        m = _parse_any_date(rec.get("modified"))

        # 必須 creationDate 與 cardIssueDate 兩者都 >= 門檻
        ok_both = _is_older_than_months(c, months_threshold) and _is_older_than_months(i, months_threshold)
        if not ok_both:
            return False

        # 可選：同時要求 modified 也 >= 門檻
        if require_modified_old:
            return _is_older_than_months(m, months_threshold)

        return True

    new_recycle_ids = []
    new_recycle_details = []

    for s_name, recs in member_groups.items():
        if len(recs) <= 1:
            continue

        # 例如 YUMIN LEE 有 [ID_0, ID_1]，ID_1 是最後一筆 (最新)
        to_recycle = recs[:-1]  # 取除了最後一個以外的所有候選

        for rec in to_recycle:
            mid = rec.get("memberId", "")
            if not mid:
                continue

            if _eligible_for_recycle(rec):
                new_recycle_ids.append(mid)
                new_recycle_details.append({
                    "搜尋姓名": s_name,
                    "回收memberId": mid,
                    "creationDate": rec.get("meta_creationDate", ""),
                    "cardIssueDate": rec.get("meta_cardIssueDate", ""),
                    "modified": rec.get("modified", ""),
                    "原因": f"同名重複 + {months_label}以上（兩日期皆符合）" + (" + modified符合" if require_modified_old else ""),
                })
# 合併入持久化彈彈藥庫
    updated_pool = set(st.session_state.persistent_recycle_pool)
    updated_pool.update(new_recycle_ids)
    st.session_state.persistent_recycle_pool = sorted(list(updated_pool))

    # 合併入「尚未指派」名單（不會因為再次檢查就被清掉）
    if missing:
        existing = list(st.session_state.persistent_missing_people)
        seen = set(existing)
        for nm in missing:
            if nm not in seen:
                existing.append(nm)
                seen.add(nm)
        st.session_state.persistent_missing_people = existing

    st.session_state.search_results = {
        "all_rows": all_rows,
        "missing": list(st.session_state.persistent_missing_people),
        "search_done": True,
    }
    st.rerun()

# ----------------------------
# 執行與預覽
# ----------------------------
res = st.session_state.search_results
if res["search_done"]:
    st.subheader("📊 盤點結果明細")
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("**命中資料表 (顯示搜尋結果順序)**")
        st.dataframe(pd.DataFrame(res["all_rows"]), use_container_width=True)
    with col2:
        st.markdown(f"**❓ 本次缺額：{len(res['missing'])} 人**")
        st.write(", ".join(res["missing"]) if res["missing"] else "無缺額")

    st.markdown("---")
    st.subheader("🚀 資源回收指派 (最新 ID 已保留)")
    
    pool = st.session_state.persistent_recycle_pool
    missing_list = res["missing"]
    
    st.info(f"當前可用舊 ID：**{len(pool)}** 個 | 等待指派人數：**{len(missing_list)}** 人")

    if pool and missing_list:
        pair_count = min(len(pool), len(missing_list))
        preview = [{"待指派 ID (舊)": pool[i], "分配給 (缺額)": missing_list[i]} for i in range(pair_count)]
        
        with st.expander("👀 指派配對預覽"):
            st.table(preview)

        if st.button(f"⚡ 確定指派這 {pair_count} 筆"):
            success_ids = []
            assign_prog = st.progress(0)
            assign_status = st.empty()

            for i in range(pair_count):
                m_id, m_name = pool[i], missing_list[i]
                assign_status.text(f"正在更新: {m_id} -> {m_name}")
                if update_member_display_name(m_id, m_name):
                    success_ids.append(m_id)
                assign_prog.progress((i + 1) / pair_count)

            # 消耗掉成功的 ID
            st.session_state.persistent_recycle_pool = [x for x in pool if x not in success_ids]
            # 移除已分配的缺額
            # 從「尚未指派」名單移除已成功指派的人名，剩下的會持續保留
            assigned_names = set(missing_list[: len(success_ids)])
            st.session_state.persistent_missing_people = [
                nm for nm in st.session_state.persistent_missing_people if nm not in assigned_names
            ]
            st.session_state.search_results["missing"] = list(st.session_state.persistent_missing_people)
            
            st.success(f"指派成功！已為 {len(success_ids)} 位會員建立票卡，剩餘庫存 {len(st.session_state.persistent_recycle_pool)} 個。")
            st.rerun()
    else:
        st.warning("暫無可用資源或無缺額需要指派。")
