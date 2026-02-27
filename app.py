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
# Page
# ----------------------------
st.set_page_config(page_title="PassKit 重複 ID 搜尋與回收工具", page_icon="🔍")
st.title("🔍 PassKit 重複 ID 搜尋與回收工具")
st.caption("每行貼一個 full name（PassKit: person.displayName），最多 150 行。用 REST Filter 查，並可將重複 ID 分配給未找到的會員。")

# ----------------------------
# Session State 初始化
# ----------------------------
if "search_done" not in st.session_state:
    st.session_state.search_done = False
    st.session_state.all_rows = []
    st.session_state.missing = []
    st.session_state.recycle_pool = []

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
    st.error(f"❌ 缺少設定：{', '.join(missing_cfg)}（請在 .env 或 Secrets 補上）")
    st.stop()

# ----------------------------
# JWT auth & API Functions
# ----------------------------
def make_jwt_for_body(body_text: str) -> str:
    now = int(time.time())
    payload = {
        "uid": PK_API_KEY,
        "iat": now,
        "exp": now + 600,
    }
    if body_text:
        payload["signature"] = hashlib.sha256(body_text.encode("utf-8")).hexdigest()

    token = jwt.encode(payload, PK_API_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def post_list_members(filters_payload: dict) -> list[dict]:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)

    token = make_jwt_for_body(body_text)
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, data=body_text, timeout=30)

    if resp.status_code == 404:
        raise RuntimeError("404 Not Found：多半是 API Prefix 用錯（pub1/pub2），或 endpoint path 拼錯。")
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Auth 失敗（{resp.status_code}）：請確認 PK_API_KEY/PK_API_SECRET、以及 API Prefix。")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")

    text = resp.text.strip()
    if not text:
        return []

    items: list[dict] = []
    lines = [ln for ln in text.split("\n") if ln.strip()]
    for ln in lines:
        try:
            items.append(json.loads(ln))
        except json.JSONDecodeError:
            items = [json.loads(text)]
            break
    return items

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int) -> list[dict]:
    rows = []
    for item in list_response_items:
        member = item.get("result") or item.get("member") or item
        if not isinstance(member, dict):
            continue

        person = member.get("person") or {}
        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName (person.displayName)": display_name,
                "memberId (member.id)": member_id,
            })

        if len(rows) >= max_hits:
            break
    return rows

def search_by_display_name(name: str, max_hits: int, operator: str) -> list[dict]:
    filters = {
        "limit": min(max_hits, 1000),
        "offset": 0,
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [{
                "filterField": "displayName",
                "filterValue": name,
                "filterOperator": operator,
            }]
        }]
    }
    items = post_list_members(filters)
    return extract_member_rows(items, name, max_hits=max_hits)

def update_member_display_name(member_id: str, new_name: str) -> bool:
    """呼叫 PassKit API 將指定的 member_id 的 displayName 替換為新名字"""
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    
    payload = {
        "id": member_id,
        "person": {
            "displayName": new_name
        }
    }
    
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    token = make_jwt_for_body(body_text)
    
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    
    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    
    if not resp.ok:
        st.error(f"❌ 更新失敗 ID: {member_id}, 錯誤訊息: {resp.text[:200]}")
        return False
        
    return True

# ----------------------------
# UI
# ----------------------------
with st.form("search_form"):
    input_text = st.text_area(
        "每行一個 full name（person.displayName）— 最多 150 行",
        height=220,
        placeholder="HSIUTING CHOU\nKUANYEN LEE\n..."
    )

    colA, colB, colC = st.columns([1, 1, 2])
    with colA:
        max_hits = st.number_input("同名最多回傳筆數", min_value=1, max_value=150, value=5, step=1)
    with colB:
        operator = st.selectbox("比對方式", options=["eq", "like"], index=0)
    with colC:
        st.caption("eq = 完全相同；like = 包含")

    submitted = st.form_submit_button("Search")

# ----------------------------
# 執行搜尋
# ----------------------------
if submitted:
    names = [n.strip() for n in (input_text or "").splitlines() if n.strip()]
    if not names:
        st.warning("請先貼上至少一行姓名。")
        st.stop()

    if len(names) > 150:
        st.warning(f"你貼了 {len(names)} 行，系統只會取前 150 行。")
        names = names[:150]

    all_rows = []
    missing = []

    prog = st.progress(0)
    status = st.empty()

    for i, name in enumerate(names, start=1):
        status.info(f"查詢中 {i}/{len(names)}：{name}")
        try:
            rows = search_by_display_name(name, max_hits=int(max_hits), operator=operator)
            if rows:
                all_rows.extend(rows)
            else:
                missing.append(name)
        except Exception as e:
            st.error(f"❌ 查詢失敗：{name} → {e}")
            missing.append(name)

        prog.progress(i / len(names))

    status.empty()
    prog.empty()
    st.success(f"完成：查詢 {len(names)} 筆，命中 {len(all_rows)} 筆。")

    # 整理重複名單與回收池
    member_groups = defaultdict(list)
    for row in all_rows:
        member_groups[row["搜尋姓名"]].append(row["memberId (member.id)"])

    recycle_pool = []
    for name, ids in member_groups.items():
        if len(ids) > 1:
            # 保留最後一個（假設最新），前面的都丟進回收池
            recycle_pool.extend(ids[:-1])

    # 存入 Session State，讓按鈕按下後不會消失
    st.session_state.all_rows = all_rows
    st.session_state.missing = missing
    st.session_state.recycle_pool = recycle_pool
    st.session_state.search_done = True

# ----------------------------
# 顯示結果與回收配對 (在 form 之外，確保狀態延續)
# ----------------------------
if st.session_state.search_done:
    all_rows = st.session_state.all_rows
    missing = st.session_state.missing
    recycle_pool = st.session_state.recycle_pool

    if all_rows:
        df = pd.DataFrame(all_rows)
        display_rows = []
        for x in all_rows:
            display_rows.append({
                "搜尋姓名": x.get("搜尋姓名", ""),
                "會員姓名": x.get("displayName (person.displayName)", x.get("會員姓名", "")),
                "Passkit ID": x.get("memberId (member.id)", x.get("Passkit ID", "")),
            })
        st.dataframe(df, use_container_width=True)

        csv = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("下載 CSV", data=csv, file_name="passkit_member_ids.csv", mime="text/csv")

    if missing:
        with st.expander(f"未找到名單（{len(missing)}）"):
            st.write("\n".join(missing))

    # --- 資源回收分配區塊 ---
    st.markdown("---")
    st.subheader("♻️ 回收與重新分配")
    
    st.write(f"系統偵測到 **{len(recycle_pool)}** 個可回收的重複 ID。")
    st.write(f"目前有 **{len(missing)}** 位未分配到 ID 的會員。")

    if recycle_pool and missing:
        pair_count = min(len(recycle_pool), len(missing))
        st.info(f"💡 點擊下方按鈕，系統將取前 {pair_count} 個重複 ID，覆蓋並分配給未找到名單中的會員。")
        
        # 建立預覽配對表
        preview_pairs = [{"舊 PassKit ID (將被覆蓋)": recycle_pool[i], "新指派姓名": missing[i]} for i in range(pair_count)]
        with st.expander("👀 預覽配對名單 (點擊展開)"):
            st.dataframe(pd.DataFrame(preview_pairs), use_container_width=True)

        if st.button("🚀 執行回收分配 (注意：此動作將直接修改 PassKit 資料)"):
            update_prog = st.progress(0)
            update_status = st.empty()
            success_count = 0
            
            for i in range(pair_count):
                target_id = recycle_pool[i]
                new_name = missing[i]
                update_status.info(f"正在將 ID `{target_id}` 分配給 👉 `{new_name}` ...")
                
                is_success = update_member_display_name(target_id, new_name)
                if is_success:
                    success_count += 1
                
                update_prog.progress((i + 1) / pair_count)
                time.sleep(0.5) # 稍微延遲避免 API Rate Limit

            update_status.empty()
            update_prog.empty()
            
            if success_count == pair_count:
                st.success(f"🎉 完美！成功回收並重新分配了 {success_count} 個 PassKit ID！為公司省下了大約 ${success_count * 0.4:.2f} 美金。")
            else:
                st.warning(f"⚠️ 執行完畢，目標 {pair_count} 筆，成功 {success_count} 筆。請查看上方是否有錯誤訊息。")
            
            # 更新完畢後，建議清除狀態讓使用者重新查詢確認
            st.session_state.search_done = False
            if st.button("重新整理畫面"):
                st.rerun()

    elif len(recycle_pool) == 0 and len(missing) > 0:
        st.write("目前沒有多餘的 ID 可以分配。")
    elif len(missing) == 0 and len(recycle_pool) > 0:
        st.write("所有會員都已有 ID，無需進行分配。")
