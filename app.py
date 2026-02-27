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
    st.session_state.persistent_recycle_pool = []  # 真正的「彈藥庫」

if "search_results" not in st.session_state:
    st.session_state.search_results = {"all_rows": [], "missing": [], "search_done": False}

# ----------------------------
# API Functions (與之前相同，略過重複定義以節省篇幅，請保留你原本的定義)
# ----------------------------
# [保留 make_jwt_for_body, post_list_members, search_by_display_name, update_member_display_name]

# --- 這裡僅補上 update_member_display_name 以確保邏輯完整 ---
def update_member_display_name(member_id: str, new_name: str) -> bool:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    payload = {"id": member_id, "person": {"displayName": new_name}}
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    token = make_jwt_for_body(body_text)
    headers = {"Authorization": token, "Content-Type": "application/json"}
    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    return resp.ok

# ----------------------------
# UI 搜尋區
# ----------------------------
with st.sidebar:
    st.header("⚙️ 設定與管理")
    if st.button("🗑️ 清空暫存回收池"):
        st.session_state.persistent_recycle_pool = []
        st.success("暫存池已清空")
        st.rerun()
    
    st.metric("📦 目前暫存 ID 數量", len(st.session_state.persistent_recycle_pool))

with st.form("search_form"):
    input_text = st.text_area("輸入會員姓名 (每行一個)", height=150)
    colA, colB = st.columns(2)
    max_hits = colA.number_input("同名最多筆數", 1, 100, 5)
    operator = colB.selectbox("比對方式", ["eq", "like"])
    submitted = st.form_submit_button("開始搜尋並盤點資源")

if submitted:
    names = [n.strip() for n in (input_text or "").splitlines() if n.strip()]
    if not names: st.stop()

    all_rows, missing = [], []
    prog = st.progress(0)
    
    for i, name in enumerate(names):
        try:
            rows = search_by_display_name(name, max_hits=int(max_hits), operator=operator)
            if rows: all_rows.extend(rows)
            else: missing.append(name)
        except Exception as e:
            st.error(f"查詢失敗: {name} - {e}")
        prog.progress((i + 1) / len(names))

    # --- 關鍵去重與回收邏輯 ---
    # 1. 實體去重 (解決你提到的 TI SU 同 ID 出現兩次的問題)
    unique_records = []
    seen_ids = set()
    for r in all_rows:
        if r["memberId (member.id)"] not in seen_ids:
            unique_records.append(r)
            seen_ids.add(r["memberId (member.id)"])

    # 2. 找出真正重複的 ID (同名但不同 ID)
    member_groups = defaultdict(list)
    for r in unique_records:
        member_groups[r["搜尋姓名"]].append(r["memberId (member.id)"])

    new_recycle_ids = []
    for ids in member_groups.values():
        if len(ids) > 1:
            new_recycle_ids.extend(ids[:-1]) # 保留最後一個，其餘回收

    # 3. 合併入持久化回收池 (去重合併)
    current_pool = set(st.session_state.persistent_recycle_pool)
    current_pool.update(new_recycle_ids)
    st.session_state.persistent_recycle_pool = list(current_pool)

    # 存入結果
    st.session_state.search_results = {
        "all_rows": all_rows,
        "missing": missing,
        "search_done": True
    }

# ----------------------------
# 顯示結果與指派功能
# ----------------------------
res = st.session_state.search_results
if res["search_done"]:
    st.subheader("📊 本次搜尋結果")
    col1, col2 = st.columns(2)
    col1.write(f"✅ 命中筆數: {len(res['all_rows'])}")
    col2.write(f"❓ 未找到人數: {len(res['missing'])}")

    if res["missing"]:
        with st.expander("查看未找到名單"):
            st.write(", ".join(res["missing"]))

    st.markdown("---")
    st.subheader("🚀 資源回收指派作業")
    
    pool = st.session_state.persistent_recycle_pool
    missing_list = res["missing"]
    
    st.info(f"庫存可用 ID：**{len(pool)}** 個 | 等待分配人數：**{len(missing_list)}** 人")

    if pool and missing_list:
        pair_count = min(len(pool), len(missing_list))
        
        # 預覽配對
        preview_data = []
        for i in range(pair_count):
            preview_data.append({"回收 ID": pool[i], "指派給新會員": missing_list[i]})
        
        st.table(preview_data[:10]) # 僅顯示前 10 筆預覽
        if pair_count > 10: st.write(f"...等共 {pair_count} 筆配對")

        if st.button(f"確認指派這 {pair_count} 筆資料"):
            success_ids = []
            bar = st.progress(0)
            status = st.empty()

            for i in range(pair_count):
                target_id = pool[i]
                target_name = missing_list[i]
                status.info(f"正在指派 {target_id} -> {target_name}")
                
                if update_member_display_name(target_id, target_name):
                    success_ids.append(target_id)
                
                bar.progress((i + 1) / pair_count)
                time.sleep(0.1)

            # --- 消耗彈藥庫 ---
            # 從持久化池中移除成功的 ID
            st.session_state.persistent_recycle_pool = [x for x in pool if x not in success_ids]
            # 從本次未找到名單中移除已分配的人
            res["missing"] = missing_list[pair_count:]
            
            status.success(f"成功完成 {len(success_ids)} 筆指派！彈藥庫剩餘 {len(st.session_state.persistent_recycle_pool)} 個 ID。")
            st.rerun()
    else:
        st.warning("目前暫存池為空，或沒有需要指派的會員。")
