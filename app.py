import os
import time
import json
import hashlib
import requests
import jwt  # PyJWT
import pandas as pd
import streamlit as st

# ============================================================
# PassKit 重複 ID 搜尋 / 回收分配工具
# - 查詢：用 person.displayName 找 memberId
# - 找出：同名多筆 / 未找到
# - 回收：PASS_ISSUED 且 meta.cardNumber 為空（或 NULL）
# - 分配：把回收的 memberId 改名給 missing 名單（PUT 更新）
# - 防重複回收：寫入 meta.recycleLock（可關閉/可改 key）
# ============================================================

# ----------------------------
# Page
# ----------------------------
st.set_page_config(
    page_title="PassKit 重複 ID 搜尋 / 回收分配工具",
    page_icon="♻️",
    layout="wide",
)
st.title("♻️ PassKit 重複 ID 搜尋 / 回收分配工具")
st.caption("回收條件：PASS_ISSUED + meta.cardNumber 為空（或 NULL）。先 Dry-run 預覽 mapping，再 Apply 批次 PUT。")

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
    st.error(f"❌ 缺少設定：{', '.join(missing_cfg)}（請在 .env 或 Streamlit Secrets 補上）")
    st.stop()

# ----------------------------
# JWT auth (PassKit style)
# ----------------------------
def make_jwt_for_body(body_text: str) -> str:
    now = int(time.time())
    payload = {"uid": PK_API_KEY, "iat": now, "exp": now + 600}
    if body_text:
        payload["signature"] = hashlib.sha256(body_text.encode("utf-8")).hexdigest()

    token = jwt.encode(payload, PK_API_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def _handle_resp_errors(resp: requests.Response) -> None:
    if resp.status_code == 404:
        raise RuntimeError("404 Not Found：多半是 API Prefix（pub1/pub2）或 endpoint path 用錯。")
    if resp.status_code in (401, 403):
        raise RuntimeError(
            f"Auth 失敗（{resp.status_code}）：請確認 PK_API_KEY/PK_API_SECRET、以及 API Prefix（pub1/pub2）。"
        )
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:1200]}")

# ----------------------------
# PassKit API calls
# ----------------------------
def post_list_members(filters_payload: dict) -> list[dict]:
    """
    POST {PK_API_PREFIX}/members/member/list/{PROGRAM_ID}
    PassKit list APIs sometimes return NDJSON (one JSON per line)
    """
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)

    token = make_jwt_for_body(body_text)
    headers = {"Authorization": token, "Content-Type": "application/json"}

    resp = requests.post(url, headers=headers, data=body_text, timeout=30)
    _handle_resp_errors(resp)

    text = (resp.text or "").strip()
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

def put_update_member(payload: dict) -> dict:
    """
    PUT {PK_API_PREFIX}/members/member
    """
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    token = make_jwt_for_body(body_text)
    headers = {"Authorization": token, "Content-Type": "application/json"}

    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    _handle_resp_errors(resp)

    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text[:1200]}

# ----------------------------
# Data helpers
# ----------------------------
def extract_member_obj(item: dict) -> dict | None:
    member = item.get("result") or item.get("member") or item
    return member if isinstance(member, dict) else None

def _get_meta_container(member: dict) -> dict:
    # 你環境回傳是 meta；也做相容（若日後變成 metaData/metadata）
    meta = member.get("meta")
    if not isinstance(meta, dict):
        meta = member.get("metaData")
    if not isinstance(meta, dict):
        meta = member.get("metadata")
    return meta if isinstance(meta, dict) else {}

def is_blank(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return (s == "") or (s.upper() == "NULL")

def normalize_name(s: str, remove_spaces: bool) -> str:
    s = (s or "").strip()
    if remove_spaces:
        s = s.replace(" ", "")
    return s

# ✅ 依你截圖：Field Key = meta.cardNumber → meta dict key = "cardNumber"
CARDNUMBER_META_DICT_KEY = "cardNumber"     # meta["cardNumber"]
CARDNUMBER_FILTER_FIELD_1 = "meta.cardNumber"
CARDNUMBER_FILTER_FIELD_2 = "cardNumber"    # 某些後端也吃這種

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int, lock_key: str) -> list[dict]:
    rows = []
    for item in list_response_items:
        member = extract_member_obj(item)
        if not member:
            continue

        person = member.get("person") or {}
        meta = _get_meta_container(member)

        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()
        pass_status = (member.get("passStatus") or "").strip()

        card_number = meta.get(CARDNUMBER_META_DICT_KEY)
        card_number = "" if card_number is None else str(card_number).strip()

        lock_val = meta.get(lock_key) if lock_key else None
        lock_val = "" if lock_val is None else str(lock_val).strip()

        created = member.get("created") or member.get("createdAt") or member.get("createdOn") or ""
        updated = member.get("updated") or member.get("updatedAt") or member.get("updatedOn") or ""

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName": display_name,
                "memberId": member_id,
                "passStatus": pass_status,
                "meta.cardNumber": card_number,
                f"meta.{lock_key}" if lock_key else "meta.lock": lock_val,
                "created": str(created),
                "updated": str(updated),
            })

        if len(rows) >= max_hits:
            break
    return rows

def search_by_display_name(name: str, max_hits: int, operator: str, lock_key: str) -> list[dict]:
    filters = {
        "limit": min(int(max_hits), 1000),
        "offset": 0,
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [{
                "filterField": "displayName",
                "filterValue": name,
                "filterOperator": operator,  # eq / like
            }]
        }]
    }
    items = post_list_members(filters)
    return extract_member_rows(items, name, max_hits=max_hits, lock_key=lock_key)

# ----------------------------
# Recycle logic
# ----------------------------
def is_recyclable_row(row: dict, lock_col: str) -> bool:
    # 你的回收條件：PASS_ISSUED + meta.cardNumber 空
    # 再加一條：lock 也必須空（避免已分配過的再被回收）
    return (
        (row.get("passStatus") == "PASS_ISSUED")
        and is_blank(row.get("meta.cardNumber"))
        and (lock_col not in row or is_blank(row.get(lock_col)))
    )

def choose_duplicate_recycle_candidates(df_hits: pd.DataFrame, lock_col: str) -> pd.DataFrame:
    """
    從同名多筆中挑回收候選：
      - 每個 displayName 保留最新 1 筆（updated/created 盡量判斷）
      - 其餘若 PASS_ISSUED + meta.cardNumber 空 + lock 空 → 回收池
    """
    if df_hits.empty:
        return df_hits.iloc[0:0].copy()

    work = df_hits.copy()
    for col in ["updated", "created"]:
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], errors="coerce")

    candidates = []
    for _, g in work.groupby("displayName", dropna=False):
        if len(g) <= 1:
            continue

        # newest first
        if g["updated"].notna().any() or g["created"].notna().any():
            g_sorted = g.sort_values(["updated", "created"], ascending=[False, False], na_position="last")
        else:
            g_sorted = g.copy()

        # keep newest (first)
        rest = g_sorted.iloc[1:]
        for _, r in rest.iterrows():
            if is_recyclable_row(r.to_dict(), lock_col=lock_col):
                candidates.append(r.to_dict())

    return pd.DataFrame(candidates) if candidates else work.iloc[0:0].copy()

def list_recycle_pool_global(limit: int, offset: int, lock_key: str) -> list[dict]:
    """
    全域回收池：
      passStatus == PASS_ISSUED
      meta.cardNumber == NULL
    注意：是否支援對 meta 欄位做 NULL filter 取決於後端。
    """
    def _call(field_name: str) -> list[dict]:
        filters = {
            "limit": min(int(limit), 1000),
            "offset": int(offset),
            "orderBy": "created",
            "orderAsc": True,
            "filterGroups": [{
                "condition": "AND",
                "fieldFilters": [
                    {"filterField": "passStatus", "filterValue": "PASS_ISSUED", "filterOperator": "eq"},
                    {"filterField": field_name, "filterValue": "NULL", "filterOperator": "eq"},
                ]
            }]
        }
        items = post_list_members(filters)

        pool = []
        for item in items:
            member = extract_member_obj(item)
            if not member:
                continue
            meta = _get_meta_container(member)
            mid = (member.get("id") or "").strip()
            ps = (member.get("passStatus") or "").strip()

            card = meta.get(CARDNUMBER_META_DICT_KEY)
            lock = meta.get(lock_key) if lock_key else None

            if mid and ps == "PASS_ISSUED" and is_blank(card) and is_blank(lock):
                pool.append({
                    "memberId": mid,
                    "passStatus": ps,
                    "meta.cardNumber": "" if card is None else str(card).strip(),
                    f"meta.{lock_key}" if lock_key else "meta.lock": "" if lock is None else str(lock).strip(),
                    "created": str(member.get("created") or ""),
                })
        return pool

    # 先用完整 Field Key（你後台 Data Fields 顯示的）
    pool = _call(CARDNUMBER_FILTER_FIELD_1)
    if pool:
        return pool
    # fallback
    return _call(CARDNUMBER_FILTER_FIELD_2)

def build_update_payload(member_id: str, new_display_name: str, lock_key: str, write_lock: bool) -> dict:
    """
    分配時 PUT 更新：
      - person.displayName = new_display_name
      - meta.recycleLock = timestamp（預設開）
    重要：不要寫 TEMP 到 meta.cardNumber（它是會顯示的欄位）
    """
    payload = {"programId": PROGRAM_ID, "id": member_id, "person": {"displayName": new_display_name}}
    if write_lock and lock_key:
        payload["meta"] = {lock_key: str(int(time.time()))}
    return payload

def put_reassign(member_id: str, new_display_name: str, lock_key: str, write_lock: bool) -> tuple[dict, bool]:
    """
    先用 nested 形式（person/meta）。
    若更新含 lock 失敗，會 fallback：只更新 displayName（不寫 lock）。
    回傳：(resp, lock_written)
    """
    new_display_name = (new_display_name or "").strip()

    # 1) try with lock
    if write_lock and lock_key:
        payload_with_lock = build_update_payload(member_id, new_display_name, lock_key, write_lock=True)
        try:
            return put_update_member(payload_with_lock), True
        except Exception:
            # 可能後端不允許未知 meta key：改走不寫 lock
            pass

    # 2) update displayName only (nested)
    payload_no_lock = {"programId": PROGRAM_ID, "id": member_id, "person": {"displayName": new_display_name}}
    try:
        return put_update_member(payload_no_lock), False
    except Exception:
        # 3) dot-key fallback
        payload_dot = {"programId": PROGRAM_ID, "id": member_id, "person.displayName": new_display_name}
        return put_update_member(payload_dot), False

# ----------------------------
# UI - Search
# ----------------------------
st.session_state.setdefault("hits_rows", [])
st.session_state.setdefault("missing_names", [])

with st.form("search_form"):
    input_text = st.text_area(
        "每行一個 displayName（person.displayName）— 最多 150 行",
        height=220,
        placeholder="MEIHUA LEE\nHSIUTING CHOU\nKUANYEN LEE\n...",
    )

    colA, colB, colC, colD, colE = st.columns([1, 1, 1, 1, 2])
    with colA:
        max_hits = st.number_input("同名最多回傳筆數", min_value=1, max_value=150, value=10, step=1)
    with colB:
        operator = st.selectbox("比對方式", options=["eq", "like"], index=0)
    with colC:
        throttle = st.number_input("每次 API 間隔秒數", min_value=0.0, max_value=2.0, value=0.15, step=0.05)
    with colD:
        remove_spaces = st.checkbox("查詢前移除空格", value=False)
    with colE:
        lock_key = st.text_input("防重複回收 lock key（存於 meta）", value="recycleLock")
        st.caption("建議保留：用 meta.recycleLock 標記已分配，避免同一張卡被再次回收。")

    submitted = st.form_submit_button("Search")

if submitted:
    raw_names = [n for n in (input_text or "").splitlines() if n.strip()]
    names = [normalize_name(n, remove_spaces=remove_spaces) for n in raw_names if normalize_name(n, remove_spaces)]
    if not names:
        st.warning("請先貼上至少一行姓名。")
        st.stop()

    if len(names) > 150:
        st.warning(f"你貼了 {len(names)} 行，系統只會取前 150 行。")
        names = names[:150]

    all_rows: list[dict] = []
    missing: list[str] = []

    prog = st.progress(0.0)
    status = st.empty()

    for i, name in enumerate(names, start=1):
        status.info(f"查詢中 {i}/{len(names)}：{name}")
        try:
            rows = search_by_display_name(name, max_hits=int(max_hits), operator=operator, lock_key=lock_key)
            if rows:
                all_rows.extend(rows)
            else:
                missing.append(name)
        except Exception as e:
            st.error(f"❌ 查詢失敗：{name} → {e}")
            missing.append(name)

        prog.progress(i / len(names))
        if float(throttle) > 0:
            time.sleep(float(throttle))

    status.empty()
    prog.empty()

    st.session_state["hits_rows"] = all_rows
    st.session_state["missing_names"] = missing

    st.success(f"完成：查詢 {len(names)} 筆，命中 {len(all_rows)} 筆；未找到 {len(missing)} 筆。")

# ----------------------------
# Render results
# ----------------------------
hits_rows = st.session_state.get("hits_rows") or []
missing_names = st.session_state.get("missing_names") or []
lock_col = f"meta.{lock_key}" if lock_key else "meta.lock"

if hits_rows:
    df_hits = pd.DataFrame(hits_rows)
    cols_order = [c for c in ["搜尋姓名", "displayName", "memberId", "passStatus", "meta.cardNumber", lock_col, "created", "updated"] if c in df_hits.columns]
    df_hits = df_hits[cols_order].copy()

    left, right = st.columns([2, 1], gap="large")
    with left:
        st.subheader("命中清單")
        st.dataframe(df_hits, use_container_width=True, height=420)
        st.download_button(
            "下載命中 CSV",
            data=df_hits.to_csv(index=False).encode("utf-8-sig"),
            file_name="passkit_member_hits.csv",
            mime="text/csv",
        )

    with right:
        st.subheader("重複統計（按 displayName）")
        dup_counts = (
            df_hits.groupby("displayName")["memberId"]
            .nunique()
            .reset_index(name="同名 memberId 數量")
            .sort_values("同名 memberId 數量", ascending=False)
        )
        dup_only = dup_counts[dup_counts["同名 memberId 數量"] > 1].copy()
        st.metric("同名重複名稱數", int(len(dup_only)))
        st.dataframe(dup_only, use_container_width=True, height=260)

        st.subheader("未找到名單（missing）")
        if missing_names:
            st.write("\n".join(missing_names))
        else:
            st.info("沒有 missing。")

elif submitted:
    st.info("沒有命中資料（hits 為 0）。若你確認資料存在，請檢查 PROGRAM_ID / API Prefix / operator。")

# ----------------------------
# Recycle & assign
# ----------------------------
st.divider()
st.header("♻️ 回收池 → 分配給 missing（PASS_ISSUED + meta.cardNumber 空）")

if not missing_names:
    st.info("目前沒有 missing 名單，因此不需要分配回收池。")
else:
    st.warning(
        "⚠️ 重要提醒：你目前沒有『Pass URL 是否曾發送/外流』的紀錄。\n"
        "PASS_ISSUED 代表 URL 已存在；即使未 installed，若 URL 曾外流，你把 memberId 改名給別人，等於轉手。\n"
        "此工具依你的要求：以『PASS_ISSUED + meta.cardNumber 空』做回收條件，並提供 Dry-run→Apply 兩段式避免誤操作。"
    )

    mode = st.radio(
        "回收池來源",
        options=[
            "A) 全域回收池：PASS_ISSUED + meta.cardNumber 為空（不依賴重複查詢）",
            "B) 同名重複回收：每個 displayName 保留最新 1 筆，其餘符合條件者回收（更貼近你截圖情境）",
        ],
        index=1,
    )

    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    with col1:
        assign_limit = st.number_input("最多分配筆數", min_value=1, max_value=5000, value=min(300, len(missing_names)), step=10)
    with col2:
        apply_throttle = st.number_input("每次 PUT 間隔秒數", min_value=0.0, max_value=2.0, value=0.2, step=0.05)
    with col3:
        write_lock = st.checkbox("Apply 時寫入 meta.lock（建議開）", value=True)
    with col4:
        st.caption("不會寫入 cardNumber（避免顯示給客人）；改用 meta.recycleLock 標記已分配，防止再次被回收。")

    recycle_ids: list[str] = []

    if mode.startswith("A)"):
        st.subheader("A) 取得全域回收池")
        pool_limit = st.number_input("回收池撈取上限", min_value=10, max_value=1000, value=300, step=50)
        fetch_pool = st.button("取得回收池（A）", type="secondary")
        if fetch_pool:
            try:
                pool = list_recycle_pool_global(limit=int(pool_limit), offset=0, lock_key=lock_key)
                st.session_state["recycle_pool_A"] = pool
                st.success(f"回收池取得完成：{len(pool)} 筆。")
            except Exception as e:
                st.error(f"❌ 取得回收池失敗：{e}")

        pool = st.session_state.get("recycle_pool_A") or []
        if pool:
            df_pool = pd.DataFrame(pool)
            st.dataframe(df_pool, use_container_width=True, height=260)
            recycle_ids = [x["memberId"] for x in pool if x.get("memberId")]
        else:
            st.info("尚未取得回收池，或回收池為空（可能是後端不支援對 meta.cardNumber 做 NULL filter）。")

    else:
        st.subheader("B) 從同名重複中挑回收候選（保留最新 1 筆，其餘 PASS_ISSUED + meta.cardNumber 空者回收）")
        if not hits_rows:
            st.info("你需要先 Search 取得命中資料，才能使用 B 模式。")
        else:
            df_hits2 = pd.DataFrame(hits_rows)
            cand_df = choose_duplicate_recycle_candidates(df_hits2, lock_col=lock_col)
            if cand_df.empty:
                st.info("找不到符合回收條件的同名候選（PASS_ISSUED + meta.cardNumber 空 + lock 空）。")
            else:
                st.success(f"找到可回收候選：{len(cand_df)} 筆。")
                show_cols = [c for c in ["displayName", "memberId", "passStatus", "meta.cardNumber", lock_col, "created", "updated"] if c in cand_df.columns]
                st.dataframe(cand_df[show_cols], use_container_width=True, height=260)
                recycle_ids = [str(x).strip() for x in cand_df["memberId"].tolist() if str(x).strip()]

    # dedupe keep order
    seen = set()
    recycle_ids = [x for x in recycle_ids if not (x in seen or seen.add(x))]

    st.subheader("📌 Dry-run：產生分配 mapping（不會寫入）")
    max_assign = int(min(assign_limit, len(missing_names), len(recycle_ids)))
    if max_assign <= 0:
        st.info("目前無法產生 mapping：可能是回收池為空，或 missing 為空。")
    else:
        mapping = [{"new_displayName": missing_names[i], "recycled_memberId": recycle_ids[i]} for i in range(max_assign)]
        df_map = pd.DataFrame(mapping)
        st.dataframe(df_map, use_container_width=True, height=260)

        st.download_button(
            "下載 mapping CSV（Dry-run）",
            data=df_map.to_csv(index=False).encode("utf-8-sig"),
            file_name="recycle_mapping_dryrun.csv",
            mime="text/csv",
        )

        st.divider()
        st.subheader("✅ Apply：依 mapping 批次更新（PUT /members/member）")

        ack = st.checkbox(
            "我了解風險：PASS_ISSUED 仍可能已被分享。若回收的 memberId/URL 曾外流，更新後會讓舊 URL 指向新會員資料（等於轉手）。",
            value=False,
        )
        do_apply = st.button("Apply 批次更新", type="primary", disabled=not ack)

        if do_apply:
            ok_rows = []
            fail_rows = []

            prog2 = st.progress(0.0)
            status2 = st.empty()

            for i, row in enumerate(mapping, start=1):
                new_name = row["new_displayName"]
                member_id = row["recycled_memberId"]

                status2.info(f"更新中 {i}/{len(mapping)}：{member_id} → {new_name}")

                try:
                    resp, lock_written = put_reassign(
                        member_id=member_id,
                        new_display_name=new_name,
                        lock_key=lock_key,
                        write_lock=write_lock,
                    )
                    ok_rows.append({
                        "memberId": member_id,
                        "new_displayName": new_name,
                        "result": "OK",
                        "lockWritten": bool(lock_written),
                        "resp": str(resp)[:500],
                    })
                except Exception as e:
                    fail_rows.append({
                        "memberId": member_id,
                        "new_displayName": new_name,
                        "result": "FAIL",
                        "error": str(e)[:1200],
                    })

                prog2.progress(i / len(mapping))
                if float(apply_throttle) > 0:
                    time.sleep(float(apply_throttle))

            status2.empty()
            prog2.empty()

            st.success(f"完成：成功 {len(ok_rows)} 筆；失敗 {len(fail_rows)} 筆。")

            if ok_rows:
                df_ok = pd.DataFrame(ok_rows)
                st.subheader("成功清單")
                st.dataframe(df_ok, use_container_width=True, height=260)
                st.download_button(
                    "下載成功 CSV",
                    data=df_ok.to_csv(index=False).encode("utf-8-sig"),
                    file_name="recycle_apply_success.csv",
                    mime="text/csv",
                )

            if fail_rows:
                df_fail = pd.DataFrame(fail_rows)
                st.subheader("失敗清單")
                st.dataframe(df_fail, use_container_width=True, height=260)
                st.download_button(
                    "下載失敗 CSV",
                    data=df_fail.to_csv(index=False).encode("utf-8-sig"),
                    file_name="recycle_apply_failed.csv",
                    mime="text/csv",
                )
