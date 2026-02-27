import os
import time
import json
import hashlib
import requests
import jwt  # from PyJWT
import pandas as pd
import streamlit as st

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="PassKit 重複 ID 搜尋 / 回收分配工具", page_icon="♻️", layout="wide")
st.title("♻️ PassKit 重複 ID 搜尋 / 回收分配工具")
st.caption("1) 用 displayName 查詢 memberId 2) 找重複/未找到 3) 回收 PASS_ISSUED 且 meta_cardNumber 為空的舊 memberId 分配給未找到名單（先 Dry-run 再 Apply）")

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
# JWT auth (PassKit style)
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

def _handle_resp_errors(resp: requests.Response) -> None:
    if resp.status_code == 404:
        raise RuntimeError("404 Not Found：多半是 API Prefix（pub1/pub2）或 endpoint path 用錯。")
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Auth 失敗（{resp.status_code}）：請確認 PK_API_KEY/PK_API_SECRET、以及 API Prefix（pub1/pub2）。")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:800]}")

def post_list_members(filters_payload: dict) -> list[dict]:
    """
    POST {PK_API_PREFIX}/members/member/list/{PROGRAM_ID}
    PassKit list APIs sometimes return NDJSON (one JSON per line)
    """
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)

    token = make_jwt_for_body(body_text)
    headers = {
        "Authorization": token,  # PassKit examples: token directly, not Bearer
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, data=body_text, timeout=30)
    _handle_resp_errors(resp)

    text = (resp.text or "").strip()
    if not text:
        return []

    items: list[dict] = []
    # Try NDJSON first
    lines = [ln for ln in text.split("\n") if ln.strip()]
    for ln in lines:
        try:
            items.append(json.loads(ln))
        except json.JSONDecodeError:
            # maybe it's a single JSON
            items = [json.loads(text)]
            break
    return items

def put_update_member(payload: dict) -> dict:
    """
    PUT {PK_API_PREFIX}/members/member
    payload includes: programId, id, person/meta updates
    """
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    token = make_jwt_for_body(body_text)
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }

    resp = requests.put(url, headers=headers, data=body_text, timeout=30)
    _handle_resp_errors(resp)

    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text[:800]}

# ----------------------------
# Helpers
# ----------------------------
def normalize_name(name: str) -> str:
    # 你說 displayName 固定全大寫 first+last、無空格；這裡只做基本 trim
    return (name or "").strip()

def extract_member_obj(item: dict) -> dict | None:
    member = item.get("result") or item.get("member") or item
    return member if isinstance(member, dict) else None

def is_blank_card_number(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return (s == "") or (s.upper() == "NULL")

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int) -> list[dict]:
    """
    Extract: person.displayName, id, passStatus, meta.cardNumber, created/updated(若有)
    """
    rows = []
    for item in list_response_items:
        member = extract_member_obj(item)
        if not member:
            continue

        person = member.get("person") or {}
        meta = member.get("meta") or {}  # ✅ 你說的容器 key：meta
        if not isinstance(meta, dict):
            meta = {}

        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()
        pass_status = (member.get("passStatus") or "").strip()

        meta_card_number = meta.get("cardNumber")
        meta_card_number = "" if meta_card_number is None else str(meta_card_number).strip()

        created = member.get("created") or member.get("createdAt") or member.get("createdOn")
        updated = member.get("updated") or member.get("updatedAt") or member.get("updatedOn")

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName": display_name,
                "memberId": member_id,
                "passStatus": pass_status,
                "cardNumber": card_number,
                "created": str(created) if created is not None else "",
                "updated": str(updated) if updated is not None else "",
            })

        if len(rows) >= max_hits:
            break
    return rows

def search_by_display_name(name: str, max_hits: int, operator: str) -> list[dict]:
    filters = {
        "limit": min(int(max_hits), 1000),
        "offset": 0,
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [{
                "filterField": "displayName",
                "filterValue": name,
                "filterOperator": operator,  # "eq" or "like"
            }]
        }]
    }
    items = post_list_members(filters)
    return extract_member_rows(items, name, max_hits=max_hits)

def list_recycle_pool_issued_cardnumber_null(limit: int = 300, offset: int = 0) -> list[dict]:
    """
    全域回收池（可選）：
    PASS_ISSUED 且 cardNumber == NULL 的 memberId
    - 有些 PassKit 後端會用 "NULL" 當作 null filterValue
    - 這裡也做二次檢查：回來後再用 is_blank_card_number() 過濾
    """
    filters = {
        "limit": min(int(limit), 1000),
        "offset": int(offset),
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [
                {"filterField": "passStatus", "filterValue": "PASS_ISSUED", "filterOperator": "eq"},
                {"filterField": "cardNumber", "filterValue": "NULL", "filterOperator": "eq"},
            ]
        }],
        "orderBy": "created",
        "orderAsc": True,
    }

    items = post_list_members(filters)
    pool = []
    for item in items:
        member = extract_member_obj(item)
        if not member:
            continue
        meta = member.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}

        mid = (member.get("id") or "").strip()
        ps = (member.get("passStatus") or "").strip()
        mcn = meta.get("cardNumber")

        if mid and ps == "PASS_ISSUED" and is_blank_card_number(mcn):
            pool.append({
                "memberId": mid,
                "passStatus": ps,
                "cardNumber": "" if mcn is None else str(mcn).strip(),
                "created": str(member.get("created") or ""),
            })
    return pool

def choose_duplicate_recycle_candidates(df_hits: pd.DataFrame) -> pd.DataFrame:
    """
    從同名多筆中挑回收候選：
    - 每個 displayName 保留 1 筆（當成最新/主筆）
    - 其餘若 passStatus=PASS_ISSUED 且 meta_cardNumber 空 → 回收池
    """
    if df_hits.empty:
        return df_hits.iloc[0:0].copy()

    work = df_hits.copy()

    # created/updated 若可解析就排序更穩
    for col in ["updated", "created"]:
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], errors="coerce")

    candidates = []
    for name, g in work.groupby("displayName", dropna=False):
        if len(g) <= 1:
            continue

        # newest first（updated > created）
        if g["updated"].notna().any() or g["created"].notna().any():
            g_sorted = g.sort_values(["updated", "created"], ascending=[False, False], na_position="last")
        else:
            g_sorted = g.copy()

        # keep newest (first row)
        rest = g_sorted.iloc[1:]

        for _, r in rest.iterrows():
            if (r.get("passStatus") == "PASS_ISSUED") and is_blank_card_number(r.get("cardNumber")):
                candidates.append(r.to_dict())

    return pd.DataFrame(candidates) if candidates else work.iloc[0:0].copy()

def build_put_payload_reassign(member_id: str, new_display_name: str) -> dict:
    """
    回收分配時：
    - 更新 person.displayName
    - 同時寫入佔位 meta_cardNumber，避免下一次又被當成可回收
    """
    new_display_name = normalize_name(new_display_name)
    return {
        "programId": PROGRAM_ID,
        "id": member_id,
        "person": {"displayName": new_display_name},
        "meta": {"cardNumber": f"TEMP_{member_id}"},
    }

# ----------------------------
# UI
# ----------------------------
with st.form("search_form"):
    input_text = st.text_area(
        "每行一個 full name（PassKit: person.displayName）— 最多 150 行",
        height=220,
        placeholder="MEIHUA LEE\nHSIUTING CHOU\nKUANYEN LEE\n..."
    )

    colA, colB, colC, colD = st.columns([1, 1, 1, 2])
    with colA:
        max_hits = st.number_input("同名最多回傳筆數", min_value=1, max_value=150, value=10, step=1)
    with colB:
        operator = st.selectbox("比對方式", options=["eq", "like"], index=0)
    with colC:
        throttle = st.number_input("每次 API 間隔秒數", min_value=0.0, max_value=2.0, value=0.15, step=0.05)
    with colD:
        st.caption("eq = 完全相同；like = 包含（較鬆，可能會回更多結果）")

    submitted = st.form_submit_button("Search")

if submitted:
    names = [normalize_name(n) for n in (input_text or "").splitlines() if normalize_name(n)]
    if not names:
        st.warning("請先貼上至少一行姓名。")
        st.stop()

    if len(names) > 150:
        st.warning(f"你貼了 {len(names)} 行，系統只會取前 150 行。")
        names = names[:150]

    all_rows = []
    missing = []

    prog = st.progress(0.0)
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

if hits_rows:
    df_hits = pd.DataFrame(hits_rows)
    cols_order = [c for c in ["搜尋姓名", "displayName", "memberId", "passStatus", "meta_cardNumber", "created", "updated"] if c in df_hits.columns]
    df_hits = df_hits[cols_order].copy()

    left, right = st.columns([2, 1], gap="large")
    with left:
        st.subheader("命中清單")
        st.dataframe(df_hits, use_container_width=True, height=420)
        csv = df_hits.to_csv(index=False).encode("utf-8-sig")
        st.download_button("下載命中 CSV", data=csv, file_name="passkit_member_hits.csv", mime="text/csv")

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

        if missing_names:
            st.subheader("未找到名單（missing）")
            st.write("\n".join(missing_names))
        else:
            st.info("沒有 missing。")

elif submitted:
    st.info("沒有命中資料（hits 為 0）。若你確認資料存在，請檢查 PROGRAM_ID / API Prefix / operator。")

# ----------------------------
# Recycle & assign
# ----------------------------
st.divider()
st.header("♻️ 回收池 → 分配給 missing（條件：PASS_ISSUED + meta.cardNumber 為空）")

if not missing_names:
    st.info("目前沒有 missing 名單，因此不需要分配回收池。")
else:
    st.warning(
        "⚠️ 重要提醒：你目前沒有『Pass URL 是否曾發送/外流』的紀錄。\n\n"
        "PASS_ISSUED 代表 URL 已存在；即使未 installed，若 URL 曾外流，你把 memberId 改名給別人，等於轉手。\n"
        "你要求的是過渡期減輕人工檢查，所以此工具用『PASS_ISSUED + meta_cardNumber 空』做保守回收。"
    )

    mode = st.radio(
        "回收池來源",
        options=[
            "A) 全域回收池：PASS_ISSUED + meta_cardNumber 為空（不依賴重複查詢）",
            "B) 同名重複回收：每個 displayName 保留最新 1 筆，其餘符合條件者回收（更貼近你截圖情境）",
        ],
        index=1
    )

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        assign_limit = st.number_input("最多分配筆數", min_value=1, max_value=5000, value=min(300, len(missing_names)), step=10)
    with col2:
        apply_throttle = st.number_input("每次 PUT 間隔秒數", min_value=0.0, max_value=2.0, value=0.2, step=0.05)
    with col3:
        st.caption("流程：先 Dry-run 產生 mapping → 勾選確認 → Apply 批次 PUT。")

    recycle_ids: list[str] = []

    if mode.startswith("A)"):
        st.subheader("A) 取得全域回收池")
        pool_limit = st.number_input("回收池撈取上限", min_value=10, max_value=1000, value=300, step=50)
        fetch_pool = st.button("取得回收池（A）", type="secondary")
        if fetch_pool:
            try:
                pool = list_recycle_pool_issued_cardnumber_null(limit=int(pool_limit), offset=0)
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
            st.info("尚未取得回收池，或回收池為空。")

    else:
        st.subheader("B) 從同名重複中挑回收候選（保留最新 1 筆，其餘 PASS_ISSUED + meta_cardNumber 空者回收）")
        if not hits_rows:
            st.info("你需要先 Search 取得命中資料，才能使用 B 模式。")
        else:
            df_hits = pd.DataFrame(hits_rows)
            cand_df = choose_duplicate_recycle_candidates(df_hits)
            if cand_df.empty:
                st.info("找不到符合回收條件的同名候選（PASS_ISSUED + meta_cardNumber 空）。")
            else:
                st.success(f"找到可回收候選：{len(cand_df)} 筆。")
                show_cols = [c for c in ["displayName", "memberId", "passStatus", "meta_cardNumber", "created", "updated"] if c in cand_df.columns]
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
            mime="text/csv"
        )

        st.divider()
        st.subheader("✅ Apply：依 mapping 批次更新（PUT /members/member）")

        ack = st.checkbox(
            "我了解風險：PASS_ISSUED 仍可能已被分享。若回收的 memberId/URL 曾外流，更新後會讓舊 URL 指向新會員資料（等於轉手）。",
            value=False
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
                    payload = build_put_payload_reassign(member_id, new_name)
                    resp = put_update_member(payload)
                    ok_rows.append({
                        "memberId": member_id,
                        "new_displayName": new_name,
                        "result": "OK",
                        "resp": str(resp)[:500],
                    })
                except Exception as e:
                    fail_rows.append({
                        "memberId": member_id,
                        "new_displayName": new_name,
                        "result": "FAIL",
                        "error": str(e)[:800],
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
                    mime="text/csv"
                )

            if fail_rows:
                df_fail = pd.DataFrame(fail_rows)
                st.subheader("失敗清單")
                st.dataframe(df_fail, use_container_width=True, height=260)
                st.download_button(
                    "下載失敗 CSV",
                    data=df_fail.to_csv(index=False).encode("utf-8-sig"),
                    file_name="recycle_apply_failed.csv",
                    mime="text/csv"
                )
