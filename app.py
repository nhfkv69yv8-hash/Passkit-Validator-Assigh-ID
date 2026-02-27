import os
import time
import json
import hashlib
import requests
import jwt  # PyJWT
import pandas as pd
import streamlit as st
from typing import Any

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="PassKit 重複 ID 搜尋 / 回收分配工具", page_icon="🔍", layout="wide")
st.title("🔍♻️ PassKit 重複 ID 搜尋 / 回收分配工具")
st.caption("① 貼 displayName 批次查詢（REST Filter）。② 產生重複/缺漏。③ 可選擇回收池 → 自動分配給 missing（先預覽，再套用）。")

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
    payload = {
        "uid": PK_API_KEY,
        "iat": now,
        "exp": now + 600,  # 10 minutes
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
        "Authorization": token,
        "Content-Type": "application/json",
    }

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
            # fallback to single JSON
            items = [json.loads(text)]
            break
    return items

def put_update_member(member_id: str, payload: dict) -> dict:
    """
    PUT {PK_API_PREFIX}/members/member
    payload must include at least: programId, id
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
# Helpers: normalize / extract
# ----------------------------
def normalize_name(name: str) -> str:
    # 依你說的規格：全大寫 + 中間無空格
    return (name or "").strip().upper().replace(" ", "")

def _pick_first_present(d: dict, keys: list[str]) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None

def extract_member_obj(item: dict) -> dict | None:
    member = item.get("result") or item.get("member") or item
    return member if isinstance(member, dict) else None

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int) -> list[dict]:
    rows = []
    for item in list_response_items:
        member = extract_member_obj(item)
        if not member:
            continue

        person = member.get("person") or {}
        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()
        pass_status = (member.get("passStatus") or "").strip()

        created = _pick_first_present(member, ["created", "createdAt", "createdOn", "createdDate", "createDate"])
        updated = _pick_first_present(member, ["updated", "updatedAt", "updatedOn", "updatedDate", "updateDate"])

        # 常見 person 欄位（不一定存在）：forename/surname/email/mobile
        forename = (person.get("forename") or "").strip()
        surname = (person.get("surname") or "").strip()
        email = (person.get("emailAddress") or "").strip()
        mobile = (person.get("mobileNumber") or "").strip()

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName": display_name,
                "memberId": member_id,
                "passStatus": pass_status,
                "created": created,
                "updated": updated,
                "forename": forename,
                "surname": surname,
                "emailAddress": email,
                "mobileNumber": mobile,
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

def list_recycle_pool_issued_blank(limit: int = 200, offset: int = 0) -> list[dict]:
    """
    回收池（較安全）：PASS_ISSUED + displayName == ""（空白佔位會員）
    注意：是否支援用 displayName eq "" 由 PassKit 後端決定；若你環境不吃空字串，會回傳 0 筆。
    """
    filters = {
        "limit": min(int(limit), 1000),
        "offset": int(offset),
        "filterGroups": [{
            "condition": "AND",
            "fieldFilters": [
                {"filterField": "passStatus", "filterValue": "PASS_ISSUED", "filterOperator": "eq"},
                {"filterField": "displayName", "filterValue": "", "filterOperator": "eq"},
            ]
        }],
        # 若後端支援排序，這裡可以讓回收池更穩定：先用較舊的
        "orderBy": "created",
        "orderAsc": True,
    }
    items = post_list_members(filters)

    rows: list[dict] = []
    for item in items:
        member = extract_member_obj(item)
        if not member:
            continue
        person = member.get("person") or {}
        member_id = (member.get("id") or "").strip()
        pass_status = (member.get("passStatus") or "").strip()
        display_name = (person.get("displayName") or "").strip()
        # 二次確保真空白
        if pass_status == "PASS_ISSUED" and member_id and display_name == "":
            rows.append({
                "memberId": member_id,
                "passStatus": pass_status,
                "created": _pick_first_present(member, ["created", "createdAt", "createdOn", "createdDate", "createDate"]),
                "updated": _pick_first_present(member, ["updated", "updatedAt", "updatedOn", "updatedDate", "updateDate"]),
            })
    return rows

def is_candidate_minimal(row: dict) -> bool:
    """
    用於「同名回收（高風險）」的保守篩選：
    - passStatus 必須 PASS_ISSUED
    - person 其他常見欄位都空（避免把舊人的 email/phone 留著）
    你可以依你實際 schema 再加更嚴格條件。
    """
    if (row.get("passStatus") or "") != "PASS_ISSUED":
        return False
    if (row.get("emailAddress") or "").strip():
        return False
    if (row.get("mobileNumber") or "").strip():
        return False
    if (row.get("forename") or "").strip():
        return False
    if (row.get("surname") or "").strip():
        return False
    return True

def choose_duplicate_recycle_candidates(df_hits: pd.DataFrame) -> pd.DataFrame:
    """
    從同名多筆中挑回收候選（高風險）：
    - 每個 displayName 保留 1 筆（盡量以 created/updated 判斷最新；若無，保留最後一筆）
    - 其餘若符合 is_candidate_minimal() 則列入回收池
    """
    if df_hits.empty:
        return df_hits.iloc[0:0].copy()

    work = df_hits.copy()

    # 嘗試把 created/updated 轉成可排序的時間（失敗就保持 NaT）
    for col in ["created", "updated"]:
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], errors="coerce")

    candidates = []

    for name, g in work.groupby("displayName", dropna=False):
        if len(g) <= 1:
            continue

        # 優先用 updated，其次 created；如果都沒有，就用原順序
        if g["updated"].notna().any():
            g_sorted = g.sort_values(["updated", "created"], ascending=[False, False], na_position="last")
        elif g["created"].notna().any():
            g_sorted = g.sort_values(["created"], ascending=[False], na_position="last")
        else:
            g_sorted = g.copy()

        # 保留第一筆（視為最新/主筆）
        keep = g_sorted.iloc[0]
        rest = g_sorted.iloc[1:]

        for _, r in rest.iterrows():
            row_dict = r.to_dict()
            if is_candidate_minimal(row_dict):
                candidates.append(row_dict)

    if not candidates:
        return work.iloc[0:0].copy()

    return pd.DataFrame(candidates)

# ----------------------------
# UI - Input
# ----------------------------
with st.form("search_form"):
    input_text = st.text_area(
        "每行一個 displayName（你定義：全大寫 + 無空格）— 最多 150 行",
        height=220,
        placeholder="HSIUTINGCHOU\nKUANYENLEE\nMEIHUALEE\n..."
    )

    colA, colB, colC, colD = st.columns([1, 1, 1, 2])
    with colA:
        max_hits = st.number_input("同名最多回傳筆數", min_value=1, max_value=150, value=10, step=1)
    with colB:
        operator = st.selectbox("比對方式", options=["eq", "like"], index=0)
    with colC:
        throttle = st.number_input("每次 API 間隔秒數", min_value=0.0, max_value=2.0, value=0.15, step=0.05)
    with colD:
        st.caption("eq = 完全相同；like = 包含（較鬆，可能回更多結果）。建議用 eq。")

    submitted = st.form_submit_button("Search")

# ----------------------------
# Run search
# ----------------------------
if submitted:
    raw_names = [n for n in (input_text or "").splitlines() if n.strip()]
    names = [normalize_name(n) for n in raw_names if normalize_name(n)]

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

    st.session_state["missing_names"] = missing
    st.session_state["hits_rows"] = all_rows

    st.success(f"完成：查詢 {len(names)} 筆，命中 {len(all_rows)} 筆；未找到 {len(missing)} 筆。")

# ----------------------------
# Render results
# ----------------------------
hits_rows = st.session_state.get("hits_rows") or []
missing_names = st.session_state.get("missing_names") or []

if hits_rows:
    df_hits = pd.DataFrame(hits_rows)
    # 較好看的欄位順序
    cols_order = [c for c in [
        "搜尋姓名", "displayName", "memberId", "passStatus", "created", "updated",
        "forename", "surname", "emailAddress", "mobileNumber"
    ] if c in df_hits.columns]
    df_hits = df_hits[cols_order].copy()

    left, right = st.columns([2, 1], gap="large")
    with left:
        st.subheader("命中清單")
        st.dataframe(df_hits, use_container_width=True, height=420)
        csv = df_hits.to_csv(index=False).encode("utf-8-sig")
        st.download_button("下載命中 CSV", data=csv, file_name="passkit_member_hits.csv", mime="text/csv")

    with right:
        st.subheader("重複統計")
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
st.header("♻️ 回收池 → 分配給 missing")

if not missing_names:
    st.info("目前沒有 missing 名單，因此不需要分配回收池。")
else:
    st.warning(
        "⚠️ 重要：你目前沒有『Pass URL 是否曾發送/外流』的紀錄。\n\n"
        "PASS_ISSUED 代表 URL 已存在；即使未 installed，若 URL 曾被任何人拿到，"
        "你把 memberId 改名給別人，未來打開舊 URL 會看到新資料（等於轉手）。\n\n"
        "因此我把『同名回收』設為高風險選項，並預設推薦『空白 ISSUED 回收池』。"
    )

    mode = st.radio(
        "選擇回收池來源",
        options=[
            "A) PASS_ISSUED + 空白資料（displayName 為空）【較安全 / 推薦】",
            "B) 同名重複中回收舊 memberId（高風險）",
        ],
        index=0
    )

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        assign_limit = st.number_input("最多分配筆數", min_value=1, max_value=5000, value=min(300, len(missing_names)), step=10)
    with col2:
        apply_throttle = st.number_input("每次 PUT 間隔秒數", min_value=0.0, max_value=2.0, value=0.2, step=0.05)
    with col3:
        st.caption("建議先做 Dry-run 預覽 mapping，確認後再 Apply。")

    recycle_pool: list[dict] = []
    pool_df: pd.DataFrame | None = None

    if mode.startswith("A)"):
        st.subheader("A) 取回收池：PASS_ISSUED + displayName == ''")
        pool_limit = st.number_input("回收池撈取上限（每次）", min_value=10, max_value=1000, value=300, step=50)
        fetch_pool = st.button("取得回收池（A）", type="secondary")

        if fetch_pool:
            try:
                recycle_pool = list_recycle_pool_issued_blank(limit=int(pool_limit), offset=0)
                pool_df = pd.DataFrame(recycle_pool) if recycle_pool else pd.DataFrame(columns=["memberId", "passStatus", "created", "updated"])
                st.session_state["recycle_pool"] = recycle_pool
                st.success(f"回收池取得完成：{len(recycle_pool)} 筆。")
            except Exception as e:
                st.error(f"❌ 取得回收池失敗：{e}")

        recycle_pool = st.session_state.get("recycle_pool") or []
        if recycle_pool:
            pool_df = pd.DataFrame(recycle_pool)
            st.dataframe(pool_df, use_container_width=True, height=240)
        else:
            st.info("尚未取得回收池，或回收池為空。")

    else:
        st.subheader("B) 從同名重複中挑回收候選（高風險）")
        st.caption("規則（保守）：只挑 PASS_ISSUED 且 person 其他欄位（email/mobile/forename/surname）都空的舊筆。")
        if hits_rows:
            df_hits = pd.DataFrame(hits_rows)
            cand_df = choose_duplicate_recycle_candidates(df_hits)
            if cand_df.empty:
                st.info("找不到符合『保守條件』的同名回收候選。你可以先用 A 模式，或調整候選條件。")
            else:
                st.success(f"找到同名回收候選：{len(cand_df)} 筆。")
                st.dataframe(
                    cand_df[["displayName", "memberId", "passStatus", "created", "updated", "emailAddress", "mobileNumber"]],
                    use_container_width=True,
                    height=260
                )
                recycle_pool = [{"memberId": x} for x in cand_df["memberId"].tolist() if str(x).strip()]
                st.session_state["recycle_pool_dup"] = recycle_pool
        else:
            st.info("你需要先 Search 取得命中資料，才能用同名回收（B）。")

        if mode.startswith("B)") and st.session_state.get("recycle_pool_dup"):
            recycle_pool = st.session_state["recycle_pool_dup"]

    # Mapping preview
    st.subheader("📌 Dry-run：產生分配 mapping（不會寫入）")

    # 池子只拿 memberId
    pool_ids = []
    for x in recycle_pool or []:
        mid = (x.get("memberId") or "").strip()
        if mid:
            pool_ids.append(mid)

    # 去重（保持順序）
    seen = set()
    pool_ids_unique = []
    for mid in pool_ids:
        if mid not in seen:
            pool_ids_unique.append(mid)
            seen.add(mid)

    max_assign = int(min(assign_limit, len(missing_names), len(pool_ids_unique)))
    if max_assign <= 0:
        st.info("目前無法產生 mapping：可能是回收池為空，或 missing 為空。")
    else:
        mapping = []
        for i in range(max_assign):
            mapping.append({
                "new_displayName": missing_names[i],
                "recycled_memberId": pool_ids_unique[i],
            })
        df_map = pd.DataFrame(mapping)
        st.dataframe(df_map, use_container_width=True, height=260)

        map_csv = df_map.to_csv(index=False).encode("utf-8-sig")
        st.download_button("下載 mapping CSV（Dry-run）", data=map_csv, file_name="recycle_mapping_dryrun.csv", mime="text/csv")

        st.divider()
        st.subheader("✅ Apply：依 mapping 批次更新（PUT）")

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
                    payload = {
                        "programId": PROGRAM_ID,
                        "id": member_id,
                        "person": {
                            "displayName": new_name
                        }
                    }
                    resp = put_update_member(member_id, payload)
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
                ok_csv = df_ok.to_csv(index=False).encode("utf-8-sig")
                st.download_button("下載成功 CSV", data=ok_csv, file_name="recycle_apply_success.csv", mime="text/csv")

            if fail_rows:
                df_fail = pd.DataFrame(fail_rows)
                st.subheader("失敗清單（請重試或檢查 API/資料）")
                st.dataframe(df_fail, use_container_width=True, height=260)
                fail_csv = df_fail.to_csv(index=False).encode("utf-8-sig")
                st.download_button("下載失敗 CSV", data=fail_csv, file_name="recycle_apply_failed.csv", mime="text/csv")
