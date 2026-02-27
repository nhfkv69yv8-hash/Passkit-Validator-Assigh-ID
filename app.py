import os
import time
import json
import hashlib
import requests
import jwt  # PyJWT
import pandas as pd
import streamlit as st

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="PassKit 重複 ID / 回收重分配工具", page_icon="♻️")
st.title("♻️ PassKit 重複 ID / 回收重分配工具")
st.caption("每行貼一個 full name（person.displayName）。先查重複/未找到，再把可回收的 PASS_ISSUED 且 cardNumber(=externalId/memberId) 為空的舊 memberId 重分配給未找到的人。")

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

# Optional: if you prefer long-lived token auth (Bearer), set this
PK_LONG_LIVED_TOKEN = get_config("PK_LONG_LIVED_TOKEN") or get_config("PK_API_TOKEN")

missing_cfg = [k for k, v in {
    "PK_API_PREFIX": PK_API_PREFIX,
    "PROGRAM_ID": PROGRAM_ID,
}.items() if not v]

# If no long-lived token, require key/secret for JWT auth
if not PK_LONG_LIVED_TOKEN:
    missing_cfg += [k for k, v in {
        "PK_API_KEY": PK_API_KEY,
        "PK_API_SECRET": PK_API_SECRET,
    }.items() if not v]

if missing_cfg:
    st.error(f"❌ 缺少設定：{', '.join(sorted(set(missing_cfg)))}（請在 .env 或 Secrets 補上）")
    st.stop()

# ----------------------------
# Auth helpers
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

def build_headers(body_text: str | None = None) -> dict:
    # Prefer long-lived token if provided
    if PK_LONG_LIVED_TOKEN:
        return {
            "Authorization": f"Bearer {PK_LONG_LIVED_TOKEN}",
            "Content-Type": "application/json",
        }

    # Otherwise JWT style (PassKit examples: token directly, no Bearer)
    token = make_jwt_for_body(body_text or "")
    return {
        "Authorization": token,
        "Content-Type": "application/json",
    }

# ----------------------------
# HTTP helpers
# ----------------------------
def parse_maybe_ndjson(text: str) -> list[dict]:
    text = (text or "").strip()
    if not text:
        return []
    items: list[dict] = []
    lines = [ln for ln in text.split("\n") if ln.strip()]
    # try NDJSON
    ok = True
    for ln in lines:
        try:
            items.append(json.loads(ln))
        except json.JSONDecodeError:
            ok = False
            break
    if ok:
        return items
    # fallback single JSON
    return [json.loads(text)]

def post_list_members(filters_payload: dict) -> list[dict]:
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member/list/{PROGRAM_ID}"
    body_text = json.dumps({"filters": filters_payload}, separators=(",", ":"), ensure_ascii=False)
    headers = build_headers(body_text)

    resp = requests.post(url, headers=headers, data=body_text, timeout=30)

    if resp.status_code == 404:
        raise RuntimeError("404 Not Found：多半是 API Prefix 用錯（pub1/pub2），或 endpoint path 拼錯。")
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Auth 失敗（{resp.status_code}）：請確認憑證/Token、以及 API Prefix（pub1/pub2）。")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")

    return parse_maybe_ndjson(resp.text)

def put_update_member(payload: dict) -> dict:
    """
    PUT https://api.pub1.passkit.io/members/member
    Payload uses PassKit "field names" style keys, e.g. person.displayName, programId, id.
    """
    url = f"{PK_API_PREFIX.rstrip('/')}/members/member"
    body_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = build_headers(body_text)

    resp = requests.put(url, headers=headers, data=body_text, timeout=30)

    if resp.status_code == 404:
        raise RuntimeError("404 Not Found：多半是 API Prefix 用錯（pub1/pub2），或 endpoint path 拼錯。")
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Auth 失敗（{resp.status_code}）：請確認憑證/Token、以及 API Prefix（pub1/pub2）。")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:800]}")

    # update typically returns JSON
    parsed = parse_maybe_ndjson(resp.text)
    return parsed[0] if parsed else {}

# ----------------------------
# Business logic
# ----------------------------
def safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def normalize_name(name: str) -> str:
    # you said: ALL CAPS, and forename/surname no internal spaces; still normalize whitespace
    return " ".join((name or "").strip().split())

def split_forename_surname(display_name: str) -> tuple[str, str, str]:
    parts = normalize_name(display_name).split(" ")
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", ""
    forename = parts[0]
    surname = parts[-1]
    other = " ".join(parts[1:-1]) if len(parts) > 2 else ""
    return forename, surname, other

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int) -> list[dict]:
    """
    Extract: displayName, id, passStatus, externalId/memberId/cardNumber, created
    """
    rows = []
    for item in list_response_items:
        member = item.get("result") or item.get("member") or item
        if not isinstance(member, dict):
            continue

        person = member.get("person") or {}
        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()

        pass_status = (member.get("passStatus") or "").strip()

        # "cardNumber" 你口頭用法：這裡用 externalId/memberId/cardNumber 盡量兼容
        external_id = member.get("externalId")
        if external_id is None:
            external_id = member.get("memberId")
        if external_id is None:
            external_id = member.get("cardNumber")
        external_id = (str(external_id).strip() if external_id is not None else "")

        created = member.get("created")
        created_str = str(created).strip() if created is not None else ""

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName": display_name,
                "memberId": member_id,
                "passStatus": pass_status,
                "cardNumber(externalId/memberId)": external_id,
                "created": created_str,
            })

        if len(rows) >= max_hits:
            break
    return rows

def search_by_display_name(name: str, max_hits: int, operator: str) -> list[dict]:
    filters = {
        "limit": min(max_hits, 1000),
        "offset": 0,
        "orderBy": "created",
        "orderAsc": True,  # oldest -> newest (so newest is last)
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

def is_cardnumber_empty(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return True
    # sometimes people store literal "NULL" or "null"
    return v.upper() == "NULL"

def is_recyclable(row: dict) -> bool:
    # Safe recycle condition:
    # 1) PASS_ISSUED (not installed)
    # 2) cardNumber/externalId/memberId is empty (so no immutable external mapping)
    return (row.get("passStatus") == "PASS_ISSUED") and is_cardnumber_empty(row.get("cardNumber(externalId/memberId)", ""))

def update_member_displayname(member_id: str, new_display_name: str) -> dict:
    new_display_name = normalize_name(new_display_name)
    forename, surname, other = split_forename_surname(new_display_name)

    payload = {
        "programId": PROGRAM_ID,
        "id": member_id,
        "person.displayName": new_display_name,
    }
    # optional but helpful
    if forename:
        payload["person.forename"] = forename
    if surname:
        payload["person.surname"] = surname
    if other:
        payload["person.otherNames"] = other

    return put_update_member(payload)

# ----------------------------
# UI - Input
# ----------------------------
with st.form("search_form"):
    input_text = st.text_area(
        "每行一個 full name（person.displayName）— 最多 150 行",
        height=220,
        placeholder="MEIHUA LEE\nHSIUTING CHOU\nKUANYEN LEE\n..."
    )

    colA, colB, colC, colD = st.columns([1, 1, 2, 1])
    with colA:
        max_hits = st.number_input("同名最多回傳筆數", min_value=1, max_value=150, value=10, step=1)
    with colB:
        operator = st.selectbox("比對方式", options=["eq", "like"], index=0)
    with colC:
        st.caption("eq = 完全相同；like = 包含（較鬆，可能會回更多結果）")
    with colD:
        gap = st.number_input("每次 API 間隔(秒)", min_value=0.0, max_value=2.0, value=0.15, step=0.05)

    submitted = st.form_submit_button("🔍 Search")

# ----------------------------
# Run search
# ----------------------------
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

    prog = st.progress(0)
    status = st.empty()

    for i, name in enumerate(names, start=1):
        status.info(f"查詢中 {i}/{len(names)}：{name}")
        try:
            rows = search_by_display_name(name, int(max_hits), operator=operator)
            if rows:
                all_rows.extend(rows)
            else:
                missing.append(name)
        except Exception as e:
            st.error(f"❌ 查詢失敗：{name} → {e}")
            missing.append(name)

        prog.progress(i / len(names))
        if gap and gap > 0:
            time.sleep(float(gap))

    status.empty()
    prog.empty()

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=[
        "搜尋姓名", "displayName", "memberId", "passStatus", "cardNumber(externalId/memberId)", "created"
    ])

    st.session_state["df"] = df
    st.session_state["missing"] = missing

    st.success(f"完成：查詢 {len(names)} 筆，命中 {len(df)} 筆。未找到 {len(missing)} 筆。")

# ----------------------------
# Results + CSV
# ----------------------------
df = st.session_state.get("df")
missing = st.session_state.get("missing", [])

if isinstance(df, pd.DataFrame) and not df.empty:
    st.subheader("查詢結果")
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("下載 CSV", data=csv, file_name="passkit_member_search.csv", mime="text/csv")

if missing:
    with st.expander(f"未找到名單（{len(missing)}）"):
        st.write("\n".join(missing))

# ----------------------------
# Recycle / Reassign
# ----------------------------
if isinstance(df, pd.DataFrame) and not df.empty:
    st.subheader("♻️ 回收 / 重分配（cardNumber 為空）")
    st.caption("只會挑選 PASS_ISSUED 且 cardNumber(externalId/memberId) 為空 的舊 memberId 進行重分配。")

    # Find duplicates by 搜尋姓名 (i.e. the requested displayName)
    dup_counts = df.groupby("搜尋姓名")["memberId"].count().reset_index(name="hits")
    dup_names = dup_counts[dup_counts["hits"] > 1]["搜尋姓名"].tolist()

    st.write(f"重複姓名數：{len(dup_names)}")

    recyclable_pool = []
    keepers = []

    for name in dup_names:
        g = df[df["搜尋姓名"] == name].copy()

        # Ensure ordered by created (best effort)
        # created may be RFC3339 or timestamp string; sort lexicographically is OK-ish for RFC3339
        g = g.sort_values(by=["created", "memberId"], ascending=[True, True])

        # Keep newest record (last row) as "keeper"
        keeper = g.iloc[-1].to_dict()
        keepers.append(keeper)

        # Others are candidates if recyclable
        candidates = g.iloc[:-1].to_dict(orient="records")
        for r in candidates:
            if is_recyclable(r):
                recyclable_pool.append(r)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        st.metric("可回收 memberId 數", len(recyclable_pool))
    with col2:
        st.metric("未找到姓名數", len(missing))
    with col3:
        st.caption("如果可回收數 < 未找到數，只會先分配一部分。")

    if recyclable_pool:
        st.markdown("**可回收池（預覽）**")
        st.dataframe(pd.DataFrame(recyclable_pool), use_container_width=True)

    # Build mapping
    mapping = []
    if recyclable_pool and missing:
        n = min(len(recyclable_pool), len(missing))
        for i in range(n):
            mapping.append({
                "分配給（新 displayName）": missing[i],
                "被回收的 memberId": recyclable_pool[i]["memberId"],
                "原本 displayName": recyclable_pool[i]["displayName"],
                "passStatus": recyclable_pool[i]["passStatus"],
                "cardNumber": recyclable_pool[i]["cardNumber(externalId/memberId)"],
            })

        st.markdown("**重分配計畫（mapping）**")
        map_df = pd.DataFrame(mapping)
        st.dataframe(map_df, use_container_width=True)

        st.warning("⚠️ 執行後，這些 memberId 的 person.displayName 會被改成新的名字。請確保它們確實是「沒被安裝」且「cardNumber/externalId 為空」的紀錄。")

        confirm = st.checkbox("我確認：只重用 PASS_ISSUED 且 cardNumber 為空 的 memberId，且接受 displayName 被改名", value=False)

        if st.button("🚀 執行重分配（PUT /members/member）", disabled=not confirm):
            results = []
            prog2 = st.progress(0)
            status2 = st.empty()

            for i, m in enumerate(mapping, start=1):
                status2.info(f"更新 {i}/{len(mapping)}：{m['被回收的 memberId']} → {m['分配給（新 displayName）']}")
                try:
                    resp = update_member_displayname(m["被回收的 memberId"], m["分配給（新 displayName）"])
                    results.append({
                        **m,
                        "結果": "OK",
                        "回應摘要": json.dumps(resp)[:300]
                    })
                except Exception as e:
                    results.append({**m, "結果": "FAIL", "回應摘要": str(e)[:300]})

                prog2.progress(i / len(mapping))
                if gap and gap > 0:
                    time.sleep(float(gap))

            status2.empty()
            prog2.empty()

            res_df = pd.DataFrame(results)
            st.session_state["reassign_results"] = res_df

            ok = (res_df["結果"] == "OK").sum()
            st.success(f"完成重分配：成功 {ok} / {len(res_df)}")

    else:
        st.info("目前沒有足夠資訊產生 mapping：需要同名重複且可回收的 memberId，並且要有未找到名單。")

# Show execution results
res_df = st.session_state.get("reassign_results")
if isinstance(res_df, pd.DataFrame) and not res_df.empty:
    st.subheader("✅ 重分配執行結果")
    st.dataframe(res_df, use_container_width=True)
    st.download_button(
        "下載 重分配結果 CSV",
        data=res_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="passkit_reassign_results.csv",
        mime="text/csv"
    )
