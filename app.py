import os
import re
import json
import datetime
from pathlib import Path
from collections import defaultdict

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    import grpc
    from passkit.io.common import common_objects_pb2, filter_pb2
    from passkit.io.member import a_rpc_pb2_grpc, member_pb2
except Exception as e:
    grpc = None
    common_objects_pb2 = None
    filter_pb2 = None
    a_rpc_pb2_grpc = None
    member_pb2 = None
    SDK_IMPORT_ERROR = e
else:
    SDK_IMPORT_ERROR = None


# ----------------------------
# Page Config
# ----------------------------
st.set_page_config(page_title="PassKit 資源回收站 V2", page_icon="♻️", layout="wide")
st.title("♻️ PassKit 資源回收指派系統 (最新保留版)")

# 回收門檻（以 UTC+0 today 計）
# Time filter（預設不套用；可選 3/4/5 個月或自訂）
time_filter_option = st.selectbox(
    "Time filter",
    options=["不選擇（不套用）", "三個月", "四個月", "五個月", "自訂"],
    index=0,
    help="套用後，只會回收 meta.creationDate 距離現在（UTC+0）超過指定月數的卡號。預設不套用。",
)

custom_months = None
if time_filter_option == "自訂":
    custom_months = st.number_input("自訂月數（>=1）", min_value=1, max_value=60, value=3, step=1)

MONTHS_MAP = {"三個月": 3, "四個月": 4, "五個月": 5}
months_threshold = None
if time_filter_option in MONTHS_MAP:
    months_threshold = MONTHS_MAP[time_filter_option]
elif time_filter_option == "自訂":
    months_threshold = int(custom_months) if custom_months else 3

st.caption("自動移除輸入重複姓名、保留最新 PassKit ID、跨次暫存回收資源。")

# ----------------------------
# Session State 初始化
# ----------------------------
if "persistent_recycle_pool" not in st.session_state:
    st.session_state.persistent_recycle_pool = []

if "persistent_missing_people" not in st.session_state:
    st.session_state.persistent_missing_people = []

if "search_results" not in st.session_state:
    st.session_state.search_results = {"all_rows": [], "missing": [], "search_done": False}


# ----------------------------
# Config & gRPC Helpers
# ----------------------------
def get_config(key: str, default=None):
    val = st.secrets.get(key) if hasattr(st, "secrets") else None
    if val is None:
        val = os.environ.get(key, default)
    if val is None:
        return None
    return str(val).replace("\\n", "\n").strip()


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).upper()


PROGRAM_ID = get_config("PROGRAM_ID")
PK_API_PREFIX = get_config("PK_API_PREFIX", "https://api.pub1.passkit.io")
PK_GRPC_HOST = (
    get_config("PK_GRPC_HOST")
    or get_config("PASSKIT_GRPC_HOST")
    or get_config("GRPC_HOST")
)
if not PK_GRPC_HOST:
    if "pub2" in (PK_API_PREFIX or ""):
        PK_GRPC_HOST = "grpc.pub2.passkit.io:443"
    else:
        PK_GRPC_HOST = "grpc.pub1.passkit.io:443"


def _load_secret_or_file(content_keys, path_keys, default_paths=None):
    for key in content_keys:
        value = get_config(key)
        if value:
            return value.encode("utf-8")

    for key in path_keys:
        path_value = get_config(key)
        if path_value and Path(path_value).exists():
            return Path(path_value).read_bytes()

    for p in (default_paths or []):
        if p and Path(p).exists():
            return Path(p).read_bytes()
    return None


CERT_PEM = _load_secret_or_file(
    ["PASSKIT_CERTIFICATE_PEM", "CERTIFICATE_PEM", "PK_CERTIFICATE_PEM"],
    ["PASSKIT_CERTIFICATE_PEM_PATH", "CERTIFICATE_PEM_PATH", "PK_CERTIFICATE_PEM_PATH"],
    ["certs/certificate.pem", "./certs/certificate.pem", "/app/certs/certificate.pem"],
)
CA_CHAIN_PEM = _load_secret_or_file(
    ["PASSKIT_CA_CHAIN_PEM", "CA_CHAIN_PEM", "PK_CA_CHAIN_PEM"],
    ["PASSKIT_CA_CHAIN_PEM_PATH", "CA_CHAIN_PEM_PATH", "PK_CA_CHAIN_PEM_PATH"],
    ["certs/ca-chain.pem", "./certs/ca-chain.pem", "/app/certs/ca-chain.pem"],
)
KEY_PEM = _load_secret_or_file(
    ["PASSKIT_KEY_PEM", "KEY_PEM", "PK_KEY_PEM"],
    ["PASSKIT_KEY_PEM_PATH", "KEY_PEM_PATH", "PK_KEY_PEM_PATH"],
    ["certs/key.pem", "./certs/key.pem", "/app/certs/key.pem"],
)


def _grpc_config_error() -> str | None:
    if SDK_IMPORT_ERROR:
        return f"PassKit gRPC SDK 載入失敗：{SDK_IMPORT_ERROR}"
    if not PROGRAM_ID:
        return "缺少 PROGRAM_ID 設定。"
    if not PK_GRPC_HOST:
        return "缺少 PassKit gRPC host 設定。"
    if not CERT_PEM:
        return "找不到 certificate.pem。"
    if not CA_CHAIN_PEM:
        return "找不到 ca-chain.pem。"
    if not KEY_PEM:
        return "找不到 key.pem（需先用 openssl 解密後再使用）。"
    return None


@st.cache_resource(show_spinner=False)
def get_members_stub():
    err = _grpc_config_error()
    if err:
        raise RuntimeError(err)

    creds = grpc.ssl_channel_credentials(
        root_certificates=CA_CHAIN_PEM,
        private_key=KEY_PEM,
        certificate_chain=CERT_PEM,
    )
    channel = grpc.secure_channel(PK_GRPC_HOST, creds)
    return a_rpc_pb2_grpc.MembersStub(channel)


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

        return _utc_today() - relativedelta(months=months)
    except Exception:
        return _utc_today() - datetime.timedelta(days=30 * months)


def _is_older_than_months(d: datetime.date | None, months: int) -> bool:
    if d is None:
        return False
    return d <= _cutoff_date_months_ago(months)


def _timestamp_to_iso(ts) -> str:
    try:
        if ts is None:
            return ""
        if hasattr(ts, "seconds") and ts.seconds == 0 and getattr(ts, "nanos", 0) == 0:
            return ""
        dt = ts.ToDatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc).isoformat()
    except Exception:
        return ""


def _member_meta_to_dict(member) -> dict:
    try:
        return dict(getattr(member, "metaData", {}) or {})
    except Exception:
        return {}


def _member_to_row(search_name: str, member) -> dict | None:
    if not member:
        return None

    person = getattr(member, "person", None)
    d_name = ""
    if person is not None:
        d_name = (getattr(person, "displayName", "") or "").strip()

    m_id = (getattr(member, "id", "") or "").strip()
    if not d_name or not m_id:
        return None

    meta = _member_meta_to_dict(member)
    return {
        "搜尋姓名": search_name,
        "displayName": d_name,
        "memberId": m_id,
        "created": _timestamp_to_iso(getattr(member, "created", None)),
        "modified": _timestamp_to_iso(getattr(member, "updated", None)),
        "meta_creationDate": meta.get("creationDate", ""),
        "meta_cardIssueDate": meta.get("cardIssueDate", ""),
    }


def search_by_display_name(name: str, max_hits: int, operator: str) -> list[dict]:
    stub = get_members_stub()
    normalized_name = normalize_name(name)

    req = member_pb2.ListRequest(
        programId=PROGRAM_ID,
        filters=filter_pb2.Filters(
            limit=min(int(max_hits), 1000),
            offset=0,
            filterGroups=[
                filter_pb2.FilterGroup(
                    condition=filter_pb2.AND,
                    fieldFilters=[
                        filter_pb2.FieldFilter(
                            filterField="displayName",
                            filterValue=normalized_name,
                            filterOperator=operator,
                        )
                    ],
                )
            ],
        ),
    )

    rows = []
    for member in stub.listMembers(req):
        row = _member_to_row(name, member)
        if row:
            rows.append(row)
    return rows


def update_member_display_name(member_id: str, new_name: str) -> bool:
    stub = get_members_stub()
    member = stub.getMemberRecordById(common_objects_pb2.Id(id=member_id))
    if not getattr(member, "id", ""):
        return False

    member.person.displayName = normalize_name(new_name)
    resp = stub.updateMember(member)
    return bool(getattr(resp, "id", ""))


# ----------------------------
# UI 控制面板
# ----------------------------
with st.sidebar:
    st.header("⚙️ 資源管理")
    st.metric("📦 可用回收 ID 庫存", len(st.session_state.persistent_recycle_pool))

    grpc_err = _grpc_config_error()
    if grpc_err:
        st.error(f"gRPC 設定未完成：{grpc_err}")
    else:
        st.caption(f"gRPC Host：{PK_GRPC_HOST}")

    with st.expander("📋 未指派 Passkit ID（可一鍵複製）", expanded=False):
        pool_ids = list(st.session_state.persistent_recycle_pool)
        if pool_ids:
            pool_text = "\n".join(pool_ids)
            st.caption("點下方按鈕即可把全部剩餘 ID 複製到剪貼簿（每行一個）。")
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
                html_block.replace("__TEXT__", json.dumps(pool_text)),
                height=62,
            )
            st.text_area("剩餘 ID（檢視用）", pool_text, height=140)
        else:
            st.info("目前沒有剩餘未指派 ID。")

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
    grpc_err = _grpc_config_error()
    if grpc_err:
        st.error(f"無法執行查詢：{grpc_err}")
        st.stop()

    raw_names = [n.strip() for n in (input_text or "").splitlines() if n.strip()]
    names = list(dict.fromkeys(raw_names))

    if len(raw_names) != len(names):
        st.info(f"💡 名單已自動去重：原始筆數 {len(raw_names)} 筆 -> 實際搜尋 {len(names)} 筆。")

    if not names:
        st.warning("請輸入姓名")
        st.stop()

    all_rows, missing = [], []
    prog = st.progress(0)
    status_txt = st.empty()

    for i, name in enumerate(names):
        status_txt.text(f"查詢中 ({i+1}/{len(names)}): {name}")
        try:
            rows = search_by_display_name(name, max_hits=int(max_hits), operator=operator)
            if rows:
                all_rows.extend(rows)
            else:
                missing.append(name)
        except Exception as e:
            st.error(f"查詢出錯: {name} -> {e}")
        prog.progress((i + 1) / len(names))

    unique_records = []
    seen_ids = set()
    for r in all_rows:
        mid = r.get("memberId", "")
        if mid and mid not in seen_ids:
            unique_records.append(r)
            seen_ids.add(mid)

    member_groups = defaultdict(list)
    for r in unique_records:
        member_groups[r["搜尋姓名"]].append(r)

    def _sort_key(rec: dict):
        created_dt = _parse_any_date(rec.get("created")) or datetime.date.min
        modified_dt = _parse_any_date(rec.get("modified")) or datetime.date.min
        return (created_dt, modified_dt, rec.get("memberId", ""))

    def _eligible_for_recycle(rec: dict) -> bool:
        if months_threshold is None:
            return False

        c = _parse_any_date(rec.get("meta_creationDate"))
        if not c:
            return False

        if not _is_older_than_months(c, months_threshold):
            return False

        return True

    new_recycle_ids = []

    for _, recs in member_groups.items():
        if len(recs) <= 1:
            continue

        recs_sorted = sorted(recs, key=_sort_key)
        to_recycle = recs_sorted[:-1]

        for rec in to_recycle:
            mid = rec.get("memberId", "")
            if not mid:
                continue

            if _eligible_for_recycle(rec):
                new_recycle_ids.append(mid)

    updated_pool = set(st.session_state.persistent_recycle_pool)
    updated_pool.update(new_recycle_ids)
    st.session_state.persistent_recycle_pool = sorted(list(updated_pool))

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
                try:
                    if update_member_display_name(m_id, m_name):
                        success_ids.append(m_id)
                except Exception as e:
                    st.error(f"更新失敗: {m_id} -> {m_name} | {e}")
                assign_prog.progress((i + 1) / pair_count)

            st.session_state.persistent_recycle_pool = [x for x in pool if x not in success_ids]
            assigned_names = set(missing_list[: len(success_ids)])
            st.session_state.persistent_missing_people = [
                nm for nm in st.session_state.persistent_missing_people if nm not in assigned_names
            ]
            st.session_state.search_results["missing"] = list(st.session_state.persistent_missing_people)

            st.success(
                f"指派成功！已為 {len(success_ids)} 位會員建立票卡，剩餘庫存 {len(st.session_state.persistent_recycle_pool)} 個。"
            )
            st.rerun()
    else:
        st.warning("暫無可用資源或無缺額需要指派。")
