def _get_meta_container(member: dict) -> dict:
    # 你實際回傳是 meta，但官方文件也常見 metaData；做相容處理
    meta = member.get("meta")
    if not isinstance(meta, dict) or meta is None:
        meta = member.get("metaData")
    if not isinstance(meta, dict) or meta is None:
        meta = member.get("metadata")
    return meta if isinstance(meta, dict) else {}

CARDNUMBER_META_KEY = "meta_cardNumber"  # 你的 field key

def extract_member_rows(list_response_items: list[dict], search_name: str, max_hits: int) -> list[dict]:
    rows = []
    for item in list_response_items:
        member = item.get("result") or item.get("member") or item
        if not isinstance(member, dict):
            continue

        person = member.get("person") or {}
        meta = _get_meta_container(member)

        display_name = (person.get("displayName") or "").strip()
        member_id = (member.get("id") or "").strip()
        pass_status = (member.get("passStatus") or "").strip()

        meta_card_number = meta.get(CARDNUMBER_META_KEY)
        meta_card_number = "" if meta_card_number is None else str(meta_card_number).strip()

        created = member.get("created") or ""
        updated = member.get("updated") or ""

        if display_name and member_id:
            rows.append({
                "搜尋姓名": search_name,
                "displayName (person.displayName)": display_name,
                "memberId (member.id)": member_id,
                "passStatus": pass_status,
                "meta_cardNumber": meta_card_number,
                "created": str(created),
                "updated": str(updated),
            })

        if len(rows) >= max_hits:
            break
    return rows
