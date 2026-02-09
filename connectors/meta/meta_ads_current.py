import os
import json
import argparse
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

KST = timezone(timedelta(hours=9))

META_API_VERSION = os.getenv("META_API_VERSION", "v24.0").strip()
GRAPH_BASE = f"https://graph.facebook.com/{META_API_VERSION}"
GRAPH_BASE_NO_VER = "https://graph.facebook.com"  # debug_token은 버전 없이도 동작

# 구매 액션 타입 후보 (계정/픽셀 세팅에 따라 다를 수 있어 넓게 잡음)
PURCHASE_ACTION_KEYS = {
    "purchase",
    "omni_purchase",
    "offsite_conversion.purchase",
    "web_in_store_purchase",
    "onsite_conversion.purchase",
}

NEEDED_PERMS = {"ads_read", "read_insights"}  # 필수급
NICE_TO_HAVE_PERMS = {"ads_management"}       # 있으면 좋은

TIMEOUT = 30


# -----------------------
# Helpers
# -----------------------
def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v

def ymd_today_kst() -> str:
    return datetime.now(KST).date().strftime("%Y-%m-%d")

def ymd_yesterday_kst() -> str:
    return (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y-%m-%d")

def normalize_act_id(ad_account_id: str) -> str:
    ad_account_id = (ad_account_id or "").strip()
    if not ad_account_id:
        return ""
    return ad_account_id if ad_account_id.startswith("act_") else f"act_{ad_account_id}"

def safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return None

def http_get(url: str, params: dict, label: str):
    r = requests.get(url, params=params, timeout=TIMEOUT)
    data = safe_json(r)
    if r.status_code != 200:
        raise RuntimeError(
            f"[HTTP ERROR] {label}\n"
            f"  url: {url}\n"
            f"  status: {r.status_code}\n"
            f"  body: {data or r.text[:300]}"
        )
    return data

def parse_purchases_from_actions(actions) -> int:
    if not actions:
        return 0
    total = 0
    for a in actions:
        at = (a.get("action_type") or "").strip()
        val = a.get("value")
        if val is None:
            continue

        is_purchase = (at in PURCHASE_ACTION_KEYS) or at.endswith(".purchase")
        if is_purchase:
            try:
                total += int(float(val))
            except ValueError:
                pass
    return total


# -----------------------
# Preflight checks
# -----------------------
def debug_token(access_token: str) -> dict:
    """
    토큰이 유효한지/만료인지/앱ID/타입 등을 확인.
    """
    url = f"{GRAPH_BASE_NO_VER}/debug_token"
    params = {"input_token": access_token, "access_token": access_token}
    return http_get(url, params, label="debug_token")

def get_permissions(access_token: str) -> dict:
    """
    /me/permissions 로 granted/declined 확인
    """
    url = f"{GRAPH_BASE}/me/permissions"
    params = {"access_token": access_token}
    return http_get(url, params, label="me/permissions")

def list_my_adaccounts(access_token: str, limit: int = 200) -> dict:
    """
    이 토큰이 접근 가능한 광고계정 목록을 가져옴.
    시스템 사용자 토큰이면 여기 목록이 '실제로 할당된' 계정들.
    """
    url = f"{GRAPH_BASE}/me/adaccounts"
    params = {"access_token": access_token, "fields": "account_id,name,account_status", "limit": limit}
    return http_get(url, params, label="me/adaccounts")

def summarize_permissions(perms_payload: dict):
    granted = set()
    declined = set()
    data = perms_payload.get("data") or []
    for row in data:
        p = (row.get("permission") or "").strip()
        s = (row.get("status") or "").strip()
        if not p:
            continue
        if s == "granted":
            granted.add(p)
        elif s == "declined":
            declined.add(p)
    return granted, declined

def preflight(profile_name: str, token: str, target_act: str):
    """
    문제를 '권한 누락' vs '자산 할당/접근 불가'로 분리해주는 사전 점검.
    """
    print(f"\n[PRECHECK] {profile_name}")
    print(f"  target ad account: {target_act}")

    # 1) debug_token
    dbg = debug_token(token)
    dbg_data = (dbg.get("data") or {})
    is_valid = dbg_data.get("is_valid")
    expires_at = dbg_data.get("expires_at")
    app_id = dbg_data.get("app_id")
    token_type = dbg_data.get("type")
    print(f"  token valid: {is_valid} | type: {token_type} | app_id: {app_id} | expires_at: {expires_at}")

    if not is_valid:
        raise RuntimeError(
            f"[TOKEN INVALID] {profile_name} 토큰이 유효하지 않습니다.\n"
            f"  - 시스템 사용자에서 새 토큰을 재발급하세요.\n"
            f"  - 또는 .env 토큰 값이 잘못 붙여넣기 되었을 수 있습니다."
        )

    # 2) permissions
    perms_payload = get_permissions(token)
    granted, declined = summarize_permissions(perms_payload)

    missing_needed = sorted(list(NEEDED_PERMS - granted))
    missing_nice = sorted(list(NICE_TO_HAVE_PERMS - granted))

    print(f"  granted perms: {', '.join(sorted(granted)) if granted else '(none?)'}")
    if missing_needed:
        print(f"  ❌ missing REQUIRED perms: {', '.join(missing_needed)}")
        raise RuntimeError(
            f"[PERMISSION MISSING] {profile_name}\n"
            f"  필수 권한이 토큰에 없습니다: {', '.join(missing_needed)}\n"
            f"  해결:\n"
            f"   1) Business Settings > System Users > Generate Token\n"
            f"   2) ads_read + read_insights 체크 후 새 토큰 발급\n"
        )
    if missing_nice:
        print(f"  ⚠️ missing optional perms: {', '.join(missing_nice)} (있으면 편함)")

    # 3) accessible ad accounts check
    adacc_payload = list_my_adaccounts(token, limit=200)
    adaccs = adacc_payload.get("data") or []
    accessible = set()
    for a in adaccs:
        acc_id = a.get("account_id")
        if acc_id:
            accessible.add(normalize_act_id(str(acc_id)))
        _id = a.get("id")
        if _id:
            accessible.add(normalize_act_id(str(_id).replace("act_", "")))

    sample = sorted(list(accessible))[:8]
    print(f"  accessible ad accounts (sample up to 8): {sample if sample else '(none)'}")
    if target_act not in accessible:
        raise RuntimeError(
            f"[AD ACCOUNT ACCESS] {profile_name}\n"
            f"  이 토큰으로는 목표 광고계정 {target_act} 에 접근할 수 없습니다.\n"
            f"  (me/adaccounts 목록에 없음)\n\n"
            f"  가장 흔한 원인:\n"
            f"   1) System User에 해당 광고계정을 Assets로 '할당'하지 않음\n"
            f"   2) 토큰/계정 매칭이 뒤바뀜\n"
            f"   3) 광고계정이 다른 Business 소유라 현재 System User에서 접근 불가\n\n"
            f"  해결:\n"
            f"   - Business Settings > System Users > (해당 System User)\n"
            f"     > Assets > Ad Accounts 에서 {target_act} 를 추가(Assign)하고 권한 부여\n"
            f"   - .env에서 토큰/계정 ID가 서로 바뀐 건 아닌지 확인\n"
        )

    print("  ✅ precheck OK")


# -----------------------
# Insights fetch (CURRENT spend)
# -----------------------
def fetch_insights_current_spend(access_token: str, ad_account_id: str, ymd: str) -> dict:
    """
    '현재 광고비' = 오늘(KST) 00:00 ~ 현재까지 누적 spend
    - Insights는 time_range since=ymd, until=ymd 로 요청하면 보통 그 날짜 누적(진행중)을 반환
    - 혹시 여러 row가 오면(시간 분할/특이 케이스) date_start==ymd 인 것만 합산
    """
    act_id = normalize_act_id(ad_account_id)
    url = f"{GRAPH_BASE}/{act_id}/insights"

    params = {
        "access_token": access_token,
        "fields": "spend,actions,date_start,date_stop",
        "level": "account",
        "time_range": json.dumps({"since": ymd, "until": ymd}),
        "time_increment": 1,  # 하루 단위 row로 받기
        "limit": 100,
    }

    r = requests.get(url, params=params, timeout=TIMEOUT)
    data = safe_json(r)

    if r.status_code != 200:
        err = (data or {}).get("error") if isinstance(data, dict) else None
        msg = err.get("message") if isinstance(err, dict) else None
        code = err.get("code") if isinstance(err, dict) else None

        hint = ""
        if r.status_code == 403 and code == 200:
            hint = (
                "\n[HINT]\n"
                "- (#200) 권한/자산할당 문제일 확률이 높습니다.\n"
                "- 위 PRECHECK에서 me/adaccounts에 목표 계정이 있는지 확인하세요.\n"
                "- 토큰 permissions에 ads_read/read_insights가 granted인지 확인하세요.\n"
            )

        raise RuntimeError(
            f"[INSIGHTS FAIL] {act_id}\n"
            f"  HTTP {r.status_code}\n"
            f"  error_message: {msg}\n"
            f"  body: {data or r.text[:300]}"
            f"{hint}"
        )

    rows = (data or {}).get("data") or []
    if not rows:
        return {"date": ymd, "spend": 0.0, "purchases": 0, "raw": data}

    spend_sum = 0.0
    purchases_sum = 0

    for row in rows:
        # 혹시라도 date_start/date_stop이 섞여오면 오늘 row만 합산
        ds = (row.get("date_start") or "").strip()
        if ds and ds != ymd:
            continue

        try:
            spend_sum += float(row.get("spend") or 0.0)
        except ValueError:
            pass

        purchases_sum += parse_purchases_from_actions(row.get("actions"))

    return {"date": ymd, "spend": float(spend_sum), "purchases": int(purchases_sum), "raw": rows[0]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD (기본: 오늘 KST)")
    parser.add_argument("--json", action="store_true", help="마지막 줄에 JSON 결과만 출력")
    args = parser.parse_args()

    # ✅ 변경: 기본값을 '오늘(KST)'로 = 현재 광고비
    target_ymd = (args.date or "").strip() or ymd_today_kst()

    profiles = [
        {
            "name": "brainology",
            "token": must_env("META_BRAINOLOGY_ACCESS_TOKEN"),
            "ad_account": must_env("META_BRAINOLOGY_AD_ACCOUNT_ID"),
        },
        {
            "name": "burdenzero",
            "token": must_env("META_BURDENZERO_ACCESS_TOKEN"),
            "ad_account": must_env("META_BURDENZERO_AD_ACCOUNT_ID"),
        },
    ]

    print(f"[INFO] Meta Ads CURRENT (KST): {target_ymd} | API={META_API_VERSION}")
    print("=" * 70)

    mapped = {}
    total_spend = 0.0
    total_purchases = 0

    for p in profiles:
        target_act = normalize_act_id(p["ad_account"])

        # ✅ 사전 점검
        preflight(p["name"], p["token"], target_act)

        # ✅ 현재(오늘 누적) 인사이트 조회
        res = fetch_insights_current_spend(p["token"], p["ad_account"], target_ymd)

        total_spend += res["spend"]
        total_purchases += res["purchases"]

        mapped[p["name"]] = {
            "date": target_ymd,
            "spend": res["spend"],
            "purchases": res["purchases"],
        }

        print(f"\n[RESULT] {p['name']} ({target_act})")
        print(f"  spend(current): {res['spend']}")
        print(f"  purchases(current): {res['purchases']}")
        print("-" * 70)

    print("\n[TOTAL]")
    print(f"  total_spend(current): {total_spend}")
    print(f"  total_purchases(current): {total_purchases}")
    print("=" * 70)

    # ✅ runner가 파싱할 마지막 줄 JSON 출력
    if args.json:
        out = {
            "date": target_ymd,
            "mapped": {
                "burdenzero": mapped.get("burdenzero", {"spend": 0.0, "purchases": 0}),
                "brainology": mapped.get("brainology", {"spend": 0.0, "purchases": 0}),
            },
            "total": {"spend": float(total_spend), "purchases": int(total_purchases)},
        }
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
