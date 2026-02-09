import os
import json
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple, List

import requests  # ✅ 추가(슬랙 웹훅)

from dotenv import load_dotenv
load_dotenv()

from google.oauth2 import service_account
from googleapiclient.discovery import build


KST = timezone(timedelta(hours=9))

SPREADSHEET_ID = "1DeSRVN4pWf6rnp1v_FeePUYe1ngjwyq_znXZUzl_kbM"

# ✅ 슬롯 허용 범위(분): 각 시간 슬롯 기준 ±N분
SLOT_TOLERANCE_MINUTES = 70

# 시간 슬롯(각각 ±SLOT_TOLERANCE_MINUTES분)
SLOTS = [
    ("10:00", 10, 0, "B"),
    ("12:00", 12, 0, "I"),
    ("14:00", 14, 0, "P"),
    ("16:00", 16, 0, "W"),
    ("18:00", 18, 0, "AD"),
    ("20:00", 20, 0, "AK"),
    ("22:00", 22, 0, "AR"),
]

# 각 슬롯에서 7개 항목을 연속으로 씀
FIELDS = [
    "meta_spend",
    "cafe24_sales",
    "cafe24_orders",
    "coupang_sales",
    "coupang_orders",
    "naver_sales",
    "naver_orders",
]

BRAND_SHEETS = {
    "burdenzero": "부담제로_지금",
    "brainology": "뉴턴젤리_지금",
}


def now_kst() -> datetime:
    return datetime.now(KST)


def today_ymd_kst() -> str:
    return now_kst().date().strftime("%Y-%m-%d")


def pick_slot(dt: datetime) -> Optional[Tuple[str, str]]:
    """
    현재 시간이 슬롯(±SLOT_TOLERANCE_MINUTES분)에 속하면 (slot_label, start_col_letter) 반환
    아니면 None
    """
    tolerance_sec = SLOT_TOLERANCE_MINUTES * 60
    for label, hh, mm, col in SLOTS:
        center = dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if abs((dt - center).total_seconds()) <= tolerance_sec:
            return (label, col)
    return None


def col_to_index(col: str) -> int:
    """A=1, B=2 ..."""
    col = col.strip().upper()
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def index_to_col(n: int) -> str:
    """1=A ..."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(r + ord("A")) + s
    return s


def run_script_json(py_path: str, args: List[str]) -> Dict[str, Any]:
    """
    스크립트를 실행하고 stdout의 마지막 JSON 라인을 파싱
    """
    cmd = ["python", py_path] + args
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if p.returncode != 0:
        raise RuntimeError(
            f"[SCRIPT FAIL] {py_path}\n"
            f"STDOUT:\n{p.stdout}\n"
            f"STDERR:\n{p.stderr}\n"
        )

    lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError(f"[SCRIPT NO OUTPUT] {py_path}")
    last = lines[-1]
    try:
        return json.loads(last)
    except Exception:
        raise RuntimeError(f"[SCRIPT JSON PARSE FAIL] {py_path}\nlast_line={last}\nFULL_STDOUT:\n{p.stdout}")


def get_sheets_service():
    """
    Service Account JSON을 env에서 받는 방식 2개 지원:
    1) GOOGLE_SERVICE_ACCOUNT_JSON: JSON 문자열
    2) GOOGLE_SERVICE_ACCOUNT_FILE: 파일 경로
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    json_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()

    if json_str:
        info = json.loads(json_str)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif json_file:
        creds = service_account.Credentials.from_service_account_file(json_file, scopes=scopes)
    else:
        raise RuntimeError(
            "ENV 필요: GOOGLE_SERVICE_ACCOUNT_JSON(문자열) 또는 GOOGLE_SERVICE_ACCOUNT_FILE(파일경로)"
        )

    return build("sheets", "v4", credentials=creds)


def get_sheet_values(svc, sheet_name: str, a1: str) -> List[List[Any]]:
    return (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!{a1}")
        .execute()
        .get("values", [])
    )


def update_sheet_values(svc, sheet_name: str, a1: str, values: List[List[Any]]):
    body = {"values": values}
    return (
        svc.spreadsheets()
        .values()
        .update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!{a1}",
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )


def append_sheet_values(svc, sheet_name: str, a1: str, values: List[List[Any]]):
    body = {"values": values}
    return (
        svc.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!{a1}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )


def find_or_create_today_row(svc, sheet_name: str, ymd: str) -> int:
    """
    A열에서 오늘(ymd) 찾고 있으면 row index 반환(1-based).
    없으면 맨 아래에 추가하고 그 row 반환.
    """
    colA = get_sheet_values(svc, sheet_name, "A:A")
    for i, row in enumerate(colA, start=1):
        if row and str(row[0]).strip() == ymd:
            return i

    append_sheet_values(svc, sheet_name, "A:A", [[ymd]])
    colA2 = get_sheet_values(svc, sheet_name, "A:A")
    return len(colA2) if colA2 else 1


def build_row_payload(
    brand: str,
    cafe24: Dict[str, Any],
    coupang: Dict[str, Any],
    naver: Dict[str, Any],
    meta: Dict[str, Any],
) -> List[Any]:
    """
    슬롯 시작 셀부터 7개 연속으로 넣을 값 순서:
    메타 광고비, 자사몰 매출, 자사몰 구매수, 쿠팡 매출, 쿠팡 구매수, 스마트스토어 매출, 스마트스토어 구매수
    """
    m = (meta.get("mapped") or {}).get(brand) or {}
    c = (cafe24.get("mapped") or {}).get(brand) or {}
    cp = (coupang.get("mapped") or {}).get(brand) or {}
    nv = (naver.get("mapped") or {}).get(brand) or {}

    meta_spend = float(m.get("spend") or 0.0)
    cafe24_sales = int(c.get("sales") or 0)
    cafe24_orders = int(c.get("orders") or 0)
    coupang_sales = int(cp.get("sales") or 0)
    coupang_orders = int(cp.get("orders") or 0)
    naver_sales = int(nv.get("sales") or 0)
    naver_orders = int(nv.get("orders") or 0)

    return [
        meta_spend,
        cafe24_sales,
        cafe24_orders,
        coupang_sales,
        coupang_orders,
        naver_sales,
        naver_orders,
    ]


# ✅ 슬랙 웹훅 발송(웹훅만 사용)
def slack_post(text: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        print("[SLACK SKIP] SLACK_WEBHOOK_URL 미설정")
        return
    r = requests.post(webhook, json={"text": text}, timeout=15)
    if r.status_code >= 300:
        raise RuntimeError(f"[SLACK WEBHOOK FAIL] {r.status_code} {r.text[:300]}")


# ✅ ROAS/CPA 계산 (요청 반영: purchases = cafe24_orders + coupang_orders + naver_orders)
def compute_roas_cpa_for_brand(brand: str, cafe24_res, coupang_res, naver_res, meta_res) -> dict:
    m = (meta_res.get("mapped") or {}).get(brand) or {}
    c = (cafe24_res.get("mapped") or {}).get(brand) or {}
    cp = (coupang_res.get("mapped") or {}).get(brand) or {}
    nv = (naver_res.get("mapped") or {}).get(brand) or {}

    spend = float(m.get("spend") or 0.0)

    cafe24_sales = int(c.get("sales") or 0)
    cafe24_orders = int(c.get("orders") or 0)
    coupang_sales = int(cp.get("sales") or 0)
    coupang_orders = int(cp.get("orders") or 0)
    naver_sales = int(nv.get("sales") or 0)
    naver_orders = int(nv.get("orders") or 0)

    revenue = cafe24_sales + coupang_sales + naver_sales
    purchases = cafe24_orders + coupang_orders + naver_orders  # ✅ 변경

    roas = (revenue / spend) if spend > 0 else 0.0
    cpa = (spend / purchases) if purchases > 0 else 0.0

    return {
        "spend": spend,
        "purchases": purchases,
        "revenue": revenue,
        "roas": roas,
        "cpa": cpa,
    }


def main():
    # 1) 현재 시간이 슬롯 범위에 속하는지 확인
    now = now_kst()
    picked = pick_slot(now)
    if not picked:
        print(
            f"[SKIP] 현재시각(KST) {now.strftime('%H:%M')} 은 지정 슬롯(±{SLOT_TOLERANCE_MINUTES}분)에 해당 없음. 기록하지 않음."
        )
        return

    slot_label, start_col = picked
    ymd = today_ymd_kst()
    print(f"[INFO] slot={slot_label} start_col={start_col} date={ymd}")

    # 2) 각 current 스크립트 실행해서 값 가져오기
    cafe24_res = run_script_json("connectors/sales/cafe24_current.py", ["--all", "--json"])
    coupang_res = run_script_json("connectors/sales/coupang_current.py", ["--json"])
    naver_res = run_script_json("connectors/sales/naver_current.py", ["--json"])
    meta_res = run_script_json("connectors/meta/meta_ads_current.py", ["--json"])

    # 3) 구글시트 연결
    svc = get_sheets_service()

    # 4) 각 브랜드별로 해당 시트에 기록
    start_idx = col_to_index(start_col)
    end_idx = start_idx + len(FIELDS) - 1
    end_col = index_to_col(end_idx)

    for brand, sheet_name in BRAND_SHEETS.items():
        row_idx = find_or_create_today_row(svc, sheet_name, ymd)

        values = build_row_payload(brand, cafe24_res, coupang_res, naver_res, meta_res)

        range_a1 = f"{start_col}{row_idx}:{end_col}{row_idx}"
        update_sheet_values(svc, sheet_name, range_a1, [values])

        print(f"[OK] {sheet_name} row={row_idx} range={range_a1} values={values}")

    # ✅ 시트 기록 후 슬랙 발송(요청 반영된 ROAS/CPA)
    bz = compute_roas_cpa_for_brand("burdenzero", cafe24_res, coupang_res, naver_res, meta_res)
    bio = compute_roas_cpa_for_brand("brainology", cafe24_res, coupang_res, naver_res, meta_res)

    msg = (
        f"*👀현재 ROAS/CPA 알림*\n"
        f"- 날짜: {ymd} / 슬롯: {slot_label}\n"
        f"\n*✅부담제로*\n"
        f"• ROAS: {bz['roas']:,.2f}\n"
        f"• CPA: {bz['cpa']:,.2f}\n"
        f"• 메타 광고비: {bz['spend']:,.0f}\n"
        f"• 구매수: {bz['purchases']:,}\n"
        f"• 현재 매출: {bz['revenue']:,}\n"
        
        f"\n*✅브레인올로지*\n"
        f"• ROAS: {bio['roas']:,.2f}\n"
        f"• CPA: {bio['cpa']:,.2f}\n"
        f"• 메타 광고비: {bio['spend']:,.0f}\n"
        f"• 구매수: {bio['purchases']:,}\n"
        f"• 현재 매출: {bio['revenue']:,}\n"

    )

    slack_post(msg)
    print("[SLACK OK] sent")


if __name__ == "__main__":
    main()
