# connectors/sales/naver_current.py
# pip install requests bcrypt pybase64 python-dotenv

import os
import json
import time
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import requests
import bcrypt
import pybase64
from dotenv import load_dotenv

API_BASE = "https://api.commerce.naver.com/external"
TOKEN_URL = f"{API_BASE}/v1/oauth2/token"
PRODUCT_ORDERS_URL = f"{API_BASE}/v1/pay-order/seller/product-orders"

TOKEN_CACHE_FILE = ".naver_token_cache.json"
KST = timezone(timedelta(hours=9))


# ---------------------------
# Time helpers (Asia/Seoul)
# ---------------------------
def now_kst() -> datetime:
    return datetime.now(KST)


def kst_day_range(target_date: datetime.date):
    """해당 날짜의 KST 00:00:00.000 ~ 23:59:59.999 ISO8601 문자열"""
    start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, 0, tzinfo=KST)
    end = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, 999000, tzinfo=KST)
    return start.isoformat(timespec="milliseconds"), end.isoformat(timespec="milliseconds")


# ---------------------------
# OAuth token (type=SELF)
# ---------------------------
def ms_timestamp() -> int:
    return int(time.time() * 1000)


def make_client_secret_sign(client_id: str, client_secret: str, timestamp_ms: int) -> str:
    """
    client_secret_sign 생성
    password = "{client_id}_{timestamp_ms}"
    bcrypt.hashpw(password, client_secret(salt))
    base64
    """
    password = f"{client_id}_{timestamp_ms}".encode("utf-8")
    salt = client_secret.encode("utf-8")
    hashed = bcrypt.hashpw(password, salt)
    return pybase64.standard_b64encode(hashed).decode("utf-8")


def load_cached_token() -> Optional[dict]:
    if not os.path.exists(TOKEN_CACHE_FILE):
        return None
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_cached_token(payload: dict) -> None:
    try:
        with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def is_token_valid(payload: dict, safety_margin_sec: int = 60) -> bool:
    if not payload:
        return False
    token = payload.get("access_token")
    expires_at = payload.get("expires_at")
    if not token or not expires_at:
        return False
    return (int(expires_at) - int(time.time())) > safety_margin_sec


def issue_token_self(client_id: str, client_secret: str) -> dict:
    ts = ms_timestamp()
    sign = make_client_secret_sign(client_id, client_secret, ts)

    data = {
        "client_id": client_id,
        "timestamp": str(ts),
        "client_secret_sign": sign,
        "grant_type": "client_credentials",
        "type": "SELF",
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[TOKEN ERROR] {r.status_code} {r.text}")

    payload = r.json()
    expires_in = int(payload.get("expires_in", 0) or 0)
    payload["expires_at"] = int(time.time()) + max(expires_in, 0)
    return payload


def get_access_token(client_id: str, client_secret: str, force_refresh: bool = False) -> str:
    if not force_refresh:
        cached = load_cached_token()
        if is_token_valid(cached):
            return cached["access_token"]

    payload = issue_token_self(client_id, client_secret)
    save_cached_token(payload)
    return payload["access_token"]


# ---------------------------
# Orders API
# ---------------------------
def http_get_json(url: str, headers: dict, params: dict) -> dict:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[API ERROR] {r.status_code} {r.text}")
    return r.json()


def iter_product_orders(
    access_token: str,
    from_iso: str,
    to_iso: str,
    statuses: list[str],
    page_size: int = 300,
):
    """
    GET /v1/pay-order/seller/product-orders
    - rangeType=PAYED_DATETIME (결제일시 기준)
    - pagination: data.pagination.hasNext 기반 순회
    """
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    page = 1

    while True:
        params = {
            "from": from_iso,
            "to": to_iso,
            "rangeType": "PAYED_DATETIME",
            "page": page,
            "size": page_size,
        }

        # ✅ statuses가 비어있으면 필터 파라미터를 아예 보내지 않음(=오늘 범위에 잡히는 주문 전부)
        if statuses:
            params["productOrderStatuses"] = ",".join(statuses)

        resp = http_get_json(PRODUCT_ORDERS_URL, headers=headers, params=params)

        data = resp.get("data") or {}
        contents = data.get("contents") or []
        pagination = data.get("pagination") or {}
        has_next = bool(pagination.get("hasNext"))

        for row in contents:
            yield row

        if not has_next:
            break
        page += 1


def safe_int(v) -> int:
    if v is None:
        return 0
    if isinstance(v, bool):
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return int(s)
    return 0


# ---------------------------
# Public API (for runner)
# ---------------------------
def get_daily_metrics(target_date: datetime.date, force_token: bool = False, raw: bool = False) -> Dict[str, Any]:
    """
    Returns a dict that runner can consume:
      {
        "status": "ok",
        "source": "naver",
        "date": "YYYY-MM-DD",
        "sales": int,
        "orders": int,              # 유니크 orderId 기준 (구매수)
        "product_order_count": int, # 라인아이템 기준
        "status_counter": {...},
      }
    """
    load_dotenv()

    client_id = (os.getenv("NAVER_COMMERCE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("NAVER_COMMERCE_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise ValueError("필수 .env: NAVER_COMMERCE_CLIENT_ID, NAVER_COMMERCE_CLIENT_SECRET")

    from_iso, to_iso = kst_day_range(target_date)

    # ✅ 변경: 상태 필터 제거 => 오늘 범위에 잡히는 주문을 전부 합산
    statuses: list[str] = []

    access_token = get_access_token(client_id, client_secret, force_refresh=force_token)

    order_ids = set()
    product_order_count = 0
    status_counter = Counter()

    # ✅ 매출 정의(기존 유지): initialProductAmount - initialProductDiscountAmount
    sales_amount = 0

    sample_printed = 0

    for row in iter_product_orders(access_token, from_iso, to_iso, statuses=statuses, page_size=300):
        content = row.get("content") or {}
        order = content.get("order") or {}
        product_order = content.get("productOrder") or {}

        order_id = order.get("orderId")
        if order_id:
            order_ids.add(str(order_id))

        status = product_order.get("productOrderStatus")
        if status:
            status_counter[str(status)] += 1

        initial_product_amount = safe_int(product_order.get("initialProductAmount"))
        initial_product_discount_amount = safe_int(product_order.get("initialProductDiscountAmount"))

        sales_amount += (initial_product_amount - initial_product_discount_amount)
        product_order_count += 1

        if raw and sample_printed < 3:
            print("\n--- SAMPLE ROW ---")
            print(json.dumps(row, ensure_ascii=False, indent=2)[:4000])
            sample_printed += 1

    return {
        "status": "ok",
        "source": "naver",
        "date": target_date.strftime("%Y-%m-%d"),
        "sales": int(sales_amount),
        "orders": int(len(order_ids)),  # 유니크 orderId 기준
        "product_order_count": int(product_order_count),
        "status_counter": dict(status_counter) if status_counter else {},
        "from": from_iso,
        "to": to_iso,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Naver SmartStore: daily sales + order count (매출=상품가-상품할인)"
    )
    parser.add_argument("--date", help="집계 날짜 (YYYY-MM-DD). 기본: 오늘(KST)", default=None)
    parser.add_argument("--force-token", action="store_true", help="토큰 강제 재발급(캐시 무시)")
    parser.add_argument("--raw", action="store_true", help="원본 row 샘플(3개) 출력(구조 확인용)")
    parser.add_argument(
        "--json",
        action="store_true",
        help="러너용: 마지막 줄에 JSON 1줄로 결과 출력",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        # ✅ 기본값: 오늘(KST)
        target_date = now_kst().date()

    result = get_daily_metrics(target_date=target_date, force_token=args.force_token, raw=args.raw)

    if args.json:
        print(json.dumps(result, ensure_ascii=False))
        return

    print("\n==============================")
    print(f"날짜(KST): {result['date']}")
    print(f"조회범위:  {result['from']}  ~  {result['to']}")
    print(f"구매수(유니크 orderId): {result['orders']:,}")
    print(f"상품주문건수(라인아이템): {result['product_order_count']:,}")
    print(f"매출(상품가-상품할인, KRW): {result['sales']:,}")
    if result.get("status_counter"):
        print(f"상태별 건수: {result['status_counter']}")
    print("==============================\n")


if __name__ == "__main__":
    main()
