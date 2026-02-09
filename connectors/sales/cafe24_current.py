# cafe24_current.py
import os
import re
import time
import json
import argparse
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

load_dotenv()

KST = timezone(timedelta(hours=9))
TIMEOUT = 20_000  # 빠르게


# ---------------------------
# Helpers
# ---------------------------
def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"{key} 환경변수가 필요합니다. .env를 확인하세요.")
    return v


def must_env_profile(profile: str, suffix: str) -> str:
    p = profile.strip().upper()
    return must_env(f"CAFE24_{p}_{suffix}")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def save_debug(page, prefix: str = "fail") -> None:
    os.makedirs("debug", exist_ok=True)
    page.screenshot(path=f"debug/{prefix}.png", full_page=True)
    with open(f"debug/{prefix}.html", "w", encoding="utf-8") as f:
        f.write(page.content())


def parse_two_numbers(raw: str) -> tuple[int, int]:
    """
    '1,234,000원 56건' / '1,234,000 56' / '...'
    -> 첫 숫자=매출, 두번째 숫자=주문수
    """
    raw = normalize_text(raw)
    nums = re.findall(r"\d[\d,]*", raw)
    if len(nums) < 2:
        raise ValueError(f"텍스트에서 숫자 2개(매출/주문수)를 파싱하지 못했습니다: {raw}")
    sales = int(nums[0].replace(",", ""))
    orders = int(nums[1].replace(",", ""))
    return sales, orders


# ---------------------------
# Cafe24 flow
# ---------------------------
def login_cafe24(page, profile: str) -> None:
    url = must_env_profile(profile, "ADMIN_URL")
    user = must_env_profile(profile, "ADMIN_ID")
    pw = must_env_profile(profile, "ADMIN_PW")

    # ✅ 로그인 페이지는 충분히 로드되게(최대 5초 정도) 보수적으로 대기
    page.goto(url, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("load", timeout=5_000)
    except PwTimeoutError:
        pass
    time.sleep(1.0)  # 잔여 렌더/스크립트 안정화

    scopes = [page] + list(page.frames)

    # ✅ 로그인 폼이 실제로 나타날 때까지 최대 5초 폴링
    deadline = time.time() + 5.0
    submitted = False
    last_err = None

    while time.time() < deadline and not submitted:
        for scope in scopes:
            try:
                id_loc = scope.locator(
                    "input[name='id'], input#id, input[name='mall_id'], input[type='text'], "
                    "input[placeholder*='아이디'], input[placeholder*='ID']"
                )
                pw_loc = scope.locator(
                    "input[name='passwd'], input#passwd, input[name='password'], input[type='password'], "
                    "input[placeholder*='비밀번호'], input[placeholder*='Password']"
                )

                if id_loc.count() == 0 or pw_loc.count() == 0:
                    continue

                # 입력 가능 상태까지 짧게 대기
                try:
                    id_loc.first.wait_for(state="visible", timeout=1_000)
                    pw_loc.first.wait_for(state="visible", timeout=1_000)
                except Exception:
                    continue

                id_loc.first.fill(user)
                pw_loc.first.fill(pw)

                btn = scope.locator(
                    "button:has-text('로그인'), [role='button']:has-text('로그인'), input[value*='로그인']"
                )
                if btn.count() > 0:
                    btn.first.click()
                    submitted = True
                    break

                pw_loc.first.press("Enter")
                submitted = True
                break

            except Exception as e:
                last_err = e
                continue

        if not submitted:
            time.sleep(0.25)

    if not submitted:
        save_debug(page, f"{profile}_login_form_not_found")
        raise RuntimeError(
            f"[{profile}] 로그인 폼/버튼을 찾지 못했습니다. "
            f"debug/{profile}_login_form_not_found.* 확인 (last_err={last_err})"
        )

    time.sleep(4)
    # ✅ 로그인 후: 다음 페이지로 실제 넘어갔는지 확인 (최대 10초)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PwTimeoutError:
        pass

    # 로그인 폼이 여전히 보이면 실패 가능성이 높음 → 디버그 저장 후 에러
    try:
        still_id = page.locator("input[name='id'], input#id, input[name='mall_id']").first
        if still_id.count() > 0 and still_id.is_visible():
            save_debug(page, f"{profile}_login_failed_still_on_login")
            raise RuntimeError(
                f"[{profile}] 로그인 후에도 로그인 화면에 머무르는 것으로 보입니다. "
                f"debug/{profile}_login_failed_still_on_login.* 확인"
            )
    except Exception:
        pass


def wait_after_login(page, profile: str) -> None:
    # networkidle은 느려질 수 있어서 최소 대기만
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PwTimeoutError:
        pass

    wait_ms_key = f"CAFE24_{profile.strip().upper()}_POST_LOGIN_WAIT_MS"
    wait_ms = int(os.getenv(wait_ms_key, os.getenv("CAFE24_POST_LOGIN_WAIT_MS", "300")))
    if wait_ms > 0:
        time.sleep(wait_ms / 1000)


def get_dashboard_url(profile: str) -> str:
    key = f"CAFE24_{profile.strip().upper()}_DASHBOARD_URL"
    v = os.getenv(key, "").strip()
    if v:
        return v

    if profile.strip().lower() == "brainology":
        return "https://brainology.cafe24.com/disp/admin/shop1/order/DashboardMain"

    raise RuntimeError(f"[{profile}] {key} 를 .env에 추가하세요.")


def scrape_by_total_order_amount_right_cell(page) -> str:
    """
    1순위:
    get_by_role("cell", name="총 주문 금액") 오른쪽 칸(inner_text) 읽기
    + DEBUG 콘솔 출력
    """
    label = page.get_by_role("cell", name="총 주문 금액").first
    label.wait_for(state="visible", timeout=TIMEOUT)

    # role 기반 방식
    try:
        row = label.locator("xpath=ancestor::*[@role='row'][1]")
        cells = row.get_by_role("cell")
        if cells.count() == 0:
            cells = row.get_by_role("gridcell")

        n = cells.count()
        idx = None

        for i in range(n):
            t = normalize_text(cells.nth(i).inner_text())
            if t == "총 주문 금액":
                idx = i
                break

        if idx is None or idx + 1 >= n:
            raise RuntimeError("오른쪽 cell 못 찾음")

        text = normalize_text(cells.nth(idx + 1).inner_text())

        # ✅ 디버그 출력
        print(f"[DEBUG] 총 주문 금액 오른쪽 raw text = '{text}'")

        if text:
            return text

    except Exception:
        pass

    # fallback JS 방식
    try:
        text = page.evaluate(
            """() => {
              const el = Array.from(document.querySelectorAll('td,th,div,[role="cell"]'))
                .find(x => (x.innerText || x.textContent || '').trim() === '총 주문 금액');
              if (!el) return '';
              const sib = el.nextElementSibling;
              return sib ? (sib.innerText || sib.textContent || '') : '';
            }"""
        )

        text = normalize_text(text)

        # ✅ 디버그 출력
        print(f"[DEBUG] 총 주문 금액 오른쪽 raw text (fallback) = '{text}'")

        if text:
            return text

    except Exception:
        pass

    raise RuntimeError("'총 주문 금액' 오른쪽 칸 텍스트를 찾지 못했습니다.")


def scrape_today_header_below_cell_text(page) -> str:
    """
    2순위 fallback: '오늘' columnheader 아래 칸
    """
    header = page.get_by_role("columnheader", name="오늘").first
    header.wait_for(state="visible", timeout=TIMEOUT)

    try:
        header_row = header.locator("xpath=ancestor::*[@role='row'][1]")
        headers = header_row.get_by_role("columnheader")
        cnt = headers.count()

        idx = None
        for i in range(cnt):
            t = normalize_text(headers.nth(i).inner_text())
            if t == "오늘":
                idx = i
                break

        if idx is None:
            raise RuntimeError("role 기반으로 '오늘' column index를 찾지 못했습니다.")

        body_row = header_row.locator("xpath=following-sibling::*[@role='row'][1]")
        cells = body_row.get_by_role("cell")
        if cells.count() == 0:
            cells = body_row.get_by_role("gridcell")

        if cells.count() <= idx:
            raise RuntimeError("role 기반으로 '오늘' 아래 cell을 찾지 못했습니다.")

        text = normalize_text(cells.nth(idx).inner_text())
        if text:
            return text
    except Exception:
        pass

    # table fallback
    try:
        text = header.evaluate(
            """(el) => {
              const th = el.closest('th') || el;
              const tr = th.closest('tr');
              const table = th.closest('table');
              if (!tr || !table) return '';
              const colIndex = (th.cellIndex !== undefined) ? th.cellIndex : Array.from(tr.children).indexOf(th);
              const nextTr = tr.nextElementSibling;
              if (!nextTr) return '';
              const td = nextTr.children[colIndex];
              if (!td) return '';
              return td.innerText || td.textContent || '';
            }"""
        )
        text = normalize_text(text)
        if text:
            return text
    except Exception:
        pass

    raise RuntimeError("'오늘' 컬럼 바로 아래 칸 텍스트를 찾지 못했습니다.")


# ---------------------------
# Public
# ---------------------------
def get_current_metrics(profile: str) -> dict:
    today = datetime.now(KST).date()
    headless = os.getenv("HEADLESS", "false").lower() == "true"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(TIMEOUT)

        try:
            login_cafe24(page, profile=profile)
            wait_after_login(page, profile=profile)

            dashboard_url = get_dashboard_url(profile)

            # ✅ 빠르게: domcontentloaded까지만
            page.goto(dashboard_url, wait_until="domcontentloaded")
            time.sleep(4)

            # ✅ 핵심 요소가 보일 때까지만 기다림
            try:
                page.get_by_role("cell", name="총 주문 금액").first.wait_for(
                    state="visible", timeout=TIMEOUT
                )
            except Exception:
                pass

            if os.getenv("CAFE24_DEBUG", "false").lower() == "true":
                os.makedirs("debug", exist_ok=True)
                page.screenshot(path=f"debug/{profile}_dashboard.png", full_page=True)
                with open(f"debug/{profile}_dashboard.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

            # ✅ 1순위: 총 주문 금액 오른쪽 칸 (+ 디버그 출력이 여기서 발생)
            try:
                raw = scrape_by_total_order_amount_right_cell(page)
            except Exception:
                # 2순위: 오늘 아래칸
                raw = scrape_today_header_below_cell_text(page)

            sales, orders = parse_two_numbers(raw)

            return {
                "status": "ok",
                "date": today.isoformat(),
                "sales": int(sales),
                "orders": int(orders),
                "raw": raw,
                "source": "cafe24",
                "profile": profile,
            }

        except Exception as e:
            save_debug(page, f"{profile}_fail")
            raise RuntimeError(
                f"[{profile}] 실패: {e} (debug/{profile}_fail.png, debug/{profile}_fail.html 저장됨)"
            ) from e
        finally:
            context.close()
            browser.close()


# ---------------------------
# CLI  ✅ 여기만 최소 수정
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="brainology", choices=["brainology", "burdenzero"])
    ap.add_argument("--all", action="store_true", help="brainology + burdenzero 둘 다 조회")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    # ✅ 변경: --all이면 둘 다 조회해서 mapped로 출력 (마지막 줄 JSON 1줄)
    if args.all:
        r_bio = get_current_metrics(profile="brainology")
        r_bz = get_current_metrics(profile="burdenzero")

        out = {
            "status": "ok",
            "source": "cafe24",
            "date": r_bio.get("date") or r_bz.get("date"),
            "mapped": {
                "brainology": {"sales": int(r_bio.get("sales", 0)), "orders": int(r_bio.get("orders", 0))},
                "burdenzero": {"sales": int(r_bz.get("sales", 0)), "orders": int(r_bz.get("orders", 0))},
            },
        }

        # 기존처럼 사람이 볼 때도 OK
        print(json.dumps(out, ensure_ascii=False) if args.json else out)
        return

    # ✅ 기존 동작 그대로: 단일 프로필 조회
    result = get_current_metrics(profile=args.profile)
    print(json.dumps(result, ensure_ascii=False) if args.json else result)


if __name__ == "__main__":
    main()
