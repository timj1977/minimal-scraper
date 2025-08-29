import asyncio
import csv
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


async def _sleep_ms(ms: int):
    await asyncio.sleep(ms / 1000.0)


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _mk_output_path() -> str:
    _ensure_dir("exports")
    return os.path.join("exports", f"scrape_{_now_ts()}.csv")


async def _safe_click(page, selector: str, timeout_ms: int = 10000):
    """
    Robust click that tries a few strategies:
      - wait_for_selector then click()
      - if that fails, force click via JS
    """
    await page.wait_for_selector(selector, timeout=timeout_ms, state="visible")
    try:
        await page.click(selector, timeout=timeout_ms)
        return
    except Exception:
        # Fallback to JS click (for inputs/links that ignore standard click)
        await page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (el) el.click();
            }""",
            selector,
        )
        # Give the browser a moment to react
        await _sleep_ms(300)


async def _type_and_submit(page, input_selector: str, submit_selector: str, value: str, timeout_ms: int):
    await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")
    await page.fill(input_selector, "")
    await page.type(input_selector, value, delay=20)
    await _safe_click(page, submit_selector, timeout_ms=timeout_ms)


async def _extract_fields(page, selectors: List[Dict[str, Any]]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for sel in selectors:
        name = sel.get("name") or "field"
        css = sel.get("selector")
        stype = sel.get("type", "text")
        attr = sel.get("attr")
        if not css:
            row[name] = None
            continue
        try:
            if stype == "text":
                el = await page.wait_for_selector(css, timeout=5000)
                row[name] = (await el.text_content() or "").strip()
            elif stype == "attr":
                if not attr:
                    row[name] = None
                else:
                    el = await page.wait_for_selector(css, timeout=5000)
                    row[name] = await el.get_attribute(attr)
            elif stype == "html":
                el = await page.wait_for_selector(css, timeout=5000)
                row[name] = await el.inner_html()
            else:
                row[name] = None
        except PlaywrightTimeout:
            row[name] = None
        except Exception:
            row[name] = None
    return row


async def scrape_append_to_csv(
    base_url: str,
    input_list: List[str],
    selectors: List[Dict[str, Any]],
    headless: bool = True,
    delay_ms_min: int = 200,
    delay_ms_max: int = 500,
    timeout_ms: int = 20000,
) -> Tuple[str, int, int, int]:
    """
    Model A: Append part-list to a base URL, scrape resulting pages.
    """
    out_path = _mk_output_path()
    total = ok = err = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        ctx = await browser.new_context()
        page = await ctx.new_page()

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            header = ["input"] + [s.get("name", f"field{i}") for i, s in enumerate(selectors)]
            writer.writerow(header)

            for item in input_list:
                total += 1
                url = f"{base_url}{item}"
                try:
                    await page.goto(url, timeout=timeout_ms)
                    await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                    data = await _extract_fields(page, selectors)
                    writer.writerow([item] + [data.get(s.get("name", "")) for s in selectors])
                    ok += 1
                except Exception:
                    err += 1
                # polite delay
                jitter = delay_ms_min + int((delay_ms_max - delay_ms_min) * 0.5)
                await _sleep_ms(jitter)

        await ctx.close()
        await browser.close()

    return out_path, total, ok, err


async def scrape_search_to_csv(
    start_url: str,
    input_list: List[str],
    selectors: List[Dict[str, Any]],
    search: Dict[str, Any],
    headless: bool = True,
    delay_ms_min: int = 200,
    delay_ms_max: int = 500,
    timeout_ms: int = 20000,
) -> Tuple[str, int, int, int]:
    """
    Model B: Go to a start page, (optionally) click a disclaimer, then
    for each input:
      - type into search input
      - click search
      - optionally click first result link
      - wait for a selector that indicates the detail page is ready
      - extract fields
      - optionally navigate back to search
    """
    out_path = _mk_output_path()
    total = ok = err = 0

    # read search config
    input_selector: str = search.get("input_selector")
    submit_selector: str = search.get("submit_selector")
    results_selector: Optional[str] = search.get("results_selector")
    detail_ready_selector: str = search.get("detail_ready_selector")

    back_to_search_selector: Optional[str] = search.get("back_to_search_selector")

    # NEW: disclaimer support
    disclaimer_selector: Optional[str] = search.get("disclaimer_selector")
    disclaimer_click_each: bool = bool(search.get("disclaimer_click_each", False))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        ctx = await browser.new_context()
        page = await ctx.new_page()

        # Navigate to start (Disclaimer or Search)
        await page.goto(start_url, timeout=timeout_ms)
        await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

        # If a disclaimer exists and we don't need to click it each time,
        # handle it once here and land on the search page.
        if disclaimer_selector and not disclaimer_click_each:
            try:
                await _safe_click(page, disclaimer_selector, timeout_ms=timeout_ms)
                # wait until search input is visible (we are now on the search page)
                await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")
            except Exception:
                # If it fails, continue; next iteration may succeed if already accepted
                pass

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            header = ["input"] + [s.get("name", f"field{i}") for i, s in enumerate(selectors)]
            writer.writerow(header)

            for item in input_list:
                total += 1
                try:
                    # If site forces disclaimer per search, handle it here each time.
                    if disclaimer_selector and disclaimer_click_each:
                        await page.goto(start_url, timeout=timeout_ms)
                        await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                        await _safe_click(page, disclaimer_selector, timeout_ms=timeout_ms)
                        await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")

                    # Ensure we're on the search page (if a previous iteration left us elsewhere)
                    try:
                        await page.wait_for_selector(input_selector, timeout=3000, state="visible")
                    except PlaywrightTimeout:
                        # Try going back once if needed
                        await page.go_back()
                        await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")

                    # Search flow
                    await _type_and_submit(page, input_selector, submit_selector, item, timeout_ms)

                    # If there is an intermediate "results" page, click through to the first result
                    if results_selector:
                        await page.wait_for_selector(results_selector, timeout=timeout_ms, state="visible")
                        await _safe_click(page, results_selector, timeout_ms=timeout_ms)

                    # Wait for detail page to be ready
                    await page.wait_for_selector(detail_ready_selector, timeout=timeout_ms, state="visible")

                    # Scrape fields
                    data = await _extract_fields(page, selectors)
                    writer.writerow([item] + [data.get(s.get("name", "")) for s in selectors])
                    ok += 1

                    # Get back to the search page for the next item
                    if back_to_search_selector:
                        # If the site has a specific "Back to Search" link/button
                        await _safe_click(page, back_to_search_selector, timeout_ms=timeout_ms)
                        await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")
                    else:
                        # Otherwise rely on back stack
                        await page.go_back()
                        # some flows may need two backs (detail -> results -> search)
                        try:
                            await page.wait_for_selector(input_selector, timeout=3000, state="visible")
                        except PlaywrightTimeout:
                            await page.go_back()
                            await page.wait_for_selector(input_selector, timeout=timeout_ms, state="visible")

                except Exception:
                    err += 1

                # polite jitter
                jitter = delay_ms_min + int((delay_ms_max - delay_ms_min) * 0.5)
                await _sleep_ms(jitter)

        await ctx.close()
        await browser.close()

    return out_path, total, ok, err
