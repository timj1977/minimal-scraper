# app/runner.py
from __future__ import annotations

import asyncio
import csv
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

EXPORTS_DIR = Path("./exports")


def _ensure_exports() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _csv_path_for(run_id: str) -> Path:
    _ensure_exports()
    return EXPORTS_DIR / f"{run_id}.csv"


async def _extract_fields(page, selectors: List[Dict[str, Any]]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for sel in selectors:
        name = sel["name"]
        stype = sel.get("type", "text")
        css = sel["selector"]
        try:
            loc = page.locator(css).first
            if stype == "text":
                txt = await loc.text_content()
                row[name] = txt.strip() if txt else None
            elif stype == "attr":
                attr = sel.get("attr")
                row[name] = await loc.get_attribute(attr) if attr else None
            else:
                row[name] = None
        except Exception:
            row[name] = None
    return row


async def _write_csv_header_once(path: Path, fieldnames: List[str]) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames + ["error"])
            writer.writeheader()


async def _append_csv_row(path: Path, fieldnames: List[str], data: Dict[str, Any], error: Optional[str] = None) -> None:
    row = {k: None for k in fieldnames}
    row.update(data)
    row["error"] = error
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames + ["error"])
        writer.writerow(row)


# -------------------------
# Mode A: APPEND
# -------------------------
async def scrape_append_to_csv(
    *,
    base_url: str,
    input_list: List[str],
    selectors: List[Dict[str, Any]],
    headless: bool,
    delay_ms_min: int,
    delay_ms_max: int,
    timeout_ms: int,
    run_id: str,
) -> Tuple[str, int, int, int]:
    output_path = str(_csv_path_for(run_id))
    fieldnames = [s["name"] for s in selectors] + ["source_url", "ts"]

    _ensure_exports()
    await _write_csv_header_once(Path(output_path), fieldnames)

    total = ok = err = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()
        try:
            for v in input_list:
                total += 1
                url = f"{base_url}{v}"
                page = await context.new_page()
                try:
                    await page.goto(url, timeout=timeout_ms)
                    await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

                    data = await _extract_fields(page, selectors)
                    data["source_url"] = url
                    data["ts"] = int(time.time())

                    await _append_csv_row(Path(output_path), fieldnames, data, None)
                    ok += 1
                except (PWTimeout, Exception) as e:
                    logger.exception(f"append: error on {url}: {e}")
                    data = {"source_url": url, "ts": int(time.time())}
                    await _append_csv_row(Path(output_path), fieldnames, data, f"{type(e).__name__}: {e}")
                    err += 1
                finally:
                    await asyncio.sleep(random.uniform(delay_ms_min, delay_ms_max) / 1000.0)
                    await page.close()
        finally:
            await context.close()
            await browser.close()

    return (output_path, total, ok, err)


# -------------------------------------------------------------
# Mode B: SEARCH → (optional results) → DETAIL → back
# -------------------------------------------------------------
async def scrape_search_to_csv(
    *,
    start_url: str,
    input_list: List[str],
    selectors: List[Dict[str, Any]],
    input_selector: str,
    submit_selector: Optional[str],
    results_selector: Optional[str],
    detail_ready_selector: Optional[str],
    back_to_search_selector: Optional[str],
    headless: bool,
    delay_ms_min: int,
    delay_ms_max: int,
    timeout_ms: int,
    run_id: str,
) -> Tuple[str, int, int, int]:

    output_path = str(_csv_path_for(run_id))
    fieldnames = [s["name"] for s in selectors] + ["source_url", "ts"]

    _ensure_exports()
    await _write_csv_header_once(Path(output_path), fieldnames)

    total = ok = err = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            for value in input_list:
                total += 1
                try:
                    # 1) Go to start/search page
                    await page.goto(start_url, timeout=timeout_ms)
                    await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

                    # 2) Fill search box
                    await page.locator(input_selector).first.wait_for(state="visible", timeout=timeout_ms)
                    await page.locator(input_selector).first.fill(value, timeout=timeout_ms)

                    # 3) Submit
                    if submit_selector:
                        await page.locator(submit_selector).first.click(timeout=timeout_ms)
                    else:
                        await page.locator(input_selector).first.press("Enter", timeout=timeout_ms)

                    # 4) Click first result if provided
                    if results_selector:
                        await page.locator(results_selector).first.wait_for(state="visible", timeout=timeout_ms)
                        await asyncio.sleep(random.uniform(0.15, 0.35))
                        await page.locator(results_selector).first.click(timeout=timeout_ms)

                    # 5) Wait for detail page content
                    if detail_ready_selector:
                        await page.locator(detail_ready_selector).first.wait_for(state="visible", timeout=timeout_ms)
                    else:
                        await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

                    # 6) Extract fields
                    data = await _extract_fields(page, selectors)
                    data["source_url"] = page.url
                    data["ts"] = int(time.time())
                    await _append_csv_row(Path(output_path), fieldnames, data, None)
                    ok += 1

                except (PWTimeout, Exception) as e:
                    logger.exception(f"search: error on value={value}: {e}")
                    data = {"source_url": page.url if page.url else start_url, "ts": int(time.time())}
                    await _append_csv_row(Path(output_path), fieldnames, data, f"{type(e).__name__}: {e}")
                    err += 1

                finally:
                    # 7) Try to return to search page for next input
                    try:
                        if back_to_search_selector:
                            await asyncio.sleep(random.uniform(0.15, 0.35))
                            await page.locator(back_to_search_selector).first.click(timeout=timeout_ms)
                            await page.locator(input_selector).first.wait_for(state="visible", timeout=timeout_ms)
                        else:
                            await page.go_back(timeout=timeout_ms)
                            # sometimes two steps: results -> search
                            try:
                                await page.locator(input_selector).first.wait_for(state="visible", timeout=1500)
                            except Exception:
                                await page.go_back(timeout=timeout_ms)
                                await page.locator(input_selector).first.wait_for(state="visible", timeout=timeout_ms)
                    except Exception:
                        # If we can't get back cleanly, just proceed; next loop loads start_url again.
                        pass

                    await asyncio.sleep(random.uniform(delay_ms_min, delay_ms_max) / 1000.0)

        finally:
            await page.close()
            await context.close()
            await browser.close()

    return (output_path, total, ok, err)
