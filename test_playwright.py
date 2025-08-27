import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as pw:
        b = await pw.chromium.launch(headless=True)
        c = await b.new_context()
        p = await c.new_page()
        await p.goto("https://example.com", timeout=20000)
        print("OK:", await p.title())
        await c.close()
        await b.close()

asyncio.run(main())
