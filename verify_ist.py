import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.goto("http://localhost:5051")
        await page.wait_for_selector("#mainChart")

        # Hover over the chart to see crosshair label
        await page.mouse.move(500, 500)
        await page.wait_for_timeout(2000)

        await page.screenshot(path="verification/ist_check.png")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
