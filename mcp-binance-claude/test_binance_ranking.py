import asyncio
import json
from playwright.async_api import async_playwright

async def intercept_ranking_api():
    """Intercept API calls from the copy-trading ranking/list page."""

    captured = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="es-AR"
        )
        page = await context.new_page()

        async def handle_response(response):
            url = response.url
            if "copy-trade" in url or "copy_trade" in url or "copyTrade" in url:
                try:
                    body = await response.text()
                    captured.append({"url": url, "status": response.status, "body": body[:5000]})
                    print(f"[CAPTURED] {response.status} {url[:150]}")
                except Exception as e:
                    print(f"[ERROR] {url[:150]} -> {e}")

        page.on("response", handle_response)

        print("Navigating to copy-trading ranking page...")
        try:
            await page.goto(
                "https://www.binance.com/es-AR/copy-trading",
                wait_until="networkidle",
                timeout=60000
            )
        except Exception as e:
            print(f"Navigation: {e}")

        await asyncio.sleep(3)

        # Try scrolling to trigger more data loads
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(3)

        await browser.close()

    print(f"\nTotal captured: {len(captured)}")
    for i, r in enumerate(captured):
        print(f"\n--- {i+1} ---")
        print(f"URL: {r['url']}")
        print(f"Status: {r['status']}")
        try:
            data = json.loads(r['body'])
            print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])
        except:
            print(r['body'][:500])

    with open("/home/javier/mcp_data_crypto/binance_ranking_captured.json", "w") as f:
        json.dump(captured, f, indent=2, ensure_ascii=False)

asyncio.run(intercept_ranking_api())
