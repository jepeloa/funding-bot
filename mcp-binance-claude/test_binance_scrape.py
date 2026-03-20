import asyncio
import json
from playwright.async_api import async_playwright

async def intercept_binance_api():
    """Intercept API calls that the Binance copy-trading page makes."""

    captured_responses = []

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
                    captured_responses.append({
                        "url": url,
                        "status": response.status,
                        "body": body[:2000]  # first 2000 chars
                    })
                    print(f"[CAPTURED] {response.status} {url[:120]}")
                except Exception as e:
                    captured_responses.append({
                        "url": url,
                        "status": response.status,
                        "error": str(e)
                    })
                    print(f"[ERROR] {url[:120]} -> {e}")

        page.on("response", handle_response)

        print("Navigating to Binance copy-trading page...")
        try:
            await page.goto(
                "https://www.binance.com/es-AR/copy-trading/lead-details/4878630112238695169?timeRange=7D",
                wait_until="networkidle",
                timeout=60000
            )
        except Exception as e:
            print(f"Navigation finished with: {e}")

        # Wait a bit for any lazy-loaded API calls
        await asyncio.sleep(5)

        # Also grab the page content
        content = await page.content()
        title = await page.title()
        print(f"\nPage title: {title}")
        print(f"Page HTML length: {len(content)}")

        await browser.close()

    print(f"\n{'='*80}")
    print(f"Total captured API responses: {len(captured_responses)}")
    print(f"{'='*80}\n")

    for i, resp in enumerate(captured_responses):
        print(f"\n--- Response {i+1} ---")
        print(f"URL: {resp['url']}")
        print(f"Status: {resp['status']}")
        if 'body' in resp:
            # Try to pretty-print JSON
            try:
                data = json.loads(resp['body'])
                print(f"Body (JSON): {json.dumps(data, indent=2, ensure_ascii=False)[:3000]}")
            except:
                print(f"Body: {resp['body'][:500]}")
        if 'error' in resp:
            print(f"Error: {resp['error']}")

    # Save all to file
    with open("/home/javier/mcp_data_crypto/binance_captured.json", "w") as f:
        json.dump(captured_responses, f, indent=2, ensure_ascii=False)
    print(f"\nAll responses saved to binance_captured.json")

asyncio.run(intercept_binance_api())
