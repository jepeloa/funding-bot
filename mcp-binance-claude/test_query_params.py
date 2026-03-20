import asyncio
import json
from playwright.async_api import async_playwright

async def test_query_list_params():
    """Test query-list with different sort/filter parameters by intercepting request payloads."""

    requests_captured = []
    responses_captured = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="es-AR"
        )
        page = await context.new_page()

        async def handle_request(request):
            url = request.url
            if "query-list" in url or "daily-picks" in url:
                try:
                    post_data = request.post_data
                    requests_captured.append({"url": url, "method": request.method, "post_data": post_data})
                    print(f"[REQ] {request.method} {url[:100]}")
                    if post_data:
                        print(f"  POST body: {post_data}")
                except:
                    pass

        async def handle_response(response):
            url = response.url
            if "query-list" in url:
                try:
                    body = await response.text()
                    data = json.loads(body)
                    # Just show first item to understand schema fully
                    if data.get("data", {}).get("list"):
                        first = data["data"]["list"][0]
                        responses_captured.append(first)
                        print(f"\n[RESP] Total traders: {data['data']['total']}")
                        print(f"First trader keys: {list(first.keys())}")
                        print(json.dumps(first, indent=2, ensure_ascii=False)[:3000])
                except Exception as e:
                    print(f"Error: {e}")

        page.on("request", handle_request)
        page.on("response", handle_response)

        print("=== Loading main page ===")
        try:
            await page.goto("https://www.binance.com/es-AR/copy-trading", wait_until="networkidle", timeout=60000)
        except:
            pass
        await asyncio.sleep(2)

        # Try to find and click sort/filter buttons to see different request params
        # Look for ROI, PnL sorting options
        print("\n=== Trying to interact with filters ===")
        try:
            # Click on different tabs or sorting options if available
            buttons = await page.query_selector_all('button, [role="tab"], [data-bn-type="tab"]')
            for btn in buttons[:10]:
                text = await btn.inner_text()
                if text:
                    print(f"  Found button: '{text.strip()[:50]}'")
        except Exception as e:
            print(f"  Error finding buttons: {e}")

        await browser.close()

    print(f"\n{'='*60}")
    print(f"Captured {len(requests_captured)} requests")
    for r in requests_captured:
        print(f"\n{r['method']} {r['url']}")
        if r['post_data']:
            try:
                print(json.dumps(json.loads(r['post_data']), indent=2))
            except:
                print(r['post_data'])

    with open("/home/javier/mcp_data_crypto/binance_query_params.json", "w") as f:
        json.dump({"requests": requests_captured, "first_trader_schema": responses_captured}, f, indent=2, ensure_ascii=False)

asyncio.run(test_query_list_params())
