"""Test: can we call the positions API directly via fetch() from a warmed-up browser page?"""
import asyncio
import json
from playwright.async_api import async_playwright

async def test_direct_fetch():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        # Warm up - visit copy-trading page
        print("Warming up browser session...")
        try:
            await page.goto("https://www.binance.com/es-AR/copy-trading", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)
        except:
            pass

        # Now try direct fetch() for positions of multiple traders
        test_ids = [
            "4878630112238695169",  # Ah Hui'er
            "4939169115437319169",  # 久保田
            "4913337547723615233",  # KINGSSSSS
        ]

        print("\nTest 1: Direct fetch for positions...")
        for pid in test_ids:
            result = await page.evaluate(f"""
                async () => {{
                    const r = await fetch(
                        'https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId={pid}',
                        {{ credentials: 'include' }}
                    );
                    return {{ status: r.status, text: await r.text() }};
                }}
            """)
            status = result['status']
            if status == 200:
                data = json.loads(result['text'])
                positions = data.get('data', [])
                active = [p for p in positions if float(p.get('positionAmount', '0')) != 0]
                print(f"  {pid}: {status} OK - {len(positions)} total, {len(active)} active positions")
                for pos in active[:3]:
                    print(f"    {pos['symbol']} {pos['positionSide']} amt={pos['positionAmount']} lev={pos['leverage']}x entry={pos['entryPrice']} pnl={pos['unrealizedProfit']}")
            else:
                print(f"  {pid}: {status} FAILED")

        # Test 2: Can we also fetch the detail endpoint via direct fetch?
        print("\nTest 2: Direct fetch for trader detail...")
        for pid in test_ids[:1]:
            result = await page.evaluate(f"""
                async () => {{
                    const r = await fetch(
                        'https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail?portfolioId={pid}',
                        {{ credentials: 'include' }}
                    );
                    return {{ status: r.status, text: await r.text() }};
                }}
            """)
            status = result['status']
            if status == 200:
                data = json.loads(result['text'])
                d = data.get('data', {})
                print(f"  {pid}: {status} OK - {d.get('nickname')} AUM={d.get('aumAmount')}")
            else:
                print(f"  {pid}: {status} FAILED")

        # Test 3: Can we fetch query-list via POST?
        print("\nTest 3: Direct POST to query-list...")
        result = await page.evaluate("""
            async () => {
                const r = await fetch(
                    'https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list',
                    {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({
                            pageNumber: 1,
                            pageSize: 5,
                            timeRange: '30D',
                            dataType: 'AUM',
                            favoriteOnly: false,
                            hideFull: false,
                            nickname: '',
                            order: 'DESC',
                            userAsset: 0,
                            portfolioType: 'ALL',
                            useAiRecommended: false
                        })
                    }
                );
                return { status: r.status, text: await r.text() };
            }
        """)
        if result['status'] == 200:
            data = json.loads(result['text'])
            total = data['data']['total']
            traders = data['data']['list']
            print(f"  query-list: {result['status']} OK - {total} total")
            for t in traders:
                print(f"    {t['nickname']} AUM={t.get('aum')}")

        # Test 4: Parallel fetch performance
        print("\nTest 4: Batch parallel fetch (10 traders positions)...")
        import time
        # First get 10 trader IDs
        result = await page.evaluate("""
            async () => {
                const r = await fetch(
                    'https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list',
                    {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({
                            pageNumber: 1, pageSize: 10, timeRange: '30D',
                            dataType: 'AUM', order: 'DESC',
                            favoriteOnly: false, hideFull: false, nickname: '',
                            userAsset: 0, portfolioType: 'ALL', useAiRecommended: false
                        })
                    }
                );
                return await r.json();
            }
        """)
        ids = [t['leadPortfolioId'] for t in result['data']['list']]
        aums = {t['leadPortfolioId']: t.get('aum', 0) for t in result['data']['list']}
        nicks = {t['leadPortfolioId']: t.get('nickname', '') for t in result['data']['list']}

        t0 = time.time()
        # Fetch all 10 positions in parallel using Promise.allSettled
        ids_json = json.dumps(ids)
        batch_result = await page.evaluate(f"""
            async () => {{
                const ids = {ids_json};
                const promises = ids.map(id =>
                    fetch(
                        `https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=${{id}}`,
                        {{ credentials: 'include' }}
                    ).then(r => r.json()).then(d => ({{ id, data: d }}))
                );
                return await Promise.allSettled(promises);
            }}
        """)
        elapsed = time.time() - t0
        print(f"  Fetched 10 traders in {elapsed:.1f}s")

        for item in batch_result:
            if item['status'] == 'fulfilled':
                val = item['value']
                pid = val['id']
                positions = val['data'].get('data', [])
                active = [p for p in positions if float(p.get('positionAmount', '0')) != 0]
                if active:
                    print(f"  {nicks.get(pid, pid)} (AUM ${aums.get(pid, 0):,.0f}): {len(active)} active")
                    for pos in active:
                        print(f"    {pos['symbol']} {pos['positionSide']} amt={pos['positionAmount']} lev={pos['leverage']}x")

        await browser.close()

asyncio.run(test_direct_fetch())
