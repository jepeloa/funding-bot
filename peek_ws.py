"""Quick peek at Binance Futures WS message formats."""
import asyncio
import json
import websockets

STREAMS = [
    "dogeusdt@depth20@100ms",
    "dogeusdt@aggTrade",
    "dogeusdt@bookTicker",
    "dogeusdt@markPrice@1s",
]

async def peek():
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(STREAMS)}"
    seen = set()
    async with websockets.connect(url) as ws:
        while len(seen) < 4:
            raw = await ws.recv()
            msg = json.loads(raw)
            stream = msg.get("stream", "")
            # Identify stream type
            stype = stream.split("@")[1] if "@" in stream else stream
            if stype in seen:
                continue
            seen.add(stype)
            data = msg.get("data", {})
            print(f"=== {stream} ===")
            print(f"Keys: {list(data.keys())}")
            print(json.dumps(data, indent=2)[:600])
            print()

asyncio.run(peek())
