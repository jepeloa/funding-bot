import urllib.request, json

url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
data = json.loads(urllib.request.urlopen(url).read())
symbols = [
    s["symbol"]
    for s in data["symbols"]
    if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
]
print(f"Total pares USDT-M activos: {len(symbols)}")
print(f"Primeros 20: {symbols[:20]}")
print(f"Últimos 10: {symbols[-10:]}")
