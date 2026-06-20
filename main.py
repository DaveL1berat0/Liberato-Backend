async def twelvedata_websocket_listener():
    if not TWELVEDATA_KEY:
        cache["health"]["twelvedata"] = {"status": "offline", "reason": "Falta TWELVEDATA_KEY"}
        return

    # Escuchamos todos los activos del heatmap y el NQ en el mismo WebSocket
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    symbols_to_subscribe = "QQQ,AAPL,MSFT,NVDA"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_msg = {
                    "action": "subscribe", 
                    "params": {"symbols": symbols_to_subscribe}
                }
                await websocket.send(json.dumps(subscribe_msg))
                cache["health"]["twelvedata"] = {"status": "online", "reason": "Conectado y escuchando ticks en vivo"}
                cache["health"]["yahoo"] = {"status": "online", "reason": "Sustituido por TwelveData Realtime"}
                
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("event") == "price":
                        sym = data.get("symbol")
                        price_val = float(data.get("price"))
                        
                        # Guardar precio del NQ
                        if sym == "QQQ":
                            _LIVE_PRICES["NQ"] = price_val * 41.2
                            
                        # Llenar el Heatmap en tiempo real directamente aquí
                        cache["heatmap"]["data"][sym] = {
                            "price": price_val,
                            "chg_pct": data.get("change_percent", 0.0) # TwelveData envía el % de cambio directo
                        }
                        
        except Exception as e:
            cache["health"]["twelvedata"] = {"status": "error", "reason": f"WebSocket: {str(e)}"}
            await asyncio.sleep(15)