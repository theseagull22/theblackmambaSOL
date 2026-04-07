# theblackmambaSOL

Isolated SOLUSDT execution contour cloned from the proven ETH contour.

## Included
- `SOL_AO_20m_Swing_Engine_v1_alert-ready.pine` - SOL fork of the current ETH Pine with the same live payload schema and live BE+ logic
- `main_v2_4_bybit_adapter_v0.py` - latest leverage-enabled adapter baseline, namespaced for SOL defaults
- `.env.example` - ready env template for Bybit Demo with qty=100 and leverage=100
- `requirements.txt`
- `render.yaml` - optional convenience file; manual Render Web Service is still the preferred path

## What changed from ETH
- Pine defaults now target `SOLUSDT`
- Pine default `engineTag` is `sol_ao_20m_v1`
- SOL core rules are preloaded as defaults:
  - impulseMinBars = 4
  - firstGreenMin = -0.66
  - firstRedMax = 0.5
  - entryOffset = 0.2
  - stopOffset = 1.21
  - tpDistance = 1.81
  - beTrigger = 1.46
  - beOffset = 0.4
  - pendingLifeHours = 36
- Adapter defaults now fall back to:
  - `DEFAULT_ORDER_QTY=100`
  - `DEFAULT_LEVERAGE=100`
  - `ADAPTER_JOURNAL_PATH=/tmp/bybit_adapter_sol_v0.json`

## Render
Build command:
`pip install -r requirements.txt`

Start command:
`uvicorn main_v2_4_bybit_adapter_v0:app --host 0.0.0.0 --port $PORT`

## Health endpoints
- `/`
- `/state`
- `/actions`
- `/adapter/state`
- `/adapter/actions`
- `/webhook/tradingview` (GET should return Method Not Allowed)

## TradingView alert
- attach the Pine to `SOLUSDT` on `20m`
- create alert on `Any alert() function call`
- webhook URL:
  `https://YOUR-RENDER-SERVICE.onrender.com/webhook/tradingview`
- do not put a manual message body in the alert; Pine emits the JSON itself through `alert()`

## Quick check after deploy
- root endpoint returns `tv_receiver_bybit_adapter_sol_v0`
- `/adapter/state` shows `default_order_qty=100` and `default_leverage=100`
- `/adapter/state` journal path is `/tmp/bybit_adapter_sol_v0.json`
