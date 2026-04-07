from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn


# -----------------------------
# TradingView models
# -----------------------------
class TVEvent(BaseModel):
    event_index: int = 0
    event: str
    engine: Optional[str] = None
    symbol: Optional[str] = None
    tf: Optional[str] = None
    bar_index: Optional[int] = None
    ts: Optional[int] = None
    id: Optional[str] = None
    side: Optional[str] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    expiry_bar: Optional[int] = None
    stage: Optional[str] = None
    reason: Optional[str] = None


class TVBatch(BaseModel):
    schema_version: Optional[str] = None
    batch_id: str
    engine: str
    symbol: str
    tf: str
    bar_index: int
    ts: int
    live_mode: Optional[bool] = None
    position_state: Optional[str] = None
    position_id: Optional[str] = None
    position_entry: Optional[float] = None
    position_stop: Optional[float] = None
    position_tp: Optional[float] = None
    position_moved_to_be: Optional[bool] = None
    live_pending_count: Optional[int] = None
    shadow_pending_count: Optional[int] = None
    pending_longs: Optional[int] = None
    pending_shorts: Optional[int] = None
    events_count: Optional[int] = None
    events: List[TVEvent] = Field(default_factory=list)


# -----------------------------
# Common helpers
# -----------------------------
MAX_ACTIONS = 500
MAX_BATCH_IDS = 1000
KEEP_SHADOW_REASONS = {"filled_other_order_hold_shadow"}
PLACEHOLDER_PREFIX = "__placeholder__"


STATE: Dict[str, Any] = {}
ACTIONS: List[Dict[str, Any]] = []
SEEN_BATCH_IDS: List[str] = []
SEEN_BATCH_LOOKUP = set()


app = FastAPI(title="TV Receiver + Bybit Demo Adapter SOL v0")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_json_loads(raw: str, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def float_to_str(value: Optional[float], fallback: Optional[str] = None) -> Optional[str]:
    if value is None:
        return fallback
    return format(Decimal(str(value)).normalize(), "f")


# -----------------------------
# Receiver state / actions
# -----------------------------
def reset_state(clear_actions: bool = True) -> None:
    global STATE, ACTIONS, SEEN_BATCH_IDS, SEEN_BATCH_LOOKUP
    STATE = {
        "last_batch": None,
        "last_received_at": None,
        "position": {
            "state": "unknown",
            "id": None,
            "entry": None,
            "stop": None,
            "tp": None,
            "moved_to_be": False,
            "side": None,
        },
        "live_pending": {},
        "shadow_pending": {},
        "meta": {
            "schema_version": None,
            "engine": None,
            "symbol": None,
            "tf": None,
            "bar_index": None,
            "ts": None,
            "live_mode": None,
            "events_count": 0,
            "live_pending_count": 0,
            "shadow_pending_count": 0,
            "pending_longs": 0,
            "pending_shorts": 0,
            "tracked_live_pending_count": 0,
            "tracked_shadow_pending_count": 0,
            "tracked_pending_longs": 0,
            "tracked_pending_shorts": 0,
            "explicit_live_pending_count": 0,
            "explicit_shadow_pending_count": 0,
            "explicit_pending_longs": 0,
            "explicit_pending_shorts": 0,
            "placeholder_live_pending_count": 0,
            "placeholder_shadow_pending_count": 0,
            "placeholder_pending_longs": 0,
            "placeholder_pending_shorts": 0,
            "counts_aligned": True,
            "recent_batch_ids": [],
        },
    }
    if clear_actions:
        ACTIONS = []
    SEEN_BATCH_IDS = []
    SEEN_BATCH_LOOKUP = set()


reset_state(clear_actions=True)


def log_action(kind: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ACTIONS.append(
        {
            "at": utc_now_iso(),
            "kind": kind,
            "message": message,
            "payload": payload or {},
        }
    )
    if len(ACTIONS) > MAX_ACTIONS:
        del ACTIONS[:-MAX_ACTIONS]


# -----------------------------
# Receiver helpers
# -----------------------------
def add_seen_batch(batch_id: str) -> None:
    if batch_id in SEEN_BATCH_LOOKUP:
        return
    SEEN_BATCH_IDS.append(batch_id)
    SEEN_BATCH_LOOKUP.add(batch_id)
    if len(SEEN_BATCH_IDS) > MAX_BATCH_IDS:
        old = SEEN_BATCH_IDS.pop(0)
        SEEN_BATCH_LOOKUP.discard(old)



def event_to_dict(event: TVEvent) -> Dict[str, Any]:
    return event.model_dump()



def pending_snapshot(event: TVEvent) -> Dict[str, Any]:
    return {
        "id": event.id,
        "side": event.side,
        "entry": event.entry,
        "sl": event.sl,
        "tp": event.tp,
        "expiry_bar": event.expiry_bar,
        "stage": event.stage,
        "reason": event.reason,
        "bar_index": event.bar_index,
        "ts": event.ts,
        "placeholder": False,
    }



def refresh_meta_from_batch(batch: TVBatch) -> None:
    STATE["last_batch"] = batch.batch_id
    STATE["last_received_at"] = utc_now_iso()
    meta = STATE["meta"]
    meta["schema_version"] = batch.schema_version
    meta["engine"] = batch.engine
    meta["symbol"] = batch.symbol
    meta["tf"] = batch.tf
    meta["bar_index"] = batch.bar_index
    meta["ts"] = batch.ts
    meta["live_mode"] = batch.live_mode
    meta["events_count"] = batch.events_count or len(batch.events)
    meta["live_pending_count"] = batch.live_pending_count or 0
    meta["shadow_pending_count"] = batch.shadow_pending_count or 0
    meta["pending_longs"] = batch.pending_longs or 0
    meta["pending_shorts"] = batch.pending_shorts or 0
    add_seen_batch(batch.batch_id)
    meta["recent_batch_ids"] = SEEN_BATCH_IDS[-20:]



def sync_position_from_batch(batch: TVBatch) -> None:
    pos = STATE["position"]
    pos["state"] = batch.position_state or pos["state"]
    pos["id"] = batch.position_id
    pos["entry"] = batch.position_entry
    pos["stop"] = batch.position_stop
    pos["tp"] = batch.position_tp
    pos["moved_to_be"] = bool(batch.position_moved_to_be)

    if batch.position_state in ("long", "short"):
        pos["side"] = batch.position_state
    elif batch.position_state == "flat":
        pos["side"] = None



def ensure_shadow_snapshot(event: TVEvent) -> None:
    if event.id:
        STATE["shadow_pending"][event.id] = pending_snapshot(event)



def is_placeholder(item: Dict[str, Any]) -> bool:
    return bool(item.get("placeholder"))



def normalize_pending_buckets() -> List[str]:
    overlaps = sorted(set(STATE["live_pending"].keys()) & set(STATE["shadow_pending"].keys()))
    for pid in overlaps:
        STATE["live_pending"].pop(pid, None)
    return overlaps



def remove_all_placeholders() -> int:
    removed = 0
    for bucket_name in ("live_pending", "shadow_pending"):
        bucket = STATE[bucket_name]
        for key in list(bucket.keys()):
            if is_placeholder(bucket[key]):
                del bucket[key]
                removed += 1
    return removed



def collect_counts(include_placeholders: bool) -> Dict[str, int]:
    live_items = list(STATE["live_pending"].values())
    shadow_items = list(STATE["shadow_pending"].values())
    if not include_placeholders:
        live_items = [item for item in live_items if not is_placeholder(item)]
        shadow_items = [item for item in shadow_items if not is_placeholder(item)]

    live_long = sum(1 for item in live_items if item.get("side") == "long")
    live_short = sum(1 for item in live_items if item.get("side") == "short")
    shadow_long = sum(1 for item in shadow_items if item.get("side") == "long")
    shadow_short = sum(1 for item in shadow_items if item.get("side") == "short")

    return {
        "live_total": len(live_items),
        "shadow_total": len(shadow_items),
        "live_long": live_long,
        "live_short": live_short,
        "shadow_long": shadow_long,
        "shadow_short": shadow_short,
        "total_long": live_long + shadow_long,
        "total_short": live_short + shadow_short,
    }



def make_placeholder(stage: str, side: str, idx: int, meta: Dict[str, Any]) -> Dict[str, Any]:
    pid = f"{PLACEHOLDER_PREFIX}:{stage}:{side}:{idx}"
    return {
        "id": pid,
        "side": side,
        "entry": None,
        "sl": None,
        "tp": None,
        "expiry_bar": None,
        "stage": stage,
        "reason": "meta_placeholder",
        "bar_index": meta.get("bar_index"),
        "ts": meta.get("ts"),
        "placeholder": True,
    }



def rebuild_placeholders_from_meta() -> Tuple[int, bool]:
    removed = remove_all_placeholders()
    meta = STATE["meta"]
    explicit = collect_counts(include_placeholders=False)

    live_target = meta.get("live_pending_count", 0)
    shadow_target = meta.get("shadow_pending_count", 0)
    long_target = meta.get("pending_longs", 0)
    short_target = meta.get("pending_shorts", 0)

    live_def = live_target - explicit["live_total"]
    shadow_def = shadow_target - explicit["shadow_total"]
    long_def = long_target - explicit["total_long"]
    short_def = short_target - explicit["total_short"]

    if min(live_def, shadow_def, long_def, short_def) < 0:
        return removed, False

    live_long_add = min(live_def, long_def)
    live_short_add = live_def - live_long_add
    shadow_long_add = long_def - live_long_add
    shadow_short_add = short_def - live_short_add

    if min(live_short_add, shadow_long_add, shadow_short_add) < 0:
        return removed, False

    if shadow_long_add + shadow_short_add != shadow_def:
        return removed, False
    if live_long_add + shadow_long_add != long_def:
        return removed, False
    if live_short_add + shadow_short_add != short_def:
        return removed, False

    idx = 1
    for _ in range(live_long_add):
        ph = make_placeholder("live", "long", idx, meta)
        STATE["live_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(live_short_add):
        ph = make_placeholder("live", "short", idx, meta)
        STATE["live_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(shadow_long_add):
        ph = make_placeholder("shadow", "long", idx, meta)
        STATE["shadow_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(shadow_short_add):
        ph = make_placeholder("shadow", "short", idx, meta)
        STATE["shadow_pending"][ph["id"]] = ph
        idx += 1

    created = live_long_add + live_short_add + shadow_long_add + shadow_short_add
    return created + removed, True



def refresh_tracked_counts() -> None:
    explicit = collect_counts(include_placeholders=False)
    tracked = collect_counts(include_placeholders=True)

    meta = STATE["meta"]
    meta["explicit_live_pending_count"] = explicit["live_total"]
    meta["explicit_shadow_pending_count"] = explicit["shadow_total"]
    meta["explicit_pending_longs"] = explicit["total_long"]
    meta["explicit_pending_shorts"] = explicit["total_short"]

    meta["tracked_live_pending_count"] = tracked["live_total"]
    meta["tracked_shadow_pending_count"] = tracked["shadow_total"]
    meta["tracked_pending_longs"] = tracked["total_long"]
    meta["tracked_pending_shorts"] = tracked["total_short"]

    meta["placeholder_live_pending_count"] = tracked["live_total"] - explicit["live_total"]
    meta["placeholder_shadow_pending_count"] = tracked["shadow_total"] - explicit["shadow_total"]
    meta["placeholder_pending_longs"] = tracked["total_long"] - explicit["total_long"]
    meta["placeholder_pending_shorts"] = tracked["total_short"] - explicit["total_short"]

    meta["counts_aligned"] = (
        meta.get("live_pending_count", 0) == tracked["live_total"]
        and meta.get("shadow_pending_count", 0) == tracked["shadow_total"]
        and meta.get("pending_longs", 0) == tracked["total_long"]
        and meta.get("pending_shorts", 0) == tracked["total_short"]
    )



def reconcile_pending_to_meta() -> None:
    touched, ok = rebuild_placeholders_from_meta()
    refresh_tracked_counts()
    if touched and ok:
        log_action(
            "pending_reconciled_to_meta",
            "Reconciled tracked pending counts to Pine meta using placeholders when needed.",
            {
                "touched_entries": touched,
                "live_pending_count": STATE["meta"].get("live_pending_count"),
                "shadow_pending_count": STATE["meta"].get("shadow_pending_count"),
                "pending_longs": STATE["meta"].get("pending_longs"),
                "pending_shorts": STATE["meta"].get("pending_shorts"),
                "placeholder_live_pending_count": STATE["meta"].get("placeholder_live_pending_count"),
                "placeholder_shadow_pending_count": STATE["meta"].get("placeholder_shadow_pending_count"),
            },
        )
    elif not ok:
        log_action(
            "pending_reconcile_failed",
            "Could not fully reconcile pending counts to Pine meta. Explicit state likely started mid-stream or is stale.",
            {
                "live_pending_count": STATE["meta"].get("live_pending_count"),
                "shadow_pending_count": STATE["meta"].get("shadow_pending_count"),
                "pending_longs": STATE["meta"].get("pending_longs"),
                "pending_shorts": STATE["meta"].get("pending_shorts"),
                "explicit_live_pending_count": STATE["meta"].get("explicit_live_pending_count"),
                "explicit_shadow_pending_count": STATE["meta"].get("explicit_shadow_pending_count"),
                "explicit_pending_longs": STATE["meta"].get("explicit_pending_longs"),
                "explicit_pending_shorts": STATE["meta"].get("explicit_pending_shorts"),
            },
        )


# -----------------------------
# Execution adapter config
# -----------------------------
@dataclass
class AdapterConfig:
    mode: str = "dry_run"  # dry_run | demo_send
    bybit_demo: bool = True
    bybit_testnet: bool = False
    bybit_category: str = "linear"
    bybit_position_idx: int = 0
    default_order_qty: str = "100"
    default_leverage: str = "100"
    qty_map: Dict[str, str] = field(default_factory=dict)
    leverage_map: Dict[str, str] = field(default_factory=dict)
    symbol_map: Dict[str, str] = field(default_factory=dict)
    api_key: str = ""
    api_secret: str = ""
    recv_window: int = 5000
    timeout: int = 10
    journal_path: str = "/tmp/bybit_adapter_sol_v0.json"
    tp_sl_mode_on_entry: str = "Full"  # Full for Market TP/SL, Partial for limit TP/SL
    trigger_by: str = "LastPrice"

    @classmethod
    def from_env(cls) -> "AdapterConfig":
        qty_map = safe_json_loads(os.getenv("EXEC_QTY_MAP_JSON", ""), {})
        leverage_map = safe_json_loads(os.getenv("EXEC_LEVERAGE_MAP_JSON", ""), {})
        symbol_map = safe_json_loads(os.getenv("SYMBOL_MAP_JSON", ""), {})
        cfg = cls(
            mode=os.getenv("EXECUTION_MODE", "dry_run").strip().lower() or "dry_run",
            bybit_demo=os.getenv("BYBIT_DEMO", "true").strip().lower() == "true",
            bybit_testnet=os.getenv("BYBIT_TESTNET", "false").strip().lower() == "true",
            bybit_category=os.getenv("BYBIT_CATEGORY", "linear").strip() or "linear",
            bybit_position_idx=int(os.getenv("BYBIT_POSITION_IDX", "0")),
            default_order_qty=os.getenv("DEFAULT_ORDER_QTY", "100").strip() or "5",
            default_leverage=os.getenv("DEFAULT_LEVERAGE", "100").strip() or "50",
            qty_map={str(k): str(v) for k, v in qty_map.items()},
            leverage_map={str(k): str(v) for k, v in leverage_map.items()},
            symbol_map={str(k): str(v) for k, v in symbol_map.items()},
            api_key=os.getenv("BYBIT_API_KEY", ""),
            api_secret=os.getenv("BYBIT_API_SECRET", ""),
            recv_window=int(os.getenv("BYBIT_RECV_WINDOW", "5000")),
            timeout=int(os.getenv("BYBIT_TIMEOUT", "10")),
            journal_path=os.getenv("ADAPTER_JOURNAL_PATH", "/tmp/bybit_adapter_sol_v0.json"),
            tp_sl_mode_on_entry=os.getenv("BYBIT_TPSL_MODE_ON_ENTRY", "Full").strip() or "Full",
            trigger_by=os.getenv("BYBIT_TRIGGER_BY", "LastPrice").strip() or "LastPrice",
        )
        if cfg.bybit_demo and cfg.bybit_testnet:
            raise ValueError("BYBIT_DEMO=true and BYBIT_TESTNET=true is intentionally blocked for v0. Use mainnet demo domain, not demo on testnet.")
        if cfg.mode not in {"dry_run", "demo_send"}:
            raise ValueError("EXECUTION_MODE must be dry_run or demo_send")
        return cfg

    def qty_for_symbol(self, tv_symbol: str) -> str:
        exchange_symbol = self.map_symbol(tv_symbol)
        return self.qty_map.get(exchange_symbol) or self.qty_map.get(tv_symbol) or self.default_order_qty

    def leverage_for_symbol(self, tv_symbol: str) -> str:
        exchange_symbol = self.map_symbol(tv_symbol)
        return self.leverage_map.get(exchange_symbol) or self.leverage_map.get(tv_symbol) or self.default_leverage

    def map_symbol(self, tv_symbol: str) -> str:
        return self.symbol_map.get(tv_symbol, tv_symbol)


# -----------------------------
# Execution adapter models
# -----------------------------
@dataclass
class InstrumentMeta:
    symbol: str
    tick_size: str = "0.01"
    qty_step: str = "0.001"
    min_order_qty: Optional[str] = None
    min_notional_value: Optional[str] = None


@dataclass
class ExecutionCommand:
    command_id: str
    kind: str
    order_id: Optional[str]
    symbol: str
    params: Dict[str, Any]
    source_event_key: str
    source_batch_id: str
    dry_run: bool


@dataclass
class AdapterJournal:
    processed_event_keys: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    processed_command_ids: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    order_registry: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    position_registry: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    leverage_registry: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    instrument_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    adapter_actions: List[Dict[str, Any]] = field(default_factory=list)

    def append_action(self, kind: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        self.adapter_actions.append(
            {
                "at": utc_now_iso(),
                "kind": kind,
                "message": message,
                "payload": payload or {},
            }
        )
        if len(self.adapter_actions) > MAX_ACTIONS:
            del self.adapter_actions[:-MAX_ACTIONS]

    def mark_event_processed(self, event_key: str, record: Dict[str, Any]) -> None:
        self.processed_event_keys[event_key] = record

    def mark_command_processed(self, command_id: str, record: Dict[str, Any]) -> None:
        self.processed_command_ids[command_id] = record

    def to_dict(self) -> Dict[str, Any]:
        return {
            "processed_event_keys": self.processed_event_keys,
            "processed_command_ids": self.processed_command_ids,
            "order_registry": self.order_registry,
            "position_registry": self.position_registry,
            "leverage_registry": self.leverage_registry,
            "instrument_cache": self.instrument_cache,
            "adapter_actions": self.adapter_actions,
        }

    @classmethod
    def from_path(cls, path: str) -> "AdapterJournal":
        target = Path(path)
        if not target.exists():
            return cls()
        try:
            data = json.loads(target.read_text())
        except (OSError, json.JSONDecodeError):
            return cls()
        return cls(
            processed_event_keys=data.get("processed_event_keys", {}),
            processed_command_ids=data.get("processed_command_ids", {}),
            order_registry=data.get("order_registry", {}),
            position_registry=data.get("position_registry", {}),
            leverage_registry=data.get("leverage_registry", {}),
            instrument_cache=data.get("instrument_cache", {}),
            adapter_actions=data.get("adapter_actions", []),
        )

    def persist(self, path: str) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))


class ExchangeGateway(Protocol):
    def get_instrument(self, category: str, symbol: str) -> InstrumentMeta:
        ...

    def set_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def place_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def amend_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def cancel_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def set_position_stop(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def sync_open_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def sync_position(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ...


class DryRunGateway:
    def __init__(self, config: AdapterConfig):
        self.config = config

    def get_instrument(self, category: str, symbol: str) -> InstrumentMeta:
        return InstrumentMeta(symbol=symbol)

    def _ack(self, op: str, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "mode": "dry_run",
            "op": op,
            "accepted": True,
            "params": params,
            "at": utc_now_iso(),
        }

    def set_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("set_leverage", params)

    def place_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("place_pending_order", params)

    def amend_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("amend_pending_order", params)

    def cancel_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("cancel_pending_order", params)

    def set_position_stop(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("set_position_stop", params)

    def sync_open_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("sync_open_orders", params)

    def sync_position(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ack("sync_position", params)


class PybitGateway:
    def __init__(self, config: AdapterConfig):
        self.config = config
        self._session = None

    def _ensure_session(self):
        if self._session is not None:
            return self._session
        try:
            from pybit.unified_trading import HTTP
        except ImportError as exc:
            raise RuntimeError("pybit is not installed. Add pybit to requirements before using demo_send mode.") from exc
        self._session = HTTP(
            testnet=self.config.bybit_testnet,
            demo=self.config.bybit_demo,
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            recv_window=self.config.recv_window,
            timeout=self.config.timeout,
        )
        return self._session

    def get_instrument(self, category: str, symbol: str) -> InstrumentMeta:
        session = self._ensure_session()
        response = session.get_instruments_info(category=category, symbol=symbol)
        result = (response or {}).get("result", {})
        items = result.get("list", [])
        if not items:
            raise RuntimeError(f"Instrument metadata not found for {category}:{symbol}")
        item = items[0]
        return InstrumentMeta(
            symbol=item.get("symbol", symbol),
            tick_size=item.get("priceFilter", {}).get("tickSize", "0.01"),
            qty_step=item.get("lotSizeFilter", {}).get("qtyStep", "0.001"),
            min_order_qty=item.get("lotSizeFilter", {}).get("minOrderQty"),
            min_notional_value=item.get("lotSizeFilter", {}).get("minNotionalValue"),
        )

    def set_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().set_leverage(**params)

    def place_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().place_order(**params)

    def amend_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().amend_order(**params)

    def cancel_pending_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().cancel_order(**params)

    def set_position_stop(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().set_trading_stop(**params)

    def sync_open_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().get_open_orders(**params)

    def sync_position(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_session().get_positions(**params)


# -----------------------------
# Execution adapter engine
# -----------------------------
class BybitDemoAdapterV0:
    def __init__(self, config: AdapterConfig):
        self.config = config
        self.journal = AdapterJournal.from_path(config.journal_path)
        self.gateway: ExchangeGateway = DryRunGateway(config) if config.mode == "dry_run" else PybitGateway(config)
        self.journal.append_action(
            "adapter_boot",
            "Adapter initialized.",
            {
                "mode": self.config.mode,
                "bybit_demo": self.config.bybit_demo,
                "bybit_testnet": self.config.bybit_testnet,
                "journal_path": self.config.journal_path,
            },
        )
        self._persist()

    def _persist(self) -> None:
        self.journal.persist(self.config.journal_path)

    def reset(self) -> None:
        self.journal = AdapterJournal()
        self.journal.append_action("adapter_reset", "Adapter journal was reset.", {})
        self._persist()

    def event_key(self, batch: TVBatch, event: TVEvent) -> str:
        return f"{batch.batch_id}:{event.event_index}:{event.event}:{event.id or '-'}"

    def command_id(self, event_key: str, kind: str, payload: Dict[str, Any]) -> str:
        digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
        return f"{event_key}:{kind}:{digest}"

    def order_link_id(self, internal_id: str, tv_symbol: Optional[str] = None, engine: Optional[str] = None) -> str:
        # Keep it deterministic, namespaced across engines/symbols, and safely short.
        symbol_part = (tv_symbol or "na").upper()
        engine_part = (engine or "na").lower()
        namespace = f"{self.config.bybit_category}:{symbol_part}:{engine_part}:{internal_id}"
        digest = hashlib.sha1(namespace.encode("utf-8")).hexdigest()[:16]
        prefix = f"tv-{symbol_part.lower()}"[:10]
        return f"{prefix}-{digest}"[:36]

    def _instrument(self, symbol: str) -> InstrumentMeta:
        cached = self.journal.instrument_cache.get(symbol)
        if cached:
            return InstrumentMeta(**cached)
        instrument = self.gateway.get_instrument(self.config.bybit_category, symbol)
        self.journal.instrument_cache[symbol] = asdict(instrument)
        self._persist()
        return instrument

    @staticmethod
    def _round_to_step(value: str, step: str) -> str:
        dec_value = Decimal(str(value))
        dec_step = Decimal(str(step))
        if dec_step <= 0:
            return format(dec_value.normalize(), "f")
        rounded = (dec_value / dec_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * dec_step
        return format(rounded.normalize(), "f")

    def _normalized_symbol(self, tv_symbol: str) -> str:
        return self.config.map_symbol(tv_symbol)

    def _normalized_qty(self, tv_symbol: str, symbol: str) -> str:
        instrument = self._instrument(symbol)
        raw_qty = self.config.qty_for_symbol(tv_symbol)
        return self._round_to_step(raw_qty, instrument.qty_step)

    @staticmethod
    def _normalized_leverage(value: str) -> str:
        dec_value = Decimal(str(value))
        if dec_value <= 0:
            raise ValueError("Leverage must be > 0")
        return format(dec_value.normalize(), "f")

    @staticmethod
    def _result_is_ok(result: Dict[str, Any]) -> bool:
        if result.get("accepted") is True:
            return True
        code = result.get("retCode")
        if code in (None, 0, "0"):
            return True
        msg = str(result.get("retMsg") or result.get("ret_msg") or "").lower()
        return "not modified" in msg and "leverage" in msg

    def _desired_leverage(self, tv_symbol: str, symbol: str) -> str:
        return self._normalized_leverage(self.config.leverage_for_symbol(tv_symbol))

    def _ensure_symbol_leverage(self, tv_symbol: str, symbol: str) -> None:
        desired_leverage = self._desired_leverage(tv_symbol, symbol)
        current = self.journal.leverage_registry.get(symbol, {})
        if current.get("buyLeverage") == desired_leverage and current.get("sellLeverage") == desired_leverage:
            return

        params = {
            "category": self.config.bybit_category,
            "symbol": symbol,
            "buyLeverage": desired_leverage,
            "sellLeverage": desired_leverage,
        }
        result = self.gateway.set_leverage(params)
        if not self._result_is_ok(result):
            raise RuntimeError(f"set_leverage rejected for {symbol}: {result}")

        self.journal.leverage_registry[symbol] = {
            "symbol": symbol,
            "buyLeverage": desired_leverage,
            "sellLeverage": desired_leverage,
            "last_result": result,
            "updated_at": utc_now_iso(),
        }
        self.journal.append_action(
            "leverage_applied",
            "Applied leverage for symbol.",
            {"symbol": symbol, "buyLeverage": desired_leverage, "sellLeverage": desired_leverage, "result": result},
        )
        self._persist()

    def _normalized_price(self, value: Optional[float], symbol: str) -> Optional[str]:
        if value is None:
            return None
        instrument = self._instrument(symbol)
        return self._round_to_step(str(value), instrument.tick_size)

    def _desired_order_state(self, batch: TVBatch, event: TVEvent) -> Dict[str, Any]:
        symbol = self._normalized_symbol(batch.symbol)
        qty = self._normalized_qty(batch.symbol, symbol)
        entry = self._normalized_price(event.entry, symbol)
        stop_loss = self._normalized_price(event.sl, symbol)
        take_profit = self._normalized_price(event.tp, symbol)
        side = "Buy" if event.side == "long" else "Sell"
        payload: Dict[str, Any] = {
            "category": self.config.bybit_category,
            "symbol": symbol,
            "side": side,
            "positionIdx": self.config.bybit_position_idx,
            "orderType": "Limit",
            "qty": qty,
            "price": entry,
            "timeInForce": "GTC",
            "orderLinkId": self.order_link_id(event.id or "missing-id", batch.symbol, batch.engine),
            "reduceOnly": False,
            "triggerBy": self.config.trigger_by,
        }
        if take_profit is not None:
            payload["takeProfit"] = take_profit
        if stop_loss is not None:
            payload["stopLoss"] = stop_loss
        if take_profit is not None or stop_loss is not None:
            payload["tpslMode"] = self.config.tp_sl_mode_on_entry
            if self.config.tp_sl_mode_on_entry == "Full":
                payload["tpOrderType"] = "Market"
                payload["slOrderType"] = "Market"
        return payload

    def _build_upsert_command(self, batch: TVBatch, event: TVEvent, event_key: str) -> Optional[ExecutionCommand]:
        if event.stage != "live" or not event.id:
            return None
        desired = self._desired_order_state(batch, event)
        current = self.journal.order_registry.get(event.id)
        desired_state = {
            "symbol": desired["symbol"],
            "side": desired["side"],
            "qty": desired["qty"],
            "price": desired["price"],
            "takeProfit": desired.get("takeProfit"),
            "stopLoss": desired.get("stopLoss"),
        }
        if current and current.get("desired_state") == desired_state and current.get("status") in {"open", "amended", "accepted"}:
            self.journal.append_action(
                "event_noop_duplicate_state",
                "Skipping upsert because desired exchange state is unchanged.",
                {"event_key": event_key, "order_id": event.id, "desired_state": desired_state},
            )
            self._persist()
            return None

        kind = "amend_pending" if current and current.get("status") in {"open", "amended", "accepted"} else "place_pending"
        params = desired.copy()
        if kind == "amend_pending":
            params = {
                "category": desired["category"],
                "symbol": desired["symbol"],
                "orderLinkId": desired["orderLinkId"],
                "qty": desired["qty"],
                "price": desired["price"],
                "takeProfit": desired.get("takeProfit"),
                "stopLoss": desired.get("stopLoss"),
                "triggerBy": desired.get("triggerBy"),
            }
        command_id = self.command_id(event_key, kind, params)
        return ExecutionCommand(
            command_id=command_id,
            kind=kind,
            order_id=event.id,
            symbol=desired["symbol"],
            params=params,
            source_event_key=event_key,
            source_batch_id=batch.batch_id,
            dry_run=self.config.mode == "dry_run",
        )

    def _build_cancel_command(self, batch: TVBatch, event: TVEvent, event_key: str) -> Optional[ExecutionCommand]:
        if event.stage != "live" or not event.id:
            return None
        symbol = self._normalized_symbol(batch.symbol)
        params = {
            "category": self.config.bybit_category,
            "symbol": symbol,
            "orderLinkId": self.order_link_id(event.id, batch.symbol, batch.engine),
        }
        command_id = self.command_id(event_key, "cancel_pending", params)
        return ExecutionCommand(
            command_id=command_id,
            kind="cancel_pending",
            order_id=event.id,
            symbol=symbol,
            params=params,
            source_event_key=event_key,
            source_batch_id=batch.batch_id,
            dry_run=self.config.mode == "dry_run",
        )

    def _build_move_stop_command(self, batch: TVBatch, event: TVEvent, event_key: str) -> Optional[ExecutionCommand]:
        if not event.id:
            return None
        symbol = self._normalized_symbol(batch.symbol)
        stop_loss = self._normalized_price(event.sl, symbol)
        take_profit = self._normalized_price(event.tp, symbol)
        params = {
            "category": self.config.bybit_category,
            "symbol": symbol,
            "positionIdx": self.config.bybit_position_idx,
            "tpslMode": "Full",
            "stopLoss": stop_loss or "0",
            "takeProfit": take_profit or "0",
            "slTriggerBy": self.config.trigger_by,
            "tpTriggerBy": self.config.trigger_by,
        }
        command_id = self.command_id(event_key, "move_stop", params)
        return ExecutionCommand(
            command_id=command_id,
            kind="move_stop",
            order_id=event.id,
            symbol=symbol,
            params=params,
            source_event_key=event_key,
            source_batch_id=batch.batch_id,
            dry_run=self.config.mode == "dry_run",
        )

    def _build_sync_command(self, batch: TVBatch, event: TVEvent, event_key: str, kind: str) -> ExecutionCommand:
        symbol = self._normalized_symbol(batch.symbol)
        params = {
            "category": self.config.bybit_category,
            "symbol": symbol,
            "openOnly": 0,
        }
        command_id = self.command_id(event_key, kind, params)
        return ExecutionCommand(
            command_id=command_id,
            kind=kind,
            order_id=event.id,
            symbol=symbol,
            params=params,
            source_event_key=event_key,
            source_batch_id=batch.batch_id,
            dry_run=self.config.mode == "dry_run",
        )

    def plan_event(self, batch: TVBatch, event: TVEvent) -> List[ExecutionCommand]:
        event_key = self.event_key(batch, event)
        if event_key in self.journal.processed_event_keys:
            self.journal.append_action(
                "event_duplicate_ignored",
                "Skipped already processed event.",
                {"event_key": event_key},
            )
            self._persist()
            return []

        commands: List[ExecutionCommand] = []
        if event.event == "upsert_pending":
            cmd = self._build_upsert_command(batch, event, event_key)
            if cmd:
                commands.append(cmd)
        elif event.event == "cancel_pending":
            cmd = self._build_cancel_command(batch, event, event_key)
            if cmd:
                commands.append(cmd)
        elif event.event == "move_stop":
            cmd = self._build_move_stop_command(batch, event, event_key)
            if cmd:
                commands.append(cmd)
        elif event.event == "entry_fill_expected":
            commands.append(self._build_sync_command(batch, event, event_key, "sync_after_fill"))
        elif event.event == "position_exit_expected":
            commands.append(self._build_sync_command(batch, event, event_key, "sync_after_exit"))

        self.journal.mark_event_processed(
            event_key,
            {
                "batch_id": batch.batch_id,
                "event": event.model_dump(),
                "planned_commands": [asdict(cmd) for cmd in commands],
                "at": utc_now_iso(),
            },
        )
        self._persist()
        return commands

    def execute_command(self, command: ExecutionCommand) -> Dict[str, Any]:
        if command.command_id in self.journal.processed_command_ids:
            prior = self.journal.processed_command_ids[command.command_id]
            self.journal.append_action(
                "command_duplicate_ignored",
                "Skipped already processed command.",
                {"command_id": command.command_id, "result": prior},
            )
            self._persist()
            return prior

        try:
            if command.kind == "place_pending":
                self._ensure_symbol_leverage(command.symbol, command.symbol)
                result = self.gateway.place_pending_order(command.params)
                self._register_order_after_place(command, result)
            elif command.kind == "amend_pending":
                self._ensure_symbol_leverage(command.symbol, command.symbol)
                result = self.gateway.amend_pending_order(command.params)
                self._register_order_after_amend(command, result)
            elif command.kind == "cancel_pending":
                result = self.gateway.cancel_pending_order(command.params)
                self._register_order_after_cancel(command, result)
            elif command.kind == "move_stop":
                result = self.gateway.set_position_stop(command.params)
                self._register_position_after_stop_move(command)
            elif command.kind in {"sync_after_fill", "sync_after_exit"}:
                result = {
                    "orders": self.gateway.sync_open_orders(command.params),
                    "positions": self.gateway.sync_position(
                        {
                            "category": self.config.bybit_category,
                            "symbol": command.symbol,
                        }
                    ),
                }
            else:
                raise RuntimeError(f"Unknown command kind: {command.kind}")

            record = {
                "command": asdict(command),
                "result": result,
                "status": "accepted",
                "at": utc_now_iso(),
            }
            self.journal.mark_command_processed(command.command_id, record)
            self.journal.append_action(
                "command_executed",
                "Execution command processed.",
                {"command_id": command.command_id, "kind": command.kind, "result": result},
            )
            self._persist()
            return record
        except Exception as exc:  # pragma: no cover - safety path
            record = {
                "command": asdict(command),
                "status": "error",
                "error": str(exc),
                "at": utc_now_iso(),
            }
            self.journal.mark_command_processed(command.command_id, record)
            self.journal.append_action(
                "command_failed",
                "Execution command failed.",
                {"command_id": command.command_id, "kind": command.kind, "error": str(exc)},
            )
            self._persist()
            return record

    def _register_order_after_place(self, command: ExecutionCommand, result: Dict[str, Any]) -> None:
        if not command.order_id:
            return
        registry = self.journal.order_registry.setdefault(command.order_id, {})
        registry.update(
            {
                "status": "accepted",
                "symbol": command.symbol,
                "order_link_id": command.params.get("orderLinkId"),
                "desired_state": {
                    "symbol": command.symbol,
                    "side": command.params.get("side"),
                    "qty": command.params.get("qty"),
                    "price": command.params.get("price"),
                    "takeProfit": command.params.get("takeProfit"),
                    "stopLoss": command.params.get("stopLoss"),
                },
                "last_result": result,
                "updated_at": utc_now_iso(),
            }
        )

    def _register_order_after_amend(self, command: ExecutionCommand, result: Dict[str, Any]) -> None:
        if not command.order_id:
            return
        registry = self.journal.order_registry.setdefault(command.order_id, {})
        registry.update(
            {
                "status": "amended",
                "symbol": command.symbol,
                "order_link_id": command.params.get("orderLinkId"),
                "desired_state": {
                    "symbol": command.symbol,
                    "side": registry.get("desired_state", {}).get("side"),
                    "qty": command.params.get("qty"),
                    "price": command.params.get("price"),
                    "takeProfit": command.params.get("takeProfit"),
                    "stopLoss": command.params.get("stopLoss"),
                },
                "last_result": result,
                "updated_at": utc_now_iso(),
            }
        )

    def _register_order_after_cancel(self, command: ExecutionCommand, result: Dict[str, Any]) -> None:
        if not command.order_id:
            return
        registry = self.journal.order_registry.setdefault(command.order_id, {})
        registry.update(
            {
                "status": "cancel_requested",
                "symbol": command.symbol,
                "order_link_id": command.params.get("orderLinkId"),
                "last_result": result,
                "updated_at": utc_now_iso(),
            }
        )

    def _register_position_after_stop_move(self, command: ExecutionCommand) -> None:
        if not command.order_id:
            return
        self.journal.position_registry[command.order_id] = {
            "symbol": command.symbol,
            "stop_loss": command.params.get("stopLoss"),
            "take_profit": command.params.get("takeProfit"),
            "updated_at": utc_now_iso(),
        }

    def process_batch_event(self, batch: TVBatch, event: TVEvent) -> List[Dict[str, Any]]:
        commands = self.plan_event(batch, event)
        return [self.execute_command(command) for command in commands]

    def get_state(self) -> Dict[str, Any]:
        return {
            "config": {
                "mode": self.config.mode,
                "bybit_demo": self.config.bybit_demo,
                "bybit_testnet": self.config.bybit_testnet,
                "bybit_category": self.config.bybit_category,
                "bybit_position_idx": self.config.bybit_position_idx,
                "default_order_qty": self.config.default_order_qty,
                "default_leverage": self.config.default_leverage,
                "qty_map": self.config.qty_map,
                "leverage_map": self.config.leverage_map,
                "symbol_map": self.config.symbol_map,
                "recv_window": self.config.recv_window,
                "timeout": self.config.timeout,
                "journal_path": self.config.journal_path,
                "tp_sl_mode_on_entry": self.config.tp_sl_mode_on_entry,
                "trigger_by": self.config.trigger_by,
                "api_key_present": bool(self.config.api_key),
                "api_secret_present": bool(self.config.api_secret),
            },
            "journal": self.journal.to_dict(),
        }


def build_adapter() -> BybitDemoAdapterV0:
    return BybitDemoAdapterV0(AdapterConfig.from_env())


ADAPTER = build_adapter()


# -----------------------------
# Receiver handlers
# -----------------------------
def handle_upsert_pending(event: TVEvent) -> None:
    snapshot = pending_snapshot(event)
    if event.stage == "shadow":
        STATE["shadow_pending"][event.id] = snapshot
        STATE["live_pending"].pop(event.id, None)
        log_action(
            "shadow_upserted",
            "Updated shadow pending order. No exchange action.",
            event_to_dict(event),
        )
        return

    STATE["live_pending"][event.id] = snapshot
    if event.id in STATE["shadow_pending"]:
        del STATE["shadow_pending"][event.id]
    log_action(
        "would_place_pending",
        "Would place/update pending order on exchange.",
        event_to_dict(event),
    )



def handle_cancel_pending(event: TVEvent) -> None:
    payload = event_to_dict(event)

    if event.stage == "shadow":
        if event.reason in KEEP_SHADOW_REASONS:
            ensure_shadow_snapshot(event)
            removed_live = STATE["live_pending"].pop(event.id, None)
            payload["kept_in_shadow"] = True
            payload["live_duplicate_removed"] = bool(removed_live)
            log_action(
                "shadow_kept",
                "Shadow pending remains stored after other order filled.",
                payload,
            )
            return

        removed_shadow = STATE["shadow_pending"].pop(event.id, None)
        payload["removed_from"] = "shadow" if removed_shadow else "shadow_assumed"
        log_action(
            "shadow_cancelled",
            "Removed shadow pending order. No exchange cancel required.",
            payload,
        )
        return

    removed_live = STATE["live_pending"].pop(event.id, None)
    payload["removed_from"] = "live" if removed_live else "live_assumed"
    log_action(
        "would_cancel_pending",
        "Would cancel pending order on exchange.",
        payload,
    )



def handle_shadow_store(event: TVEvent) -> None:
    STATE["shadow_pending"][event.id] = pending_snapshot(event)
    STATE["live_pending"].pop(event.id, None)
    log_action(
        "stored_in_shadow",
        "Stored pending order in shadow state. No exchange action.",
        event_to_dict(event),
    )



def handle_shadow_drop(event: TVEvent) -> None:
    STATE["shadow_pending"].pop(event.id, None)
    log_action(
        "dropped_from_shadow",
        "Dropped shadow pending order.",
        event_to_dict(event),
    )



def handle_move_stop(event: TVEvent) -> None:
    pos = STATE["position"]
    if pos.get("id") == event.id or pos.get("state") in ("long", "short"):
        pos["stop"] = event.sl
        if event.reason == "be_activated":
            pos["moved_to_be"] = True
    log_action(
        "would_move_stop",
        "Would move stop on exchange.",
        event_to_dict(event),
    )



def handle_entry_fill_expected(event: TVEvent) -> None:
    pos = STATE["position"]
    pos["state"] = event.side or pos.get("state")
    pos["side"] = event.side
    pos["id"] = event.id
    pos["entry"] = event.entry
    pos["stop"] = event.sl
    pos["tp"] = event.tp
    pos["moved_to_be"] = False
    STATE["live_pending"].pop(event.id, None)
    STATE["shadow_pending"].pop(event.id, None)
    log_action(
        "position_marked_open",
        "Marked position as open from expected fill.",
        event_to_dict(event),
    )



def handle_position_exit_expected(event: TVEvent) -> None:
    STATE["position"] = {
        "state": "flat",
        "id": None,
        "entry": None,
        "stop": None,
        "tp": None,
        "moved_to_be": False,
        "side": None,
    }
    log_action(
        "position_marked_closed",
        "Marked position as closed from expected exit.",
        event_to_dict(event),
    )



def route_event(event: TVEvent) -> None:
    if event.event == "upsert_pending":
        handle_upsert_pending(event)
    elif event.event == "cancel_pending":
        handle_cancel_pending(event)
    elif event.event == "shadow_store":
        handle_shadow_store(event)
    elif event.event == "shadow_drop":
        handle_shadow_drop(event)
    elif event.event == "move_stop":
        handle_move_stop(event)
    elif event.event == "entry_fill_expected":
        handle_entry_fill_expected(event)
    elif event.event == "position_exit_expected":
        handle_position_exit_expected(event)
    else:
        log_action(
            "event_unhandled",
            "Received event with no handler.",
            event_to_dict(event),
        )


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "tv_receiver_bybit_adapter_sol_v0",
        "time": utc_now_iso(),
        "execution_mode": ADAPTER.config.mode,
    }


@app.get("/state")
def get_state() -> Dict[str, Any]:
    return STATE


@app.get("/actions")
def get_actions() -> Dict[str, Any]:
    return {"count": len(ACTIONS), "items": ACTIONS}


@app.get("/adapter/state")
def get_adapter_state() -> Dict[str, Any]:
    return ADAPTER.get_state()


@app.get("/adapter/actions")
def get_adapter_actions() -> Dict[str, Any]:
    return {"count": len(ADAPTER.journal.adapter_actions), "items": ADAPTER.journal.adapter_actions}


@app.post("/reset")
def reset_endpoint() -> Dict[str, Any]:
    reset_state(clear_actions=True)
    ADAPTER.reset()
    log_action("state_reset", "State, actions, and adapter journal were reset.", {})
    return {"status": "ok", "message": "state + adapter reset"}


@app.post("/webhook/tradingview")
def tradingview_webhook(batch: TVBatch) -> Dict[str, Any]:
    if batch.batch_id in SEEN_BATCH_LOOKUP:
        log_action(
            "duplicate_batch_ignored",
            "Ignored duplicate TradingView batch.",
            {
                "batch_id": batch.batch_id,
                "events_count": batch.events_count or len(batch.events),
                "schema_version": batch.schema_version,
            },
        )
        ADAPTER.journal.append_action(
            "duplicate_batch_ignored",
            "Ignored duplicate TradingView batch before adapter execution.",
            {
                "batch_id": batch.batch_id,
                "events_count": batch.events_count or len(batch.events),
            },
        )
        ADAPTER._persist()
        return {
            "status": "duplicate_ignored",
            "batch_id": batch.batch_id,
            "events_received": batch.events_count or len(batch.events),
        }

    refresh_meta_from_batch(batch)
    sync_position_from_batch(batch)

    log_action(
        "batch_received",
        "Received TradingView batch.",
        {
            "batch_id": batch.batch_id,
            "events_count": batch.events_count or len(batch.events),
            "schema_version": batch.schema_version,
        },
    )

    execution_results: List[Dict[str, Any]] = []
    for event in batch.events:
        route_event(event)
        execution_results.extend(ADAPTER.process_batch_event(batch, event))

    duplicate_ids = normalize_pending_buckets()
    if duplicate_ids:
        log_action(
            "pending_bucket_deduped",
            "Removed duplicate pending ids from live bucket because matching shadow ids were present.",
            {"ids": duplicate_ids},
        )

    reconcile_pending_to_meta()

    return {
        "status": "accepted",
        "batch_id": batch.batch_id,
        "events_received": batch.events_count or len(batch.events),
        "execution_results": execution_results,
    }


@app.exception_handler(Exception)
def generic_exception_handler(_, exc: Exception):
    raise HTTPException(status_code=500, detail=str(exc))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False)
