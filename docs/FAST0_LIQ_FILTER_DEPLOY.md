# FAST0 Liquidation Filter — Deploy Deliverable

## 1) Root cause / Files changed

**Goal:** FAST0 live entry and Telegram use only when **5000 < liq_long_usd_30s ≤ 25000**. No changes to TP/SL, short_pump active mode, Bybit/runner/broker/risk.

| File | Change |
|------|--------|
| `short_pump/fast0_sampler.py` | Read `FAST0_LIQ_MIN_USD` / `FAST0_LIQ_MAX_USD`; `should_fast0_entry_ok()` requires liq in (min, max], returns reasons `liq_missing`, `liq_le_5000`, `liq_gt_25000`; logs **FAST0_LIQ_FILTERED** on reject; FAST0 TG gate uses `is_fast0_tg_entry_allowed()` (logs `tg_liq_range` when TG skipped). |
| `short_pump/telegram.py` | `is_fast0_tg_entry_allowed(payload)` True only when `FAST0_LIQ_MIN_USD < liq_long_usd_30s <= FAST0_LIQ_MAX_USD`. |
| `trading/broker.py` | `allow_entry_short_pump_fast0(signal)` allows entry only when liq in (min, max]; helpers `_fast0_liq_min_usd()` / `_fast0_liq_max_usd()` from env. |
| `scripts/smoke_fast0_liq_trade_gate.py` | Smoke: block liq=0, 3k, 50k; allow 10k; trade row only for in-range; uses `path_mode="live"` for dataset path. |
| `scripts/smoke_tg_tradeable_gate.py` | Set `FAST0_LIQ_MIN_USD`/`FAST0_LIQ_MAX_USD` before import; FAST0 TG tests use 5000 < liq ≤ 25000. |

---

## 2) Env lines for systemd override

Add to the systemd override (e.g. `/etc/systemd/system/<pump_short_service>.d/override.conf` under `[Service]`):

```ini
Environment=FAST0_LIQ_MIN_USD=5000
Environment=FAST0_LIQ_MAX_USD=25000
```

Or in a single line:

```ini
Environment=FAST0_LIQ_MIN_USD=5000 FAST0_LIQ_MAX_USD=25000
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart <pump_short_service>
```

---

## 3) VPS commands to verify after deploy

```bash
# From project root (e.g. /root/pump_short)
cd /root/pump_short

# Smoke: FAST0 entry gate (block 0/3k/50k, allow 10k)
PYTHONPATH=. python3 scripts/smoke_fast0_liq_trade_gate.py

# Smoke: TG tradeable gate (FAST0 5k–25k liq)
PYTHONPATH=. python3 scripts/smoke_tg_tradeable_gate.py
```

Expected: both scripts exit 0 and print OK lines.

---

## 4) Expected journalctl grep lines

After a live run, filter FAST0 liq filter events and allowed entries:

```bash
# Filtered-out FAST0 (rejected by liq range)
journalctl -u <pump_short_service> --no-pager -n 5000 | grep FAST0_LIQ_FILTERED
```

Example log lines:

- Rejected (no send, no entry):
  - `FAST0_LIQ_FILTERED ... "reason":"liq_missing"`
  - `FAST0_LIQ_FILTERED ... "reason":"liq_le_5000"`
  - `FAST0_LIQ_FILTERED ... "reason":"liq_gt_25000"`
  - `FAST0_LIQ_FILTERED ... "reason":"tg_liq_range"`

To confirm allowed FAST0 entries (run_id/symbol in your log format):

```bash
journalctl -u <pump_short_service> --no-pager -n 5000 | grep -E "ENTRY_OK|short_pump_fast0|FAST0"
```

Adjust service name and log pattern to match your deployment.
