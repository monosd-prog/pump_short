# Pump Trading Runner (systemd)

Runs the trading runner on a timer: consumes signals from queue, opens paper positions, closes on OUTCOME.

## Install

1. Copy unit files to systemd:
   ```bash
   sudo cp pump-trading-runner.service pump-trading-runner.timer /etc/systemd/system/
   ```

2. Edit paths if needed (default: `/root/pump_short`, venv at `/root/pump_short/venv`):
   ```bash
   sudo nano /etc/systemd/system/pump-trading-runner.service
   ```

3. Create `.env` in project root with:
   ```
   AUTO_TRADING_ENABLE=1
   AUTO_TRADING_MODE=paper
   # ... other vars (PAPER_EQUITY_USD, etc.)
   ```

4. Enable and start timer:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable pump-trading-runner.timer
   sudo systemctl start pump-trading-runner.timer
   ```

## Usage

- **Status:** `sudo systemctl status pump-trading-runner.timer`
- **List timers:** `systemctl list-timers pump-trading-runner*`
- **Run once manually:** `sudo systemctl start pump-trading-runner.service`
- **Logs:** `journalctl -u pump-trading-runner.service -f`

## Concurrency

The runner uses a lock file (`datasets/trading_runner.lock`). If a run is already in progress, the next invocation exits cleanly without overlapping.
