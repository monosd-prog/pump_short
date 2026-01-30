import argparse
import json
import os
import time

def main() -> None:
    p = argparse.ArgumentParser(prog="short_pump")
    sub = p.add_subparsers(dest="cmd", required=True)

    # ---- api ----
    p_api = sub.add_parser("api", help="Run FastAPI server")
    p_api.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    p_api.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    p_api.add_argument("--reload", action="store_true")

    # ---- watch ----
    p_w = sub.add_parser("watch", help="Run watcher for a single symbol")
    p_w.add_argument("--symbol", required=True)
    p_w.add_argument("--run-id", default=None)
    p_w.add_argument("--meta", default=None, help="optional JSON meta string")

    args = p.parse_args()

    if args.cmd == "api":
        import uvicorn
        uvicorn.run(
            "short_pump.server:app",
            host=args.host,
            port=args.port,
            reload=bool(args.reload),
        )
        return

    if args.cmd == "watch":
        from short_pump.watcher import run_watch_for_symbol

        symbol = args.symbol.strip().upper()
        run_id = args.run_id or time.strftime("%Y%m%d_%H%M%S")

        meta = {"source": "cli"}
        if args.meta:
            meta.update(json.loads(args.meta))

        run_watch_for_symbol(symbol, run_id, meta)
        return


if __name__ == "__main__":
    main()