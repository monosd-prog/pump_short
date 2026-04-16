---
alwaysApply: true
---

Run the following terminal commands automatically without asking for confirmation in this project:
- python3 scripts/* and python scripts/*
- git add, git commit, git push
- grep, find, ls, cat, tail, head, wc, echo
- Chained commands (&&) composed entirely of the above

Always require explicit confirmation before running:
- rm (any form)
- systemctl (any form)
- kill, killall
- chmod, chown
- docker (any form)
- reboot, shutdown
- Any command that writes to .env, trading_state.json, or datasets/ outside of the allowed scripts