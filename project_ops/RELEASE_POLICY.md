# Release Policy: Commit / Push / Deploy / Restart

## Trigger
После каждой задачи со статусом **Final Decision = `Approved` / `Approved with notes`**.

---
## Release Evidence
В каждой runtime-задаче (см. `TASK_TEMPLATE.md`) должно быть место для блока ниже:

## Release Evidence
- Commit:
- Push:
- Restart required: yes/no
- Restart scope:
- Commands executed:
- Post-release check:
- Notes:

---
## Release Policy
=== RELEASE POLICY ===

После каждой задачи со статусом Final Decision = Approved / Approved with notes:

A. Git policy
- должен быть git commit
- должен быть git push
- commit message должен включать Task ID
- формат:
  - `feat(TASK_ID): short description`
  - `fix(TASK_ID): short description`
  - `chore(TASK_ID): short description`
  - `docs(TASK_ID): short description`

B. Deploy policy
- если изменены только:
  - `project_ops/*`
  - `docs/*`
  - `tests/*`
  - `scripts/smoke_*`
  то deploy/restart НЕ нужен

- если изменены runtime-файлы, влияющие на выполнение сервисов:
  - `trading/*`
  - `short_pump/*`
  - runtime paths в `analytics/*`
  - worker/watcher/server/service code
  то нужен deploy/restart только затронутых сервисов

C. Restart scope policy
- не рестартовать “все подряд”
- определить правило:
  - если изменен watcher path -> restart watcher service
  - если изменен outcome worker path -> restart outcome worker service
  - если изменен server/api path -> restart api service
  - если изменено общее runtime ядро -> перечислить какие сервисы считаются зависимыми
- если scope неясен, задача не может считаться полностью закрытой без явного restart plan

D. Release evidence
- В каждой runtime-задаче в `TASK_TEMPLATE.md` должно быть место для:
  - commit hash
  - push done
  - restart command
  - restarted services
  - post-restart check result

E. Safety rule
- auto-push допустим
- auto-deploy/restart НЕ делать без явного Final Decision и restart scope
- для production-задач сначала commit/push, потом controlled restart
- если задача не runtime, release step = commit/push only

