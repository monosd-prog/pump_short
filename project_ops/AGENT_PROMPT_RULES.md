# How to prompt AGENT safely

Всегда использовать формат:

"Реализуй строго задачу из TASK_<ID>.md

Ограничения:
- не выходи за рамки Spec
- не изменяй файлы вне Inputs
- не добавляй новую логику без явного указания
- если видишь потенциальное улучшение — опиши, но не внедряй

Перед началом:
- кратко опиши план изменений
- проверь `project_ops/MODEL_USAGE_POLICY.md` и используй модель, соответствующую текущей стадии

После:
- покажи diff
- объясни, что изменено
- укажи возможные риски"

## Model Selection
- `PLAN` stage must use the strong model.
- `AGENT` implementation should prefer the mid coding model.
- `QA` / `Safety` stages must return to the strong model.
- `Auto` mode допустим только для low-risk utility work, logs parsing, report generation и markdown/doc cleanup.

