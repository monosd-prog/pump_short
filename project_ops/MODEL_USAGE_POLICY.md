# Model Usage Policy

This policy standardizes which model class should be used at each stage of work in Cursor for `pump_short`.

## Goal
- Keep high-stakes planning and review on a strong model.
- Keep implementation work on a coding-focused model.
- Keep low-risk utility work on a fast model.
- Make stage transitions explicit so users and agents follow the same process.

## Model Tiers

### Strong model
Use for:
- `PLAN`
- RCA / debugging analysis
- architecture and design
- QA review
- Production Safety review
- Final Decision

### Mid coding model
Use for:
- `AGENT` implementation
- test harnesses
- ETL and audit scripts
- localized bugfixes

### Cheap / fast model
Use for:
- log parsing
- report generation
- markdown / doc cleanup
- repetitive low-risk utility tasks

## Required Routing Rules
- `Auto` mode is allowed only for low-risk utility work.
- For high-stakes planning, review, and safety work, use an explicit strong model, not `Auto`.
- Before a stage transition, the user must manually switch the model according to this policy:
  - `PLAN` -> strong model
  - `AGENT` implementation -> mid coding model
  - `QA` / `Safety` -> strong model

## Practical Defaults
- If the task may change architecture, data contracts, risk policy, or release readiness, use the strong model.
- If the task is a narrow code change or script implementation with a known Spec, use the mid coding model.
- If the task is only reading logs or producing a report/markdown artifact, use the fast model.

## Recommended Mapping
- `PLAN`: strong
- `AGENT`: mid coding
- `QA / Validation`: strong
- `Production Safety`: strong
- logs / report / doc utility: fast

## Notes
- This is a process policy, not an automatic Cursor routing feature.
- If a task starts in one stage and expands into another, switch to the model required by the new stage before continuing.
- When in doubt, prefer the stronger model for safety and correctness.
