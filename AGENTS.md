Work only in the Notar-IA backend repository.

Rules:
- Read `outputs/_feedback_corpus/feedback_events.jsonl` and the latest `outputs/CASE-*/case_state.json` files before changing backend logic.
- Do not modify the frontend repo.
- Prefer small fixes in prompts, pipeline rules, parsers, validators, and regression tests.
- Run the narrowest relevant checks before changing deployment state.
- If a backend-only fix is safe and checks pass, you may rebuild and restart the backend service on the VPS.
- If git credentials exist, commit backend-only changes; otherwise summarize the manual push step.
