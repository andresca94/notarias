Work only in the Notar-IA backend repository.

Rules:
- Read `outputs/_feedback_corpus/feedback_events.jsonl` and the latest `outputs/CASE-*/case_state.json` files before changing backend logic.
- Do not modify the frontend repo.
- Do not create or modify personal workspace files such as `HEARTBEAT.md`, `IDENTITY.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `MEMORY.md`, or `memory/*`.
- Prefer small fixes in prompts, pipeline rules, parsers, validators, and regression tests.
- Before changing files, run `git status --short`. If the worktree is not clean, stop without changes.
- When the task or prompt says to modify only a specific file or small allowlist of files, treat that as a hard boundary. Abort instead of touching any other path.
- For smoke tests, prefer the exact requested file change over broader backend improvements.
- Run the narrowest relevant checks before changing deployment state.
- If a backend-only fix is safe and checks pass, you may rebuild and restart the backend service on the VPS.
- If git credentials exist, commit backend-only changes; otherwise summarize the manual push step.
