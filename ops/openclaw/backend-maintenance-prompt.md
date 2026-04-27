# Notar-IA Backend Maintenance Prompt

Work only inside the isolated Notar-IA backend maintenance workspace. Do not use the live deployment checkout for edits.

Rules:

1. Read the latest case state files and feedback comment files from the absolute outputs root path supplied by the trigger before proposing backend changes.
2. Read the feedback corpus JSONL from that same absolute outputs root when it exists, and prioritize repeated correction patterns across cases and iterations.
3. Look for repeated legal drafting failures, placeholder leakage, and feedback patterns from reviewed Word files.
4. Do not edit the frontend repo.
5. Prefer focused fixes in the pipeline, parsers, prompts, validation rules, and regression tests.
6. Before changing files, verify `git status --short` is clean. If it is not clean, stop without changes.
7. Work on the isolated maintenance branch configured by the trigger. Do not dirty unrelated files or create workspace-noise files.
8. Run the narrowest relevant checks before proposing or applying a deploy.
9. The main goal of this maintenance path is to turn expert Word feedback into a real backend improvement whenever there is any safe generalizable rule, parser, prompt, validation, or regression fix to apply.
10. Do not use `skipped` as the default outcome. If even one relevant comment can become a small backend fix plus a focused regression, prefer that over a no-op.
11. Use `skipped` only when all relevant comments are purely case-specific, depend on non-generalizable facts, or are already covered by the current backend without a meaningful change.
12. If the change is safe and checks pass, you may update the backend maintenance workspace on the VPS.
13. If git credentials are configured, commit backend-only changes with a narrow message. When the trigger explicitly authorizes it, you may also push to `origin main`.
14. When the trigger explicitly authorizes deploy, pull the pushed commit into the live deployment checkout, rebuild and restart only the backend service, then verify `http://127.0.0.1:8080/docs`.
15. The trigger may include an exact backend maintenance callback command. Use it to report `completed`, `skipped`, or `failed`; never claim completion before the deploy verification succeeds.
16. If the change is risky, the feedback is ambiguous, or the worktree contains unexpected files, stop and summarize the uncertainty instead of guessing.
