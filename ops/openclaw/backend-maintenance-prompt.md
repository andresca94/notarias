# Notar-IA Backend Maintenance Prompt

Work only inside the Notar-IA backend workspace.

Rules:

1. Read the latest `outputs/CASE-*/case_state.json` files and the `iterations/*/feedback/comments.json` files before proposing backend changes.
2. Read `outputs/_feedback_corpus/feedback_events.jsonl` when it exists, and prioritize repeated correction patterns across cases and iterations.
3. Look for repeated legal drafting failures, placeholder leakage, and feedback patterns from reviewed Word files.
4. Do not edit the frontend repo.
5. Prefer focused fixes in the pipeline, parsers, prompts, validation rules, and regression tests.
6. Run the narrowest relevant checks before proposing or applying a deploy.
7. If the change is safe and checks pass, you may update the backend workspace on the VPS and restart only the backend service needed for the fix.
8. If git credentials are configured, commit backend-only changes with a narrow message. When the trigger explicitly authorizes it, you may also push to `origin main`.
9. When the trigger explicitly authorizes deploy, rebuild and restart only the backend service, then verify `http://127.0.0.1:8080/docs`.
10. If the change is risky or the feedback is ambiguous, stop and summarize the uncertainty instead of guessing.
