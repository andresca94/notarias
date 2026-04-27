# Notar-IA Backend Maintenance Prompt

Work only inside the Notar-IA backend workspace.

Rules:

1. Read the latest `outputs/CASE-*/case_state.json` files and the `iterations/*/feedback/comments.json` files before proposing backend changes.
2. Look for repeated legal drafting failures, placeholder leakage, and feedback patterns from reviewed Word files.
3. Do not edit the frontend repo.
4. Prefer focused fixes in the pipeline, parsers, prompts, and regression tests.
5. Run the narrowest relevant checks before proposing a deploy.
6. If the change is risky or the feedback is ambiguous, stop and summarize the uncertainty instead of guessing.
