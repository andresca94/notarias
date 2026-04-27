import json
from pathlib import Path

from app.services.case_manager import compose_iteration_commentary, save_case_state


def test_compose_iteration_commentary_includes_word_feedback(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    case_dir = tmp_path / "outputs" / "CASE-26485"
    feedback_dir = case_dir / "iterations" / "1" / "feedback"
    feedback_dir.mkdir(parents=True, exist_ok=True)

    comments_path = feedback_dir / "comments.json"
    comments_path.write_text(
        json.dumps(
            [
                {
                    "comment_id": "0",
                    "comment_text": "No identifica a las partes : comprador y vendedor",
                    "anchor_text": "COMPRAVENTA DE BIENES INMUEBLES",
                    "paragraph_text": "ACTO 1: COMPRAVENTA DE BIENES INMUEBLES",
                    "resolved": False,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    state = {
        "radicado": "26485",
        "status": "feedback_uploaded",
        "latest_iteration": 1,
        "base_comentario": "Cliente solicita entrega inmediata.",
        "template_id": None,
        "created_at": "2026-04-26T00:00:00Z",
        "updated_at": "2026-04-26T00:00:00Z",
        "inputs": {"scanner_files": [], "documentos_files": []},
        "iterations": {
            "1": {
                "iteration": 1,
                "status": "feedback_uploaded",
                "created_at": "2026-04-26T00:00:00Z",
                "updated_at": "2026-04-26T00:00:00Z",
                "source_comentario": "Cliente solicita entrega inmediata.",
                "artifacts": {},
                "feedback": {
                    "reviewed_docx_path": "iterations/1/feedback/reviewed.docx",
                    "comments_json_path": "iterations/1/feedback/comments.json",
                    "comments_count": 1,
                    "uploaded_at": "2026-04-26T00:00:00Z",
                },
            }
        },
    }
    save_case_state(state)

    composed = compose_iteration_commentary(state)

    assert "Cliente solicita entrega inmediata." in composed
    assert "No identifica a las partes : comprador y vendedor" in composed
    assert "COMPRAVENTA DE BIENES INMUEBLES" in composed
