from pathlib import Path
from zipfile import ZipFile

import pytest

from app.services.docx_feedback import parse_docx_comments
from tests.helpers_docx import write_feedback_docx


def test_parse_docx_comments_extracts_anchor_and_resolved(tmp_path: Path):
    docx_path = write_feedback_docx(
        tmp_path / "reviewed.docx",
        comment_text="No identifica a las partes",
        paragraph_text="ACTO 1: COMPRAVENTA DE BIENES INMUEBLES",
        anchor_text="COMPRAVENTA DE BIENES INMUEBLES",
        resolved=True,
    )

    comments = parse_docx_comments(docx_path)

    assert len(comments) == 1
    assert comments[0]["comment_text"] == "No identifica a las partes"
    assert comments[0]["anchor_text"] == "COMPRAVENTA DE BIENES INMUEBLES"
    assert comments[0]["paragraph_text"] == "ACTO 1: COMPRAVENTA DE BIENES INMUEBLES"
    assert comments[0]["resolved"] is True


def test_parse_docx_comments_falls_back_to_paragraph_text(tmp_path: Path):
    docx_path = write_feedback_docx(
        tmp_path / "reviewed.docx",
        comment_text="Modificó el formato",
        paragraph_text="FOLIO DE MATRÍCULA INMOBILIARIA: 300-62293",
        with_anchor=False,
    )

    comments = parse_docx_comments(docx_path)

    assert comments[0]["anchor_text"] == "FOLIO DE MATRÍCULA INMOBILIARIA: 300-62293"
    assert comments[0]["paragraph_text"] == "FOLIO DE MATRÍCULA INMOBILIARIA: 300-62293"


def test_parse_docx_comments_rejects_docx_without_comments(tmp_path: Path):
    docx_path = tmp_path / "empty.docx"
    with ZipFile(docx_path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"></Types>
""",
        )

    with pytest.raises(ValueError, match="no contiene comentarios"):
        parse_docx_comments(docx_path)
