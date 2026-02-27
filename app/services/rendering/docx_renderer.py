# app/services/rendering/docx_renderer.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from docx import Document


def _replace_in_paragraph(paragraph, key: str, value: str) -> None:
    """
    Reemplaza texto aunque el placeholder esté partido en runs.
    """
    if key not in paragraph.text:
        return

    # junta runs
    full = "".join(run.text for run in paragraph.runs)
    if key not in full:
        return

    full = full.replace(key, value)

    # borra runs y deja uno nuevo
    for run in paragraph.runs:
        run.text = ""
    paragraph.runs[0].text = full


def _insert_paragraphs_after(paragraph, blocks: List[str]) -> None:
    """
    Inserta bloques como párrafos después de 'paragraph'.
    """
    parent = paragraph._p.getparent()
    idx = parent.index(paragraph._p)

    # creamos párrafos en un Document temporal para clonar estilo básico
    doc = paragraph.part.document
    for b in blocks:
        new_p = doc.add_paragraph(b)
        # movemos el XML del final al lugar correcto
        p_xml = new_p._p
        parent.remove(p_xml)
        parent.insert(idx + 1, p_xml)
        idx += 1


def render_docx(template_path: str, context: Dict, out_docx_path: str) -> str:
    """
    Render DOCX “notarial-friendly”.
    - Reemplaza placeholders simples tipo {{RADICADO}} en todo el documento.
    - Para {{CONTENIDO_IA}}: inserta párrafos reales (split por doble salto).
    """
    template_path = str(template_path)
    out_docx_path = str(out_docx_path)

    doc = Document(template_path)

    # 1) placeholders simples ({{RADICADO}}, {{COMENTARIO}}, etc.)
    simple_keys = []
    for k, v in context.items():
        if k == "CONTENIDO_IA":
            continue
        simple_keys.append((f"{{{{{k}}}}}", "" if v is None else str(v)))
        simple_keys.append((f"{{{{ {k} }}}}", "" if v is None else str(v)))

    # 2) contenido IA como párrafos
    contenido = context.get("CONTENIDO_IA") or ""
    contenido = str(contenido).replace("\r\n", "\n").replace("\r", "\n").strip()
    blocks = [b.strip() for b in contenido.split("\n\n") if b.strip()]

    for p in doc.paragraphs:
        # reemplaza simples
        for ph, val in simple_keys:
            if ph in p.text:
                _replace_in_paragraph(p, ph, val)

    # Busca el párrafo donde está el placeholder de contenido
    placeholder_found = False
    for p in doc.paragraphs:
        if "{{CONTENIDO_IA}}" in p.text or "{{ CONTENIDO_IA }}" in p.text:
            placeholder_found = True
            # limpia el placeholder en ese párrafo
            _replace_in_paragraph(p, "{{CONTENIDO_IA}}", "")
            _replace_in_paragraph(p, "{{ CONTENIDO_IA }}", "")
            # inserta párrafos después
            _insert_paragraphs_after(p, blocks)
            break

    if not placeholder_found:
        # fallback: si no existe placeholder, lo agrega al final
        doc.add_paragraph("")
        for b in blocks:
            doc.add_paragraph(b)

    Path(out_docx_path).parent.mkdir(parents=True, exist_ok=True)
    doc.save(out_docx_path)
    return out_docx_path
