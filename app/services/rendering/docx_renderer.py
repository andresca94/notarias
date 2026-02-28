# app/services/rendering/docx_renderer.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import RGBColor


# ── Colores notariales ──────────────────────────────────────────────────────
_COLOR_PENDIENTE = RGBColor(0xCC, 0x00, 0x00)  # Rojo  → [[PENDIENTE: ...]]
_COLOR_VARIABLE  = RGBColor(0xE6, 0x4A, 0x00)  # Naranja → [[VARIABLE_LLENA]]

# ── Patrones regex ───────────────────────────────────────────────────────────
# Variables notariales [[...]]
_VAR_RE = re.compile(r'(\[\[.*?\]\])', re.DOTALL)

# Secuencias de 2+ palabras totalmente en MAYÚSCULAS (nombres/entidades)
# Acepta letras latinas con tilde. No matchea palabras sueltas (requiere ≥2).
_CAPS_SEQ = re.compile(
    r'[A-ZÁÉÍÓÚÜÑ]{2,}(?:\s[A-ZÁÉÍÓÚÜÑ]{2,})+'
)

# Línea de encabezado: "CAMPO: valor" (etiqueta en mayúsculas seguida de dos puntos)
_HEADER_LINE_RE = re.compile(r'^[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ0-9 \(\)\/]{1,50}:\s')


def _is_header_block(block: str) -> bool:
    """True si ≥60% de las líneas son del tipo 'CAMPO: valor' (bloque de metadatos/encabezado)."""
    lines = [l.strip() for l in block.split('\n') if l.strip()]
    if len(lines) < 2:
        return False
    n = sum(1 for l in lines if _HEADER_LINE_RE.match(l))
    return n >= len(lines) * 0.6


def _is_title_line(line: str) -> bool:
    """True si la línea es un título de sección corto predominantemente en mayúsculas.
    Ejemplos: 'OTORGAMIENTO y AUTORIZACIÓN', 'DE LA CAPACIDAD:', 'DERECHOS NOTARIALES:'
    """
    s = line.strip()
    if not s or len(s) > 100:
        return False
    letters = [c for c in s if c.isalpha()]
    if len(letters) < 3:
        return False
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    return upper_ratio >= 0.65 and len(s.split()) <= 10


# ── Helpers de formato ───────────────────────────────────────────────────────

def _add_line_to_para(para, line: str) -> None:
    """
    Agrega una línea al párrafo con:
    - Variables [[...]] coloreadas (rojo para PENDIENTE, naranja para llenas).
    - Secuencias en MAYÚSCULAS (nombres / entidades) en negrita.
    - Resto del texto en formato normal.
    """
    segments = _VAR_RE.split(line)
    for seg in segments:
        if not seg:
            continue
        # ¿Es una variable notarial?
        if re.fullmatch(r'\[\[.*?\]\]', seg, re.DOTALL):
            run = para.add_run(seg)
            run.bold = True
            if "PENDIENTE" in seg.upper():
                run.font.color.rgb = _COLOR_PENDIENTE
            else:
                run.font.color.rgb = _COLOR_VARIABLE
        else:
            # Texto normal: bold para secuencias en MAYÚSCULAS
            pos = 0
            for m in _CAPS_SEQ.finditer(seg):
                before = seg[pos:m.start()]
                if before:
                    para.add_run(before)
                run = para.add_run(m.group())
                run.bold = True
                pos = m.end()
            tail = seg[pos:]
            if tail:
                para.add_run(tail)


def _insert_table_after(paragraph, text_block: str) -> None:
    """
    Crea una tabla de una columna (sin encabezado) donde cada línea del bloque es una fila.
    La tabla se inserta en el XML inmediatamente después de 'paragraph'.
    """
    lines = [
        l for l in text_block.split("\n")
        if l.strip() not in ("###TABLE_START###", "###TABLE_END###")
    ]
    if not lines:
        return

    parent = paragraph._p.getparent()
    idx = list(parent).index(paragraph._p)

    doc = paragraph.part.document
    table = doc.add_table(rows=len(lines), cols=1)
    try:
        table.style = "Table Grid"
    except Exception:
        pass

    for i, row_text in enumerate(lines):
        cell = table.rows[i].cells[0]
        cell.text = ""
        para = cell.paragraphs[0]
        para.alignment = WD_ALIGN_PARAGRAPH.LEFT
        if _is_title_line(row_text):
            run = para.add_run(row_text.strip())
            run.bold = True
        else:
            _add_line_to_para(para, row_text)

    # Mover la tabla al lugar correcto (después del párrafo de anclaje)
    tbl_xml = table._tbl
    parent.remove(tbl_xml)
    parent.insert(idx + 1, tbl_xml)


def _insert_paragraphs_after(paragraph, blocks: List[str]) -> None:
    """
    Inserta bloques como párrafos notariales después de 'paragraph'.
    - Bloques de encabezado ('CAMPO: valor'): alineación LEFT, etiquetas en negrita.
    - Líneas de título (cortas, >65% mayúsculas): negrita completa.
    - Resto: justificado (izquierda y derecha).
    - Los saltos simples (\\n) se preservan como saltos de línea Word.
    """
    parent = paragraph._p.getparent()
    idx = parent.index(paragraph._p)
    doc = paragraph.part.document

    for b in blocks:
        # Bloque de tabla UIAF: crear tabla de una columna en lugar de párrafo
        if b.strip().startswith("###TABLE_START###"):
            # Insertar párrafo vacío de anclaje y luego la tabla
            anchor = doc.add_paragraph()
            anchor_xml = anchor._p
            parent.remove(anchor_xml)
            parent.insert(idx + 1, anchor_xml)
            idx += 1
            _insert_table_after(anchor, b)
            idx += 1
            continue

        new_p = doc.add_paragraph()
        is_header = _is_header_block(b)
        new_p.alignment = WD_ALIGN_PARAGRAPH.LEFT if is_header else WD_ALIGN_PARAGRAPH.JUSTIFY

        lines = b.split('\n')
        for i, line in enumerate(lines):
            if i > 0:
                # Salto de línea dentro del mismo párrafo Word
                br_run = new_p.add_run()
                br_run.add_break()
            # Línea de título: bold completo sin procesamiento adicional
            if not is_header and _is_title_line(line):
                run = new_p.add_run(line.strip())
                run.bold = True
            else:
                _add_line_to_para(new_p, line)

        # Mover el XML al lugar correcto
        p_xml = new_p._p
        parent.remove(p_xml)
        parent.insert(idx + 1, p_xml)
        idx += 1


def _replace_in_paragraph(paragraph, key: str, value: str) -> None:
    """
    Reemplaza texto aunque el placeholder esté partido en runs.
    """
    if key not in paragraph.text:
        return

    full = "".join(run.text for run in paragraph.runs)
    if key not in full:
        return

    full = full.replace(key, value)

    for run in paragraph.runs:
        run.text = ""
    paragraph.runs[0].text = full


def render_docx(template_path: str, context: Dict, out_docx_path: str) -> str:
    """
    Render DOCX "notarial-friendly".
    - Reemplaza placeholders simples tipo {{RADICADO}} en todo el documento.
    - Para {{CONTENIDO_IA}}: inserta párrafos justificados con formato notarial:
        · Variables [[...]] coloreadas.
        · Nombres / entidades en MAYÚSCULAS en negrita.
        · Texto justificado.
    """
    template_path = str(template_path)
    out_docx_path = str(out_docx_path)

    doc = Document(template_path)

    # 1) Placeholders simples ({{RADICADO}}, {{COMENTARIO}}, etc.)
    simple_keys = []
    for k, v in context.items():
        if k == "CONTENIDO_IA":
            continue
        simple_keys.append((f"{{{{{k}}}}}", "" if v is None else str(v)))
        simple_keys.append((f"{{{{ {k} }}}}", "" if v is None else str(v)))

    # 2) Contenido IA → párrafos
    contenido = context.get("CONTENIDO_IA") or ""
    contenido = str(contenido).replace("\r\n", "\n").replace("\r", "\n").strip()
    blocks = [b.strip() for b in contenido.split("\n\n") if b.strip()]

    for p in doc.paragraphs:
        for ph, val in simple_keys:
            if ph in p.text:
                _replace_in_paragraph(p, ph, val)

    # Busca el párrafo donde está el placeholder de contenido
    placeholder_found = False
    for p in doc.paragraphs:
        if "{{CONTENIDO_IA}}" in p.text or "{{ CONTENIDO_IA }}" in p.text:
            placeholder_found = True
            _replace_in_paragraph(p, "{{CONTENIDO_IA}}", "")
            _replace_in_paragraph(p, "{{ CONTENIDO_IA }}", "")
            _insert_paragraphs_after(p, blocks)
            break

    if not placeholder_found:
        doc.add_paragraph("")
        for b in blocks:
            if b.strip().startswith("###TABLE_START###"):
                anchor = doc.add_paragraph()
                _insert_table_after(anchor, b)
                continue
            new_p = doc.add_paragraph()
            is_header = _is_header_block(b)
            new_p.alignment = WD_ALIGN_PARAGRAPH.LEFT if is_header else WD_ALIGN_PARAGRAPH.JUSTIFY
            lines = b.split('\n')
            for i, line in enumerate(lines):
                if i > 0:
                    br_run = new_p.add_run()
                    br_run.add_break()
                if not is_header and _is_title_line(line):
                    run = new_p.add_run(line.strip())
                    run.bold = True
                else:
                    _add_line_to_para(new_p, line)

    Path(out_docx_path).parent.mkdir(parents=True, exist_ok=True)
    doc.save(out_docx_path)
    return out_docx_path
