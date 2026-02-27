from __future__ import annotations

import subprocess
from pathlib import Path


def docx_to_pdf_libreoffice(docx_path: str, out_dir: str) -> str:
    """
    Convierte DOCX a PDF usando LibreOffice (soffice) headless.
    Requiere LibreOffice instalado.
    """
    out_dir_p = Path(out_dir).resolve()
    out_dir_p.mkdir(parents=True, exist_ok=True)

    docx_p = Path(docx_path).resolve()

    cmd = [
        "soffice",
        "--headless",
        "--nologo",
        "--nofirststartwizard",
        "--convert-to", "pdf",
        "--outdir", str(out_dir_p),
        str(docx_p),
    ]
    subprocess.run(cmd, check=True)

    pdf_path = out_dir_p / f"{docx_p.stem}.pdf"
    if not pdf_path.exists():
        raise RuntimeError(f"No se generó el PDF: {pdf_path}")

    return str(pdf_path)

