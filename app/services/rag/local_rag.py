# app/services/rag/local_rag.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

from app.core.config import settings
from app.services.rag_store import LocalRAGStore
from app.services.rag.knowledge_rag import KnowledgeRAG


def split_metadata_and_body(raw: str) -> Dict[str, str]:
    """
    Permite tener archivos .txt con dos formatos de separador:

    Formato 1 (legado):
      ---METADATA---  ...  ---BODY---  ...

    Formato 2 (templates de actos notariales):
      ==...==  ACTO: ...  ==...==  {Crear metadata...}  OTORGANTES: ...  {Cuerpo del Acto}  Compareció...

    Si no existe ningún separador, todo se va como contenido_legal.
    """
    if not raw:
        return {"metadata_especifica": "", "contenido_legal": ""}

    s = raw.strip()

    if "---BODY---" in s:
        parts = s.split("---BODY---", 1)
        meta = parts[0].replace("---METADATA---", "").strip()
        body = parts[1].strip()
        return {"metadata_especifica": meta, "contenido_legal": body}

    # Templates de actos notariales usan {Cuerpo del Acto} como separador de sección
    if "{Cuerpo del Acto}" in s:
        parts = s.split("{Cuerpo del Acto}", 1)
        meta = parts[0].strip()
        body = parts[1].strip()
        return {"metadata_especifica": meta, "contenido_legal": body}

    return {"metadata_especifica": "", "contenido_legal": s}


class LocalRAG:
    """
    RAG local basado en archivos .txt dentro de un directorio.
    Usa BM25 si está instalado (rank_bm25).
    """

    def __init__(self, store_dir: Optional[str] = None):
        # 1) toma de settings o env o default
        raw = store_dir or getattr(settings, "RAG_STORE_DIR", None) or os.getenv("RAG_STORE_DIR") or "rag_store"

        p = Path(raw)

        # 2) si es relativo, resuélvelo desde el root del repo (asumimos que uvicorn corre en el root)
        if not p.is_absolute():
            # intenta con BASE_DIR si existe
            base = getattr(settings, "BASE_DIR", None)
            if base:
                p = Path(base) / p
            else:
                p = Path.cwd() / p

        self.store_dir = p.resolve()

        if not self.store_dir.exists():
            raise RuntimeError(f"RAG_STORE_DIR no existe: {self.store_dir}")

        self.store = LocalRAGStore(str(self.store_dir))
        self.store.load()
        self.knowledge = KnowledgeRAG()

    def retrieve_acto_text(self, acto_nombre: str) -> str:
        """
        Retorna el texto del template más relevante para el acto (ej: COMPRAVENTA).
        """
        q = (acto_nombre or "").strip()
        if not q:
            return "TEXTO NO ENCONTRADO"

        hits = self.store.search_acto(q, top_k=1)
        if not hits:
            return "TEXTO NO ENCONTRADO"

        return hits[0]["text"] or "TEXTO NO ENCONTRADO"
