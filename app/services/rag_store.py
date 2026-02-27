from __future__ import annotations
from typing import Dict, List, Tuple
from pathlib import Path

try:
    from rank_bm25 import BM25Okapi
except Exception:
    BM25Okapi = None

class LocalRAGStore:
    """
    RAG local: carga .txt de rag_store y recupera el más relevante por nombre/keyword.
    Para tu caso (actos), normalmente basta con matching de palabras.
    """
    def __init__(self, rag_dir: str):
        self.dir = Path(rag_dir)
        self.docs: List[Tuple[str, str]] = []  # (filename, text)
        self._bm25 = None
        self._tokens = None

    def load(self) -> None:
        self.docs = []
        for p in sorted(self.dir.glob("*.txt")):
            self.docs.append((p.name, p.read_text(encoding="utf-8", errors="ignore")))
        if BM25Okapi and self.docs:
            self._tokens = [self._tokenize(t) for _, t in self.docs]
            self._bm25 = BM25Okapi(self._tokens)

    def _tokenize(self, s: str) -> List[str]:
        s = s.lower()
        for ch in ",.;:()[]{}<>/\\\n\r\t":
            s = s.replace(ch, " ")
        return [w for w in s.split(" ") if w.strip()]

    def search_acto(self, acto_query: str, top_k: int = 1) -> List[Dict]:
        if not self.docs:
            self.load()
        if not self.docs:
            return []

        q = acto_query.strip().lower()
        # heurística rápida: si el query está en el nombre de archivo, prioridad máxima
        exact = []
        for fn, txt in self.docs:
            if q.replace(" ", "_") in fn.lower():
                exact.append({"file": fn, "text": txt, "score": 999.0})
        if exact:
            return exact[:top_k]

        if self._bm25:
            qtok = self._tokenize(q)
            scores = self._bm25.get_scores(qtok)
            ranked = sorted(list(enumerate(scores)), key=lambda x: x[1], reverse=True)[:top_k]
            out = []
            for idx, sc in ranked:
                fn, txt = self.docs[idx]
                out.append({"file": fn, "text": txt, "score": float(sc)})
            return out

        # fallback: substring
        out = []
        for fn, txt in self.docs:
            sc = 1.0 if q in txt.lower() else 0.0
            out.append({"file": fn, "text": txt, "score": sc})
        out.sort(key=lambda d: d["score"], reverse=True)
        return out[:top_k]
