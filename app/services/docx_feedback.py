from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET
from zipfile import ZipFile


NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "w15": "http://schemas.microsoft.com/office/word/2012/wordml",
    "w16cid": "http://schemas.microsoft.com/office/word/2016/wordml/cid",
}


@dataclass
class CommentAnchor:
    comment_id: str
    anchor_text: str
    paragraph_text: str


def _text_from_node(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    parts = [text.text or "" for text in node.findall(".//w:t", NS)]
    return "".join(parts).strip()


def _comment_order_meta(archive: ZipFile) -> List[Dict[str, Any]]:
    comments_root = ET.fromstring(archive.read("word/comments.xml"))
    comments = comments_root.findall("w:comment", NS)

    para_ids: List[Optional[str]] = []
    if "word/commentsIds.xml" in archive.namelist():
        ids_root = ET.fromstring(archive.read("word/commentsIds.xml"))
        for node in list(ids_root):
            para_ids.append(node.attrib.get(f"{{{NS['w16cid']}}}paraId"))

    resolved_map: Dict[str, bool] = {}
    if "word/commentsExtended.xml" in archive.namelist():
        ext_root = ET.fromstring(archive.read("word/commentsExtended.xml"))
        for node in list(ext_root):
            para_id = node.attrib.get(f"{{{NS['w15']}}}paraId")
            if not para_id:
                continue
            resolved_map[para_id] = node.attrib.get(f"{{{NS['w15']}}}done", "0") == "1"

    items: List[Dict[str, Any]] = []
    for idx, comment in enumerate(comments):
        para_id = para_ids[idx] if idx < len(para_ids) else None
        items.append(
            {
                "comment_id": comment.attrib.get(f"{{{NS['w']}}}id", str(idx)),
                "author": comment.attrib.get(f"{{{NS['w']}}}author", ""),
                "date": comment.attrib.get(f"{{{NS['w']}}}date", ""),
                "comment_text": _text_from_node(comment),
                "resolved": bool(resolved_map.get(para_id, False)) if para_id else False,
            }
        )
    return items


def _extract_document_anchors(archive: ZipFile) -> Dict[str, CommentAnchor]:
    document_root = ET.fromstring(archive.read("word/document.xml"))
    anchors: Dict[str, CommentAnchor] = {}

    active_ranges: List[str] = []
    for paragraph in document_root.findall(".//w:p", NS):
        paragraph_text = _text_from_node(paragraph)
        paragraph_anchors: Dict[str, List[str]] = {}

        for element in paragraph.iter():
            tag = element.tag
            if tag == f"{{{NS['w']}}}commentRangeStart":
                comment_id = element.attrib.get(f"{{{NS['w']}}}id")
                if comment_id:
                    active_ranges.append(comment_id)
                    paragraph_anchors.setdefault(comment_id, [])
            elif tag == f"{{{NS['w']}}}commentRangeEnd":
                comment_id = element.attrib.get(f"{{{NS['w']}}}id")
                if comment_id and comment_id in active_ranges:
                    active_ranges = [value for value in active_ranges if value != comment_id]
            elif tag == f"{{{NS['w']}}}commentReference":
                comment_id = element.attrib.get(f"{{{NS['w']}}}id")
                if comment_id:
                    paragraph_anchors.setdefault(comment_id, [])
                    anchor_text = "".join(paragraph_anchors.get(comment_id, [])).strip()
                    anchors.setdefault(
                        comment_id,
                        CommentAnchor(
                            comment_id=comment_id,
                            anchor_text=anchor_text or paragraph_text,
                            paragraph_text=paragraph_text,
                        ),
                    )
            elif tag == f"{{{NS['w']}}}t":
                text = element.text or ""
                for comment_id in active_ranges:
                    paragraph_anchors.setdefault(comment_id, []).append(text)

        for comment_id, text_parts in paragraph_anchors.items():
            if comment_id not in anchors:
                anchor_text = "".join(text_parts).strip()
                anchors[comment_id] = CommentAnchor(
                    comment_id=comment_id,
                    anchor_text=anchor_text or paragraph_text,
                    paragraph_text=paragraph_text,
                )

    return anchors


def parse_docx_comments(path: str | Path) -> List[Dict[str, Any]]:
    docx_path = Path(path)
    with ZipFile(docx_path) as archive:
        if "word/comments.xml" not in archive.namelist():
            raise ValueError("El archivo DOCX no contiene comentarios de Word.")

        comment_meta = _comment_order_meta(archive)
        if not comment_meta:
            raise ValueError("El archivo DOCX no contiene comentarios de Word.")

        anchors = _extract_document_anchors(archive)

    comments: List[Dict[str, Any]] = []
    for item in comment_meta:
        anchor = anchors.get(item["comment_id"])
        comments.append(
            {
                "comment_id": item["comment_id"],
                "author": item["author"],
                "date": item["date"],
                "anchor_text": (anchor.anchor_text if anchor else "") or "",
                "paragraph_text": (anchor.paragraph_text if anchor else "") or "",
                "comment_text": item["comment_text"],
                "resolved": bool(item["resolved"]),
            }
        )

    if not comments:
        raise ValueError("El archivo DOCX no contiene comentarios de Word.")

    return comments
