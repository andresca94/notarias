from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile


CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="xml" ContentType="application/xml"/>
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/comments.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"/>
  <Override PartName="/word/commentsIds.xml" ContentType="application/vnd.ms-word.commentsIds+xml"/>
  <Override PartName="/word/commentsExtended.xml" ContentType="application/vnd.ms-word.commentsExtended+xml"/>
</Types>
"""


RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"></Relationships>
"""


def write_feedback_docx(
    path: Path,
    *,
    comment_text: str = "No identifica a las partes",
    paragraph_text: str = "ACTO 1: COMPRAVENTA DE BIENES INMUEBLES",
    anchor_text: str = "COMPRAVENTA DE BIENES INMUEBLES",
    resolved: bool = False,
    with_anchor: bool = True,
) -> Path:
    if with_anchor:
        document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r><w:t>ACTO 1: </w:t></w:r>
      <w:commentRangeStart w:id="0"/>
      <w:r><w:t>{anchor_text}</w:t></w:r>
      <w:commentRangeEnd w:id="0"/>
      <w:r><w:commentReference w:id="0"/></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>{paragraph_text}</w:t></w:r>
    </w:p>
  </w:body>
</w:document>
"""
    else:
        document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r><w:t>{paragraph_text}</w:t></w:r>
      <w:r><w:commentReference w:id="0"/></w:r>
    </w:p>
  </w:body>
</w:document>
"""

    comments_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:comments xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:comment w:id="0" w:author="Margarita" w:date="2026-03-12T13:33:00Z">
    <w:p><w:r><w:t>{comment_text}</w:t></w:r></w:p>
  </w:comment>
</w:comments>
"""

    comments_ids_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w16cid:commentsIds xmlns:w16cid="http://schemas.microsoft.com/office/word/2016/wordml/cid">
  <w16cid:commentId w16cid:paraId="23A65762" w16cid:durableId="451DB726"/>
</w16cid:commentsIds>
"""

    comments_extended_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w15:commentsEx xmlns:w15="http://schemas.microsoft.com/office/word/2012/wordml">
  <w15:commentEx w15:paraId="23A65762" w15:done="{1 if resolved else 0}"/>
</w15:commentsEx>
"""

    with ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", CONTENT_TYPES)
        archive.writestr("_rels/.rels", RELS)
        archive.writestr("word/document.xml", document_xml)
        archive.writestr("word/comments.xml", comments_xml)
        archive.writestr("word/commentsIds.xml", comments_ids_xml)
        archive.writestr("word/commentsExtended.xml", comments_extended_xml)
    return path
