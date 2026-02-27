from __future__ import annotations
from typing import Tuple, Optional
from distro import name
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

import requests

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

class GoogleDriveDocs:
    def __init__(self, sa_file: str):
        creds = service_account.Credentials.from_service_account_file(sa_file, scopes=SCOPES)
        self.drive = build("drive", "v3", credentials=creds)
        self.docs = build("docs", "v1", credentials=creds)

    def download_doc_as_text(self, doc_id: str) -> str:
        # export Google Doc -> text/plain
        request = self.drive.files().export_media(fileId=doc_id, mimeType="text/plain")
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue().decode("utf-8", errors="ignore")

    def create_folder(self, name: str, parent_folder_id: str = "root") -> str:
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id] if parent_folder_id else ["root"],
        }
        folder = self.drive.files().create(body=file_metadata, fields="id").execute()
        return folder["id"]

    def copy_doc(self, template_doc_id: str, name: str, folder_id: str) -> str:
        body = {"name": name, "parents": [folder_id]}
        copied = self.drive.files().copy(fileId=template_doc_id, body=body, fields="id").execute()
        return copied["id"]

    def replace_all_text(self, doc_id: str, placeholder: str, replacement: str) -> None:
        requests = [{
            "replaceAllText": {
                "containsText": {"text": placeholder, "matchCase": True},
                "replaceText": replacement
            }
        }]
        self.docs.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

    def export_doc_to_pdf_bytes(self, doc_id: str) -> bytes:
        request = self.drive.files().export_media(fileId=doc_id, mimeType="application/pdf")
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue()

    def upload_pdf(self, pdf_bytes: bytes, name: str, folder_id: str) -> str:
        from googleapiclient.http import MediaInMemoryUpload
        media = MediaInMemoryUpload(pdf_bytes, mimetype="application/pdf", resumable=False)
        metadata = {"name": name, "parents": [folder_id]}
        created = self.drive.files().create(body=metadata, media_body=media, fields="id").execute()
        return created["id"]
    
    def create_doc(self, name: str, parent_folder_id: str) -> str:
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.document",
            "parents": [parent_folder_id],
        }
        created = self.drive.files().create(body=file_metadata, fields="id").execute()
        return created["id"]
    
    def write_doc_text(self, doc_id: str, text: str) -> None:
        requests = [{
            "insertText": {
            "location": {"index": 1},
            "text": text
            }
        }]
        self.docs.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()