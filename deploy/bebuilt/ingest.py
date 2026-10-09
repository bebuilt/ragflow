"""Ingestion worker: one client's confirmed selection → that client's RAGFlow dataset. Runs on the box.

    python3 deploy/bebuilt/ingest.py          # one pass: plan, send, check parsing (systemd timer runs this)

Config, all this client's own (the box leaves our hands; nothing here reaches another client):
    /etc/bebuilt/worker.env         WORKER_DB_URL (worker_<slug>: RLS admits this org's rows only),
                                    ORG_ID, COMPOSIO_API_KEY, COMPOSIO_USER_ID (only for a connection that
                                    records no identity of its own), ONYX_TEXT_DIR (optional, below)

Stores are Google Drive or Dropbox (2026-09-28). A Dropbox store is keyed by path (`external_id = path_lower`) and
read through Composio's proxy to the raw API, because Composio's Dropbox tools cannot continue a listing or download.

Each person adds folders from their own Drive into the one shared index (2026-09-22), so a store can have several
connections, each under its own Composio identity. Everything here is keyed by connection: its roots, its changes
token, what it downloads. A file two people can reach is ONE document, owned by one connection (`documents.
connection_id`); a live owner is kept, otherwise the document goes to a connection that sees it. With more than
one connection, nothing is removed except by a pass that walks every live connection completely and finds the file
under none of them; a document whose owner isn't live is left alone.
    /etc/bebuilt/ragflow-tenant.json  api_key, dataset_id (written by tenant-setup.py)

A pass:
  1. plan   what the confirmed selection holds now. Once a day (and whenever the ticked folders change) it
            walks every folder in full: a new file or a moved revision becomes `pending`, and a file gone
            from a COMPLETE walk is removed from RAGFlow. An incomplete or failed walk never removes
            anything. Every pass in between asks Drive what CHANGED since the last one, which is one or two
            calls whatever the size of the selection — a 1,300-folder selection walked every five minutes
            would be ~374,000 Composio calls a day (Molzer, 2026-09-21).
  2. send   pending → download through Composio (Google Docs/Sheets/Slides exported to Office formats) →
            upload to RAGFlow, tag with its source, start parsing. The old copy goes first on a revision move.
  0. reconcile  our records against everything RAGFlow holds: orphans deleted, finished parses indexed,
                failed or stalled ones retried (it runs first, so a pass starts from what is actually there).
"""
import csv
import fcntl
import io
import json
import mimetypes
import os
import re
import sys
import time
import urllib.parse
import zipfile
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

import psycopg
import requests

RAGFLOW = "http://127.0.0.1:8080/api/v1"
COMPOSIO = "https://backend.composio.dev/api/v3.1"
COMPOSIO_PROXY = "https://backend.composio.dev/api/v3/tools/execute/proxy"  # verified against CDC's team, 2026-09-28
DROPBOX_API = "https://api.dropboxapi.com/2"
DROPBOX_CONTENT = "https://content.dropboxapi.com/2"
# Connection drops, 429s and 5xx from the proxy (or from Dropbox behind it) are retried: Onyx's Dropbox connector
# lost whole walks to a single dropped connection until it retried. Seconds, doubled each try.
PROXY_TRIES = 5
BACKOFF = 2
DRIVE_TOOLS_VERSION = "20260915_00"  # keep in step with bebuilt-app src/lib/storage/googledrive.ts
BATCH = 100  # files sent per pass
# Keep RAGFlow fed but never buried. Sending everything at once made Molzer's queue 2,337 deep, which made
# the priority order meaningless and left the executor grinding phantom entries after a cancel; sending 25 a
# pass then starved a box that had become fast (0.8% CPU with 5,900 files waiting, 2026-09-21). So a pass
# sends up to BATCH files, and sends nothing at all while RAGFlow still holds QUEUE_HIGH of them.
QUEUE_HIGH = 60
MAX_BYTES = 100 * 1024 * 1024
MAX_ATTEMPTS = 3
# RAGFlow runs its vision pipeline (layout detection and OCR, ~6 s a page) over every PDF page, even pages
# whose text is already in the file. Half a real client's PDFs have a text layer (Molzer, 2026-09-21), and
# reading it takes milliseconds. So every PDF is parsed as plain text first; one that comes back with nothing
# is a scan, and only those are sent back through OCR. The choice is recorded on the RAGFlow document, so a
# pass can tell a scan it has already escalated from one it has not.
PLAIN = {"layout_recognize": "Plain Text"}
OCR = {"layout_recognize": "DeepDOC"}
# A parse whose progress has not moved in this long is stalled, not slow: a RAGFlow restart mid-embed (2026-09-18)
# left five files RUNNING at 80% for hours with nothing working on them, and RAGFlow never retries those itself.
STALL_SECONDS = 30 * 60
PROGRESS_FILE = "/var/lib/bebuilt/ingest-progress.json"  # {ragflow_doc_id: {"mark", "since"}} between passes
# Re-listing every folder each pass costs one call per folder: fine for ten folders, 1,300 calls a pass for a
# real client selection (Molzer, 2026-09-21). Between full walks the pass asks Drive what CHANGED instead,
# which is one or two calls however large the selection. The full walk still runs daily as the backstop for
# anything the changes feed does not carry.
SYNC_FILE = "/var/lib/bebuilt/drive-sync.json"  # {"<corpus>:<provider>:<connection>": {token, folders, roots, walked_at, walk?}}
# A Dropbox connection keeps {cursors: {root: cursor}, walked: {root: time}, roots, walk?} under the same key.
# Weekly (Brandon, 2026-09-21). The walk is no longer how changes are noticed, only the backstop for what the
# feed might not carry — so every walk counts what the feed never reported (`missed`) into `ingest_runs`, and
# the health check alerts on it. Zero for a few weeks is the evidence for dropping it further.
FULL_WALK_SECONDS = 7 * 24 * 60 * 60

FOLDER = "application/vnd.google-apps.folder"
SHORTCUT = "application/vnd.google-apps.shortcut"
# Google's own formats are exported to Office formats, which RAGFlow parses with their structure intact.
EXPORT = {
    "application/vnd.google-apps.document": ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"),
    "application/vnd.google-apps.spreadsheet": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
    "application/vnd.google-apps.presentation": ("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"),
}
INDEXED = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-powerpoint": ".ppt",
    "text/plain": ".txt",
    "text/markdown": ".md",
    "text/csv": ".csv",
    "text/html": ".html",
    "application/rtf": ".rtf",
    "application/json": ".json",
}
# Dropbox names carry the type only as an extension. Images are Dropbox-only for now (D12, 2026-09-28): 439 of
# CDC's Onyx documents were scans with text, mostly lease and SOMA pages. Drive stores keep skipping images, so
# no Drive client pays OCR for every photo in their folders without asking for it.
IMAGES = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".tif": "image/tiff", ".tiff": "image/tiff",
    ".bmp": "image/bmp", ".gif": "image/gif", ".webp": "image/webp",
}
# RAGFlow's picture parser OCRs an image and, when that finds little, asks the tenant's vision model to describe it;
# with no vision model the parse fails with this (api/db/joint_services/tenant_model_service.py). An image that fails
# so, or parses to nothing, has no text to index: it is skipped as `no_text` at once, not retried, until its rev moves.
NO_VISION = ("No default vision model is set", "No default image2text model is set")
DROPBOX_TYPES = {**{ext: mime for mime, ext in INDEXED.items()}, ".htm": "text/html", **IMAGES}
# A Dropbox cloud doc (Google Docs/Sheets/Slides or Paper kept in Dropbox) has no bytes of its own; a plain download
# answers with an HTML stub (Onyx's override, 2025). It is recorded under this type and exported instead.
DROPBOX_CLOUD = "application/vnd.dropbox.cloud-doc"
# CDC's scans were read once already, by Onyx (unstructured.io hi_res), and OCR here costs ~6 s a page. That text sits
# on the box as a sidecar: index.json {path_lower: {rev, modified, file, chunks}} and the .md files beside it. A Dropbox PDF or
# image whose rev the index names is sent as that text, parsed plain, and never downloaded; any other rev, or no index
# at all, goes the usual way. Text that parses to nothing sends the file itself instead, once per rev.
ONYX_TEXT_DIR = os.environ.get("ONYX_TEXT_DIR", "/var/lib/bebuilt/onyx-text")
ONYX_EMPTY_FILE = "/var/lib/bebuilt/onyx-empty.json"  # {path_lower: rev} whose Onyx text parsed to nothing


def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def load_env(path):
    out = {}
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


class Blocked(Exception):
    """Google has flagged this file and hands it over only if the caller acknowledges the warning."""


class Composio:
    def token(self, acct):
        """This connection's own Google token. Used for one thing: fetching a file the client has explicitly
        allowed, which Composio's download tool cannot do because it takes no acknowledgeAbuse argument."""
        _, account_id = acct
        r = self.s.get(f"{COMPOSIO}/connected_accounts/{account_id}", timeout=60)
        r.raise_for_status()
        tok = ((r.json().get("state") or {}).get("val") or {}).get("access_token")
        if not tok:
            raise RuntimeError("Composio returned no access token for this connection")
        return tok

    def __init__(self, key, default_user=None):
        self.s = requests.Session()
        self.s.headers["x-api-key"] = key
        self.default_user = default_user

    def run(self, acct, slug, args):
        """`acct` is (Composio identity, connected account): each person's account lives under their own identity."""
        user, account_id = acct
        r = self.s.post(f"{COMPOSIO}/tools/execute/{slug}", timeout=120, json={
            "connected_account_id": account_id, "user_id": user or self.default_user, "version": DRIVE_TOOLS_VERSION, "arguments": args,
        })
        r.raise_for_status()
        body = r.json()
        if not body.get("successful"):
            raise RuntimeError(f"{slug}: {body.get('error') or 'failed'}")
        return body.get("data") or {}

    def proxy(self, acct, endpoint, body=None, headers=None):
        """One raw API call through the connection, as {data, status, headers, binary_data?}. `status` is the
        provider's own: a Dropbox 409 comes back inside an HTTP 200. Drops, 429s and 5xx are retried with backoff."""
        _, account_id = acct
        req = {"endpoint": endpoint, "method": "POST", "connected_account_id": account_id,
               "parameters": [{"name": k, "value": v, "type": "header"} for k, v in (headers or {}).items()]}
        if body is not None:
            req["body"] = body
        for attempt in range(PROXY_TRIES):
            wait = BACKOFF * 2 ** attempt
            try:
                r = self.s.post(COMPOSIO_PROXY, timeout=300, json=req)
                if r.status_code != 429 and r.status_code < 500:
                    r.raise_for_status()
                    out = r.json()
                    status = out.get("status")
                    if status != 429 and not (isinstance(status, int) and status >= 500):
                        return out
                    retry_after = {k.lower(): v for k, v in (out.get("headers") or {}).items()}.get("retry-after")
                    wait = max(wait, int(retry_after)) if str(retry_after or "").isdigit() else wait
                    why = f"{endpoint} answered {status}"
                else:
                    why = f"proxy answered {r.status_code}"
            except (requests.ConnectionError, requests.Timeout) as e:
                why = f"{endpoint}: {e}"
            if attempt + 1 == PROXY_TRIES:
                raise RuntimeError(f"{why}, {PROXY_TRIES} tries")
            time.sleep(wait)


class RAGFlow:
    def __init__(self, key, dataset):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {key}"
        self.ds = dataset

    def _ok(self, r):
        r.raise_for_status()
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(f"RAGFlow: {body.get('message')}")
        return body.get("data")

    def upload(self, filename, content):
        data = self._ok(self.s.post(f"{RAGFLOW}/datasets/{self.ds}/documents", files={"file": (filename, content)}, timeout=300))
        return data[0]["id"]

    def tag(self, doc_id, meta):
        self._ok(self.s.patch(f"{RAGFLOW}/datasets/{self.ds}/documents/{doc_id}", json={"meta_fields": meta}, timeout=60))

    def configure(self, doc_id, parser_config):
        self._ok(self.s.put(f"{RAGFLOW}/datasets/{self.ds}/documents/{doc_id}", json={"parser_config": parser_config}, timeout=60))

    def parse(self, doc_ids):
        self._ok(self.s.post(f"{RAGFLOW}/datasets/{self.ds}/chunks", json={"document_ids": doc_ids}, timeout=60))

    def stop(self, doc_ids):
        self._ok(self.s.delete(f"{RAGFLOW}/datasets/{self.ds}/chunks", json={"document_ids": doc_ids}, timeout=60))

    def queued(self):
        """Documents RAGFlow is still working on. One cheap call: the listing reports the total."""
        data = self._ok(self.s.get(f"{RAGFLOW}/datasets/{self.ds}/documents", params={"page": 1, "page_size": 1, "run": "RUNNING"}, timeout=60))
        return int(data.get("total") or 0) if isinstance(data, dict) else 0

    def all_docs(self):
        """{doc_id: doc} for everything in the dataset, or raise if the listing comes back short. A doc is
        only ever treated as missing after a COMPLETE listing: a partial answer (seen while RAGFlow was
        restarting, 2026-09-18) once sent 39 parsing files back for re-upload beside their live copies."""
        out, page, total = {}, 1, None
        while True:
            data = self._ok(self.s.get(f"{RAGFLOW}/datasets/{self.ds}/documents", params={"page": page, "page_size": 100}, timeout=60))
            docs = data.get("docs", []) if isinstance(data, dict) else (data or [])
            total = data.get("total") if isinstance(data, dict) else None
            out.update({d["id"]: d for d in docs})
            if len(docs) < 100:
                break
            page += 1
        if total is None or len(out) != total:
            raise RuntimeError(f"RAGFlow listed {len(out)} of {total} documents")
        return out

    def delete(self, doc_ids):
        if doc_ids:
            self._ok(self.s.delete(f"{RAGFLOW}/datasets/{self.ds}/documents", json={"ids": doc_ids}, timeout=120))


# --- Google Drive -------------------------------------------------------------------------------------

def drive_walk(cx, acct, root):
    """Every file under `root` (breadth-first, paged). Returns (files, folders, complete)."""
    seen, files, queue, complete = {root}, {}, [root], True
    folders = {root}
    if root == "root":
        # "root" is Drive's alias for My Drive, but the changes feed names a file's parent by My Drive's real id, so
        # without it a file saved straight into My Drive waited for the weekly walk (bebuilt, 2026-09-28: five).
        try:
            real = cx.run(acct, "GOOGLEDRIVE_GET_FILE_METADATA", {"fileId": "root", "fields": "id"})
            real = (real.get("file") or real).get("id")
            if real:
                seen.add(real)
                folders.add(real)
        except Exception as e:
            log(f"plan: could not resolve My Drive's id ({e}); files saved straight into it wait for the walk")
    fields = "nextPageToken,incompleteSearch,files(id,name,mimeType,size,modifiedTime,version,md5Checksum,webViewLink,shortcutDetails)"
    while queue:
        folder = queue.pop(0)
        token = None
        while True:
            # supportsAllDrives/includeItemsFromAllDrives: without them Drive silently omits anything in a
            # shared drive, including a folder another company shared with this person (Molzer, 2026-09-21).
            args = {"folder_id": folder, "q": "trashed = false", "pageSize": 1000, "fields": fields,
                    "supportsAllDrives": True, "includeItemsFromAllDrives": True}
            if token:
                args["pageToken"] = token
            page = cx.run(acct, "GOOGLEDRIVE_FIND_FILE", args)
            complete = complete and not page.get("incompleteSearch")
            for f in page.get("files") or []:
                if f["id"] in seen:
                    continue
                seen.add(f["id"])
                if f["mimeType"] == FOLDER:
                    queue.append(f["id"])
                    folders.add(f["id"])
                elif f["mimeType"] != SHORTCUT and not f.get("shortcutDetails"):
                    files[f["id"]] = f
            token = page.get("nextPageToken")
            if not token:
                break
    return files, folders, complete


def drive_start_token(cx, acct):
    """Drive's bookmark for 'changes from here on'. Taken before a walk, so nothing during it is missed."""
    d = cx.run(acct, "GOOGLEDRIVE_GET_CHANGES_START_PAGE_TOKEN", {"supportsAllDrives": True})
    return d.get("startPageToken") or d.get("start_page_token")


def drive_changes(cx, acct, token):
    """(changes, next token) for everything this account can see that changed since `token`."""
    fields = ("nextPageToken,newStartPageToken,changes(removed,fileId,file(id,name,mimeType,size,modifiedTime,"
              "version,md5Checksum,webViewLink,parents,trashed,shortcutDetails))")
    changes = []
    while True:
        page = cx.run(acct, "GOOGLEDRIVE_LIST_CHANGES", {
            "pageToken": token, "pageSize": 1000, "fields": fields, "includeRemoved": True,
            "includeItemsFromAllDrives": True, "supportsAllDrives": True, "restrictToMyDrive": False, "spaces": "drive",
        })
        changes += page.get("changes") or []
        nxt = page.get("nextPageToken")
        if not nxt:
            return changes, page.get("newStartPageToken") or token
        token = nxt


def drive_download(cx, acct, f, acknowledged=False):
    """(filename, bytes) for a file RAGFlow can parse, or raise Skip/Blocked."""
    mime = f["mimeType"]
    if acknowledged and mime not in EXPORT:
        # A file an admin of this org allowed: straight to Drive, warning acknowledged, this file only.
        ext = INDEXED[mime]
        r = requests.get(f"https://www.googleapis.com/drive/v3/files/{f['external_id']}",
                         params={"alt": "media", "acknowledgeAbuse": "true", "supportsAllDrives": "true"},
                         headers={"Authorization": f"Bearer {cx.token(acct)}"}, timeout=300)
        r.raise_for_status()
        name = f["name"] if f["name"].lower().endswith(ext) else f"{f['name']}{ext}"
        return name, r.content
    if mime in EXPORT:
        export_mime, ext = EXPORT[mime]
        data = cx.run(acct, "GOOGLEDRIVE_DOWNLOAD_FILE", {"fileId": f["external_id"], "mime_type": export_mime})
        if data.get("export_size_limit_exceeded"):
            raise Skip("Google can export at most 10 MB of this file type")
    else:
        ext = INDEXED[mime]
        data = cx.run(acct, "GOOGLEDRIVE_DOWNLOAD_FILE", {"fileId": f["external_id"]})
    content = data.get("downloaded_file_content") or {}
    url = content.get("s3url")
    if not url:
        refusal = json.dumps(data)[:500]
        # Google names the flag in its refusal; the file needs someone in the client's org to confirm.
        if "acknowledgeAbuse" in refusal or "cannot be downloaded" in refusal.lower():
            raise Blocked("Google has flagged this file and will only release it if someone confirms")
        raise RuntimeError("Composio returned no file")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    name = f["name"] if f["name"].lower().endswith(ext) else f"{f['name']}{ext}"
    return name, r.content


class Skip(Exception):
    pass


# --- Spreadsheets as rows ------------------------------------------------------------------------------
# RAGFlow's own Excel parser packs rows into chunks as "header：value; …" with the tab name on the end, and nothing
# says which file a row came from. A whole-dataset search then ranks any prose that repeats the building's name above
# every row: Molzer's rent payments sat indexed in the "Operating Income/Deposits" tab of the Holtman Expense/Budget
# Tracker while a search for Holtman rent payments returned only lease PDFs (2026-10-09). So a spreadsheet goes to
# RAGFlow as text, one line per row, each line carrying the file's name, its tab and the column headers:
#     Holtman Expense/Budget Tracker · Operating Income/Deposits · Date: 2026-10-06; Description: …; Amount: 2500
# Standard library only (the worker-only update installs nothing). A file this can't read goes as before.
SHEET_TEXT_LIMIT = 20 * 1024 * 1024  # bytes of text; a bigger workbook goes to RAGFlow's own parser
SHEET_XML_LIMIT = 200 * 1024 * 1024  # uncompressed sheet XML read at most, so a zip bomb is refused, not read
_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
_BUILTIN_DATES = set(range(14, 23)) | set(range(27, 37)) | set(range(45, 48)) | set(range(50, 59))


def _clean(v):
    return re.sub(r"\s+", " ", str(v)).strip()


def _number(text):
    """A cell's stored number as a person reads it: whole numbers bare, money to the cent, small rates to four places.
    The display format is not applied (124874.996667 is stored, $124,875.00 shown); rounding keeps the noise out."""
    x = float(text)
    if x.is_integer() and abs(x) < 1e15:
        return str(int(x))
    return f"{x:.{2 if abs(x) >= 100 else 4}f}".rstrip("0").rstrip(".")


def _date_format(code):
    code = re.sub(r'"[^"]*"|\[[^\]]*\]|\\.', "", code or "")
    return bool(re.search(r"[dy]", code, re.I))


def _col(ref):
    n = 0
    for ch in ref:
        if not ch.isalpha():
            break
        n = n * 26 + ord(ch.upper()) - 64
    return n - 1


def _read(z, name):
    info = z.getinfo(name)
    if info.file_size > SHEET_XML_LIMIT:
        raise ValueError(f"{name} is {info.file_size} bytes uncompressed")
    return z.read(name)


def xlsx_tabs(content):
    """[(tab name, [[cell text, …], …])] for every sheet of an .xlsx, in workbook order. Dates come out as ISO dates."""
    z = zipfile.ZipFile(io.BytesIO(content))
    names = set(z.namelist())
    wb = ElementTree.fromstring(_read(z, "xl/workbook.xml"))
    pr = wb.find("m:workbookPr", _NS)
    epoch = datetime(1904, 1, 1) if pr is not None and pr.get("date1904") in ("1", "true") else datetime(1899, 12, 30)
    rels = ElementTree.fromstring(_read(z, "xl/_rels/workbook.xml.rels"))
    target = {}
    for r in rels:
        t = r.get("Target", "")
        target[r.get("Id")] = t.lstrip("/") if t.startswith("/") else "xl/" + t
    shared = []
    if "xl/sharedStrings.xml" in names:
        for si in ElementTree.fromstring(_read(z, "xl/sharedStrings.xml")).findall("m:si", _NS):
            # Plain text, or rich-text runs joined; phonetic guides (rPh) are not the text.
            shared.append("".join(t.text or "" for t in si.findall("m:t", _NS) + si.findall("m:r/m:t", _NS)))
    dates = set()
    if "xl/styles.xml" in names:
        st = ElementTree.fromstring(_read(z, "xl/styles.xml"))
        custom = {int(f.get("numFmtId")): f.get("formatCode") for f in st.findall("m:numFmts/m:numFmt", _NS)}
        for i, xf in enumerate(st.findall("m:cellXfs/m:xf", _NS)):
            fid = int(xf.get("numFmtId", "0"))
            if fid in _BUILTIN_DATES or (fid in custom and _date_format(custom[fid])):
                dates.add(i)
    tabs = []
    for sh in wb.findall("m:sheets/m:sheet", _NS):
        path = target.get(sh.get(_REL))
        if not path or path not in names:
            continue
        rows = []
        for row in ElementTree.fromstring(_read(z, path)).iterfind("m:sheetData/m:row", _NS):
            cells, nxt = {}, 0
            for c in row.findall("m:c", _NS):
                i = _col(c.get("r")) if c.get("r") else nxt
                nxt = i + 1
                kind, v = c.get("t"), c.find("m:v", _NS)
                if kind == "inlineStr":
                    text = "".join(t.text or "" for t in c.iter(f"{{{_NS['m']}}}t"))
                elif v is None or v.text is None:
                    continue
                elif kind == "s":
                    text = shared[int(v.text)]
                elif kind == "b":
                    text = "TRUE" if v.text == "1" else "FALSE"
                elif kind in ("str", "e"):
                    text = "" if kind == "e" else v.text
                else:
                    try:
                        if int(c.get("s", "0")) in dates:
                            d = epoch + timedelta(days=float(v.text))
                            text = d.strftime("%Y-%m-%d") if d.time() == datetime.min.time() else d.strftime("%Y-%m-%d %H:%M")
                        else:
                            text = _number(v.text)
                    except (ValueError, OverflowError):
                        text = v.text
                text = _clean(text)
                if text:
                    cells[i] = text
            if cells:
                width = max(cells) + 1
                rows.append([cells.get(i, "") for i in range(width)])
        tabs.append((_clean(sh.get("name", "")), rows))
    return tabs


def csv_tabs(content):
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    rows = [[_clean(c) for c in r] for r in csv.reader(io.StringIO(text))]
    return [("", [r for r in rows if any(r)])]


def header_row(rows):
    """Index of the column-header row among the first 20, or None: the first row with at least two cells, mostly text,
    at least half as wide as the widest of the rows below it. Title rows above it ("Income/Deposits") are skipped."""
    for i, r in enumerate(rows[:20]):
        filled = [c for c in r if c]
        if len(filled) < 2:
            continue
        below = [sum(1 for c in b if c) for b in rows[i + 1:i + 21]]
        if below and len(filled) * 2 < max(below):
            continue
        texty = sum(1 for c in filled if not re.fullmatch(r"[-+$(]?[\d,.]+%?\)?|\d{4}-\d{2}-\d{2}( \d{2}:\d{2})?", c))
        if texty * 2 >= len(filled):
            return i
    return None


def sheet_text(display_name, tabs):
    """The text RAGFlow indexes for a workbook: per tab, its title lines, then one line per row naming the file, the
    tab and each value's column. Returns None for a workbook with no rows."""
    out = []
    name = _clean(display_name)
    for tab, rows in tabs:
        if not rows:
            continue
        where = f"{name} · {tab}" if tab else name
        h = header_row(rows)
        out.append(f"{where}")
        if h is None:
            out.extend(f"{where} · " + "; ".join(c for c in r if c) for r in rows)
            continue
        out.extend(f"{where} · " + " ".join(c for c in r if c) for r in rows[:h])
        heads = rows[h]
        for r in rows[h + 1:]:
            filled = [i for i, c in enumerate(r) if c]
            if len(filled) >= 2 and sum(1 for i in filled if i < len(heads) and r[i] == heads[i]) * 2 >= len(filled):
                heads = r  # a block below repeats (or re-states) the headers: its own headers from here on
                continue
            pairs = []
            for i, c in enumerate(r):
                if c:
                    head = heads[i] if i < len(heads) and heads[i] else ""
                    pairs.append(f"{head}: {c}" if head else c)
            if pairs:
                out.append(f"{where} · " + "; ".join(pairs))
        out.append("")
    text = "\n".join(out).strip()
    return text or None


def as_sheet_text(display_name, filename, content):
    """(filename, bytes) of a workbook's rows as text, or None to send the file itself (not a workbook this reads,
    unreadable, empty, or too big)."""
    low = filename.lower()
    try:
        if low.endswith(".xlsx"):
            tabs = xlsx_tabs(content)
        elif low.endswith(".csv"):
            tabs = csv_tabs(content)
        else:
            return None
        text = sheet_text(display_name, tabs)
    except Exception as e:  # a workbook this reader can't follow still goes, to RAGFlow's own parser
        log(f"send: {display_name}: read as rows failed ({e}); sending the file itself")
        return None
    if not text:
        return None
    data = text.encode("utf-8")
    if len(data) > SHEET_TEXT_LIMIT:
        log(f"send: {display_name}: {len(data)} bytes as rows; sending the file itself")
        return None
    return f"{filename}.txt", data


# --- Dropbox ------------------------------------------------------------------------------------------

class DropboxError(Exception):
    def __init__(self, endpoint, status, data):
        self.status = status
        self.summary = (data.get("error_summary") if isinstance(data, dict) else str(data or "")) or ""
        super().__init__(f"{endpoint.rsplit('/2/', 1)[-1]}: Dropbox answered {status} {self.summary[:300]}".rstrip())


def dropbox_namespace(cx, acct):
    """The team's root namespace. Every later call names it in Dropbox-API-Path-Root: without it the proxy lands in
    the person's home folder, where a team folder is `path/not_found` (CDC, 2026-09-28)."""
    endpoint = f"{DROPBOX_API}/users/get_current_account"
    out = cx.proxy(acct, endpoint)
    if out.get("status") != 200:
        raise DropboxError(endpoint, out.get("status"), out.get("data"))
    return str(out["data"]["root_info"]["root_namespace_id"])


def dropbox_call(cx, acct, ns, endpoint, body=None, arg=None):
    headers = {"Dropbox-API-Path-Root": json.dumps({".tag": "root", "root": ns})}
    if arg is not None:
        headers["Dropbox-API-Arg"] = json.dumps(arg)
    out = cx.proxy(acct, endpoint, body, headers)
    if out.get("status") != 200:
        raise DropboxError(endpoint, out.get("status"), out.get("data"))
    return out


def dropbox_list(cx, acct, ns, root, cursor=None):
    """(entries, cursor): the whole recursive listing of `root`, or everything since `cursor`. The cursor returned
    continues exactly this listing, so each ticked root keeps its own (D10)."""
    if cursor:
        out = dropbox_call(cx, acct, ns, f"{DROPBOX_API}/files/list_folder/continue", {"cursor": cursor})["data"]
    else:
        out = dropbox_call(cx, acct, ns, f"{DROPBOX_API}/files/list_folder",
                           {"path": root, "recursive": True, "limit": 2000})["data"]
    entries = list(out.get("entries") or [])
    while out.get("has_more"):
        out = dropbox_call(cx, acct, ns, f"{DROPBOX_API}/files/list_folder/continue", {"cursor": out["cursor"]})["data"]
        entries += out.get("entries") or []
    return entries, out["cursor"]


def dropbox_link(path_display):
    """Where a citation opens the file: Dropbox's own preview of it inside its folder (D8). Built from the path, so
    no sharing call per file; it opens for anyone signed in to the team, and a moved file's link heals on the walk."""
    folder, _, name = path_display.rpartition("/")
    return "https://www.dropbox.com/home" + urllib.parse.quote(folder) + "?preview=" + urllib.parse.quote(name, safe="")


def dropbox_file(e):
    """A listing entry in the shape the Drive code already writes."""
    name = e["name"]
    if e.get("is_downloadable", True):
        mime = DROPBOX_TYPES.get(os.path.splitext(name)[1].lower()) or mimetypes.guess_type(name)[0] or "application/octet-stream"
    else:
        mime = DROPBOX_CLOUD
    return {"id": e["path_lower"], "name": name, "mimeType": mime, "size": e.get("size"), "version": e["rev"],
            "webViewLink": dropbox_link(e["path_display"])}


def dropbox_download(cx, acct, ns, f):
    """(filename, bytes, tags) for one file by its path. The proxy answers with a presigned link that lives about
    an hour; a fetch that fails (or a link that has expired) asks for a new one."""
    cloud = f["mimeType"] == DROPBOX_CLOUD
    endpoint = f"{DROPBOX_CONTENT}/files/" + ("export" if cloud else "download")
    err = None
    for _ in range(3):
        try:
            out = dropbox_call(cx, acct, ns, endpoint, arg={"path": f["external_id"]})
        except DropboxError as e:
            if cloud and e.status == 409:
                raise Skip(f"Dropbox can't export this file ({e.summary.rstrip('/') or 'no export'})")
            raise
        headers = {k.lower(): v for k, v in (out.get("headers") or {}).items()}
        meta = json.loads(headers.get("dropbox-api-result") or "{}")
        url = (out.get("binary_data") or {}).get("url")
        if not url:
            raise RuntimeError("Composio returned no file")
        try:
            r = requests.get(url, timeout=300)
            r.raise_for_status()
        except requests.RequestException as e:
            err = e
            continue
        name = f["name"]
        if cloud:
            # The export names its own format (a Google Sheet comes back as .xlsx, Paper as .md).
            name = (meta.get("export_metadata") or {}).get("name") or name
            ext = os.path.splitext(name)[1].lower()
            if ext not in INDEXED.values():
                raise Skip(f"Dropbox exports this file as {ext or 'an unknown type'}, which isn't indexed yet")
            meta = meta.get("file_metadata") or {}
        elif name.lower().endswith(".tiff"):
            name = name[:-1]  # RAGFlow knows images by extension, and its list has .tif but not .tiff
        return name, r.content, {"modified_at": meta.get("server_modified") or ""}
    raise RuntimeError(f"fetching the file failed: {err}")


def dropbox_modified(cx, acct, ns, f):
    """The tag a download reads from its headers, for a file sent without downloading it."""
    out = dropbox_call(cx, acct, ns, f"{DROPBOX_API}/files/get_metadata", {"path": f["external_id"]})
    return {"modified_at": (out.get("data") or {}).get("server_modified") or ""}


def load_onyx():
    """The Onyx sidecar's index, or None: no sidecar (the feature is off), or one that can't be read (off this pass)."""
    path = os.path.join(ONYX_TEXT_DIR, "index.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path) as f:
            index = json.load(f)
        if not isinstance(index, dict):
            raise ValueError("not a JSON object")
        return index
    except (OSError, ValueError) as e:
        log(f"send: Onyx text index unreadable ({e}); every file goes the usual way this pass")
        return None


def onyx_text(index, f, revision):
    """(filename, bytes, tags) of Onyx's text for exactly this rev, or None. Named `<name>.md`, so RAGFlow reads it as
    text. The index was built from the listing that gave the rev, so its `modified` is that rev's server_modified."""
    entry = index.get(f["external_id"])
    if not isinstance(entry, dict) or entry.get("rev") != revision or not entry.get("file"):
        return None
    try:
        with open(os.path.join(ONYX_TEXT_DIR, os.path.basename(entry["file"])), "rb") as t:
            return f"{f['name']}.md", t.read(), {"modified_at": entry["modified"]} if entry.get("modified") else None
    except OSError as e:
        log(f"send: {f['name']}: no Onyx text file ({e}); downloading it instead")
        return None


def indexable(mime, size, provider=None):
    dropbox_only = provider == "dropbox" and (mime in IMAGES.values() or mime == DROPBOX_CLOUD)
    if mime not in EXPORT and mime not in INDEXED and not dropbox_only:
        return "this file type isn't indexed yet"
    if size and size > MAX_BYTES:
        return "larger than 100 MB"
    return None


# --- the pass -----------------------------------------------------------------------------------------

def file_revision(provider, f):
    """What a file's content is, so that only new content re-queues it. Dropbox: its `rev`, which moves only with
    the content. Drive: its checksum, or for Google's own formats (which have none) when it was last modified. Not
    Drive's `version`: Drive raises that for sharing, comments and other changes to the file's metadata too, and the
    changes feed of a person the file was shared with never reports those, so every weekly walk counted and
    re-parsed files nobody had touched (2026-09-28: 455 at Molzer, vendor catalogs last edited in 2024)."""
    if provider == "dropbox":
        return str(f.get("version") or "")
    return str(f.get("md5Checksum") or f.get("modifiedTime") or f.get("version") or "")


def legacy_revision(provider, f):
    """The revision the worker recorded for this file before `file_revision` (Drive's `version`), or None when
    there is nothing to carry over. A row still holding the file's CURRENT version has not changed since it was
    recorded, so it takes the new revision in place instead of being parsed again: the switch is self-limiting,
    since a row holds a version only until the first pass that sees its file."""
    old = str(f.get("version") or "")
    return old if provider != "dropbox" and old and old != file_revision(provider, f) else None


def same_revision(held, provider, f):
    """Whether a revision we hold is this file as it is now (in either form; see `legacy_revision`)."""
    return held is not None and held in (file_revision(provider, f), legacy_revision(provider, f))


def upsert_file(db, org, corpus, provider, f, owner, keep=()):
    """One Drive file into `documents`: new work becomes pending, a moved revision re-queues, a kind we
    cannot read is recorded as skipped so the screens can say why. `owner` is the connection it is read through;
    an existing owner listed in `keep` (the live connections, from a changes feed) stays. A group walk passes no
    `keep`, because it has already decided the owner. A revision recorded as the file's current Drive `version`
    is carried over to its content revision first (`legacy_revision`), so the switch re-queues nothing."""
    size = int(f["size"]) if f.get("size") else None
    why_not = indexable(f["mimeType"], size, provider)
    # The stored revisions, as they would read had they been recorded in today's form.
    held = {col: f"case when documents.{col} = %(legacy)s then %(revision)s else documents.{col} end"
            for col in ("source_revision", "indexed_revision", "sent_revision")}
    db.execute(
        f"""insert into documents (org_id, corpus_id, provider, external_id, name, mime_type, size, web_url,
                                  source_revision, state, last_error, connection_id, seen_at, updated_at)
           values (%(org)s, %(corpus)s, %(provider)s, %(id)s, %(name)s, %(mime)s, %(size)s, %(url)s,
                   %(revision)s, %(state)s, %(why_not)s, %(owner)s, now(), now())
           on conflict (corpus_id, provider, external_id) do update set
             connection_id = case when documents.connection_id = any(%(keep)s::uuid[]) then documents.connection_id
                                  else excluded.connection_id end,
             name = excluded.name, mime_type = excluded.mime_type, size = excluded.size,
             web_url = excluded.web_url, source_revision = excluded.source_revision, seen_at = now(),
             indexed_revision = {held["indexed_revision"]}, sent_revision = {held["sent_revision"]},
             state = case
               when excluded.state = 'skipped' then 'skipped'
               when documents.state = 'removed' then 'pending'
               when {held["indexed_revision"]} is distinct from excluded.source_revision
                    and documents.state in ('indexed', 'skipped') then 'pending'
               when documents.state = 'failed' and {held["source_revision"]} is distinct from excluded.source_revision then 'pending'
               when documents.state = 'failed' and documents.attempts < 3 then 'pending'
               else documents.state end,
             attempts = case when {held["source_revision"]} is distinct from excluded.source_revision then 0 else documents.attempts end,
             last_error = case when excluded.state = 'skipped' then excluded.last_error else documents.last_error end,
             updated_at = now()""",
        {"org": org, "corpus": corpus, "provider": provider, "id": f["id"], "name": f["name"], "mime": f["mimeType"],
         "size": size, "url": f.get("webViewLink"), "revision": file_revision(provider, f),
         "legacy": legacy_revision(provider, f), "state": "skipped" if why_not else "pending", "why_not": why_not,
         "owner": owner, "keep": list(keep)})


def drop_file(db, org, corpus, provider, external_id):
    """A file that left the selection: out of RAGFlow, marked removed here. 1 if we held it, else 0."""
    row = db.execute("select id, ragflow_doc_id from documents where org_id = %s and corpus_id = %s and provider = %s "
                     "and external_id = %s and state <> 'removed'", (org, corpus, provider, external_id)).fetchone()
    if not row:
        return 0
    doc_id, rf = row
    if rf:
        rag.delete([rf])
    db.execute("update documents set state = 'removed', ragflow_doc_id = null, indexed_revision = null, updated_at = now() where id = %s", (doc_id,))
    return 1


def load_sync():
    try:
        with open(SYNC_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_sync(state):
    os.makedirs(os.path.dirname(SYNC_FILE), exist_ok=True)
    tmp = SYNC_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, SYNC_FILE)


def walk_group(db, cx, org, corpus, provider, conns, state, keys):
    """Every live connection in one store, each over its own ticked folders, listed in full. Removals happen only
    when EVERY walk is complete (D32: a partial listing once sent 39 live files back for re-upload), and only for
    documents no connection saw whose owner is one of these live connections (or nobody): a document read through
    a connection that has stopped working stays until that connection is back. Owners: a live owner that still
    sees the file keeps it, otherwise the first connection that saw it takes it. Returns (files, folders, missed)."""
    seen = {}  # external_id -> (file, [connections that saw it])
    complete, folder_count = True, 0
    count_missed = all((state.get(keys[cid]) or {}).get("token") for cid in conns)
    fresh = {}
    for cid in sorted(conns):
        entry = conns[cid]
        if not entry["roots"]:  # nothing ticked any more: nothing to list, and nothing of theirs is kept
            fresh[cid] = {"token": None, "folders": [], "roots": [], "walked_at": time.time()}
            continue
        token = None
        try:  # the bookmark is taken BEFORE the walk, so a change during it is caught by the next pass
            token = drive_start_token(cx, entry["acct"])
        except Exception as e:
            log(f"plan: no changes bookmark for connection {cid} ({e}); next pass walks in full again")
        files, folders, ok = {}, set(), True
        for root in entry["roots"]:
            try:
                got, walked, root_ok = drive_walk(cx, entry["acct"], root)
            except Exception as e:  # a failed walk removes nothing
                log(f"plan: walking {provider}:{root} for connection {cid} failed: {e}")
                got, walked, root_ok = {}, set(), False
            files.update(got)
            folders |= walked
            ok = ok and root_ok
        complete = complete and ok
        folder_count += len(folders)
        for external_id, f in files.items():
            seen.setdefault(external_id, (f, []))[1].append(cid)
        fresh[cid] = {"token": token, "folders": sorted(folders), "roots": sorted(entry["roots"]), "walked_at": time.time()} if ok and token else None
    held = {}
    if seen:
        held = {r[0]: (r[1], r[2]) for r in db.execute(
            "select external_id, source_revision, connection_id::text from documents where org_id = %s and corpus_id = %s "
            "and provider = %s and state <> 'removed' and external_id = any(%s::text[])", (org, corpus, provider, list(seen))).fetchall()}
    # What the changes feeds should already have brought us: anything here that we do not hold at this revision
    # was missed (only meaningful when every feed was running, not on a first walk).
    missed = 0
    if count_missed:
        for external_id, (f, _) in seen.items():
            if not same_revision((held.get(external_id) or (None, None))[0], provider, f):
                missed += 1
    for external_id, (f, saw) in seen.items():
        current = (held.get(external_id) or (None, None))[1]
        upsert_file(db, org, corpus, provider, f, current if current in saw else saw[0])
    if complete:
        gone = db.execute(
            "select external_id from documents where org_id = %s and corpus_id = %s and provider = %s and state <> 'removed' "
            "and not (external_id = any(%s::text[])) and (connection_id is null or connection_id = any(%s::uuid[]))",
            (org, corpus, provider, list(seen), list(conns))).fetchall()
        for (external_id,) in gone:
            drop_file(db, org, corpus, provider, external_id)
        if gone:
            log(f"plan: removed {len(gone)} file(s) no longer in the selection")
    else:
        log(f"plan: {provider} walk incomplete; nothing removed this pass")
    for cid in conns:
        if fresh[cid]:
            state[keys[cid]] = fresh[cid]
        else:
            state.pop(keys[cid], None)
    who = f" across {len(conns)} connections" if len(conns) > 1 else ""
    log(f"plan: walked {folder_count} folder(s), {len(seen)} file(s){who}" + (f", {missed} missed by the changes feed" if count_missed else ""))
    return len(seen), folder_count, missed


def owned_by(db, org, corpus, provider, external_id, cid):
    row = db.execute("select 1 from documents where org_id = %s and corpus_id = %s and provider = %s and external_id = %s "
                     "and state <> 'removed' and connection_id = %s", (org, corpus, provider, external_id, cid)).fetchone()
    return row is not None


def incremental(db, cx, org, corpus, provider, cid, acct, saved, live):
    """What changed for one connection since the last pass, in one or two calls. False if the feed could not be
    read, which sends the store back to a full walk. Drive reports `removed` when a file leaves THIS person's view,
    and a move out of their folders only for them: with other connections in the store someone else may still
    reach it, so instead of removing it here, a document this connection owns asks for a walk of the whole store
    next pass. A trashed file is gone for everyone and goes at once."""
    try:
        changes, token = drive_changes(cx, acct, saved["token"])
    except Exception as e:
        log(f"plan: changes feed failed for connection {cid} ({e})")
        return False
    shared = len(live) > 1
    folders = set(saved["folders"])
    fresh, touched = [], 0

    def left(external_id):
        if not shared:
            return drop_file(db, org, corpus, provider, external_id)
        if owned_by(db, org, corpus, provider, external_id, cid):
            saved["walk"] = True
        return 0

    for ch in changes:
        f = ch.get("file") or {}
        external_id = ch.get("fileId") or f.get("id")
        if not external_id:
            continue
        if f.get("trashed"):
            touched += drop_file(db, org, corpus, provider, external_id)
            continue
        if ch.get("removed"):
            touched += left(external_id)
            continue
        parents = f.get("parents") or []
        inside = any(p in folders for p in parents)
        if f.get("mimeType") == FOLDER:
            if inside and external_id not in folders:
                fresh.append(external_id)  # walked below, so the files already in it arrive too
            continue
        if f.get("mimeType") == SHORTCUT or f.get("shortcutDetails"):
            continue
        if inside:
            upsert_file(db, org, corpus, provider, f, cid, live)
            touched += 1
        elif parents:  # moved out of every folder this person ticked
            touched += left(external_id)
    for folder in fresh:
        try:
            got, walked, _ = drive_walk(cx, acct, folder)
        except Exception as e:
            log(f"plan: walking new folder {folder} failed: {e}")
            continue
        folders |= walked
        for f in got.values():
            upsert_file(db, org, corpus, provider, f, cid, live)
        touched += len(got)
    saved["token"] = token
    saved["folders"] = sorted(folders)
    if changes:
        log(f"plan: connection {cid}: {len(changes)} change(s), {len(fresh)} new folder(s), {touched} file(s) touched"
            + ("; a whole-store walk is due" if saved.get("walk") else ""))
    return True


def drop_under(db, org, corpus, provider, path, owners):
    """Everything at or under `path` (a Dropbox `deleted` entry names a folder once, not each file in it), among
    documents these connections own or nobody owns. Returns how many went."""
    rows = db.execute("select external_id from documents where org_id = %s and corpus_id = %s and provider = %s "
                      "and state <> 'removed' and (external_id = %s or starts_with(external_id, %s || '/')) "
                      "and (connection_id is null or connection_id = any(%s::uuid[]))",
                      (org, corpus, provider, path, path, list(owners))).fetchall()
    return sum(drop_file(db, org, corpus, provider, external_id) for (external_id,) in rows)


def dropbox_root(root):
    """A ticked folder as Dropbox lists it and as the rows under it begin: lower case, no trailing slash, the
    team root as ""."""
    return root.rstrip("/").lower()


def dropbox_group(db, cx, org, corpus, conns, state, keys):
    """A Dropbox store, root by root (D10). A root with a cursor asks what changed since it; one without (new, reset,
    due its weekly walk, or never finished) is listed in full, and only that complete listing removes what it did
    not see under that root. Each root's rows and cursor are saved as soon as its listing ends, so a first walk
    longer than the service's timeout still finishes over several passes. Returns the pass's (mode, folders,
    files, missed)."""
    provider = "dropbox"
    live = list(conns)
    walked_any, folders, files, missed = False, 0, 0, 0
    everywhere = sorted({dropbox_root(r) for c in conns.values() for r in c["roots"]})
    # A folder nobody has ticked any more: what was read from it (by a live connection, or by nobody) goes.
    gone = db.execute(
        "select external_id from documents d where org_id = %s and corpus_id = %s and provider = %s and state <> 'removed' "
        "and (connection_id is null or connection_id = any(%s::uuid[])) and not exists (select 1 from unnest(%s::text[]) r "
        "where d.external_id = r or starts_with(d.external_id, r || '/'))", (org, corpus, provider, live, everywhere)).fetchall()
    for (external_id,) in gone:
        drop_file(db, org, corpus, provider, external_id)
    if gone:
        log(f"plan: removed {len(gone)} file(s) no longer in the selection")
        db.commit()
    for cid in sorted(conns):
        acct, roots = conns[cid]["acct"], sorted({dropbox_root(r) for r in conns[cid]["roots"]})
        saved = state.get(keys[cid]) or {}
        cursors = {r: c for r, c in (saved.get("cursors") or {}).items() if r in roots}
        stamps = {r: t for r, t in (saved.get("walked") or {}).items() if r in roots}
        if saved.get("walk"):
            cursors = {}
        saved = state[keys[cid]] = {"cursors": cursors, "walked": stamps, "roots": roots}
        ns = None
        for root in roots:
            try:
                if ns is None:  # once per connection per pass, and not at all for a connection with nothing ticked
                    ns = dropbox_namespace(cx, acct)
                cursor = cursors.get(root)
                if cursor and time.time() - (stamps.get(root) or 0) <= FULL_WALK_SECONDS:
                    try:
                        entries, cursor = dropbox_list(cx, acct, ns, root, cursor)
                        touched = 0
                        for e in entries:  # in order: a move is its delete and then its add
                            if e.get(".tag") == "deleted":
                                touched += drop_under(db, org, corpus, provider, e["path_lower"], [cid])
                            elif e.get(".tag") == "file":
                                upsert_file(db, org, corpus, provider, dropbox_file(e), cid, live)
                                touched += 1
                        db.commit()
                        cursors[root] = cursor
                        save_sync(state)
                        if entries:
                            log(f"plan: connection {cid}: {root or '/'}: {len(entries)} change(s), {touched} file(s) touched")
                        continue
                    except DropboxError as e:
                        if e.status != 409 or not e.summary.startswith("reset"):
                            raise
                        cursors.pop(root)
                        log(f"plan: connection {cid}: Dropbox reset the cursor for {root or '/'}; listing it again")
                # A full listing. The cursor it ends on covers everything after it.
                entries, cursor = dropbox_list(cx, acct, ns, root)
                seen = {}
                for e in entries:
                    if e.get(".tag") == "file":
                        seen[e["path_lower"]] = dropbox_file(e)
                    elif e.get(".tag") == "folder":
                        folders += 1
                if root in cursors:  # the feed was running: anything not held at this revision, it missed
                    held = dict(db.execute(
                        "select external_id, source_revision from documents where org_id = %s and corpus_id = %s and provider = %s "
                        "and state <> 'removed' and external_id = any(%s::text[])", (org, corpus, provider, list(seen))).fetchall())
                    missed += sum(1 for k, f in seen.items() if held.get(k) != f["version"])
                for f in seen.values():
                    upsert_file(db, org, corpus, provider, f, cid, live)
                stale = db.execute(
                    "select external_id from documents where org_id = %s and corpus_id = %s and provider = %s and state <> 'removed' "
                    "and starts_with(external_id, %s || '/') and not (external_id = any(%s::text[])) "
                    "and (connection_id is null or connection_id = %s)", (org, corpus, provider, root, list(seen), cid)).fetchall()
                for (external_id,) in stale:
                    drop_file(db, org, corpus, provider, external_id)
                db.commit()
                cursors[root], stamps[root] = cursor, time.time()
                save_sync(state)
                walked_any, files, folders = True, files + len(seen), folders + 1
                log(f"plan: connection {cid}: walked {root or '/'}: {len(seen)} file(s)" + (f", removed {len(stale)}" if stale else ""))
            except Exception as e:  # this root keeps what it had; nothing is removed on a failed listing
                db.rollback()
                log(f"plan: {provider}:{root or '/'} for connection {cid} failed: {e}")
    return ("walk" if walked_any else "changes", folders, files, missed)


def claim_unowned(db, org):
    """Documents written before this worker knew about owners (or by an older worker after the migration that added
    them) belong to the store's one live connection, when it has exactly one. With several, the next walk decides."""
    db.execute(
        """update documents d set connection_id = c.id from corpus_connections c
           where d.org_id = %s and d.connection_id is null and d.state <> 'removed'
             and c.corpus_id = d.corpus_id and c.provider = d.provider and c.status = 'ACTIVE'
             and (select count(*) from corpus_connections c2 where c2.corpus_id = d.corpus_id and c2.provider = d.provider
                  and c2.status = 'ACTIVE') = 1""", (org,))


def plan(db, cx, org):
    claim_unowned(db, org)
    # A ticked folder is read through the connection that added it. One ticked before connections were recorded
    # on folders has none, and belongs to its adder's connection (the only person who could browse that store).
    sources = db.execute(
        """select c.id::text, c.composio_user_id, c.composio_connected_account_id, s.corpus_id::text, s.provider, s.external_id
           from corpus_sources s join corpus_connections c
             on c.corpus_id = s.corpus_id and c.provider = s.provider
            and (c.id = s.connection_id or (s.connection_id is null and c.grant_holder_user_id = s.added_by))
           where s.org_id = %s and s.confirmed_at is not null and c.status = 'ACTIVE'""", (org,)).fetchall()
    stores = {}  # (corpus, provider) -> {connection: {acct, roots}}
    # Every live connection belongs to its store even with nothing ticked: when someone removes their last folder,
    # the walk that follows is what takes its files out.
    for cid, user, account, corpus, provider in db.execute(
            "select id::text, composio_user_id, composio_connected_account_id, corpus_id::text, provider from corpus_connections "
            "where org_id = %s and status = 'ACTIVE' and provider in ('googledrive', 'dropbox')", (org,)).fetchall():
        stores.setdefault((corpus, provider), {})[cid] = {"acct": (user, account), "roots": []}
    for cid, user, account, corpus, provider, root in sources:
        if provider not in ("googledrive", "dropbox"):
            log(f"plan: no ingestion adapter for {provider} yet; skipping {root}")
            continue
        stores.setdefault((corpus, provider), {}).setdefault(cid, {"acct": (user, account), "roots": []})["roots"].append(root)

    state = load_sync()
    kept, ran = set(), []
    for (corpus, provider), conns in stores.items():
        keys = {cid: f"{corpus}:{provider}:{cid}" for cid in conns}
        kept |= set(keys.values())
        if provider == "dropbox":
            ran.append(dropbox_group(db, cx, org, corpus, conns, state, keys))
            continue
        old = f"{corpus}:{provider}"  # the one-connection key; carried over so the upgrade forces no full walk
        if old in state and len(conns) == 1 and next(iter(keys.values())) not in state:
            state[next(iter(keys.values()))] = state.pop(old)

        def needs_walk(cid):
            saved = state.get(keys[cid]) or {}
            if not conns[cid]["roots"]:
                # Nothing ticked: a walk only when they still own documents (their last folder just went), so a
                # person who has connected but not chosen anything yet costs no walk of the whole store.
                return saved.get("roots") != [] and db.execute(
                    "select 1 from documents where connection_id = %s and state <> 'removed' limit 1", (cid,)).fetchone() is not None
            usable = saved.get("token") and saved.get("folders") and saved.get("roots") == sorted(conns[cid]["roots"]) and not saved.get("walk")
            return not usable or time.time() - (saved.get("walked_at") or 0) > FULL_WALK_SECONDS

        # A full walk when any connection has nothing to go on, changed its ticked folders or is due, and then of
        # every connection in the store: with several, only the whole store's walk can say a file is gone.
        walk = any(needs_walk(cid) for cid in conns)
        if not walk:
            for cid in sorted(c for c in conns if conns[c]["roots"]):
                if not incremental(db, cx, org, corpus, provider, cid, conns[cid]["acct"], state[keys[cid]], list(conns)):
                    walk = True
                    break
            if not walk:
                ran.append(("changes", 0, 0, 0))
        if walk:
            files, folders, missed = walk_group(db, cx, org, corpus, provider, conns, state, keys)
            ran.append(("walk", folders, files, missed))
    save_sync({k: v for k, v in state.items() if k in kept})
    # The worker's heartbeat: a pass that changes nothing still proves the worker is alive, which document
    # timestamps no longer can now that most passes write nothing.
    if ran:
        mode = "walk" if any(r[0] == "walk" for r in ran) else "changes"
        db.execute("insert into ingest_runs (org_id, mode, folders, files, missed) values (%s, %s, %s, %s, %s)",
                   (org, mode, sum(r[1] for r in ran), sum(r[2] for r in ran), sum(r[3] for r in ran)))
    db.commit()


def send(db, cx, org):
    try:
        waiting = rag.queued()
    except Exception as e:
        log(f"send: cannot read RAGFlow's queue ({e}); sending nothing this pass")
        return
    if waiting >= QUEUE_HIGH:
        log(f"send: RAGFlow still holds {waiting} document(s); waiting rather than burying it")
        return
    rows = db.execute(
        """select d.id, d.provider, d.external_id, d.name, d.mime_type, d.web_url, d.source_revision, d.ragflow_doc_id,
                  d.attempts, d.acknowledged, c.composio_user_id, c.composio_connected_account_id
           from documents d join corpus_connections c on c.id = d.connection_id
           where d.org_id = %s and d.state = 'pending' and c.status = 'ACTIVE'
           -- Documents an operator marked as wanted first (`npm run org -- priority`), then oldest waiting.
           order by d.priority desc, d.updated_at limit %s""", (org, BATCH)).fetchall()
    started, namespaces = [], {}
    onyx, from_onyx = load_onyx(), 0
    empties = load_empties() if onyx else {}
    for doc_id, provider, ext_id, name, mime, web_url, revision, old_rf, attempts, acknowledged, user, account in rows:
        db.execute("update documents set state = 'uploading', updated_at = now() where id = %s", (doc_id,))
        db.commit()
        try:
            f = {"external_id": ext_id, "name": name, "mimeType": mime}
            # A PDF tries its text layer first; an image has none to try.
            parse = "plain" if mime == "application/pdf" else "ocr" if mime in IMAGES.values() else "native"
            if provider == "dropbox":
                if account not in namespaces:
                    namespaces[account] = dropbox_namespace(cx, (user, account))
                text = onyx_text(onyx, f, revision) if onyx and parse != "native" and empties.get(ext_id) != revision else None
                if text:  # Onyx read this very rev already: its text goes instead of the file, which stays the citation
                    filename, content, extra = text
                    extra = extra or dropbox_modified(cx, (user, account), namespaces[account], f)  # an entry without `modified`
                    parse = "onyx"
                else:
                    filename, content, extra = dropbox_download(cx, (user, account), namespaces[account], f)
            else:
                (filename, content), extra = drive_download(cx, (user, account), f, acknowledged), {}
            if parse == "native":
                rows = as_sheet_text(name, filename, content)
                if rows:
                    (filename, content), parse = rows, "sheet-rows"
            if old_rf:
                rag.delete([old_rf])
            rf = rag.upload(filename, content)
            if parse in ("ocr", "plain", "onyx"):
                rag.configure(rf, OCR if parse == "ocr" else PLAIN)
            rag.tag(rf, {"provider": provider, "external_id": ext_id, "revision": revision, "web_url": web_url or "",
                         "parse": parse, **extra})
            db.execute("update documents set state = 'parsing', ragflow_doc_id = %s, sent_revision = %s, last_error = null, updated_at = now() where id = %s",
                       (rf, revision, doc_id))
            started.append(rf)
            from_onyx += parse == "onyx"
        except Skip as e:
            db.execute("update documents set state = 'skipped', last_error = %s, updated_at = now() where id = %s", (str(e), doc_id))
        except Blocked as e:
            # Not a failure and not ours to decide: it waits for an admin of this org to allow or skip it.
            db.execute("update documents set state = 'blocked', last_error = %s, updated_at = now() where id = %s", (str(e), doc_id))
            log(f"send: {name} needs a decision: {e}")
        except Exception as e:
            state = "failed" if attempts + 1 >= MAX_ATTEMPTS else "pending"
            db.execute("update documents set state = %s, attempts = attempts + 1, last_error = %s, updated_at = now() where id = %s",
                       (state, str(e)[:500], doc_id))
            log(f"send: {name}: {e}")
        db.commit()
    if onyx is not None:
        log(f"send: {from_onyx} file(s) sent from Onyx text")
    if started:
        rag.parse(started)
        log(f"send: {len(started)} file(s) uploaded and parsing")


def reconcile(db, org):
    """Our records against what RAGFlow holds (D32's startup reconcile), at the start of every pass:
    RAGFlow documents nothing refers to are deleted; a record whose document is really gone is re-queued;
    finished parses become indexed; failed or stalled ones are retried up to MAX_ATTEMPTS."""
    try:
        held = rag.all_docs()
    except Exception as e:
        log(f"reconcile: skipped, {e}")
        return
    rows = db.execute("select id, name, state, ragflow_doc_id, sent_revision, source_revision, mime_type, external_id from documents "
                      "where org_id = %s and ragflow_doc_id is not null", (org,)).fetchall()
    marks, now, empties = load_progress(), time.time(), load_empties()
    running = set()
    orphans = [rf for rf in held if rf not in {r[3] for r in rows}]
    if orphans:
        rag.delete(orphans)
        log(f"reconcile: deleted {len(orphans)} RAGFlow document(s) no record refers to")
    escalated, fell_back = 0, 0
    for doc_id, name, state, rf, sent, current, mime, ext_id in rows:
        d = held.get(rf)
        if d is None:
            db.execute("update documents set state = 'pending', ragflow_doc_id = null, last_error = 'gone from RAGFlow', updated_at = now() "
                       "where id = %s and state in ('parsing', 'indexed')", (doc_id,))
            continue
        if state != "parsing":
            continue
        run = str(d.get("run"))
        meta = d.get("meta_fields") or {}
        if run in ("DONE", "3") and not d.get("chunk_count") and meta.get("parse") == "onyx":
            # Onyx's text held nothing to index. That says nothing yet about the file, so it goes the usual way next
            # send (a PDF plain then OCR, an image OCR), once for this rev, with no attempt spent.
            empties[ext_id] = sent
            db.execute("update documents set state = 'pending', last_error = 'Onyx text parsed to nothing; reading the file itself', "
                       "updated_at = now() where id = %s", (doc_id,))
            fell_back += 1
            continue
        if run in ("DONE", "3") and mime == "application/pdf" and not d.get("chunk_count") and meta.get("parse") == "plain":
            # Nothing came out of the text layer: this one really is a scan, so it earns the slow parser.
            try:
                rag.configure(rf, OCR)
                rag.tag(rf, {**meta, "parse": "ocr"})
                rag.parse([rf])
                escalated += 1
            except Exception as e:
                log(f"reconcile: sending {name} to OCR failed: {e}")
            continue
        if mime in IMAGES.values() and meta.get("parse") != "onyx" and (
                run in ("DONE", "3") and not d.get("chunk_count")
                or run in ("FAIL", "4") and any(m in (d.get("progress_msg") or "") for m in NO_VISION)):
            # Recorded against what was sent, like an indexed file, so only a new revision sends it again.
            db.execute("update documents set state = 'skipped', last_error = 'no_text', indexed_revision = %s, chunk_count = 0, "
                       "updated_at = now() where id = %s", (sent, doc_id))
            continue
        if run in ("DONE", "3"):
            # Recorded against what was sent; if the file moved on meanwhile, it goes straight back to pending.
            db.execute("update documents set state = %s, indexed_revision = %s, chunk_count = %s, last_error = null, updated_at = now() where id = %s",
                       ("indexed" if sent == current else "pending", sent, d.get("chunk_count"), doc_id))
        elif run in ("FAIL", "4", "CANCEL", "2"):
            # Parsing can fail for passing reasons (a timed-out embedding call, a restart); retry before giving up.
            retry(db, doc_id, (d.get("progress_msg") or "parsing failed")[-500:])
        elif run in ("RUNNING", "1"):
            # Waiting is not stalling. A document behind others in RAGFlow's queue shows no progress for as
            # long as the queue takes, and RAGFlow says so ("N tasks are ahead in the queue"). Counting that
            # as stranded re-sent 309 live documents on LaborTech's box until they ran out of attempts
            # (2026-09-22). Only a document RAGFlow claims to be working on can stall.
            if "ahead in the queue" in (d.get("progress_msg") or ""):
                marks.pop(rf, None)
                continue
            # RAGFlow keeps touching a stranded doc's update_time, so only its progress shows whether work is happening.
            mark = f"{d.get('progress')}|{len(d.get('progress_msg') or '')}"
            seen = marks.get(rf)
            if not seen or seen["mark"] != mark:
                marks[rf] = {"mark": mark, "since": now}
                running.add(rf)
            elif now - seen["since"] >= STALL_SECONDS:
                try:
                    rag.stop([rf])
                except Exception as e:  # already finished or gone: the next pass sees its real state
                    log(f"reconcile: stopping stalled {name} failed: {e}")
                retry(db, doc_id, f"parsing stalled: no progress for {STALL_SECONDS // 60} minutes")
                log(f"reconcile: {name} stalled; sent back to be retried")
            else:
                running.add(rf)
    if escalated:
        log(f"reconcile: {escalated} scanned PDF(s) sent through OCR")
    if fell_back:
        log(f"reconcile: {fell_back} file(s) whose Onyx text parsed to nothing go the usual way")
    db.commit()
    save_progress({rf: m for rf, m in marks.items() if rf in running})
    if fell_back:
        save_empties(empties)


def retry(db, doc_id, why):
    """Back to pending for another send (which replaces the RAGFlow copy), or failed once MAX_ATTEMPTS are spent."""
    db.execute("update documents set state = case when attempts + 1 >= %s then 'failed' else 'pending' end, "
               "attempts = attempts + 1, last_error = %s, updated_at = now() where id = %s", (MAX_ATTEMPTS, why, doc_id))


def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_progress(marks):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(marks, f)
    os.replace(tmp, PROGRESS_FILE)


def load_empties():
    try:
        with open(ONYX_EMPTY_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_empties(empties):
    os.makedirs(os.path.dirname(ONYX_EMPTY_FILE), exist_ok=True)
    tmp = ONYX_EMPTY_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(empties, f)
    os.replace(tmp, ONYX_EMPTY_FILE)


def main():
    lock = open("/run/bebuilt-ingest.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another pass is running; exiting")
        return
    cfg = load_env("/etc/bebuilt/worker.env")
    tenant = json.load(open("/etc/bebuilt/ragflow-tenant.json"))
    global rag, ONYX_TEXT_DIR
    ONYX_TEXT_DIR = cfg.get("ONYX_TEXT_DIR") or ONYX_TEXT_DIR
    rag = RAGFlow(tenant["api_key"], tenant["dataset_id"])
    cx = Composio(cfg["COMPOSIO_API_KEY"], cfg.get("COMPOSIO_USER_ID"))
    org = cfg["ORG_ID"]
    t = time.time()
    with psycopg.connect(cfg["WORKER_DB_URL"], connect_timeout=20) as db:
        reconcile(db, org)
        plan(db, cx, org)
        send(db, cx, org)
        counts = dict(db.execute("select state, count(*) from documents where org_id = %s group by state", (org,)).fetchall())
    log(f"pass done in {time.time() - t:.0f}s: {counts}")


rag = None

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"pass failed: {e}")
        sys.exit(1)
