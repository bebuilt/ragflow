"""Ingestion worker: one client's confirmed selection → that client's RAGFlow dataset. Runs on the box.

    python3 deploy/bebuilt/ingest.py          # one pass: plan, send, check parsing (systemd timer runs this)

Config, all this client's own (the box leaves our hands; nothing here reaches another client):
    /etc/bebuilt/worker.env         WORKER_DB_URL (worker_<slug>: RLS admits this org's rows only),
                                    ORG_ID, COMPOSIO_API_KEY, COMPOSIO_USER_ID
    /etc/bebuilt/ragflow-tenant.json  api_key, dataset_id (written by tenant-setup.py)

A pass:
  1. plan   walk every confirmed folder of every connected store, upsert what is there into `documents`;
            a new file or a moved revision becomes `pending`; a file gone from a COMPLETE walk is removed
            from RAGFlow. An incomplete or failed walk never removes anything.
  2. send   pending → download through Composio (Google Docs/Sheets/Slides exported to Office formats) →
            upload to RAGFlow, tag with its source, start parsing. The old copy goes first on a revision move.
  0. reconcile  our records against everything RAGFlow holds: orphans deleted, finished parses indexed,
                failed or stalled ones retried (it runs first, so a pass starts from what is actually there).
"""
import fcntl
import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg
import requests

RAGFLOW = "http://127.0.0.1:8080/api/v1"
COMPOSIO = "https://backend.composio.dev/api/v3.1"
DRIVE_TOOLS_VERSION = "20260915_00"  # keep in step with bebuilt-app src/lib/storage/googledrive.ts
BATCH = 25  # files sent per pass
MAX_BYTES = 100 * 1024 * 1024
MAX_ATTEMPTS = 3
# A parse whose progress has not moved in this long is stalled, not slow: a RAGFlow restart mid-embed (2026-09-18)
# left five files RUNNING at 80% for hours with nothing working on them, and RAGFlow never retries those itself.
STALL_SECONDS = 30 * 60
PROGRESS_FILE = "/var/lib/bebuilt/ingest-progress.json"  # {ragflow_doc_id: {"mark", "since"}} between passes

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


class Composio:
    def __init__(self, key, user_id):
        self.s = requests.Session()
        self.s.headers["x-api-key"] = key
        self.user_id = user_id

    def run(self, account_id, slug, args):
        r = self.s.post(f"{COMPOSIO}/tools/execute/{slug}", timeout=120, json={
            "connected_account_id": account_id, "user_id": self.user_id, "version": DRIVE_TOOLS_VERSION, "arguments": args,
        })
        r.raise_for_status()
        body = r.json()
        if not body.get("successful"):
            raise RuntimeError(f"{slug}: {body.get('error') or 'failed'}")
        return body.get("data") or {}


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

    def parse(self, doc_ids):
        self._ok(self.s.post(f"{RAGFLOW}/datasets/{self.ds}/chunks", json={"document_ids": doc_ids}, timeout=60))

    def stop(self, doc_ids):
        self._ok(self.s.delete(f"{RAGFLOW}/datasets/{self.ds}/chunks", json={"document_ids": doc_ids}, timeout=60))

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

def drive_walk(cx, account_id, root):
    """Every file under `root` (breadth-first, paged). Returns (files, complete)."""
    seen, files, queue, complete = {root}, {}, [root], True
    fields = "nextPageToken,incompleteSearch,files(id,name,mimeType,size,modifiedTime,version,webViewLink,shortcutDetails)"
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
            page = cx.run(account_id, "GOOGLEDRIVE_FIND_FILE", args)
            complete = complete and not page.get("incompleteSearch")
            for f in page.get("files") or []:
                if f["id"] in seen:
                    continue
                seen.add(f["id"])
                if f["mimeType"] == FOLDER:
                    queue.append(f["id"])
                elif f["mimeType"] != SHORTCUT and not f.get("shortcutDetails"):
                    files[f["id"]] = f
            token = page.get("nextPageToken")
            if not token:
                break
    return files, complete


def drive_download(cx, account_id, f):
    """(filename, bytes) for a file RAGFlow can parse, or raise Skip."""
    mime = f["mimeType"]
    if mime in EXPORT:
        export_mime, ext = EXPORT[mime]
        data = cx.run(account_id, "GOOGLEDRIVE_DOWNLOAD_FILE", {"fileId": f["external_id"], "mime_type": export_mime})
        if data.get("export_size_limit_exceeded"):
            raise Skip("Google can export at most 10 MB of this file type")
    else:
        ext = INDEXED[mime]
        data = cx.run(account_id, "GOOGLEDRIVE_DOWNLOAD_FILE", {"fileId": f["external_id"]})
    content = data.get("downloaded_file_content") or {}
    url = content.get("s3url")
    if not url:
        raise RuntimeError("Composio returned no file")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    name = f["name"] if f["name"].lower().endswith(ext) else f"{f['name']}{ext}"
    return name, r.content


class Skip(Exception):
    pass


def indexable(mime, size):
    if mime not in EXPORT and mime not in INDEXED:
        return "this file type isn't indexed yet"
    if size and size > MAX_BYTES:
        return "larger than 100 MB"
    return None


# --- the pass -----------------------------------------------------------------------------------------

def plan(db, cx, org):
    sources = db.execute(
        "select s.corpus_id, s.provider, s.external_id, c.composio_connected_account_id "
        "from corpus_sources s join corpus_connections c on c.corpus_id = s.corpus_id and c.provider = s.provider "
        "where s.org_id = %s and s.confirmed_at is not null and c.status = 'ACTIVE'", (org,)).fetchall()
    walked = {}  # (corpus, provider) -> (files, complete)
    for corpus, provider, root, account in sources:
        if provider != "googledrive":
            log(f"plan: no ingestion adapter for {provider} yet; skipping {root}")
            continue
        try:
            files, complete = drive_walk(cx, account, root)
        except Exception as e:  # a failed walk removes nothing
            log(f"plan: walking {provider}:{root} failed: {e}")
            files, complete = {}, False
        prev = walked.get((corpus, provider), ({}, True))
        walked[(corpus, provider)] = ({**prev[0], **files}, prev[1] and complete)

    for (corpus, provider), (files, complete) in walked.items():
        for f in files.values():
            size = int(f["size"]) if f.get("size") else None
            revision = str(f.get("version") or f.get("modifiedTime") or "")
            why_not = indexable(f["mimeType"], size)
            db.execute(
                """insert into documents (org_id, corpus_id, provider, external_id, name, mime_type, size, web_url,
                                          source_revision, state, last_error, seen_at, updated_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                   on conflict (corpus_id, provider, external_id) do update set
                     name = excluded.name, mime_type = excluded.mime_type, size = excluded.size,
                     web_url = excluded.web_url, source_revision = excluded.source_revision, seen_at = now(),
                     state = case
                       when excluded.state = 'skipped' then 'skipped'
                       when documents.state = 'removed' then 'pending'
                       when documents.indexed_revision is distinct from excluded.source_revision
                            and documents.state in ('indexed', 'skipped') then 'pending'
                       when documents.state = 'failed' and documents.source_revision is distinct from excluded.source_revision then 'pending'
                       when documents.state = 'failed' and documents.attempts < 3 then 'pending'
                       else documents.state end,
                     attempts = case when documents.source_revision is distinct from excluded.source_revision then 0 else documents.attempts end,
                     last_error = case when excluded.state = 'skipped' then excluded.last_error else documents.last_error end,
                     updated_at = now()""",
                (org, corpus, provider, f["id"], f["name"], f["mimeType"], size, f.get("webViewLink"), revision,
                 "skipped" if why_not else "pending", why_not))
        if complete:
            gone = db.execute(
                "select id, ragflow_doc_id from documents where org_id = %s and corpus_id = %s and provider = %s "
                "and state <> 'removed' and not (external_id = any(%s::text[]))", (org, corpus, provider, list(files))).fetchall()
            for doc_id, rf in gone:
                if rf:
                    rag.delete([rf])
                db.execute("update documents set state = 'removed', ragflow_doc_id = null, indexed_revision = null, updated_at = now() where id = %s", (doc_id,))
            if gone:
                log(f"plan: removed {len(gone)} file(s) no longer in the selection")
        else:
            log(f"plan: {provider} walk incomplete; nothing removed this pass")
    db.commit()


def send(db, cx, org):
    rows = db.execute(
        """select d.id, d.provider, d.external_id, d.name, d.mime_type, d.web_url, d.source_revision, d.ragflow_doc_id,
                  d.attempts, c.composio_connected_account_id
           from documents d join corpus_connections c on c.corpus_id = d.corpus_id and c.provider = d.provider
           where d.org_id = %s and d.state = 'pending' and c.status = 'ACTIVE'
           order by d.updated_at limit %s""", (org, BATCH)).fetchall()
    started = []
    for doc_id, provider, ext_id, name, mime, web_url, revision, old_rf, attempts, account in rows:
        db.execute("update documents set state = 'uploading', updated_at = now() where id = %s", (doc_id,))
        db.commit()
        try:
            filename, content = drive_download(cx, account, {"external_id": ext_id, "name": name, "mimeType": mime})
            if old_rf:
                rag.delete([old_rf])
            rf = rag.upload(filename, content)
            rag.tag(rf, {"provider": provider, "external_id": ext_id, "revision": revision, "web_url": web_url or ""})
            db.execute("update documents set state = 'parsing', ragflow_doc_id = %s, sent_revision = %s, last_error = null, updated_at = now() where id = %s",
                       (rf, revision, doc_id))
            started.append(rf)
        except Skip as e:
            db.execute("update documents set state = 'skipped', last_error = %s, updated_at = now() where id = %s", (str(e), doc_id))
        except Exception as e:
            state = "failed" if attempts + 1 >= MAX_ATTEMPTS else "pending"
            db.execute("update documents set state = %s, attempts = attempts + 1, last_error = %s, updated_at = now() where id = %s",
                       (state, str(e)[:500], doc_id))
            log(f"send: {name}: {e}")
        db.commit()
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
    rows = db.execute("select id, name, state, ragflow_doc_id, sent_revision, source_revision from documents "
                      "where org_id = %s and ragflow_doc_id is not null", (org,)).fetchall()
    marks, now = load_progress(), time.time()
    running = set()
    orphans = [rf for rf in held if rf not in {r[3] for r in rows}]
    if orphans:
        rag.delete(orphans)
        log(f"reconcile: deleted {len(orphans)} RAGFlow document(s) no record refers to")
    for doc_id, name, state, rf, sent, current in rows:
        d = held.get(rf)
        if d is None:
            db.execute("update documents set state = 'pending', ragflow_doc_id = null, last_error = 'gone from RAGFlow', updated_at = now() "
                       "where id = %s and state in ('parsing', 'indexed')", (doc_id,))
            continue
        if state != "parsing":
            continue
        run = str(d.get("run"))
        if run in ("DONE", "3"):
            # Recorded against what was sent; if the file moved on meanwhile, it goes straight back to pending.
            db.execute("update documents set state = %s, indexed_revision = %s, chunk_count = %s, last_error = null, updated_at = now() where id = %s",
                       ("indexed" if sent == current else "pending", sent, d.get("chunk_count"), doc_id))
        elif run in ("FAIL", "4", "CANCEL", "2"):
            # Parsing can fail for passing reasons (a timed-out embedding call, a restart); retry before giving up.
            retry(db, doc_id, (d.get("progress_msg") or "parsing failed")[-500:])
        elif run in ("RUNNING", "1"):
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
    db.commit()
    save_progress({rf: m for rf, m in marks.items() if rf in running})


def retry(db, doc_id, why):
    """Back to pending for another send (which replaces the RAGFlow copy), or failed once MAX_ATTEMPTS are spent."""
    db.execute("update documents set state = case when attempts + 1 >= %s then 'failed' else 'pending' end, "
               "attempts = attempts + 1, last_error = %s, updated_at = now() where id = %s", (MAX_ATTEMPTS, why, doc_id))


def load_progress():
    try:
        return json.load(open(PROGRESS_FILE))
    except (OSError, ValueError):
        return {}


def save_progress(marks):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(marks, f)
    os.replace(tmp, PROGRESS_FILE)


def main():
    lock = open("/run/bebuilt-ingest.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another pass is running; exiting")
        return
    cfg = load_env("/etc/bebuilt/worker.env")
    tenant = json.load(open("/etc/bebuilt/ragflow-tenant.json"))
    global rag
    rag = RAGFlow(tenant["api_key"], tenant["dataset_id"])
    cx = Composio(cfg["COMPOSIO_API_KEY"], cfg["COMPOSIO_USER_ID"])
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
