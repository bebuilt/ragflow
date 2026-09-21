"""Ingestion worker: one client's confirmed selection → that client's RAGFlow dataset. Runs on the box.

    python3 deploy/bebuilt/ingest.py          # one pass: plan, send, check parsing (systemd timer runs this)

Config, all this client's own (the box leaves our hands; nothing here reaches another client):
    /etc/bebuilt/worker.env         WORKER_DB_URL (worker_<slug>: RLS admits this org's rows only),
                                    ORG_ID, COMPOSIO_API_KEY, COMPOSIO_USER_ID
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
SYNC_FILE = "/var/lib/bebuilt/drive-sync.json"  # {"<corpus>:<provider>": {token, folders, roots, walked_at}}
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

    def configure(self, doc_id, parser_config):
        self._ok(self.s.put(f"{RAGFLOW}/datasets/{self.ds}/documents/{doc_id}", json={"parser_config": parser_config}, timeout=60))

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
    """Every file under `root` (breadth-first, paged). Returns (files, folders, complete)."""
    seen, files, queue, complete = {root}, {}, [root], True
    folders = {root}
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
                    folders.add(f["id"])
                elif f["mimeType"] != SHORTCUT and not f.get("shortcutDetails"):
                    files[f["id"]] = f
            token = page.get("nextPageToken")
            if not token:
                break
    return files, folders, complete


def drive_start_token(cx, account_id):
    """Drive's bookmark for 'changes from here on'. Taken before a walk, so nothing during it is missed."""
    d = cx.run(account_id, "GOOGLEDRIVE_GET_CHANGES_START_PAGE_TOKEN", {"supportsAllDrives": True})
    return d.get("startPageToken") or d.get("start_page_token")


def drive_changes(cx, account_id, token):
    """(changes, next token) for everything this account can see that changed since `token`."""
    fields = ("nextPageToken,newStartPageToken,changes(removed,fileId,file(id,name,mimeType,size,modifiedTime,"
              "version,webViewLink,parents,trashed,shortcutDetails))")
    changes = []
    while True:
        page = cx.run(account_id, "GOOGLEDRIVE_LIST_CHANGES", {
            "pageToken": token, "pageSize": 1000, "fields": fields, "includeRemoved": True,
            "includeItemsFromAllDrives": True, "supportsAllDrives": True, "restrictToMyDrive": False, "spaces": "drive",
        })
        changes += page.get("changes") or []
        nxt = page.get("nextPageToken")
        if not nxt:
            return changes, page.get("newStartPageToken") or token
        token = nxt


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

def upsert_file(db, org, corpus, provider, f):
    """One Drive file into `documents`: new work becomes pending, a moved revision re-queues, a kind we
    cannot read is recorded as skipped so the screens can say why."""
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
        return json.load(open(SYNC_FILE))
    except (OSError, ValueError):
        return {}


def save_sync(state):
    os.makedirs(os.path.dirname(SYNC_FILE), exist_ok=True)
    tmp = SYNC_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, SYNC_FILE)


def full_walk(db, cx, org, corpus, provider, account, roots, count_missed=False):
    """Every confirmed folder, listed in full. Removals happen only from a COMPLETE walk (D32): a partial
    listing once sent 39 live files back for re-upload. Returns (sync state, files, folders, missed)."""
    files, folders, complete = {}, set(), True
    token = None
    try:  # the bookmark is taken BEFORE the walk, so a change during it is caught by the next pass
        token = drive_start_token(cx, account)
    except Exception as e:
        log(f"plan: no changes bookmark ({e}); next pass walks in full again")
    for root in roots:
        try:
            got, walked, ok = drive_walk(cx, account, root)
        except Exception as e:  # a failed walk removes nothing
            log(f"plan: walking {provider}:{root} failed: {e}")
            got, walked, ok = {}, set(), False
        files.update(got)
        folders |= walked
        complete = complete and ok
    # What the changes feed should already have brought us: anything here that we do not hold at this
    # revision was missed (only meaningful when a feed was running, not on a first walk).
    missed = 0
    if count_missed and files:
        held = dict(db.execute(
            "select external_id, source_revision from documents where org_id = %s and corpus_id = %s and provider = %s "
            "and state <> 'removed' and external_id = any(%s::text[])", (org, corpus, provider, list(files))).fetchall())
        for external_id, f in files.items():
            revision = str(f.get("version") or f.get("modifiedTime") or "")
            if held.get(external_id) != revision:
                missed += 1
    for f in files.values():
        upsert_file(db, org, corpus, provider, f)
    if complete:
        gone = db.execute(
            "select external_id from documents where org_id = %s and corpus_id = %s and provider = %s "
            "and state <> 'removed' and not (external_id = any(%s::text[]))", (org, corpus, provider, list(files))).fetchall()
        for (external_id,) in gone:
            drop_file(db, org, corpus, provider, external_id)
        if gone:
            log(f"plan: removed {len(gone)} file(s) no longer in the selection")
    else:
        log(f"plan: {provider} walk incomplete; nothing removed this pass")
    log(f"plan: walked {len(folders)} folder(s), {len(files)} file(s)" + (f", {missed} missed by the changes feed" if count_missed else ""))
    state = {"token": token, "folders": sorted(folders), "roots": sorted(roots), "walked_at": time.time()} if complete and token else None
    return state, len(files), len(folders), missed


def incremental(db, cx, org, corpus, provider, account, saved):
    """What changed since the last pass, in one or two calls. False if the feed could not be read, which
    sends this pass back to a full walk."""
    try:
        changes, token = drive_changes(cx, account, saved["token"])
    except Exception as e:
        log(f"plan: changes feed failed ({e})")
        return False
    folders = set(saved["folders"])
    fresh, touched = [], 0
    for ch in changes:
        f = ch.get("file") or {}
        external_id = ch.get("fileId") or f.get("id")
        if not external_id:
            continue
        if ch.get("removed") or f.get("trashed"):
            touched += drop_file(db, org, corpus, provider, external_id)
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
            upsert_file(db, org, corpus, provider, f)
            touched += 1
        elif parents:  # moved out of every ticked folder
            touched += drop_file(db, org, corpus, provider, external_id)
    for folder in fresh:
        try:
            got, walked, _ = drive_walk(cx, account, folder)
        except Exception as e:
            log(f"plan: walking new folder {folder} failed: {e}")
            continue
        folders |= walked
        for f in got.values():
            upsert_file(db, org, corpus, provider, f)
        touched += len(got)
    saved["token"] = token
    saved["folders"] = sorted(folders)
    if changes:
        log(f"plan: {len(changes)} change(s), {len(fresh)} new folder(s), {touched} file(s) touched")
    return True


def plan(db, cx, org):
    sources = db.execute(
        "select s.corpus_id, s.provider, s.external_id, c.composio_connected_account_id "
        "from corpus_sources s join corpus_connections c on c.corpus_id = s.corpus_id and c.provider = s.provider "
        "where s.org_id = %s and s.confirmed_at is not null and c.status = 'ACTIVE'", (org,)).fetchall()
    selections = {}  # (corpus, provider) -> {account, roots}
    for corpus, provider, root, account in sources:
        if provider != "googledrive":
            log(f"plan: no ingestion adapter for {provider} yet; skipping {root}")
            continue
        entry = selections.setdefault((corpus, provider), {"account": account, "roots": []})
        entry["roots"].append(root)

    state = load_sync()
    ran = []
    for (corpus, provider), entry in selections.items():
        key = f"{corpus}:{provider}"
        saved = state.get(key) or {}
        # A full walk when there is nothing to go on, when the ticked folders change, or once a day.
        usable = saved.get("token") and saved.get("folders") and saved.get("roots") == sorted(entry["roots"])
        due = time.time() - (saved.get("walked_at") or 0) > FULL_WALK_SECONDS
        if usable and not due and incremental(db, cx, org, corpus, provider, entry["account"], saved):
            state[key] = saved
            ran.append(("changes", 0, 0, 0))
        else:
            fresh, files, folders, missed = full_walk(db, cx, org, corpus, provider, entry["account"], entry["roots"], count_missed=usable)
            ran.append(("walk", folders, files, missed))
            if fresh:
                state[key] = fresh
            else:
                state.pop(key, None)
    save_sync({k: v for k, v in state.items() if k in {f"{c}:{p}" for c, p in selections}})
    # The worker's heartbeat: a pass that changes nothing still proves the worker is alive, which document
    # timestamps no longer can now that most passes write nothing.
    if ran:
        mode = "walk" if any(r[0] == "walk" for r in ran) else "changes"
        db.execute("insert into ingest_runs (org_id, mode, folders, files, missed) values (%s, %s, %s, %s, %s)",
                   (org, mode, sum(r[1] for r in ran), sum(r[2] for r in ran), sum(r[3] for r in ran)))
    db.commit()


def send(db, cx, org):
    rows = db.execute(
        """select d.id, d.provider, d.external_id, d.name, d.mime_type, d.web_url, d.source_revision, d.ragflow_doc_id,
                  d.attempts, c.composio_connected_account_id
           from documents d join corpus_connections c on c.corpus_id = d.corpus_id and c.provider = d.provider
           where d.org_id = %s and d.state = 'pending' and c.status = 'ACTIVE'
           -- Documents an operator marked as wanted first (`npm run org -- priority`), then oldest waiting.
           order by d.priority desc, d.updated_at limit %s""", (org, BATCH)).fetchall()
    started = []
    for doc_id, provider, ext_id, name, mime, web_url, revision, old_rf, attempts, account in rows:
        db.execute("update documents set state = 'uploading', updated_at = now() where id = %s", (doc_id,))
        db.commit()
        try:
            filename, content = drive_download(cx, account, {"external_id": ext_id, "name": name, "mimeType": mime})
            if old_rf:
                rag.delete([old_rf])
            rf = rag.upload(filename, content)
            plain = mime == "application/pdf"
            if plain:
                rag.configure(rf, PLAIN)
            rag.tag(rf, {"provider": provider, "external_id": ext_id, "revision": revision, "web_url": web_url or "",
                         "parse": "plain" if plain else "native"})
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
    rows = db.execute("select id, name, state, ragflow_doc_id, sent_revision, source_revision, mime_type from documents "
                      "where org_id = %s and ragflow_doc_id is not null", (org,)).fetchall()
    marks, now = load_progress(), time.time()
    running = set()
    orphans = [rf for rf in held if rf not in {r[3] for r in rows}]
    if orphans:
        rag.delete(orphans)
        log(f"reconcile: deleted {len(orphans)} RAGFlow document(s) no record refers to")
    escalated = 0
    for doc_id, name, state, rf, sent, current, mime in rows:
        d = held.get(rf)
        if d is None:
            db.execute("update documents set state = 'pending', ragflow_doc_id = null, last_error = 'gone from RAGFlow', updated_at = now() "
                       "where id = %s and state in ('parsing', 'indexed')", (doc_id,))
            continue
        if state != "parsing":
            continue
        run = str(d.get("run"))
        meta = d.get("meta_fields") or {}
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
    if escalated:
        log(f"reconcile: {escalated} scanned PDF(s) sent through OCR")
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
