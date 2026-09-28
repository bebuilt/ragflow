"""Tests for ingest.py's selection, ownership and removal logic, against a real Postgres and a fake Drive and Dropbox.

    docker run -d --rm --name ingest-test-pg -e POSTGRES_PASSWORD=pw -p 127.0.0.1:55433:5432 postgres:17
    INGEST_TEST_DSN=postgresql://postgres:pw@127.0.0.1:55433/postgres python3 -m unittest deploy/bebuilt/test_ingest.py

Needs psycopg and requests (the box has both). The database cases are skipped without INGEST_TEST_DSN. Each person's
Drive is a fake account with its own folders; a Dropbox account is a fake team behind a fake Composio proxy; Composio,
RAGFlow and the file download are fakes that record what was asked of them. Onyx's text sidecar, when a case wants one,
is written into the case's temporary directory.
"""
import json
import os
import sys
import tempfile
import unittest
import uuid

import requests

sys.path.insert(0, os.path.dirname(__file__))
DSN = os.environ.get("INGEST_TEST_DSN")

if DSN:
    import psycopg
try:
    import ingest
except ImportError:  # psycopg missing: only the database cases need it, and they are skipped already
    ingest = None

SCHEMA = """
drop table if exists documents, corpus_sources, corpus_connections, ingest_runs cascade;
create table corpus_connections (
  id uuid primary key, corpus_id uuid not null, org_id uuid not null, provider text not null,
  composio_connected_account_id text not null, status text not null, grant_holder_user_id uuid not null,
  composio_user_id text not null);
create table corpus_sources (
  corpus_id uuid not null, org_id uuid not null, provider text not null, external_id text not null,
  added_by uuid not null, confirmed_at timestamptz, connection_id uuid, primary key (corpus_id, provider, external_id));
create table documents (
  id uuid primary key default gen_random_uuid(), org_id uuid not null, corpus_id uuid not null, provider text not null,
  external_id text not null, name text not null, mime_type text not null, size bigint, web_url text,
  source_revision text not null, indexed_revision text, sent_revision text, ragflow_doc_id text,
  state text not null default 'pending', attempts integer not null default 0, last_error text, chunk_count integer,
  seen_at timestamptz not null default now(), updated_at timestamptz not null default now(),
  priority smallint not null default 0, connection_id uuid, acknowledged boolean not null default false,
  unique (corpus_id, provider, external_id));
create table ingest_runs (org_id uuid not null, at timestamptz not null default clock_timestamp(), mode text not null,
  folders integer not null, files integer not null, missed integer not null);
"""

ORG, CORPUS = str(uuid.uuid4()), str(uuid.uuid4())
# Fixed ids so the walk order (sorted by connection id) is known: A before B.
A, B = "00000000-0000-0000-0000-00000000000a", "00000000-0000-0000-0000-00000000000b"
UA, UB = str(uuid.uuid4()), str(uuid.uuid4())
PDF = "application/pdf"


def file(fid, rev="1", md5=None):
    """A Drive file. Without `md5` its revision falls back to `version`, as the older cases expect."""
    return {"id": fid, "name": f"{fid}.pdf", "mimeType": PDF, "size": "10", "version": rev, "webViewLink": f"https://drive/{fid}",
            **({"md5Checksum": md5} if md5 else {})}


class FakeComposio:
    """Per account: folder -> children; what each account can download; a queue of changes for its feed."""

    def __init__(self):
        self.drives = {}  # account -> {folder: [file dicts]}
        self.changes = {}  # account -> [change]
        self.fail = set()  # accounts whose listings raise
        self.calls = []  # (user, account, slug)
        self.tokens = 0

    def run(self, acct, slug, args):
        user, account = acct
        self.calls.append((user, account, slug))
        drive = self.drives.get(account, {})
        if slug == "GOOGLEDRIVE_FIND_FILE":
            if account in self.fail:
                raise RuntimeError("listing failed")
            return {"files": list(drive.get(args["folder_id"], []))}
        if slug == "GOOGLEDRIVE_GET_FILE_METADATA":  # only ever asked for My Drive's real id
            assert args == {"fileId": "root", "fields": "id"}, args
            return {"id": f"mydrive-{account}"}
        if slug == "GOOGLEDRIVE_GET_CHANGES_START_PAGE_TOKEN":
            self.tokens += 1
            return {"startPageToken": f"t{self.tokens}"}
        if slug == "GOOGLEDRIVE_LIST_CHANGES":
            out, self.changes[account] = self.changes.get(account, []), []
            self.tokens += 1
            return {"changes": out, "newStartPageToken": f"t{self.tokens}"}
        if slug == "GOOGLEDRIVE_DOWNLOAD_FILE":
            if not any(f["id"] == args["fileId"] for files in drive.values() for f in files):
                raise RuntimeError(f"{account} cannot see {args['fileId']}")
            return {"downloaded_file_content": {"s3url": f"fake://{account}/{args['fileId']}"}}
        raise AssertionError(slug)

    def downloads(self):
        return [(u, a) for (u, a, s) in self.calls if s == "GOOGLEDRIVE_DOWNLOAD_FILE"]

    def proxy(self, acct, endpoint, body=None, headers=None):
        return self.dbx.proxy(acct, endpoint, body, headers)


NS = "2552214643"
ROOT_HEADER = json.dumps({".tag": "root", "root": NS})


def entry(path, rev="1", size=10, **extra):
    """A Dropbox file as list_folder reports it; `path` is its display path."""
    return {".tag": "file", "name": path.rsplit("/", 1)[1], "path_display": path, "path_lower": path.lower(),
            "id": f"id:{path.lower()}", "rev": rev, "size": size, "server_modified": f"2026-09-0{rev}T12:00:00Z",
            "is_downloadable": True, **extra}


class FakeDropboxProxy:
    """Composio's proxy in front of one Dropbox team per account. A listing is paged two entries at a time; each
    root's cursor continues that root only, and returns whatever the test queued for it (`changes`)."""

    def __init__(self):
        self.teams = {}  # account -> {path_display: entry}
        self.changes = {}  # (account, root) -> [entry]
        self.reset = set()  # (account, root) whose next continue answers `reset`
        self.fail = set()  # (account, root) whose listing fails
        self.calls = []  # (account, endpoint tail, headers)
        self.pages = {}
        self.n = 0

    @staticmethod
    def answer(data, status=200, **extra):
        return {"data": data, "status": status, "headers": {}, **extra}

    def page(self, root, entries):
        self.n += 1
        cursor = f"{root}|{self.n}"
        if len(entries) > 2:
            self.pages[cursor] = (root, entries[2:])
        return {"entries": entries[:2], "cursor": cursor, "has_more": len(entries) > 2}

    def proxy(self, acct, endpoint, body=None, headers=None):
        _, account = acct
        tail = endpoint.split("/2/", 1)[1]
        self.calls.append((account, tail, dict(headers or {})))
        if tail == "users/get_current_account":
            return self.answer({"root_info": {".tag": "team", "root_namespace_id": NS, "home_namespace_id": "999"}})
        if (headers or {}).get("Dropbox-API-Path-Root") != ROOT_HEADER:  # the home namespace has no team folders
            return self.answer({"error_summary": "path/not_found/"}, 409)
        team = self.teams.get(account, {})
        if tail == "files/list_folder":
            root = body["path"]
            assert body["recursive"] is True and body["limit"] == 2000
            if (account, root) in self.fail:
                return self.answer("listing failed", 500)
            under = [e for p, e in sorted(team.items()) if p.lower().startswith(root + "/")]
            return self.answer(self.page(root, under))
        if tail == "files/list_folder/continue":
            root, rest = self.pages.pop(body["cursor"], (body["cursor"].split("|")[0], None))
            if rest is None:  # a stored cursor: the changes since it
                if (account, root) in self.reset:
                    self.reset.discard((account, root))
                    return self.answer({"error": {".tag": "reset"}, "error_summary": "reset/"}, 409)
                rest = self.changes.pop((account, root), [])
            return self.answer(self.page(root, rest))
        if tail == "files/get_metadata":
            f = next((e for e in team.values() if e["path_lower"] == body["path"]), None)
            return self.answer(f) if f else self.answer({"error_summary": "path/not_found/"}, 409)
        path = json.loads(headers["Dropbox-API-Arg"])["path"]
        f = next((e for e in team.values() if e["path_lower"] == path), None)
        if f is None:
            return self.answer({"error_summary": "path/not_found/"}, 409)
        if tail == "files/download":
            return self.answer({}, headers={"dropbox-api-result": json.dumps(f)}, binary_data={"url": f"fake://dropbox{path}"})
        if tail == "files/export":
            if not f.get("export_as"):
                return self.answer({"error": {".tag": "non_exportable"}, "error_summary": "non_exportable/"}, 409)
            name = f["name"].rsplit(".", 1)[0] + "." + f["export_as"]
            meta = {"export_metadata": {"name": name, "size": 5}, "file_metadata": f}
            return self.answer({}, headers={"dropbox-api-result": json.dumps(meta)}, binary_data={"url": f"fake://dropbox{path}"})
        raise AssertionError(tail)

    def tails(self):
        return [t for (_, t, _) in self.calls]


class FakeRAG:
    def __init__(self):
        self.docs, self.n, self.uploads, self.contents = {}, 0, [], {}
        self.tags, self.configs, self.deleted = {}, {}, []

    def queued(self):
        return 0

    def upload(self, filename, content):
        self.n += 1
        rf = f"rf{self.n}"
        self.docs[rf] = {"id": rf, "run": "UNSTART"}
        self.uploads.append(filename)
        self.contents[filename] = content
        return rf

    def configure(self, rf, parser_config):
        self.configs[rf] = parser_config

    def tag(self, rf, meta):
        self.tags[rf] = meta

    def parse(self, ids):
        pass

    def stop(self, ids):
        pass

    def delete(self, ids):
        for rf in ids:
            self.deleted.append(rf)
            self.docs.pop(rf, None)

    def all_docs(self):
        return dict(self.docs)


class _Resp:
    content = b"%PDF"

    def raise_for_status(self):
        pass


class _Expired:
    def raise_for_status(self):
        raise requests.HTTPError("403 Request has expired")


@unittest.skipUnless(DSN, "set INGEST_TEST_DSN to a throwaway Postgres")
class IngestTest(unittest.TestCase):
    def setUp(self):
        self.db = psycopg.connect(DSN, autocommit=False)
        self.db.execute(SCHEMA)
        self.db.commit()
        self.tmp = tempfile.mkdtemp()
        ingest.SYNC_FILE = os.path.join(self.tmp, "drive-sync.json")
        ingest.PROGRESS_FILE = os.path.join(self.tmp, "progress.json")
        ingest.rag = self.rag = FakeRAG()
        ingest.BACKOFF = 0
        ingest.ONYX_TEXT_DIR = os.path.join(self.tmp, "onyx-text")  # absent unless a case writes one
        ingest.ONYX_EMPTY_FILE = os.path.join(self.tmp, "onyx-empty.json")
        self.logged, self._log = [], ingest.log
        ingest.log = lambda msg: (self.logged.append(msg), self._log(msg))
        self.fetched, self.expired = [], set()
        self._get = ingest.requests.get
        ingest.requests.get = self.fetch
        self.cx = FakeComposio()
        self.dbx = self.cx.dbx = FakeDropboxProxy()

    def tearDown(self):
        ingest.requests.get = self._get
        ingest.log = self._log
        self.db.close()

    def fetch(self, url, timeout=None):
        self.fetched.append(url)
        if url in self.expired:
            self.expired.discard(url)
            return _Expired()
        return _Resp()

    # --- fixture helpers ---
    def connect(self, cid, holder, user, account, status="ACTIVE", provider="googledrive"):
        self.db.execute("insert into corpus_connections values (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (cid, CORPUS, ORG, provider, account, status, holder, user))
        self.db.commit()

    def tick(self, folder, cid, holder, legacy=False, provider="googledrive"):
        self.db.execute("insert into corpus_sources values (%s, %s, %s, %s, %s, now(), %s)",
                        (CORPUS, ORG, provider, folder, holder, None if legacy else cid))
        self.db.commit()

    def untick(self, folder):
        self.db.execute("delete from corpus_sources where external_id = %s", (folder,))
        self.db.commit()

    def two_people(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.connect(B, UB, f"org-ask-{UB}", "ca_b")
        self.cx.drives["ca_a"] = {"FA": [file("a1"), file("a2")]}
        self.cx.drives["ca_b"] = {"FB": [file("b1")]}
        self.tick("FA", A, UA)
        self.tick("FB", B, UB)

    def run_pass(self):
        ingest.plan(self.db, self.cx, ORG)
        ingest.send(self.db, self.cx, ORG)

    def state(self):
        with open(ingest.SYNC_FILE) as f:
            return json.load(f)

    def save_state(self, state):
        with open(ingest.SYNC_FILE, "w") as f:
            json.dump(state, f)

    def force_walk(self):
        state = self.state()
        for v in state.values():
            v["walk"] = True
        self.save_state(state)

    def walked(self):
        return "GOOGLEDRIVE_FIND_FILE" in [s for (_, _, s) in self.cx.calls]

    def docs(self):
        return {r[0]: (r[1], r[2]) for r in self.db.execute(
            "select external_id, state, connection_id::text from documents").fetchall()}

    def row(self, external_id, *cols):
        return self.db.execute(f"select {', '.join(cols)} from documents where external_id = %s", (external_id,)).fetchone()

    def team(self):
        """One Dropbox connection (Amy's) with two ticked team folders."""
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {e["path_display"]: e for e in [
            entry("/Team/Leases/a.pdf"), entry("/Team/Leases/b.pdf"), entry("/Team/Leases/Old/c.pdf"),
            entry("/Team/Contracts/d.pdf"), entry("/Other/e.pdf")]}
        self.tick("/team/leases", A, UA, provider="dropbox")
        self.tick("/team/contracts", A, UA, provider="dropbox")

    def cursors(self):
        return self.state()[f"{CORPUS}:dropbox:{A}"]["cursors"]

    # --- cases ---
    def test_each_person_is_walked_and_downloaded_through_their_own_identity(self):
        self.two_people()
        self.run_pass()
        d = self.docs()
        self.assertEqual(d, {"a1": ("parsing", A), "a2": ("parsing", A), "b1": ("parsing", B)})
        self.assertEqual(sorted(self.cx.downloads()), [("org-ask", "ca_a"), ("org-ask", "ca_a"), (f"org-ask-{UB}", "ca_b")])
        self.assertEqual(len(self.rag.uploads), 3, "nothing uploaded twice")
        listed = {(u, a) for (u, a, s) in self.cx.calls if s == "GOOGLEDRIVE_FIND_FILE"}
        self.assertEqual(listed, {("org-ask", "ca_a"), (f"org-ask-{UB}", "ca_b")})
        files = self.db.execute("select files from ingest_runs").fetchone()[0]
        self.assertEqual(files, 3)

    def test_one_persons_walk_never_removes_anothers_files(self):
        self.two_people()
        self.run_pass()
        self.cx.drives["ca_a"]["FA"] = [file("a1")]  # a2 deleted from A's folder
        self.force_walk()
        self.run_pass()
        d = self.docs()
        self.assertEqual(d["a2"][0], "removed")
        self.assertEqual(d["b1"], ("parsing", B))
        self.assertEqual(d["a1"], ("parsing", A))

    def test_an_incomplete_walk_removes_nothing_for_anyone(self):
        self.two_people()
        self.run_pass()
        self.cx.drives["ca_a"]["FA"] = []
        self.cx.fail.add("ca_b")
        self.force_walk()
        self.run_pass()
        self.assertTrue(all(state != "removed" for state, _ in self.docs().values()))

    def test_a_file_both_can_reach_is_one_document_handed_on_when_its_owner_stops_seeing_it(self):
        self.two_people()
        self.cx.drives["ca_a"]["FA"].append(file("shared"))
        self.cx.drives["ca_b"]["FB"].append(file("shared"))
        self.run_pass()
        d = self.docs()
        self.assertEqual(d["shared"], ("parsing", A), "one document, owned by the first connection that saw it")
        self.assertEqual(len(self.rag.uploads), 4)
        self.untick("FA")  # A takes their folder out: a1 and a2 go, the shared file stays with B
        self.run_pass()
        d = self.docs()
        self.assertEqual(d["a1"][0], "removed")
        self.assertEqual(d["a2"][0], "removed")
        self.assertEqual(d["shared"], ("parsing", B))

    def test_leaving_one_persons_view_waits_for_the_whole_store_walk(self):
        self.two_people()
        self.run_pass()
        # a1 moves from A's folder into B's: A's feed says it left, B's feed says it arrived.
        self.cx.drives["ca_a"]["FA"] = [file("a2")]
        self.cx.drives["ca_b"]["FB"].append(file("a1"))
        self.cx.changes["ca_a"] = [{"fileId": "a1", "file": {**file("a1"), "parents": ["elsewhere"]}}]
        self.run_pass()
        self.assertEqual(self.docs()["a1"], ("parsing", A), "not removed on one person's feed")
        self.assertTrue(self.state()[f"{CORPUS}:googledrive:{A}"].get("walk"), "a whole-store walk is due")
        self.run_pass()
        self.assertEqual(self.docs()["a1"], ("parsing", B), "the walk handed it to the person who still has it")

    def test_a_trashed_file_goes_at_once(self):
        self.two_people()
        self.run_pass()
        self.cx.changes["ca_a"] = [{"fileId": "a1", "file": {**file("a1"), "trashed": True, "parents": ["FA"]}}]
        self.run_pass()
        self.assertEqual(self.docs()["a1"][0], "removed")

    # --- a Drive file's revision is its content, not Drive's `version` ---
    def one_drive(self, *files):
        """One person, one folder, these files, walked once and parsed; the uploads so far are forgotten."""
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": list(files)}
        self.tick("FA", A, UA)
        self.run_pass()
        self.parsed()
        self.rag.uploads.clear()
        self.cx.calls.clear()

    def parsed(self):
        self.db.execute("update documents set state = 'indexed', sent_revision = source_revision, indexed_revision = source_revision")
        self.db.commit()

    def last_walk_missed(self):
        return self.db.execute("select missed from ingest_runs where mode = 'walk' order by at desc limit 1").fetchone()[0]

    def test_a_version_bump_without_new_content_is_neither_parsed_again_nor_missed(self):
        self.one_drive(file("a1", md5="m1"))
        self.cx.drives["ca_a"]["FA"] = [file("a1", rev="9", md5="m1")]  # shared, commented on: Drive moves `version`
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.row("a1", "state", "source_revision", "indexed_revision"), ("indexed", "m1", "m1"))
        self.assertEqual(self.rag.uploads, [])
        self.assertEqual(self.last_walk_missed(), 0)

    def test_new_content_is_parsed_again_and_a_walk_counts_it_when_the_feed_did_not(self):
        self.one_drive(file("a1", md5="m1"))
        self.cx.drives["ca_a"]["FA"] = [file("a1", rev="2", md5="m2")]
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.row("a1", "state", "source_revision", "sent_revision"), ("parsing", "m2", "m2"))
        self.assertEqual(len(self.rag.uploads), 1)
        self.assertEqual(self.last_walk_missed(), 1)

    def test_a_google_doc_has_no_checksum_so_its_last_edit_is_its_revision(self):
        doc = {**file("d1"), "mimeType": "application/vnd.google-apps.document", "name": "d1"}
        self.one_drive({**doc, "modifiedTime": "2026-09-01T00:00:00Z"})
        self.assertEqual(self.row("d1", "source_revision")[0], "2026-09-01T00:00:00Z")
        self.cx.changes["ca_a"] = [{"fileId": "d1", "file": {**doc, "version": "5", "modifiedTime": "2026-09-01T00:00:00Z", "parents": ["FA"]}}]
        self.run_pass()
        self.assertEqual(self.row("d1", "state")[0], "indexed", "a version bump alone")
        self.cx.changes["ca_a"] = [{"fileId": "d1", "file": {**doc, "version": "6", "modifiedTime": "2026-09-02T00:00:00Z", "parents": ["FA"]}}]
        self.run_pass()
        self.assertEqual(self.row("d1", "state", "sent_revision"), ("parsing", "2026-09-02T00:00:00Z"))

    def test_rows_recorded_under_drives_version_carry_over_and_only_a_moved_version_is_parsed(self):
        """The first walk after the switch: every row holds the `version` it was recorded at. One still at the file's
        current version cannot have new content and takes the checksum in place; one behind it might, so it goes."""
        self.one_drive(file("same", rev="7", md5="m1"), file("moved", rev="9", md5="m2"), file("parsing", rev="4", md5="m3"))
        self.db.execute("update documents set source_revision = '7', sent_revision = '7', indexed_revision = '7' where external_id = 'same'")
        self.db.execute("update documents set source_revision = '5', sent_revision = '5', indexed_revision = '5' where external_id = 'moved'")
        self.db.execute("update documents set state = 'parsing', source_revision = '4', sent_revision = '4', indexed_revision = '3' "
                        "where external_id = 'parsing'")
        self.db.commit()
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.row("same", "state", "source_revision", "sent_revision", "indexed_revision"), ("indexed", "m1", "m1", "m1"))
        self.assertEqual(self.row("moved", "state", "source_revision"), ("parsing", "m2"))
        self.assertEqual(self.row("parsing", "state", "source_revision", "sent_revision", "indexed_revision"), ("parsing", "m3", "m3", "3"),
                         "what is in flight is recognised when it lands")
        self.assertEqual(len(self.rag.uploads), 1)
        self.assertEqual(self.last_walk_missed(), 1, "only the row behind the file's version")
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.row("same", "source_revision")[0], "m1", "carried over once, then held in today's form")

    def test_a_file_saved_straight_into_my_drive_arrives_through_the_feed(self):
        """Someone who ticked all of My Drive ticked Drive's alias `root`; the feed names the real id as the parent."""
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"root": [file("a1")]}
        self.tick("root", A, UA)
        self.run_pass()
        self.cx.calls.clear()
        self.cx.drives["ca_a"]["root"].append(file("new"))
        self.cx.changes["ca_a"] = [{"fileId": "new", "file": {**file("new"), "parents": ["mydrive-ca_a"]}}]
        self.run_pass()
        self.assertFalse(self.walked())
        self.assertEqual(self.docs()["new"], ("parsing", A))

    def test_the_feed_carries_a_row_over_the_same_way(self):
        self.one_drive(file("a1", rev="7", md5="m1"))
        self.db.execute("update documents set source_revision = '7', sent_revision = '7', indexed_revision = '7'")
        self.db.commit()
        self.cx.changes["ca_a"] = [{"fileId": "a1", "file": {**file("a1", rev="7", md5="m1"), "parents": ["FA"]}}]
        self.run_pass()
        self.assertFalse(self.walked())
        self.assertEqual(self.row("a1", "state", "source_revision", "indexed_revision"), ("indexed", "m1", "m1"))
        self.assertEqual(self.rag.uploads, [])

    def test_a_dead_connections_documents_stay(self):
        self.two_people()
        self.run_pass()
        self.db.execute("update corpus_connections set status = 'EXPIRED' where id = %s", (B,))
        self.db.commit()
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.docs()["b1"], ("parsing", B))

    def test_one_connection_behaves_as_before(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1"), file("a2")]}
        self.tick("FA", A, UA)
        self.run_pass()
        self.cx.changes["ca_a"] = [{"fileId": "a1", "removed": True}]
        self.run_pass()
        self.assertEqual(self.docs()["a1"][0], "removed", "gone from the only person's view: removed at once")
        self.cx.drives["ca_a"]["FA"] = []
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.docs()["a2"][0], "removed")

    def test_the_old_one_connection_state_is_carried_over_without_a_walk(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1")]}
        self.tick("FA", A, UA)
        import time
        self.save_state({f"{CORPUS}:googledrive": {"token": "t0", "folders": ["FA"], "roots": ["FA"], "walked_at": time.time()}})
        self.run_pass()
        self.assertFalse(self.walked(), "no full walk")
        self.assertIn("GOOGLEDRIVE_LIST_CHANGES", [s for (_, _, s) in self.cx.calls])
        self.assertEqual(list(self.state()), [f"{CORPUS}:googledrive:{A}"])

    def test_removing_the_last_folder_takes_its_files_out_then_goes_quiet(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1"), file("a2")]}
        self.tick("FA", A, UA)
        self.run_pass()
        self.untick("FA")
        self.run_pass()
        self.assertEqual({k: v[0] for k, v in self.docs().items()}, {"a1": "removed", "a2": "removed"})
        self.cx.calls.clear()
        self.run_pass()
        self.assertEqual(self.cx.calls, [], "nothing ticked and nothing owned: no calls at all")

    def test_someone_connecting_before_choosing_anything_costs_no_walk(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1")]}
        self.tick("FA", A, UA)
        self.run_pass()
        self.connect(B, UB, f"org-ask-{UB}", "ca_b")
        self.cx.calls.clear()
        self.run_pass()
        self.assertFalse(self.walked(), "B has nothing ticked and owns nothing")
        self.assertEqual(self.docs()["a1"], ("parsing", A))

    def test_documents_without_an_owner_are_claimed_by_the_only_connection(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.db.execute("insert into documents (org_id, corpus_id, provider, external_id, name, mime_type, source_revision) "
                        "values (%s, %s, 'googledrive', 'old', 'old.pdf', %s, '1')", (ORG, CORPUS, PDF))
        self.db.commit()
        ingest.plan(self.db, self.cx, ORG)
        self.assertEqual(self.docs()["old"][1], A)

    def test_a_folder_ticked_before_connections_were_recorded_is_read_through_its_adders(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.connect(B, UB, f"org-ask-{UB}", "ca_b")
        self.cx.drives["ca_a"] = {"FA": [file("a1")]}
        self.cx.drives["ca_b"] = {"FB": [file("b1")]}
        self.tick("FA", A, UA, legacy=True)
        self.run_pass()
        self.assertEqual(self.docs(), {"a1": ("parsing", A)})

    # --- Dropbox ---
    def test_dropbox_walks_each_root_in_the_team_namespace_and_keeps_a_cursor_per_root(self):
        self.team()
        self.run_pass()
        self.assertEqual(self.dbx.calls[0][1], "users/get_current_account")
        self.assertEqual(self.dbx.tails().count("users/get_current_account"), 2, "once for the walk, once for the send")
        for account, tail, headers in self.dbx.calls:
            if tail != "users/get_current_account":
                self.assertEqual(headers.get("Dropbox-API-Path-Root"), ROOT_HEADER, tail)
        self.assertEqual([t for t in self.dbx.tails() if t.startswith("files/list")],
                         ["files/list_folder", "files/list_folder", "files/list_folder/continue"],
                         "Contracts in one page; Leases in two")
        self.assertEqual(set(self.cursors()), {"/team/leases", "/team/contracts"})
        self.assertEqual({k: v[0] for k, v in self.docs().items()}, {
            "/team/leases/a.pdf": "parsing", "/team/leases/b.pdf": "parsing", "/team/leases/old/c.pdf": "parsing",
            "/team/contracts/d.pdf": "parsing"}, "nothing outside the ticked folders")
        self.assertEqual(self.row("/team/leases/old/c.pdf", "name", "mime_type", "size", "source_revision", "web_url"),
                         ("c.pdf", "application/pdf", 10, "1", "https://www.dropbox.com/home/Team/Leases/Old?preview=c.pdf"))
        self.assertEqual(sorted(self.fetched), sorted(f"fake://dropbox{p}" for p in self.docs()), "bytes come from binary_data.url")
        rf = self.row("/team/leases/a.pdf", "ragflow_doc_id")[0]
        self.assertEqual(self.rag.tags[rf], {"provider": "dropbox", "external_id": "/team/leases/a.pdf", "revision": "1",
                                             "web_url": "https://www.dropbox.com/home/Team/Leases?preview=a.pdf",
                                             "parse": "plain", "modified_at": "2026-09-01T12:00:00Z"})
        self.assertEqual(self.db.execute("select mode, files from ingest_runs").fetchone(), ("walk", 4))

    def test_dropbox_changes_come_from_each_roots_own_cursor(self):
        self.team()
        self.dbx.teams["ca_x"]["/Team/Leases/Sub/f.pdf"] = entry("/Team/Leases/Sub/f.pdf")
        self.run_pass()
        self.db.execute("update documents set state = 'indexed', indexed_revision = source_revision")  # parsed
        self.db.commit()
        old_rf = self.row("/team/leases/b.pdf", "ragflow_doc_id")[0]
        self.dbx.calls.clear()
        self.dbx.changes[("ca_x", "/team/leases")] = [
            entry("/Team/Leases/new.pdf"),  # added
            entry("/Team/Leases/b.pdf", rev="2"),  # modified
            {".tag": "deleted", "name": "a.pdf", "path_lower": "/team/leases/a.pdf", "path_display": "/Team/Leases/a.pdf"},
            {".tag": "deleted", "name": "Old", "path_lower": "/team/leases/old", "path_display": "/Team/Leases/Old"},
            # a move: out of Sub, into the folder's top
            {".tag": "deleted", "name": "f.pdf", "path_lower": "/team/leases/sub/f.pdf", "path_display": "/Team/Leases/Sub/f.pdf"},
            entry("/Team/Leases/f.pdf"),
        ]
        team = self.dbx.teams["ca_x"]
        for gone in ("/Team/Leases/a.pdf", "/Team/Leases/Old/c.pdf", "/Team/Leases/Sub/f.pdf"):
            del team[gone]
        team.update({e["path_display"]: e for e in self.dbx.changes[("ca_x", "/team/leases")] if e[".tag"] == "file"})
        self.run_pass()
        self.assertNotIn("files/list_folder", self.dbx.tails(), "no full listing")
        self.assertEqual(self.dbx.tails().count("files/list_folder/continue"), 4, "one per root, then two more pages of Leases' six changes")
        d = {k: v[0] for k, v in self.docs().items()}
        self.assertEqual(d["/team/leases/new.pdf"], "parsing")
        self.assertEqual(d["/team/contracts/d.pdf"], "indexed", "untouched")
        self.assertEqual(d["/team/leases/a.pdf"], "removed")
        self.assertEqual(d["/team/leases/old/c.pdf"], "removed", "a deleted folder takes its whole subtree")
        self.assertEqual(d["/team/leases/sub/f.pdf"], "removed")
        self.assertEqual(d["/team/leases/f.pdf"], "parsing")
        self.assertEqual(self.row("/team/leases/b.pdf", "state", "source_revision", "sent_revision"), ("parsing", "2", "2"))
        self.assertIn(old_rf, self.rag.deleted, "the old copy of a modified file goes first")
        self.assertNotIn(old_rf, self.rag.docs)
        self.assertEqual(self.db.execute("select mode from ingest_runs order by at desc limit 1").fetchone()[0], "changes")

    def test_dropbox_reset_lists_only_that_root_again(self):
        self.team()
        self.run_pass()
        before = self.cursors()
        self.dbx.calls.clear()
        self.dbx.reset.add(("ca_x", "/team/contracts"))
        del self.dbx.teams["ca_x"]["/Team/Contracts/d.pdf"]
        self.run_pass()
        self.assertEqual(self.dbx.tails().count("files/list_folder"), 1, "one root listed again")
        self.assertEqual(self.docs()["/team/contracts/d.pdf"][0], "removed", "the new complete listing of that root removes")
        self.assertEqual(self.docs()["/team/leases/a.pdf"][0], "parsing")
        after = self.cursors()
        self.assertNotEqual(after["/team/contracts"], before["/team/contracts"])

    def test_dropbox_saves_each_roots_cursor_as_its_listing_ends(self):
        self.team()
        self.dbx.fail.add(("ca_x", "/team/leases"))
        self.run_pass()
        self.assertEqual(set(self.cursors()), {"/team/contracts"}, "the finished root keeps its cursor")
        self.assertEqual(set(self.docs()), {"/team/contracts/d.pdf"})
        self.dbx.fail.clear()
        self.dbx.calls.clear()
        self.run_pass()
        self.assertEqual([t for t in self.dbx.tails() if t.startswith("files/list")], ["files/list_folder/continue", "files/list_folder", "files/list_folder/continue"],
                         "Contracts continues; Leases is listed for the first time")
        self.assertEqual(set(self.cursors()), {"/team/leases", "/team/contracts"})
        self.assertEqual(len(self.docs()), 4)

    def test_dropbox_complete_walk_and_unticking_remove_what_is_gone(self):
        self.team()
        self.run_pass()
        del self.dbx.teams["ca_x"]["/Team/Leases/b.pdf"]
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.docs()["/team/leases/b.pdf"][0], "removed")
        self.untick("/team/contracts")
        self.dbx.calls.clear()
        self.run_pass()
        self.assertEqual(self.docs()["/team/contracts/d.pdf"][0], "removed")
        self.assertEqual(self.docs()["/team/leases/a.pdf"][0], "parsing")
        self.assertEqual(set(self.cursors()), {"/team/leases"})

    def test_dropbox_failed_listing_removes_nothing(self):
        self.team()
        self.run_pass()
        self.dbx.teams["ca_x"] = {}
        self.dbx.fail |= {("ca_x", "/team/leases"), ("ca_x", "/team/contracts")}
        self.force_walk()
        self.run_pass()
        self.assertTrue(all(state == "parsing" for state, _ in self.docs().values()))

    def test_dropbox_cloud_docs_are_exported_or_skipped_with_a_reason(self):
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {e["path_display"]: e for e in [
            entry("/T/Budget.gsheet", size=0, is_downloadable=False, export_as="xlsx"),
            entry("/T/Board.gdraw", size=0, is_downloadable=False)]}
        self.tick("/t", A, UA, provider="dropbox")
        self.run_pass()
        self.assertEqual(self.row("/t/budget.gsheet", "state", "mime_type"), ("parsing", ingest.DROPBOX_CLOUD))
        self.assertIn("Budget.xlsx", self.rag.uploads)
        self.assertEqual(self.row("/t/board.gdraw", "state", "last_error"),
                         ("skipped", "Dropbox can't export this file (non_exportable)"))
        self.assertNotIn("files/download", self.dbx.tails())
        self.assertEqual(self.dbx.tails().count("files/export"), 2)

    def test_dropbox_images_go_straight_to_ocr(self):
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {e["path_display"]: e for e in [entry("/T/scan.JPG"), entry("/T/page.tiff"), entry("/T/a.pdf")]}
        self.tick("/t", A, UA, provider="dropbox")
        self.run_pass()
        for path in ("/t/scan.jpg", "/t/page.tiff"):
            rf = self.row(path, "ragflow_doc_id")[0]
            self.assertEqual(self.rag.configs[rf], ingest.OCR, path)
            self.assertEqual(self.rag.tags[rf]["parse"], "ocr")
        self.assertEqual(self.row("/t/scan.jpg", "mime_type")[0], "image/jpeg")
        self.assertIn("page.tif", self.rag.uploads, "RAGFlow knows .tif, not .tiff")
        self.assertEqual(self.rag.configs[self.row("/t/a.pdf", "ragflow_doc_id")[0]], ingest.PLAIN)

    def no_text(self, parsed):
        """An image and a PDF through one pass, RAGFlow's verdict on both, then reconcile and two more passes."""
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {e["path_display"]: e for e in [entry("/T/photo.jpg"), entry("/T/scan.pdf")]}
        self.tick("/t", A, UA, provider="dropbox")
        self.run_pass()
        for path in ("/t/photo.jpg", "/t/scan.pdf"):
            rf = self.row(path, "ragflow_doc_id")[0]
            self.rag.docs[rf] = {"id": rf, **parsed, "meta_fields": self.rag.tags[rf]}
        ingest.reconcile(self.db, ORG)
        self.assertEqual(self.row("/t/photo.jpg", "state", "last_error", "attempts"), ("skipped", "no_text", 0))
        uploads = self.rag.uploads.count("photo.jpg")
        self.force_walk()
        self.run_pass()
        ingest.reconcile(self.db, ORG)
        self.assertEqual(self.row("/t/photo.jpg", "state", "last_error"), ("skipped", "no_text"), "same rev: left alone")
        self.assertEqual(self.rag.uploads.count("photo.jpg"), uploads, "never sent again")
        self.dbx.teams["ca_x"]["/T/photo.jpg"] = entry("/T/photo.jpg", rev="2")
        self.force_walk()
        self.run_pass()
        self.assertEqual(self.row("/t/photo.jpg", "state", "sent_revision"), ("parsing", "2"), "a new rev is tried again")

    def test_an_image_ragflow_wants_a_vision_model_for_is_skipped_as_no_text(self):
        self.no_text({"run": "FAIL", "progress_msg": "[ERROR]No default vision model is set."})
        self.assertEqual(self.row("/t/scan.pdf", "state", "attempts"), ("parsing", 1), "a failed PDF spends an attempt and is sent again, as before")

    def test_an_image_that_parses_to_nothing_is_skipped_as_no_text(self):
        self.no_text({"run": "DONE", "chunk_count": 0})
        rf = self.row("/t/scan.pdf", "ragflow_doc_id")[0]
        self.assertEqual((self.row("/t/scan.pdf", "state")[0], self.rag.configs[rf]), ("parsing", ingest.OCR),
                         "an empty PDF still goes on to OCR")

    def test_dropbox_asks_for_a_new_link_when_the_old_one_has_expired(self):
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {"/T/a.pdf": entry("/T/a.pdf")}
        self.tick("/t", A, UA, provider="dropbox")
        self.expired.add("fake://dropbox/t/a.pdf")
        self.run_pass()
        self.assertEqual(self.dbx.tails().count("files/download"), 2)
        self.assertEqual(self.row("/t/a.pdf", "state", "attempts"), ("parsing", 0))

    def test_drive_and_dropbox_in_one_org_each_go_their_own_way(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1"), {**file("pic"), "mimeType": "image/png"}]}
        self.tick("FA", A, UA)
        self.connect(B, UB, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {"/T/x.pdf": entry("/T/x.pdf")}
        self.tick("/t", B, UB, provider="dropbox")
        self.run_pass()
        self.assertEqual(self.docs(), {"a1": ("parsing", A), "pic": ("skipped", A), "/t/x.pdf": ("parsing", B)},
                         "a Drive image is still skipped")
        self.assertEqual(self.cx.downloads(), [("org-ask", "ca_a")])
        self.assertEqual(self.dbx.tails().count("files/download"), 1)
        self.assertNotIn("modified_at", self.rag.tags[self.row("a1", "ragflow_doc_id")[0]], "Drive's tag is unchanged")

    # --- Onyx's text ---
    def onyx(self, entries, modified=True):
        """Onyx's sidecar: {path_lower: (rev, text)}, one .md per entry beside index.json; `modified` as the listing
        entry() reports it, unless the case wants an index without it."""
        os.makedirs(ingest.ONYX_TEXT_DIR, exist_ok=True)
        index = {}
        for i, (path, (rev, text)) in enumerate(sorted(entries.items())):
            index[path] = {"rev": rev, "file": f"{i}.md", "chunks": 1}
            if modified:
                index[path]["modified"] = f"2026-09-0{rev}T12:00:00Z"
            with open(os.path.join(ingest.ONYX_TEXT_DIR, f"{i}.md"), "w") as f:
                f.write(f"# {path.rsplit('/', 1)[-1]}\n\n{text}")
        with open(os.path.join(ingest.ONYX_TEXT_DIR, "index.json"), "w") as f:
            json.dump(index, f)

    def scans(self, *paths):
        self.connect(A, UA, "org-ask", "ca_x", provider="dropbox")
        self.dbx.teams["ca_x"] = {p: entry(p) for p in paths}
        self.tick("/t", A, UA, provider="dropbox")

    def finish(self, path, **parsed):
        rf = self.row(path, "ragflow_doc_id")[0]
        self.rag.docs[rf] = {"id": rf, **parsed, "meta_fields": self.rag.tags[rf]}
        return rf

    def sent_from_onyx(self, display, path, modified=True):
        self.scans(display)
        self.onyx({path: ("1", "LEASE AGREEMENT between ...")}, modified)
        self.run_pass()
        name = display.rsplit("/", 1)[1]
        self.assertNotIn("files/download", self.dbx.tails(), "no Dropbox download at all")
        self.assertEqual(self.dbx.tails().count("files/get_metadata"), 0 if modified else 1,
                         "modified_at from the index; asked of Dropbox only when the entry lacks it")
        self.assertEqual(self.fetched, [])
        self.assertEqual(self.rag.uploads, [f"{name}.md"])
        self.assertEqual(self.rag.contents[f"{name}.md"], f"# {name.lower()}\n\nLEASE AGREEMENT between ...".encode())
        rf = self.row(path, "ragflow_doc_id")[0]
        self.assertEqual(self.rag.configs[rf], ingest.PLAIN)
        self.assertEqual(self.rag.tags[rf], {"provider": "dropbox", "external_id": path, "revision": "1",
                                             "web_url": ingest.dropbox_link(display),
                                             "parse": "onyx", "modified_at": "2026-09-01T12:00:00Z"})
        self.assertEqual(self.row(path, "state", "name", "source_revision", "sent_revision"), ("parsing", name, "1", "1"))
        self.assertIn("send: 1 file(s) sent from Onyx text", self.logged)
        self.finish(path, run="DONE", chunk_count=4)
        ingest.reconcile(self.db, ORG)
        self.assertEqual(self.row(path, "state", "indexed_revision", "chunk_count", "attempts"), ("indexed", "1", 4, 0))

    def test_a_pdf_onyx_already_read_is_sent_as_its_text(self):
        self.sent_from_onyx("/T/Lease 4.pdf", "/t/lease 4.pdf")

    def test_an_image_onyx_already_read_is_sent_as_its_text_not_ocrd(self):
        self.sent_from_onyx("/T/SOMA page.jpg", "/t/soma page.jpg")

    def test_an_index_entry_without_modified_asks_dropbox_for_it(self):
        self.sent_from_onyx("/T/Lease 4.pdf", "/t/lease 4.pdf", modified=False)

    def test_onyx_text_for_another_rev_is_not_used(self):
        self.scans("/T/a.pdf")
        self.onyx({"/t/a.pdf": ("0", "an older rev's text")})
        self.run_pass()
        self.assertEqual(self.dbx.tails().count("files/download"), 1)
        self.assertEqual(self.rag.uploads, ["a.pdf"])
        rf = self.row("/t/a.pdf", "ragflow_doc_id")[0]
        self.assertEqual((self.rag.configs[rf], self.rag.tags[rf]["parse"]), (ingest.PLAIN, "plain"))
        self.assertIn("send: 0 file(s) sent from Onyx text", self.logged)

    def test_a_sidecar_directory_without_an_index_changes_nothing(self):
        os.makedirs(ingest.ONYX_TEXT_DIR)
        self.test_dropbox_images_go_straight_to_ocr()
        self.assertFalse([m for m in self.logged if "Onyx" in m], "nothing said about a feature that is off")

    def test_a_malformed_index_is_logged_and_everything_goes_the_usual_way(self):
        self.scans("/T/a.pdf", "/T/b.jpg")
        os.makedirs(ingest.ONYX_TEXT_DIR)
        with open(os.path.join(ingest.ONYX_TEXT_DIR, "index.json"), "w") as f:
            f.write('{"/t/a.pdf": {"rev": "1", "fi')
        self.run_pass()
        self.assertEqual(len([m for m in self.logged if "Onyx text index unreadable" in m]), 1)
        self.assertEqual(sorted(self.rag.uploads), ["a.pdf", "b.jpg"])
        self.assertEqual(self.rag.configs[self.row("/t/b.jpg", "ragflow_doc_id")[0]], ingest.OCR)

    def test_onyx_text_that_parses_to_nothing_sends_the_file_itself_once(self):
        self.scans("/T/scan.pdf", "/T/photo.jpg")
        self.onyx({"/t/scan.pdf": ("1", " "), "/t/photo.jpg": ("1", " ")})
        self.run_pass()
        onyx_rfs = [self.finish(p, run="DONE", chunk_count=0) for p in ("/t/scan.pdf", "/t/photo.jpg")]
        ingest.reconcile(self.db, ORG)
        for path in ("/t/scan.pdf", "/t/photo.jpg"):
            self.assertEqual(self.row(path, "state", "attempts"), ("pending", 0), f"{path}: not no_text, no attempt spent")
        self.assertEqual(self.rag.configs[onyx_rfs[0]], ingest.PLAIN, "not escalated to OCR as a plain PDF would be")
        self.run_pass()
        self.assertEqual(self.dbx.tails().count("files/download"), 2, "the files themselves this time")
        self.assertTrue(set(onyx_rfs) <= set(self.rag.deleted), "the empty text goes first")
        pdf, img = self.row("/t/scan.pdf", "ragflow_doc_id")[0], self.row("/t/photo.jpg", "ragflow_doc_id")[0]
        self.assertEqual((self.rag.configs[pdf], self.rag.tags[pdf]["parse"]), (ingest.PLAIN, "plain"))
        self.assertEqual((self.rag.configs[img], self.rag.tags[img]["parse"]), (ingest.OCR, "ocr"))
        # The usual rules from here: the PDF escalates to OCR, the image fails and is retried, still not from Onyx.
        self.finish("/t/scan.pdf", run="DONE", chunk_count=0)
        self.finish("/t/photo.jpg", run="FAIL", progress_msg="Read timed out. (read timeout=30)")
        ingest.reconcile(self.db, ORG)
        self.assertEqual((self.row("/t/scan.pdf", "state")[0], self.rag.configs[pdf]), ("parsing", ingest.OCR))
        self.assertEqual(self.row("/t/photo.jpg", "state", "attempts"), ("pending", 1))
        self.run_pass()
        self.assertEqual(self.rag.uploads.count("photo.jpg.md"), 1, "Onyx's text was tried once")
        self.assertEqual(self.rag.uploads.count("photo.jpg"), 2)
        self.finish("/t/photo.jpg", run="DONE", chunk_count=0)
        ingest.reconcile(self.db, ORG)
        self.assertEqual(self.row("/t/photo.jpg", "state", "last_error"), ("skipped", "no_text"), "the file itself had no text")

    def test_a_drive_file_is_never_sent_from_onyx_text(self):
        self.connect(A, UA, "org-ask", "ca_a")
        self.cx.drives["ca_a"] = {"FA": [file("a1")]}
        self.tick("FA", A, UA)
        self.onyx({"a1": ("1", "text")})
        self.run_pass()
        self.assertEqual(self.cx.downloads(), [("org-ask", "ca_a")])
        self.assertEqual(self.rag.uploads, ["a1.pdf"])
        self.assertEqual(self.rag.tags[self.row("a1", "ragflow_doc_id")[0]]["parse"], "plain")


class _Answer:
    def __init__(self, code, body=None):
        self.status_code, self.body = code, body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self.body


@unittest.skipUnless(ingest, "needs psycopg and requests (ingest.py imports both)")
class ProxyTest(unittest.TestCase):
    """The proxy call itself and the pure Dropbox helpers: no database needed."""

    def setUp(self):
        ingest.BACKOFF = 0

    def test_drops_429s_and_5xx_are_retried_then_the_answer_returned(self):
        answers = [requests.ConnectionError("reset by peer"), _Answer(503), _Answer(200, {"status": 429, "data": {}}),
                   _Answer(200, {"status": 500, "data": {}}), _Answer(200, {"status": 409, "data": {"error_summary": "path/not_found/"}})]
        sent = []

        def post(url, timeout=None, json=None):
            sent.append((url, json))
            a = answers.pop(0)
            if isinstance(a, Exception):
                raise a
            return a
        cx = ingest.Composio("key")
        cx.s.post = post
        out = cx.proxy(("u", "ca_x"), "https://api.dropboxapi.com/2/files/list_folder", {"path": ""}, {"Dropbox-API-Path-Root": ROOT_HEADER})
        self.assertEqual(out["status"], 409, "a Dropbox refusal is an answer, not a retry")
        self.assertEqual(len(sent), 5)
        url, req = sent[0]
        self.assertEqual(url, "https://backend.composio.dev/api/v3/tools/execute/proxy")
        self.assertEqual(req, {"endpoint": "https://api.dropboxapi.com/2/files/list_folder", "method": "POST",
                               "connected_account_id": "ca_x", "body": {"path": ""},
                               "parameters": [{"name": "Dropbox-API-Path-Root", "value": ROOT_HEADER, "type": "header"}]})
        self.assertEqual(cx.s.headers["x-api-key"], "key")

    def test_retries_give_up(self):
        cx = ingest.Composio("key")
        cx.s.post = lambda url, timeout=None, json=None: _Answer(502)
        with self.assertRaises(RuntimeError):
            cx.proxy(("u", "ca_x"), "https://api.dropboxapi.com/2/users/get_current_account")

    def test_the_citation_link_is_dropboxs_preview_of_the_path(self):
        self.assertEqual(ingest.dropbox_link("/Carson Development Team Folder/00 SHARE DRIVE/Lease #4 & A.pdf"),
                         "https://www.dropbox.com/home/Carson%20Development%20Team%20Folder/00%20SHARE%20DRIVE"
                         "?preview=Lease%20%234%20%26%20A.pdf")

    def test_images_are_indexed_for_dropbox_only(self):
        self.assertIsNone(ingest.indexable("image/png", 10, "dropbox"))
        self.assertEqual(ingest.indexable("image/png", 10, "googledrive"), "this file type isn't indexed yet")
        self.assertEqual(ingest.indexable("image/png", ingest.MAX_BYTES + 1, "dropbox"), "larger than 100 MB")


if __name__ == "__main__":
    unittest.main()
