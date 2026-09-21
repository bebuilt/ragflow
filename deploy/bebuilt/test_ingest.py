"""Tests for ingest.py's selection, ownership and removal logic, against a real Postgres and a fake Drive.

    docker run -d --rm --name ingest-test-pg -e POSTGRES_PASSWORD=pw -p 127.0.0.1:55433:5432 postgres:17
    INGEST_TEST_DSN=postgresql://postgres:pw@127.0.0.1:55433/postgres python3 -m unittest deploy/bebuilt/test_ingest.py

Needs psycopg and requests (the box has both). Skipped without INGEST_TEST_DSN. Each person's Drive is a fake
account with its own folders; Composio, RAGFlow and the file download are fakes that record what was asked of them.
"""
import json
import os
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(__file__))
DSN = os.environ.get("INGEST_TEST_DSN")

if DSN:
    import psycopg
    import ingest

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
  priority smallint not null default 0, connection_id uuid, unique (corpus_id, provider, external_id));
create table ingest_runs (org_id uuid not null, at timestamptz not null default clock_timestamp(), mode text not null,
  folders integer not null, files integer not null, missed integer not null);
"""

ORG, CORPUS = str(uuid.uuid4()), str(uuid.uuid4())
# Fixed ids so the walk order (sorted by connection id) is known: A before B.
A, B = "00000000-0000-0000-0000-00000000000a", "00000000-0000-0000-0000-00000000000b"
UA, UB = str(uuid.uuid4()), str(uuid.uuid4())
PDF = "application/pdf"


def file(fid, rev="1"):
    return {"id": fid, "name": f"{fid}.pdf", "mimeType": PDF, "size": "10", "version": rev, "webViewLink": f"https://drive/{fid}"}


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


class FakeRAG:
    def __init__(self):
        self.docs, self.n, self.uploads = {}, 0, []

    def upload(self, filename, content):
        self.n += 1
        rf = f"rf{self.n}"
        self.docs[rf] = {"id": rf, "run": "UNSTART"}
        self.uploads.append(filename)
        return rf

    def tag(self, rf, meta):
        pass

    def parse(self, ids):
        pass

    def stop(self, ids):
        pass

    def delete(self, ids):
        for rf in ids:
            self.docs.pop(rf, None)

    def all_docs(self):
        return dict(self.docs)


class _Resp:
    content = b"%PDF"

    def raise_for_status(self):
        pass


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
        ingest.requests.get = lambda url, timeout=None: _Resp()
        self.cx = FakeComposio()

    def tearDown(self):
        self.db.close()

    # --- fixture helpers ---
    def connect(self, cid, holder, user, account, status="ACTIVE"):
        self.db.execute("insert into corpus_connections values (%s, %s, %s, 'googledrive', %s, %s, %s, %s)",
                        (cid, CORPUS, ORG, account, status, holder, user))
        self.db.commit()

    def tick(self, folder, cid, holder, legacy=False):
        self.db.execute("insert into corpus_sources values (%s, %s, 'googledrive', %s, %s, now(), %s)",
                        (CORPUS, ORG, folder, holder, None if legacy else cid))
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


if __name__ == "__main__":
    unittest.main()
