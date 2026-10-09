# deploy/bebuilt

bebuilt's deployment of this fork: one RAGFlow box per client, built to be handed over. This directory is
the only thing bebuilt adds to the fork; everything else tracks upstream.

| File | What it is |
|---|---|
| `box-setup.sh` | Brings a box to its intended state. Runs as root from `/opt/ragflow` at the pinned ref. Re-runnable. |
| `compose.bebuilt.yml` | Compose override: nothing is published to the host except nginx on `127.0.0.1:8080`, which cloudflared reaches. |
| `env.bebuilt` | Non-secret settings layered onto `docker/.env`: the image pinned by digest, OpenSearch, local embeddings, no self-registration. |
| `tenant-setup.py` | Runs inside the RAGFlow container: this box's app user, its API key and the `shared` dataset (embedding model pinned; never changed once the dataset exists). Writes `/etc/bebuilt/ragflow-tenant.json`. |
| `ingest.py` + `bebuilt-ingest.{service,timer}` | The ingestion worker, every five minutes: walks the confirmed selection through Composio, sends new and changed files to RAGFlow, records progress in the platform DB as `worker_<slug>` (RLS: this org's rows only). |
| `test_ingest.py` | The worker's selection, ownership and removal logic against a throwaway Postgres, a fake Drive per person and a fake Dropbox team behind the Composio proxy (see its header for the two commands). |
| `ragflow-dump.sh` | Nightly consistent MySQL dump onto the box's own disk (keeps three), so each Hetzner backup holds a clean copy. |

It is driven from the laptop by `scripts/ragflow-provision.sh <client> <host> [ref]` in
`bebuilt/bebuilt-platform-v2`, which pushes that client's secrets from its own 1Password vault to
`/etc/bebuilt/` and checks this repo out at a pinned tag (`bebuilt-YYYY-MM-DD`). The same run serves the
first build, a rebuild after restore, and the handoff to a new owner.

Rules this directory keeps:

- **Code reaches a box only through git**, at a tag. No files are copied onto a box by hand.
- **No port on a public interface.** `box-setup.sh` fails if any container publishes one.
- **No secret in this repo** (it is public). Secrets live in the client's vault and in `/etc/bebuilt/` (0600).
- **Nothing multi-tenant on a box.** Every credential on it belongs to that client alone.
- **The image is pinned by digest** to a build that contains the CVE-2026-93013 fix (PR #19591); the
  released `v0.27.2` does not.

## Operating a box (learned on bebuilt's box, 2026-09-18)

- **Never restart RAGFlow while documents are parsing.** A restart (a settings change, `compose up`, a
  reboot) strands every in-flight parse: RAGFlow keeps it `RUNNING` forever and never retries it. Five files
  sat at 80% for three hours this way. Before any restart: `systemctl stop bebuilt-ingest.timer`, stop the
  running parses (`DELETE /api/v1/datasets/<id>/chunks` with their `document_ids`), restart, then set those
  documents back to `pending` with `attempts = 0` and start the timer. `ingest.py` (from `.10`) also re-sends
  any parse whose progress has not moved for 30 minutes, at the cost of one of its three attempts.
- **Embedding settings on a CPU box are load-bearing.** RAGFlow waits 30 s for each TEI call (hard-coded),
  and a second document's OCR slows the first one's embedding past it. `env.bebuilt` pins
  `EMBEDDING_BATCH_SIZE=2` and `MAX_CONCURRENT_TASKS=1` (from `.11`). A file failing with
  `Read timed out. (read timeout=30)` means raise neither; look at what else was running.
- **Settings reach a box through its tag**, like code: tag the change, then re-run
  `scripts/ragflow-provision.sh <client> <host>` (it checks out the tag and merges `env.bebuilt` into
  `docker/.env`). That restarts RAGFlow, so the rule above applies.
- **A worker-only change needs no restart.** When a tag changes nothing outside `deploy/bebuilt/`, update with
  `scripts/ragflow-worker-update.sh <client> <host> <ref>` (bebuilt-platform-v2): it stops the timer, lets a
  running pass finish, checks the tag out without `--force` (the box's edited `docker/.env` is carried over)
  and starts the timer again. RAGFlow's containers are never touched. It refuses a tag that changes anything
  else; that goes through provisioning in a quiet window.
- **Each person's Drive is its own connection** (from `.21.5`). Every member adds folders from their own Drive
  into the one shared dataset, each connection under its own Composio identity (`corpus_connections.
  composio_user_id`). A file two people can reach is one document, read through one connection. With more than
  one connection in a store, nothing is removed except by a walk of every live connection that finds the file
  under none of them; one person's changes feed saying a file left their view only schedules that walk.
- **Failed for good is not stuck.** After three attempts a document is `failed` and the app stops counting it
  as work in progress; it is retried only when the file changes in the store. Re-queue it by hand
  (`pending`, `attempts = 0`) once the cause is fixed.
- **An image with no text is skipped, not failed** (Dropbox stores). An image RAGFlow parses to nothing, or fails for want of a
  vision model (`No default vision model is set.`), ends `skipped` with `last_error = 'no_text'` at once, no attempts
  spent, and is sent again only when its Dropbox rev changes; the cutover diff lists it as `skipped:no_text`.
- **A scan Onyx already read is indexed from Onyx's text** (Dropbox stores, CDC). `ONYX_TEXT_DIR` (in `worker.env`,
  default `/var/lib/bebuilt/onyx-text`) holds Onyx's sidecar: `index.json` (`{path_lower: {rev, modified, file, chunks}}`) and
  the `.md` files beside it. A PDF or image whose Dropbox rev the index names is uploaded as `<name>.md`, parsed plain,
  tagged `parse: onyx`, and never downloaded or OCR'd; citations still open the Dropbox file. Another rev, or no
  index, goes the usual way; a malformed index is one log line and the usual way for that pass. Text that parses to
  nothing sends the file itself once for that rev (`/var/lib/bebuilt/onyx-empty.json`). Each pass logs
  `send: N file(s) sent from Onyx text`.
- **A spreadsheet is indexed as rows** (`.xlsx`, an exported Google Sheet, `.csv`). The worker reads the workbook itself
  (standard library only) and uploads `<name>.txt`, one line per row: `<file name> · <tab> · <header>: <value>; …`, dates
  as ISO dates, numbers to the cent, title rows above the headers kept as lines, a block that repeats the headers
  starting over with them. Tagged `parse: sheet-rows`, parsed with the dataset's defaults; citations still open the
  file. Why: RAGFlow's own Excel parser never puts the file's name on a row, so a whole-dataset search ranked lease
  PDFs above every row of Molzer's rent-deposit tab (2026-10-09). A workbook it can't read (not a zip, `.xls`,
  over 20 MB of text) goes as the file itself, `parse: native`, with one log line. Spreadsheets indexed before
  this change keep RAGFlow's parse until their revision moves; to re-index them now, run in the client's Supabase:
  `update documents set state = 'pending', attempts = 0, updated_at = now() where org_id = '<org>' and state = 'indexed'
  and mime_type in ('application/vnd.google-apps.spreadsheet', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'text/csv');`
  Each pass sends at most `BATCH` and waits while RAGFlow's queue is above `QUEUE_HIGH`, so a large re-queue drains
  over several passes; a document is out of search between its old copy's delete and its new parse finishing.
