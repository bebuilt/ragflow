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
| `test_ingest.py` | The worker's selection, ownership and removal logic against a throwaway Postgres and a fake Drive per person (see its header for the two commands). |
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
