# Grimmory format reconciliation service

The service listens on `:8080` by default. `GET /health` is unauthenticated;
`GET /formats` and `POST /sync/{libraryId}/{bookId}` require `Authorization: Bearer
<API_KEY>`. The old `/convert` endpoint is intentionally not present.

Pass `--poll` to keep these HTTP endpoints available while a background library
poller scans configured Grimmory libraries. Without the flag the service remains
manual-only. Polling performs an immediate scan and uses a start-to-start
ticker; ticks during a running scan are discarded.

Required Grimmory settings are `GRIMMORY_BASE_URL`, `GRIMMORY_USERNAME`, and
`GRIMMORY_PASSWORD`. The service accepts an absolute HTTP or HTTPS URL for
`GRIMMORY_BASE_URL`.
`API_KEY` is optional; when unset,
a random key is loaded from or generated at `$DATA_DIR/api-key` with mode `0600`.
`DATA_DIR` defaults to `/data` and contains only `api-key` and `state.db`.

`LIBRARY_IDS` is required and is a comma-separated integer allowlist. Format
settings are `OUTPUT_FORMATS` (default `mobi,azw3`) and
`SUPPORTED_INPUT_FORMATS` (default `epub,azw3,mobi`). Values
are comma/whitespace-separated, normalized to lowercase, deduplicated, and
validated before startup. Limits can be tuned with `MAX_FILE_BYTES`,
`MAX_RESPONSE_BYTES`, `HTTP_TIMEOUT`, `CONVERSION_TIMEOUT`, and
`DATABASE_BUSY_TIMEOUT`; each is bounded. Polling settings are
`POLL_INTERVAL` (default `5m`), `POLL_MAX_ATTEMPTS` (default `5`),
`POLL_RETRY_BASE` (default `30s`), and `POLL_RETRY_MAX` (default `15m`).
They are validated at startup. `IGNORE_PROCESSING_TAG` and
`FAILED_PROCESSING_TAG` are optional; a blank value disables each behavior.
Identical non-empty values fail startup.
`MAX_CONCURRENT_BOOKS` defaults to one and is bounded to 16.

The service obtains a Grimmory token, reads one scoped library/book reference,
resolves its library policy, chooses a source in library priority order, and
creates only configured and allowed formats. It uses the
deployment-specific Grimmory API shape: downloads use
`GET /api/v1/books/{bookId}/content?bookType=...`, and uploads use
`POST /api/v1/books/{bookId}/files` with multipart fields `file`, `isBook=true`,
and uppercase `bookType`. Every upload is followed by a book GET verification
before SQLite state is changed. This endpoint choice is intentional for the
deployment and is not a runtime configuration switch.

Polling lists books through each library endpoint, obtains each book's scoped
detail, persists a stable observation, and reconciles only pending or due retry
observations. Successful syncs are
verified with a fresh post-upload read before the observation is marked
current. Retries use capped exponential jitter and are limited to transient
network, timeout, selected remote HTTP, SQLite busy/locked, and upload
visibility failures.

The poller cannot detect a source change that produces no observable Grimmory
API change: it does not perform byte-only source change detection.

An existing configured derivative whose checkpoint is missing, incomplete, or
stale is reported as a blocked rebuild with the stable
`safe_replacement_unavailable` code. The service has no atomic replace
operation, so it never deletes or overwrites an existing derivative. Missing
derivatives are still created; unknown or untracked files are never silently
treated as current. Before a missing-target upload, SQLite records its exact
source hash, output hash, generation fingerprint, and desired name. A retry
adopts an existing target only when one stable remote candidate matches every
intent field, including its exact output hash; otherwise it remains blocked.
The intent is cleared atomically with the confirmed derived state. Dry runs
report the same blocked rebuild plan.

`state.db` is a pure-Go `modernc.org/sqlite` database configured with WAL,
foreign keys, and a busy timeout. Temporary downloads and Calibre outputs are
private, bounded, hashed, and removed after each sync. Calibre is invoked
directly as `ebook-convert` by default; no Grimmory library path is mounted or
accepted.
