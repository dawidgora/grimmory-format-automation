# Deployment examples

The runnable example is the root [`docker-compose.yml`](../docker-compose.yml).
It connects to an existing Grimmory deployment over HTTP and does not install
Grimmory or provide a local library directory.

## Compose

Start the Air development service:

```sh
docker compose up --build
```

In another terminal, check readiness and retrieve the generated key:

```sh
curl --fail http://127.0.0.1:8080/health
docker compose exec -T grimmory-format-service cat /data/api-key
```

The named `converter_dev_data` volume stores `/data/api-key` and SQLite state.
If `API_KEY` is not supplied, the service generates a
random key on first start and persists it with private permissions:

For production-style polling, use the standalone file rather than merging it
with the development Compose file:

```sh
docker compose -f docker-compose.poll.yml up -d --build
```

Polling uses the persistent `converter_data` volume and performs real
reconciliation writes for due books. It is not a dry-run mode.

Keep that value private. An explicit `API_KEY` overrides the generated value;
changing it and restarting rotates the service credential. The key
is not a Grimmory credential. Inject Grimmory URL and least-privilege API
credentials through the sync server's runtime Secret configuration; do not put
them in this repository or in the request body.

Configure `LIBRARY_IDS` with the allowed integer library IDs, then run one book
manually:

```sh
export API_KEY='retrieve-or-inject-the-service-key'
curl --fail-with-body -X POST 'http://127.0.0.1:8080/sync/LIBRARY_ID/BOOK_ID?dryRun=false&force=false' \
  -H "Authorization: Bearer ${API_KEY}"
```

Or with HTTPie:

```sh
http POST http://127.0.0.1:8080/sync/LIBRARY_ID/BOOK_ID \
  "Authorization:Bearer ${API_KEY}" \
  dryRun:=false force:=false
```

## Grimmory contract check

Before enabling writes, fetch the target instance's
`GET /api/openapi.json`. Confirm authentication, book/file reads, source
download, upload fields, responses, and conflict behavior. The reconciliation
documentation assumes `POST /api/v1/books/{bookId}/files` only as a provisional
write operation; that is not a claim about the current public Grimmory
contract. Use the deployed OpenAPI document instead of guessing paths.

See the root [README](../README.md) for the state algorithm, environment
policy, security, exclusions, and Calibre GPLv3 obligations.
