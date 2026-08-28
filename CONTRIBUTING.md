# Contributing

Use this process to contribute:

- Fork this repository.
- Create a branch for one change.
- Add or update tests when you change behavior.
- Run the local checks from `converter/`:

  ```sh
  cd converter
  test -z "$(gofmt -l .)"
  go vet ./...
  go test ./...
  go build ./...
  ```

- If Docker Compose is available, validate both files from the repository root:

  ```sh
  docker compose -f docker-compose.yml config -q
  docker compose -f docker-compose.poll.yml config -q
  ```

GitHub Actions already runs these checks from `converter/`:

- The `gofmt` check.
- `go vet ./...`.
- `go test ./...`.
- `go build ./...`.

- Open a pull request from your fork. Describe the change and the tests you ran.
- Reply to review feedback with new commits.

Maintainers squash-merge accepted pull requests.

Do not commit credentials, API keys, or generated data.
