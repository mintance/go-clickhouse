# Contributing to go-clickhouse

Thanks for your interest in contributing! Here's how to get started.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```
   git clone git@github.com:YOUR_USERNAME/go-clickhouse.git
   ```
3. Create a feature branch:
   ```
   git checkout -b my-feature
   ```

## Development

### Requirements

- Go 1.21+
- Docker (for integration tests)

### Running unit tests

```
go test -race ./...
```

### Running integration tests

Integration tests require a running ClickHouse instance:

```
docker run -d --name clickhouse-test \
  -p 8123:8123 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -e CLICKHOUSE_PASSWORD="" \
  clickhouse/clickhouse-server:latest
```

Then run:

```
CLICKHOUSE_HOST=localhost:8123 go test -v -race -tags=integration -run=TestIntegration ./...
```

Clean up:

```
docker rm -f clickhouse-test
```

### Code style

- Run `go vet ./...` before committing
- Run `go fmt ./...` to format code
- Keep the public API minimal — prefer adding to existing types over new ones
- All public methods should accept `context.Context` as the first parameter
- Write tests for new functionality — both unit and integration where applicable

## Pull Requests

1. Make sure all unit tests pass: `go test -race ./...`
2. Make sure `go vet` is clean
3. Add tests for any new functionality
4. Add integration tests for features that interact with ClickHouse
5. Update README.md if the public API changes
6. Keep commits focused — one logical change per commit
7. Open a PR against the `master` branch

## Reporting Issues

- Use GitHub Issues
- Include Go version, ClickHouse version, and a minimal reproduction case
- For bugs, include the error message and expected vs actual behavior

## Adding New Type Support

When adding support for new ClickHouse types:

1. Add `unmarshal` case in `marshal.go`
2. Add `marshal` case if the type can be used in inserts
3. Add unit tests in `marshal_test.go`
4. Add integration test in `integration_test.go` using a real ClickHouse query
5. Document in README if it's a commonly used type
