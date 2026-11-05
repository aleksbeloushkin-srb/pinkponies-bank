GO   ?= go
PKGS := ./...
BIN  := $(CURDIR)/bin

# Pinned tool versions. Each one is installed into its own directory under
# ./bin (git-ignored), so changing a version here installs the new one.
BUF_VERSION                ?= v1.65.0
PROTOC_GEN_GO_VERSION      ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
STATICCHECK_VERSION        ?= 2025.1.1
PROMETHEUS_IMAGE           ?= prom/prometheus:v3.5.0

BUF                := $(BIN)/buf-$(BUF_VERSION)/buf
PROTOC_GEN_GO      := $(BIN)/protoc-gen-go-$(PROTOC_GEN_GO_VERSION)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(BIN)/protoc-gen-go-grpc-$(PROTOC_GEN_GO_GRPC_VERSION)/protoc-gen-go-grpc
STATICCHECK        := $(BIN)/staticcheck-$(STATICCHECK_VERSION)/staticcheck

.PHONY: run test test-integration lint proto proto-check proto-lint check-alerts e2e compose-up compose-down

## run: build and start the whole stack in the foreground (Ctrl-C stops it)
run:
	docker compose up --build

## test: unit tests with the race detector (no Docker needed)
test:
	$(GO) test -race -count=1 $(PKGS)

## test-integration: adds the PostgreSQL and Redpanda tests (needs Docker)
test-integration:
	$(GO) test -race -count=1 -tags integration $(PKGS)

## lint: formatting, vet with and without the integration tag, staticcheck
lint: $(STATICCHECK)
	@test -z "$$(gofmt -s -l .)" || { gofmt -s -l .; echo "run gofmt -s -w ."; exit 1; }
	$(GO) vet $(PKGS)
	$(GO) vet -tags integration $(PKGS)
	$(STATICCHECK) -checks all $(PKGS)
	$(STATICCHECK) -checks all -tags integration $(PKGS)

## proto: regenerate gen/ from api/proto with pinned buf and plugins
proto: $(BUF) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	find gen -name '*.pb.go' -delete
	PATH="$(dir $(PROTOC_GEN_GO)):$(dir $(PROTOC_GEN_GO_GRPC)):$$PATH" $(BUF) generate

## proto-check: fail if the committed generated code differs from a fresh run
proto-check: proto
	@git diff --exit-code -- gen || { echo "gen/ is stale: run make proto and commit"; exit 1; }
	@test -z "$$(git status --porcelain -- gen)" || { git status --porcelain -- gen; echo "gen/ has untracked files: run make proto and commit"; exit 1; }

## proto-lint: buf lint and format check of api/proto
proto-lint: $(BUF)
	$(BUF) lint
	$(BUF) format --diff --exit-code

## check-alerts: validate the Prometheus config and run the alert rule tests
check-alerts:
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" --entrypoint promtool \
		$(PROMETHEUS_IMAGE) check config /etc/prometheus/prometheus.yml
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus --entrypoint promtool \
		$(PROMETHEUS_IMAGE) test rules alerts_test.yml

## e2e: start the stack, drive every scenario with curl, tear it down
e2e:
	./scripts/e2e.sh

## compose-up: start the stack in the background and wait until healthy
compose-up:
	docker compose up --build -d --wait

## compose-down: stop the stack and delete its volumes
compose-down:
	docker compose down -v

$(BUF):
	GOBIN=$(dir $@) $(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

$(PROTOC_GEN_GO):
	GOBIN=$(dir $@) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(PROTOC_GEN_GO_GRPC):
	GOBIN=$(dir $@) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

$(STATICCHECK):
	GOBIN=$(dir $@) $(GO) install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
