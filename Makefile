# -------------------------------
# Mirador NRT Aggregator Makefile
# -------------------------------

SHELL := /bin/bash
.DEFAULT_GOAL := help

# --- Project metadata ---
MODULE        ?= github.com/platformbuilds/mirador-nrt-aggregator
MAIN_PKG      ?= ./cmd/mirador-nrt-aggregator
BIN_DIR       ?= bin
BIN_NAME      ?= mirador-core

GO            ?= go
GIT           ?= git

# Versioning (override via env if needed)
GIT_SHA       := $(shell $(GIT) rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_TAG       := $(shell $(GIT) describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
BUILD_DATE    := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BRANCH        := $(shell $(GIT) rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
# If VERSION env is set, prefer it; else use GIT_TAG (strip leading v for -X)
VERSION       ?= $(GIT_TAG)
VERSION_PLAIN := $(shell echo "$(VERSION)")

# Try to populate common ldflags (safe even if vars don’t exist in main)
LDFLAGS+= -X main.version=$(VERSION_PLAIN)
LDFLAGS+= -X main.commit=$(GIT_SHA)
LDFLAGS+= -X main.date=$(BUILD_DATE)
LDFLAGS+= -X main.branch=$(BRANCH)
LDFLAGS+= -s -w

# CGO / Static flags
CGO_ENABLED ?= 0

# OS/Arch
HOST_OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH := $(shell uname -m)
# Normalize ARCH values
ifeq ($(HOST_ARCH),x86_64)
	HOST_ARCH := amd64
endif
ifeq ($(HOST_ARCH),aarch64)
	HOST_ARCH := arm64
endif

# Docker image
IMAGE_REPO   ?= platformbuilds
IMAGE_NAME   ?= mirador-nrt-aggregator
IMAGE_TAG    ?= $(VERSION_PLAIN)
IMAGE        ?= $(IMAGE_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_LATEST ?= $(IMAGE_REPO)/$(IMAGE_NAME):latest

# Buildx platforms
PLATFORMS  ?= linux/amd64,linux/arm64

# Tools
TOOLS_BIN    ?= $(GOPATH)/bin
GOIMPORTS    ?= $(TOOLS_BIN)/goimports
GOLANGCI     ?= $(TOOLS_BIN)/golangci-lint
GOVULN       ?= $(TOOLS_BIN)/govulncheck
PROTOC_GEN_GO      ?= $(TOOLS_BIN)/protoc-gen-go
PROTOC_GEN_GRPC    ?= $(TOOLS_BIN)/protoc-gen-go-grpc
SWAG         ?= $(TOOLS_BIN)/swag

# -------------
# Help
# -------------
help: ## Show this help
	@echo ""
	@echo "Development & Build:"
	@echo "  setup                     Install tools, generate proto, download deps."
	@echo "  build                     Static linux/amd64 build to bin/$(BIN_NAME)."
	@echo "  build-native              Build for host OS/Arch to bin/$(BIN_NAME)-<os>-<arch>."
	@echo "  build-linux-amd64         Build for linux/amd64."
	@echo "  build-linux-arm64         Build for linux/arm64."
	@echo "  build-linux-multi         Build linux binaries for amd64 and arm64."
	@echo "  build-darwin-arm64        Build for macOS arm64 (Apple Silicon)."
	@echo "  build-windows-amd64       Build for Windows amd64 (.exe)."
	@echo "  build-all                 Build common targets for all platforms above."
	@echo "  dev-build                 Development build with debug symbols."
	@echo "  dev                       Run server locally via 'go run'."
	@echo "  run                       Alias to 'dev'."
	@echo "  clean-build               Clean then perform a fresh build."
	@echo ""
	@echo "Testing & Quality:"
	@echo "  test                      Run unit tests with race detector and coverage."
	@echo "  fmt                       Format code (go fmt, goimports)."
	@echo "  lint                      Run golangci-lint on the repo."
	@echo "  tools                     Install dev tools (protoc-gen-*, lint, swag, govulncheck)."
	@echo "  check-tools               Verify required tools are installed."
	@echo "  vuln                      Run govulncheck vulnerability scan."
	@echo ""
	@echo "Docker Images:"
	@echo "  docker                    Alias for docker-build (host arch)."
	@echo "  docker-build              Build single-arch image for host architecture."
	@echo "  docker-build-native       Build native-arch image via buildx and load locally."
	@echo "  buildx-ensure             Ensure containerized buildx builder exists/active."
	@echo "  dockerx-build             Multi-arch build with buildx (no push)."
	@echo "  dockerx-build-local-multi Build and load per-arch images locally (-amd64/-arm64)."
	@echo "  dockerx-push              Multi-arch build with buildx and push."
	@echo ""
	@echo "Release & Versioning:"
	@echo "  release                   Run tests then dockerx-push."
	@echo "  version                   Print MIRADOR-NRT-AGGREGATOR version/build metadata."
	@echo "  version-human             Print server components for $(VERSION)."
	@echo "  version-ci                Compute CI-friendly version from env/branch."
	@echo "  tag-release               Create and push git tag $(VERSION)."
	@echo "  docker-publish-release    Push server fanout: vX.Y.Z, vX.Y, vX, latest, stable."
	@echo "  docker-publish-pr         Push PR tag 0.0.0-pr.<PR#>.<sha> and pr-<PR#>."
	@echo ""

# -------------
# Development & Build
# -------------
setup: tools deps proto ## Install tools, generate proto, download deps.

deps: ## Download module dependencies
	$(GO) mod download

proto: ## (Optional) Generate protobuf stubs if you add local protos
	@if command -v protoc >/dev/null 2>&1; then \
		echo ">> protoc found; if you have local protos, generate them here"; \
	else \
		echo "!! protoc not found — skipping (OTLP protos are pulled via Go modules)"; \
	fi

build: ## Static linux/amd64 build
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME) $(MAIN_PKG)

build-native: ## Build for host OS/Arch
	@mkdir -p $(BIN_DIR)
	GOOS=$(HOST_OS) GOARCH=$(HOST_ARCH) $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME)-$(HOST_OS)-$(HOST_ARCH) $(MAIN_PKG)

build-linux-amd64:
	@mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME)-linux-amd64 $(MAIN_PKG)

build-linux-arm64:
	@mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME)-linux-arm64 $(MAIN_PKG)

build-linux-multi: build-linux-amd64 build-linux-arm64 ## Build linux binaries for amd64 and arm64

build-darwin-arm64:
	@mkdir -p $(BIN_DIR)
	GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME)-darwin-arm64 $(MAIN_PKG)

build-windows-amd64:
	@mkdir -p $(BIN_DIR)
	GOOS=windows GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN_NAME)-windows-amd64.exe $(MAIN_PKG)

build-all: build-linux-multi build-darwin-arm64 build-windows-amd64 ## Build common targets for all platforms

dev-build: ## Dev build (no -s -w, keep symbols)
	@mkdir -p $(BIN_DIR)
	GOOS=$(HOST_OS) GOARCH=$(HOST_ARCH) $(GO) build -trimpath -ldflags "-X main.version=$(VERSION_PLAIN) -X main.commit=$(GIT_SHA) -X main.date=$(BUILD_DATE) -X main.branch=$(BRANCH)" -o $(BIN_DIR)/$(BIN_NAME)-dev $(MAIN_PKG)

dev run: ## Run locally
	$(GO) run $(MAIN_PKG) --config=config.example.yaml

clean:
	@rm -rf $(BIN_DIR)

clean-build: clean build ## Clean then fresh build

# -------------
# Testing & Quality
# -------------
test: ## Unit tests with race & coverage
	$(GO) test ./... -race -coverprofile=coverage.out -covermode=atomic

fmt: ## go fmt + goimports
	$(GO) fmt ./...
	@command -v $(GOIMPORTS) >/dev/null 2>&1 && $(GOIMPORTS) -w . || echo "goimports not installed; run 'make tools'"

lint: ## golangci-lint
	@command -v $(GOLANGCI) >/dev/null 2>&1 || (echo "golangci-lint not found; run 'make tools'" && exit 1)
	$(GOLANGCI) run

vuln: ## govulncheck
	@command -v $(GOVULN) >/dev/null 2>&1 || (echo "govulncheck not found; run 'make tools'" && exit 1)
	$(GOVULN) ./...

tools: ## Install dev tools (protoc-gen-*, lint, swag, govulncheck)
	@echo ">> Installing dev tools"
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install github.com/swaggo/swag/cmd/swag@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

check-tools: ## Verify required tools installed
	@for t in goimports golangci-lint govulncheck; do \
		if ! command -v $$t >/dev/null 2>&1; then echo "Missing tool: $$t"; exit 1; fi; \
	done; echo "All required tools OK."

# -------------
# Docker
# -------------
docker: docker-build ## Alias

docker-build: ## Build single-arch image for host arch
	docker build -t $(IMAGE) -t $(IMAGE_LATEST) .

docker-build-native: buildx-ensure ## Build native-arch with buildx and load
	docker buildx build --load -t $(IMAGE) -t $(IMAGE_LATEST) --platform $(HOST_OS)/$(HOST_ARCH) .

buildx-ensure: ## Ensure buildx builder exists/active
	@if ! docker buildx inspect mirador-builder >/dev/null 2>&1; then \
		echo ">> Creating buildx builder 'mirador-builder'"; \
		docker buildx create --name mirador-builder --use; \
	else \
		echo ">> Using existing buildx builder 'mirador-builder'"; \
		docker buildx use mirador-builder; \
	fi

dockerx-build: buildx-ensure ## Multi-arch build (no push)
	docker buildx build \
	  --platform $(PLATFORMS) \
	  -t $(IMAGE) \
	  -t $(IMAGE_LATEST) \
	  .

dockerx-build-local-multi: buildx-ensure ## Build and load per-arch locally
	docker buildx build --load --platform linux/amd64 -t $(IMAGE)-amd64 .
	docker buildx build --load --platform linux/arm64 -t $(IMAGE)-arm64 .

dockerx-push: buildx-ensure ## Multi-arch build and push
	docker buildx build \
	  --platform $(PLATFORMS) \
	  -t $(IMAGE) \
	  -t $(IMAGE_LATEST) \
	  --push \
	  .

# -------------
# Release & Versioning
# -------------
release: test dockerx-push ## Run tests then push multi-arch image

version: ## Print version/build metadata
	@echo "version:       $(VERSION_PLAIN)"
	@echo "git tag:       $(GIT_TAG)"
	@echo "git commit:    $(GIT_SHA)"
	@echo "date (UTC):    $(BUILD_DATE)"
	@echo "branch:        $(BRANCH)"
	@echo "image:         $(IMAGE)"

version-human: ## Human-friendly version output
	@echo "Mirador NRT Aggregator $(VERSION_PLAIN) (commit $(GIT_SHA)) built $(BUILD_DATE)"

version-ci: ## Compute CI-friendly version from env/branch
	@# Usage: make version-ci VERSION_OVERRIDE=1.2.3
	@if [ -n "$$VERSION_OVERRIDE" ]; then echo "$$VERSION_OVERRIDE"; \
	else \
	  if [[ "$(BRANCH)" == "main" || "$(BRANCH)" == "master" ]]; then echo "$(VERSION_PLAIN)"; \
	  else echo "$(VERSION_PLAIN)-$(BRANCH)-$(GIT_SHA)"; fi; \
	fi

tag-release: ## Create and push git tag $(VERSION)
	@if [ "$(VERSION)" = "v0.0.0" ] || [ -z "$(VERSION)" ]; then echo "Set VERSION=vX.Y.Z"; exit 1; fi
	$(GIT) tag -a $(VERSION) -m "Release $(VERSION)"
	$(GIT) push origin $(VERSION)

docker-publish-release: ## Push fanout tags: vX.Y.Z, vX.Y, vX, latest, stable
	@major=$$(echo $(VERSION_PLAIN) | cut -d. -f1); \
	minor=$$(echo $(VERSION_PLAIN) | cut -d. -f2); \
	patch=$$(echo $(VERSION_PLAIN) | cut -d. -f3); \
	base=$(IMAGE_REPO)/$(IMAGE_NAME); \
	echo "Pushing $(base):$(VERSION_PLAIN) as primary"; \
	docker pull $(base):$(VERSION_PLAIN) || true; \
	for tag in "v$${major}.$${minor}.$${patch}" "v$${major}.$${minor}" "v$${major}" "latest" "stable"; do \
	  echo "Tagging and pushing $$tag"; \
	  docker tag $(base):$(VERSION_PLAIN) $(base):$$tag; \
	  docker push $(base):$$tag; \
	done

docker-publish-pr: ## Push PR-tagged images: 0.0.0-pr.<PR#>.<sha> and pr-<PR#>
	@if [ -z "$$PR_NUMBER" ]; then echo "Set PR_NUMBER=<PR#>"; exit 1; fi
	@base=$(IMAGE_REPO)/$(IMAGE_NAME); \
	tag1=0.0.0-pr.$${PR_NUMBER}.$(GIT_SHA); \
	tag2=pr-$${PR_NUMBER}; \
	echo "Tagging $$tag1 and $$tag2"; \
	docker tag $(base):$(VERSION_PLAIN) $(base):$$tag1; \
	docker tag $(base):$(VERSION_PLAIN) $(base):$$tag2; \
	docker push $(base):$$tag1; \
	docker push $(base):$$tag2
