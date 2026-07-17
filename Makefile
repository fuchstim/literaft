LOCALBIN ?= $(PWD)/bin
GO ?= $(shell which go)
GOBIN = $(LOCALBIN)

BUF_VERSION ?= v1.61.0
PROTOBUF_VERSION := $(shell go list -m -f '{{.Version}}' google.golang.org/protobuf)
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.2
GINKGO_VERSION ?= v2.32.0

$(LOCALBIN):
	mkdir -p $(LOCALBIN) || true

$(LOCALBIN)/buf: | $(LOCALBIN)
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

$(LOCALBIN)/protoc-gen-go: | $(LOCALBIN)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOBUF_VERSION)

$(LOCALBIN)/protoc-gen-go-grpc: | $(LOCALBIN)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

$(LOCALBIN)/ginkgo: | $(LOCALBIN)
	$(GO) install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)


.PHONY: generate
generate: $(LOCALBIN)/buf $(LOCALBIN)/protoc-gen-go $(LOCALBIN)/protoc-gen-go-grpc
	PATH="$(LOCALBIN):$$PATH" $(GO) generate ./...

.PHONY: test/unit
test/unit: $(LOCALBIN)/ginkgo
	$(LOCALBIN)/ginkgo run -r -p --keep-going --fail-on-pending --race --cover --coverprofile coverage.out --skip-package ./integration  ./...

.PHONY: test/correctness
test/correctness: $(LOCALBIN)/ginkgo
	$(LOCALBIN)/ginkgo run -r -p --keep-going --fail-on-pending --race --cover --coverprofile coverage.out --skip-file ./integration/throughput_test.go ./integration
