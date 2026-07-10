LOCALBIN ?= $(PWD)/bin
GO ?= $(shell which go)
GOBIN ?= $(LOCALBIN)

BUF_VERSION ?= v1.61.0
PROTOBUF_VERSION := $(shell go list -m -f '{{.Version}}' google.golang.org/protobuf)

$(LOCALBIN):
	mkdir -p $(LOCALBIN) || true

$(LOCALBIN)/buf: | $(LOCALBIN)
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

$(LOCALBIN)/protoc-gen-go: | $(LOCALBIN)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOBUF_VERSION)


.PHONY: generate
generate: $(LOCALBIN)/buf $(LOCALBIN)/protoc-gen-go
	PATH="$(LOCALBIN):$$PATH" $(GO) generate ./...
