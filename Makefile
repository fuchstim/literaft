LOCALBIN ?= $(PWD)/bin
GO ?= $(shell which go)
PROTOBUF_VERSION := $(shell go list -m -f '{{.Version}}' google.golang.org/protobuf)

$(LOCALBIN):
	mkdir -p $(LOCALBIN) || true

$(LOCALBIN)/protoc-gen-go: | $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOBUF_VERSION)

.PHONY: generate
generate: $(LOCALBIN)/protoc-gen-go
	PATH="$(LOCALBIN):$$PATH" $(GO) generate ./...
