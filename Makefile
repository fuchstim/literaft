GOBIN := $(shell go env GOPATH)/bin
PROTOBUF_VERSION := $(shell go list -m -f '{{.Version}}' google.golang.org/protobuf)

.PHONY: generate
generate:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOBUF_VERSION)
	PATH="$(GOBIN):$$PATH" go generate ./...
