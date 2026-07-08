# NCRUCES_VERSION must match the github.com/ncruces/go-sqlite3 version in
# go.mod. Bump both together, then re-run `make vendor-shm` to refresh the
# pinned reference copies under shm/upstream/ (docs/NCRUCES_NOTES.md
# §vendoring).
NCRUCES_VERSION := v0.35.2
NCRUCES_REPO    := https://github.com/ncruces/go-sqlite3.git

# Files shm/ is adapted from (OFD locking + the shm interface/constants).
# Copied verbatim into shm/upstream/ for diffing on dependency bumps; not
# compiled -- see shm/README.md for why literal reuse isn't possible.
NCRUCES_SHM_FILES := vfs/os_linux.go vfs/os_darwin.go vfs/shm_ofd.go vfs/const.go

.PHONY: vendor-shm
vendor-shm:
	@set -eu; \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	echo "cloning $(NCRUCES_REPO)@$(NCRUCES_VERSION)..."; \
	git clone --quiet --depth 1 --branch $(NCRUCES_VERSION) $(NCRUCES_REPO) "$$tmp/src"; \
	commit=$$(git -C "$$tmp/src" rev-parse HEAD); \
	mkdir -p shm/upstream; \
	rm -f shm/upstream/*.upstream; \
	for f in $(NCRUCES_SHM_FILES); do \
		cp "$$tmp/src/$$f" "shm/upstream/$$(basename $$f).upstream"; \
	done; \
	{ \
		echo "# UPSTREAM"; \
		echo; \
		echo "Reference copies of the ncruces/go-sqlite3 files shm/ is adapted"; \
		echo "from, kept for diffing on dependency bumps"; \
		echo "(docs/NCRUCES_NOTES.md §vendoring). These are plain-text references"; \
		echo "(.upstream extension) and are NOT compiled -- see shm/README.md for"; \
		echo "why literal reuse isn't possible and what shm/ builds instead."; \
		echo; \
		echo "Vendored from: $(NCRUCES_REPO)"; \
		echo "Version:       $(NCRUCES_VERSION)"; \
		echo "Commit:        $$commit"; \
		echo "Vendored at:   $$(date -u +%Y-%m-%dT%H:%M:%SZ)"; \
		echo; \
		echo "Files:"; \
		for f in $(NCRUCES_SHM_FILES); do echo "  - $$f"; done; \
	} > shm/UPSTREAM.md; \
	echo "vendored $(NCRUCES_VERSION) ($$commit) into shm/upstream/"
