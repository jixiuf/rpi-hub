# rpi-mesh — build & install the plugin dylib into ~/.rpi/agent/extensions
#
# macOS note: the dylib MUST be removed before copying. Overwriting the file
# in place keeps the same inode, and a new process then maps stale cached
# signature pages for it — the kernel kills the process (CODESIGNING
# Invalid Page). `rm` first forces a fresh inode.

EXT_DIR ?= $(HOME)/.rpi/agent/extensions
DYLIB := librpi_mesh.dylib

.PHONY: install build reinstall clean help
.DEFAULT_GOAL := help

build: ## cargo build --release for the mesh plugin
	cargo build --release -p rpi-mesh

install: build ## build + replace the installed dylib (rm first — see note above)
	rm -f $(EXT_DIR)/$(DYLIB)
	cp target/release/$(DYLIB) $(EXT_DIR)/
	@codesign -v $(EXT_DIR)/$(DYLIB) \
		&& echo "installed → $(EXT_DIR)" \
		|| echo "warning: codesign check failed"

reinstall: ## copy the already-built dylib without rebuilding
	rm -f $(EXT_DIR)/$(DYLIB)
	cp target/release/$(DYLIB) $(EXT_DIR)/
	@codesign -v $(EXT_DIR)/$(DYLIB) \
		&& echo "installed → $(EXT_DIR)" \
		|| echo "warning: codesign check failed"

test: ## run the workspace unit tests
	cargo test -p rpi-mesh-core -p rpi-mesh --locked --lib

smoke: ## end-to-end against a real rpi binary (RPI_BIN to override)
	scripts/smoke.sh $(RPI_BIN)

clean: ## remove release artifacts
	cargo clean

help: ## show this help
	@grep -E '^[a-z_-]+:' $(MAKEFILE_LIST) | awk -F: '{printf "  make %-12s %s\n", $$1, $$2}'
