# rpi-hub — build & install the plugin dylibs into ~/.rpi/agent/extensions
#
# macOS note: the dylib MUST be removed before copying. Overwriting the file
# in place keeps the same inode, and a new process then maps stale cached
# signature pages for it — the kernel kills the process (CODESIGNING
# Invalid Page). `rm` first forces a fresh inode.

EXT_DIR ?= $(HOME)/.rpi/agent/extensions
PLUGINS := rpi-hub rpi-wechat

DYLIBS    := $(PLUGINS:%=target/release/%.dylib)   # not real paths: cdylib names are lib<PKG>.dylib
INSTALLED := $(PLUGINS:%=$(EXT_DIR)/lib%.dylib)

.PHONY: install build reinstall clean help
.DEFAULT_GOAL := help

build: ## cargo build --release for both plugins
	cargo build --release -p rpi-mesh

install: build ## build + replace the installed dylibs (rm first — see note above)
	rm -f $(EXT_DIR)/librpi_mesh.dylib $(EXT_DIR)/librpi_mesh.dylib
	cp target/release/librpi_mesh.dylib target/release/librpi_mesh.dylib $(EXT_DIR)/
	@codesign -v $(EXT_DIR)/librpi_mesh.dylib $(EXT_DIR)/librpi_mesh.dylib \
		&& echo "installed → $(EXT_DIR)" \
		|| echo "warning: codesign check failed"

reinstall: ## copy already-built dylibs without rebuilding
	rm -f $(EXT_DIR)/librpi_mesh.dylib $(EXT_DIR)/librpi_mesh.dylib
	cp target/release/librpi_mesh.dylib target/release/librpi_mesh.dylib $(EXT_DIR)/
	@codesign -v $(EXT_DIR)/librpi_mesh.dylib $(EXT_DIR)/librpi_mesh.dylib \
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
