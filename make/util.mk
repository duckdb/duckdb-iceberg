ACTIVE_CATALOG_FILE := .catalogs/.active_catalog

# Stops whatever catalog is currently marked as active
define stop_active_catalog
	@if [ -f "$(ACTIVE_CATALOG_FILE)" ]; then \
		active=$$(cat $(ACTIVE_CATALOG_FILE)); \
		echo "Stopping active catalog: $$active"; \
		$(MAKE) $${active}-stop; \
	fi
	@rm -f $(ACTIVE_CATALOG_FILE)
endef

# Usage: $(call set_active_catalog,<name>)
define set_active_catalog
	@mkdir -p $(dir $(ACTIVE_CATALOG_FILE))
	@echo "$(1)" > $(ACTIVE_CATALOG_FILE)
endef

# Standalone C++ logic tests for manifest Avro compression. Dependency-free (only zlib, for the Avro
# OCF deflate round-trip), so they build and run without a catalog or the DuckDB unittest harness.
# They cover codec resolution and the OCF recompress/varint round-trip the SQL integration tests
# cannot reach. Run with `make test_logic`.
CXX ?= c++
.PHONY: test_logic
test_logic:
	@mkdir -p build
	$(CXX) -std=c++17 test/cpp/test_compression_logic.cpp -lz -o build/test_compression_logic
	./build/test_compression_logic