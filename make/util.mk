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

# Standalone C++ logic tests. Dependency-free (the compression suite uses zlib for the Avro OCF
# deflate round-trip), so they build and run without a catalog or the DuckDB unittest harness. They
# cover codec resolution / OCF recompress, the manifest bin-packing / merge-decision logic, and the
# commit-retry backoff / status-classification / ancestry / delete-attribution logic the SQL
# integration tests cannot reach. Run with `make test_logic`.
CXX ?= c++
.PHONY: test_logic
test_logic:
	@mkdir -p build
	$(CXX) -std=c++17 test/cpp/test_compression_logic.cpp -lz -o build/test_compression_logic
	./build/test_compression_logic
	$(CXX) -std=c++17 test/cpp/test_merge_logic.cpp -o build/test_merge_logic
	./build/test_merge_logic
	$(CXX) -std=c++17 test/cpp/test_retry_logic.cpp -o build/test_retry_logic
	./build/test_retry_logic