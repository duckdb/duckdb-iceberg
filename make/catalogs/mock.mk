.PHONY: mock mock-stop test_mock test_mock_release test_mock_debug test_mock_reldebug test_mock_relassert

MOCK_TEST_BINARY ?= $(PROJ_DIR)build/release/test/unittest
MOCK_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
# Tests that attach the mock with their own options instead of the config's my_datalake. A separate unittest
# invocation: the test specs of one invocation are AND-ed, so a second glob would match nothing.
MOCK_CUSTOM_TEST_FILTER ?= test/sql/local/catalog_custom_setup/mock/*

mock: mock-stop
	$(call stop_active_catalog)
	python3 -m scripts.mock_rest_catalog.lifecycle start
	$(call set_active_catalog,mock)

mock-stop:
	python3 -m scripts.mock_rest_catalog.lifecycle stop
	@if [ -f "$(ACTIVE_CATALOG_FILE)" ] && [ "$$(cat "$(ACTIVE_CATALOG_FILE)")" = mock ]; then \
		rm -f "$(ACTIVE_CATALOG_FILE)"; \
	fi

test_mock_release test_mock_debug test_mock_reldebug test_mock_relassert:
	$(MAKE) test_mock MOCK_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_mock_%,%,$@)/test/unittest"

test_mock:
	@set -e; \
	if [ "$(SKIP_TESTS)" = "1" ]; then echo "Mock catalog tests are skipped."; exit 0; fi; \
	trap '$(MAKE) mock-stop' EXIT; \
	$(MAKE) mock; \
	"$(MOCK_TEST_BINARY)" --test-config "$$(scripts/catalog_test_config.sh)" \
		"$(MOCK_TEST_FILTER)" "exclude:*.test_slow"; \
	"$(MOCK_TEST_BINARY)" --test-config "$$(scripts/catalog_test_config.sh)" \
		"$(MOCK_CUSTOM_TEST_FILTER)" "exclude:*.test_slow"

# The native backend owns its state inside each unittest DatabaseInstance.
# It neither consults nor changes the active service catalog.
# LOAD_TESTS also registers absolute aliases. Select the repository-relative
# tests once, matching the paths in the catalog config's skip list on every OS.
MOCK_NATIVE_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
.PHONY: test_mock_native test_mock_native_release test_mock_native_debug test_mock_native_reldebug test_mock_native_relassert
test_mock_native_release test_mock_native_debug test_mock_native_reldebug test_mock_native_relassert:
	$(MAKE) test_mock_native MOCK_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_mock_native_%,%,$@)/test/unittest"

test_mock_native:
	@if [ "$(SKIP_TESTS)" = "1" ]; then \
		echo "Native mock catalog tests are skipped."; \
	else \
		"$(MOCK_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/mock_native.json" \
			"test/sql/local/native_catalog/*" && \
		"$(MOCK_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/mock_native.json" \
			"$(MOCK_NATIVE_TEST_FILTER)" "exclude:*.test_slow"; \
	fi
