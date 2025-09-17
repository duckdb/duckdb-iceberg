# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb-avro
		GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
)
endif()

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-mqtt.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-kinesis.a;../../vcpkg_installed/wasm32-emscripten/lib/liblzma.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-auth.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libroaring.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-cal.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-sdkutils.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sso.a;../../vcpkg_installed/wasm32-emscripten/lib/libs2n.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-common.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-checksums.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sts.a;../../vcpkg_installed/wasm32-emscripten/lib/libsnappy.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-compression.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-cognito-identity.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-crt-cpp.a;../../vcpkg_installed/wasm32-emscripten/lib/libssl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-event-stream.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-core.a;../../vcpkg_installed/wasm32-emscripten/lib/libcrypto.a;../../vcpkg_installed/wasm32-emscripten/lib/libz.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-http.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-dynamodb.a;../../vcpkg_installed/wasm32-emscripten/lib/libcurl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-io.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-identity-management.a;../../vcpkg_installed/wasm32-emscripten/lib/libjansson.a;../../third_party/mbedtls/libduckdb_mbedtls.a"
)

if (NOT EMSCRIPTEN)
duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG dbb022506e21c27fc4d4cd3d14995af89955401a
)


if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG f855eb3dce37700bfd36fe906a683e4be17dcaf6
    )
endif ()
endif()
