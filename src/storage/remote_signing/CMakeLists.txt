add_library(
  iceberg_storage_remote_signing OBJECT
  iceberg_remote_signed_file_system.cpp iceberg_remote_signer.cpp
  iceberg_remote_signing.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_remote_signing>
    PARENT_SCOPE)
