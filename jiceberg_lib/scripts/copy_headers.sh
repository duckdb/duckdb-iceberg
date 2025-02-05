#!/bin/sh

# Make sure we are in the correct folder
working_dir=$(pwd)
if [ "${working_dir%jiceberg_lib}" != "${working_dir}" ]; then
  cd scripts
else
    if [ "${working_dir%jiceberg_lib/scripts}" == "${working_dir}" ]; then
      echo "Please run from jiceberg_lib/ folder"
      exit
    fi
fi

echo "Copying header files..."

rm -rf ../../src/include/jiceberg_generated
mkdir ../../src/include/jiceberg_generated
cp ../app/build/native/nativeCompile/*.h ../../src/include/jiceberg_generated/
