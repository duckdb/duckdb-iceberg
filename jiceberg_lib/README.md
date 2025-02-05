Apache Iceberg does not have a library written in C++. This project uses the Java library
to create helper methods to be used in C++.

# Instructions
* Install [SDKMAN](https://sdkman.io/install/)
* Install GraalVM CE 21
```build
sdk install java 21.0.2-graalce
```
* Build library
``` 
gradle nativeBuild
```
* Copy header files
```
./scripts/copy_headers.sh
```
* In `src/include/jiceberg_generated`, replace <angled> includes with "quotes" in the header files starting with `libjiceberg...`

