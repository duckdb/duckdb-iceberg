package com.fivetran.iceberg;

import org.apache.iceberg.*;
import org.apache.iceberg.io.*;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.catalog.TableIdentifier;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class App {
    @CEntryPoint(name = "append_to_table")
    public static CCharPointer appendToTable(
            IsolateThread thread,
            CCharPointer uri,
            CCharPointer creds,
            CCharPointer warehouse,
            CCharPointer schemaName,
            CCharPointer tableName,
            CCharPointer filename,
            int numRecords) {
        try {
            String newMetadataLoc = append(
                    CTypeConversion.toJavaString(uri),
                    CTypeConversion.toJavaString(creds),
                    CTypeConversion.toJavaString(warehouse),
                    CTypeConversion.toJavaString(schemaName),
                    CTypeConversion.toJavaString(tableName),
                    CTypeConversion.toJavaString(filename),
                    numRecords);
            try (CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(newMetadataLoc)) {
                return holder.get();
            }
        } catch (IOException e) {
            return WordFactory.nullPointer();
        } finally {
            // Delete the local file no matter what
            Path localPath = Paths.get(CTypeConversion.toJavaString(filename));
            try {
                java.nio.file.Files.deleteIfExists(localPath);
            } catch (IOException e) {
                System.out.println("Unable to delete datafile");
            }
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println(
                append("https://polaris.fivetran.com/api/catalog",
                        "client_id:client_secret",
                        "catalog",
                        "schema",
                        "new_table",
                        "datafile.parquet",
                        1
            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String append(
            String uri,
            String creds,
            String warehouse,
            String schemaName,
            String tableName,
            String filename,
            int numRecords) throws IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, uri);
        properties.put("credential", creds);
        properties.put("oauth2-server-uri", uri + "/v1/oauth/tokens");
        properties.put("scope", "PRINCIPAL_ROLE:ALL");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

        try(RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("catalog", properties);
            TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);
            Table table = catalog.loadTable(tableId);

            // Copy the local file to the S3-backed OutputFile
            Path localPath = Paths.get(filename);
            try(FileIO fileIO = table.io()) {
                OutputFile outputFile = fileIO.newOutputFile(
                        table.location() + "/data/" + filename);
                try (PositionOutputStream out = outputFile.createOrOverwrite()) {
                    // Use Java's Files.copy to transfer the contents
                    java.nio.file.Files.copy(localPath, out);
                }

                PartitionSpec spec = table.spec();
                DataFile dataFile = DataFiles.builder(spec)
                        .withRecordCount(numRecords)
                        .withFileSizeInBytes(outputFile.toInputFile().getLength())
                        .withInputFile(outputFile.toInputFile())
                        .withFormat(FileFormat.PARQUET)
                        .build();

                // Force a new metadata commit
                table.updateProperties()
                        .set("last-modified-time", String.valueOf(System.currentTimeMillis()))
                        .commit();

                // Optionally, you can try to expire old metadata
                table.expireSnapshots()
                        .expireOlderThan(System.currentTimeMillis())
                        .commit();

                // Append the data file
                table.newAppend()
                        .appendFile(dataFile)
                        .set("write.format.default", "parquet")
                        .set("format-version", "2")
                        .set("write.metadata.compression-codec", "none")
                        .set("snapshot-id", UUID.randomUUID().toString())  // Force new snapshot
                        .commit();

                return ((BaseTable) table).operations().current().metadataFileLocation();
            }
        }
    }
}
