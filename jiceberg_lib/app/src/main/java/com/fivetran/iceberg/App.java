package com.fivetran.iceberg;

import org.apache.iceberg.*;
import org.apache.iceberg.io.*;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.catalog.TableIdentifier;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class App {
    @CEntryPoint(name = "append_to_table")
    public static int appendToTable(
            IsolateThread thread,
            CCharPointer uri,
            CCharPointer creds,
            CCharPointer warehouse,
            CCharPointer schemaName,
            CCharPointer tableName,
            CCharPointer filename,
            int numRecords) {
        try {
            return append(
                    CTypeConversion.toJavaString(uri),
                    CTypeConversion.toJavaString(creds),
                    CTypeConversion.toJavaString(warehouse),
                    CTypeConversion.toJavaString(schemaName),
                    CTypeConversion.toJavaString(tableName),
                    CTypeConversion.toJavaString(filename),
                    numRecords);
        } catch (IOException e) {
            return -1;
        } finally {
            // Delete the local file
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
            append("https://polaris.fivetran.com/api/catalog",
                    "client_id:client_secret",
                    "catalog",
                    "schema",
                    "new_table",
                    "datafile.parquet",
                    1
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static int append(
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

                // Append the data file
                table.newAppend()
                        .appendFile(dataFile)
                        .commit();
            }

//            Snapshot snapshot = table.currentSnapshot();
//            FileIO io = table.io();
//            // For newer Iceberg versions, use `allManifests(io)`:
//            for (ManifestFile mf : snapshot.allManifests(io)) {
//                System.out.println("Manifest path: " + mf.path()
//                        + ", addedFilesCount=" + mf.addedFilesCount());
//            }
//
//            Iterable<DataFile> newlyAddedFiles = snapshot.addedDataFiles(table.io());
//            for (DataFile df : newlyAddedFiles) {
//                System.out.println("Added file path: " + df.path());
//            }
//
//            table.newScan()
//                    .planFiles()
//                    .forEach(task -> {
//                        System.out.println("Active data file path: " + task.file().path());
//                        System.out.println("Row count: " + task.file().recordCount());
//                    });

//            Schema schema = table.schema();
//            // Iterate over each file in the table scan
//            for (FileScanTask task : table.newScan().planFiles()) {
//                // Print file information
//                DataFile dataFile = task.file();
//
//                // Convert the DataFile to an Iceberg InputFile
//                InputFile inputFile = table.io().newInputFile(dataFile.path().toString());
//
//                // Build a Parquet reader using Iceberg's Parquet API
//                try (CloseableIterable<Record> reader = Parquet.read(inputFile)
//                        .project(schema) // Project the full schema or a subset
//                        .createReaderFunc(messageType -> GenericParquetReaders.buildReader(schema, messageType))
//                        .build()) {
//
//                    // Iterate over the records and process them
//                    for (Record record : reader) {
//                        System.out.println("Record: " + record);
//                    }
//                } catch (IOException e) {
//                    System.err.println("Failed to read data from file: " + dataFile.path());
//                    e.printStackTrace();
//                }
//            }

            return 0;
        }
    }
}
