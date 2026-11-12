/*
 * Generate the Java Iceberg v3 identity-partition fixture from the repository root:
 *
 * java --class-path "scripts/data_generators/iceberg-spark-runtime-4.1_2.13-1.11.0.jar:<pyspark>/jars/*" \
 *     scripts/persistent/PartitionTimestamptzNs.java
 *
 * The destination must not already exist. Paths remain relative to the repository.
 */
import java.time.OffsetDateTime;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

class PartitionTimestamptzNs {
    public static void main(String[] args) throws Exception {
        String location = "data/persistent/partition_timestamptz_ns";
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withZone())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("ts").build();
        Table table = new HadoopTables(new Configuration()).create(
            schema, spec, Map.of("format-version", "3"), location
        );
        String[] timestamps = {
            "2026-10-09T10:00:00.123456789Z",
            "2026-10-09T12:00:00.123456790+02:00",
            "1969-12-31T23:59:59.999999999Z",
            "1970-01-01T00:00:00Z",
            null
        };
        AppendFiles append = table.newAppend();
        for (int i = 0; i < timestamps.length; i++) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("id", i + 1);
            record.setField("ts", timestamps[i] == null ? null : OffsetDateTime.parse(timestamps[i]));
            PartitionKey partition = new PartitionKey(spec, schema);
            partition.partition(new InternalRecordWrapper(schema.asStruct()).wrap(record));
            String path = table.locationProvider().newDataLocation(spec, partition, "data-" + i + ".parquet");
            DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path))
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(spec)
                .withPartition(partition)
                .build();
            try (writer) {
                writer.write(record);
            }
            append.appendFile(writer.toDataFile());
        }
        append.commit();
    }
}
