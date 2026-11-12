/*
 * Generate the Java Iceberg v3 encryption fixtures from the repository root:
 *
 * java --class-path "scripts/data_generators/iceberg-spark-runtime-4.1_2.13-1.11.0.jar:<pyspark>/jars/*" \
 *     scripts/persistent/EncryptedTable.java
 *
 * Both table directories must be absent. An optional argument overrides the
 * default root, data/persistent/encrypted_table. Paths stay repository-relative.
 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

class EncryptedTable {
    private static final Map<String, String> PROPERTIES =
        Map.of("format-version", "3", "encryption.key-id", "keyA");

    // Fixture-only KMS: a fixed local wrapping key makes the files readable
    // without any external service. File keys and GCM nonces remain random.
    private static class FixtureKms implements KeyManagementClient {
        private static final byte[] KEY =
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

        public void initialize(Map<String, String> properties) {}

        private byte[] crypt(int mode, byte[] input, byte[] nonce, String keyId) {
            if (!"keyA".equals(keyId)) {
                throw new IllegalArgumentException("Unknown fixture key: " + keyId);
            }
            try {
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(mode, new SecretKeySpec(KEY, "AES"), new GCMParameterSpec(128, nonce));
                return cipher.doFinal(input);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        public ByteBuffer wrapKey(ByteBuffer key, String keyId) {
            byte[] nonce = new byte[12];
            new SecureRandom().nextBytes(nonce);
            byte[] plain = new byte[key.remaining()];
            key.duplicate().get(plain);
            byte[] wrapped = crypt(Cipher.ENCRYPT_MODE, plain, nonce, keyId);
            return ByteBuffer.allocate(nonce.length + wrapped.length).put(nonce).put(wrapped).flip();
        }

        public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String keyId) {
            ByteBuffer buffer = wrappedKey.duplicate();
            byte[] nonce = new byte[12];
            buffer.get(nonce);
            byte[] wrapped = new byte[buffer.remaining()];
            buffer.get(wrapped);
            return ByteBuffer.wrap(crypt(Cipher.DECRYPT_MODE, wrapped, nonce, keyId));
        }
    }

    // HadoopTables supplies filesystem commits but no encryption manager.
    // Wire in Iceberg's standard manager and persist its manifest-list keys,
    // as an encryption-aware catalog does.
    private static class EncryptingOperations implements TableOperations {
        private final TableOperations delegate;
        private final EncryptionManager encryption;

        EncryptingOperations(Table table) {
            delegate = ((HasTableOperations) table).operations();
            encryption = EncryptionUtil.createEncryptionManager(
                delegate.current().encryptionKeys(), table.properties(), new FixtureKms());
        }

        public TableMetadata current() { return delegate.current(); }
        public TableMetadata refresh() { return delegate.refresh(); }
        public EncryptionManager encryption() { return encryption; }
        public FileIO io() { return EncryptingFileIO.combine(delegate.io(), encryption); }
        public LocationProvider locationProvider() { return delegate.locationProvider(); }
        public String metadataFileLocation(String name) { return delegate.metadataFileLocation(name); }

        public void commit(TableMetadata base, TableMetadata metadata) {
            TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
            EncryptionUtil.encryptionKeys(encryption).values().forEach(builder::addEncryptionKey);
            delegate.commit(base, builder.build());
        }
    }

    private static Table encrypted(Table table) {
        return new BaseTable(new EncryptingOperations(table), table.name());
    }

    private static void verifyMagic(String location, String expected) throws Exception {
        try (var input = Files.newInputStream(Path.of(location))) {
            String actual = new String(input.readNBytes(4), StandardCharsets.US_ASCII);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("Unexpected file magic at " + location + ": " + actual);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String root = args.length == 0 ? "data/persistent/encrypted_table" : args[0];
        String populated = root + "/t_enc";
        String empty = root + "/t_enc_empty";
        if (Files.exists(Path.of(populated)) || Files.exists(Path.of(empty))) {
            throw new IllegalArgumentException("Fixture table directories must not already exist");
        }
        // RawLocalFileSystem avoids machine-local Hadoop .crc sidecars.
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        HadoopTables tables = new HadoopTables(conf);
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = encrypted(tables.create(schema, spec, PROPERTIES, populated));
        tables.create(schema, spec, PROPERTIES, empty);

        String dataPath = table.locationProvider().newDataLocation("data.parquet");
        var output = table.encryption().encrypt(table.io().newOutputFile(dataPath));
        DataWriter<Record> writer = Parquet.writeData(output)
            .forTable(table)
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(spec)
            .withKeyMetadata(output.keyMetadata())
            .build();
        try (writer) {
            for (long id = 1; id <= 2; id++) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField("data", id == 1 ? "secret-a" : "secret-b");
                writer.write(record);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();

        // Reload with a fresh manager so verification uses the persisted keys.
        Table reloaded = encrypted(tables.load(populated));
        if (reloaded.currentSnapshot().keyId() == null ||
            ((HasTableOperations) reloaded).operations().current().encryptionKeys().isEmpty()) {
            throw new IllegalStateException("Snapshot encryption metadata was not persisted");
        }
        verifyMagic(dataPath, "PARE");
        verifyMagic(reloaded.currentSnapshot().manifestListLocation(), "AGS1");
        for (var manifest : reloaded.currentSnapshot().allManifests(reloaded.io())) {
            verifyMagic(manifest.path(), "AGS1");
        }
        Map<Long, String> actual = new TreeMap<>();
        try (var records = IcebergGenerics.read(reloaded).build()) {
            for (Record record : records) {
                actual.put((Long) record.getField("id"), record.getField("data").toString());
            }
        }
        if (!actual.equals(Map.of(1L, "secret-a", 2L, "secret-b"))) {
            throw new IllegalStateException("Java decryption did not recover the expected rows: " + actual);
        }
        System.out.println("Verified Java encrypted table: " + actual);
    }
}
