
#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "ic_create_table_as_op.hpp"
#include "jiceberg_generated/libjiceberg.h"
#include "jiceberg_generated/graal_isolate.h" 

namespace duckdb {

template <typename T, typename U>
void ProcessColumnData(const T *data_ptr, 
        arrow::NumericBuilder<U> &builder, 
        const UnifiedVectorFormat &vdata, 
        std::shared_ptr<arrow::Array> &arr,
        idx_t num_rows) {
    
    auto &validity = vdata.validity;
    auto sel = vdata.sel;
    for (idx_t row_idx = 0; row_idx < num_rows; row_idx++) {
        idx_t source_idx = sel->get_index(row_idx);
        if (validity.RowIsValid(source_idx)) {
            if (!(builder.Append(data_ptr[source_idx])).ok()) {
                throw std::runtime_error("Failed to append data");
            };
        } else {
            if (!(builder.AppendNull()).ok()) {
                throw std::runtime_error("Failed to append NULL");
            };
        }
    }

    if (!(builder.Finish(&arr)).ok()) {
        throw std::runtime_error("Failed to build Int32 Arrow array");
    }
}

std::shared_ptr<arrow::Table> DataChunkToArrowTable(duckdb::DataChunk &chunk, ColumnList &columnList) {
    namespace ar = arrow;
    using namespace duckdb;

    int num_columns = chunk.ColumnCount();
    int64_t num_rows = chunk.size();

    // Prepare to build schema (one Arrow field per DuckDB column)
    std::vector<std::shared_ptr<ar::Field>> fields;
    fields.reserve(num_columns);

    // Prepare a container for the final Arrow arrays (one per column)
    std::vector<std::shared_ptr<ar::Array>> arrays;
    arrays.reserve(num_columns);

    for (int col_idx = 0; col_idx < num_columns; col_idx++) {
        auto &col_vector = chunk.data[col_idx];
        auto &duck_type = col_vector.GetType();
        string column_name = columnList.GetColumnNames()[col_idx];
        // We'll unify the vector so we can safely read data regardless of whether it's flat, constant, dictionary, etc.
        UnifiedVectorFormat vdata;
        col_vector.ToUnifiedFormat(num_rows, vdata);
        // A selection vector mapping row_idx -> actual index in `vdata.data`
        auto sel = vdata.sel;
        // Validity (null) mask for each row
        auto &validity = vdata.validity;

        auto metadata = std::make_shared<ar::KeyValueMetadata>();
        metadata->Append("PARQUET:field_id", std::to_string(col_idx+1));

        // TODO: Handle other types as needed: LogicalTypeId::FLOAT, ::BOOLEAN, ::DATE, etc.
        switch (duck_type.id()) {
        case LogicalTypeId::INTEGER: {
            fields.push_back(
                ar::field(column_name, ar::int32())->WithMetadata(metadata));
            auto data_ptr = reinterpret_cast<const int32_t *>(vdata.data);
            ar::Int32Builder builder;
            std::shared_ptr<ar::Array> arr;
            ProcessColumnData(data_ptr, builder, vdata, arr, num_rows);
            arrays.push_back(arr);
            break;
        }

        case LogicalTypeId::BIGINT: {
            fields.push_back(ar::field(column_name, ar::int64())->WithMetadata(metadata));
            auto data_ptr = reinterpret_cast<const int64_t *>(vdata.data);
            ar::Int64Builder builder;
            std::shared_ptr<ar::Array> arr;
            ProcessColumnData(data_ptr, builder, vdata, arr, num_rows);
            arrays.push_back(arr);
            break;
        }

        case LogicalTypeId::DOUBLE: {
            fields.push_back(ar::field(column_name, ar::float64())->WithMetadata(metadata));
            auto data_ptr = reinterpret_cast<const double *>(vdata.data);
            ar::DoubleBuilder builder;
            std::shared_ptr<ar::Array> arr;
            ProcessColumnData(data_ptr, builder, vdata, arr, num_rows);
            arrays.push_back(arr);
            break;
        }

        case LogicalTypeId::VARCHAR: {
            fields.push_back(ar::field(column_name, ar::utf8())->WithMetadata(metadata));
            ar::StringBuilder builder;
            for (idx_t row_idx = 0; row_idx < num_rows; row_idx++) {
                idx_t source_idx = sel->get_index(row_idx);
                if (validity.RowIsValid(source_idx)) {
                    auto val = col_vector.GetValue(source_idx);
                    builder.Append(val.ToString());
                } else {
                    builder.AppendNull();
                }
            }
            std::shared_ptr<ar::Array> arr;
            if (!(builder.Finish(&arr)).ok()) {
                throw std::runtime_error("Failed to build Int32 Arrow array");
            }
            arrays.push_back(arr);
            break;
        }

        default:
            throw std::runtime_error("Unsupported type: " + duck_type.ToString() + " for column " + column_name);
        }
    }

    // Build a final Arrow schema and table
    auto schema = std::make_shared<ar::Schema>(fields);
    return ar::Table::Make(schema, arrays, 1);
}

OperatorResultType ICCreateTableAsOp::Execute(ExecutionContext &context, 
                                                       DataChunk &input, 
                                                       DataChunk &chunk,
                                                       GlobalOperatorState &gstate, 
                                                       OperatorState &state) const {
    auto *table_info = dynamic_cast<CreateTableInfo *>(info->base.get());

    // Create arrow table to write to parquet file
    auto arrow_table = DataChunkToArrowTable(input, table_info->columns);
    
    // Create a file output stream
    const std::string datafile_filename = 
        schemaEntry->schema_data->iceberg_catalog + "." + table_info->schema + "." + table_info->table + ".parquet";
    auto open_result = arrow::io::FileOutputStream::Open(datafile_filename);
    
    if (!open_result.ok()) {
        throw std::runtime_error("Failed to open file: " + open_result.status().ToString());
    }

    std::shared_ptr<arrow::io::FileOutputStream> datafile = *open_result;    
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    builder.version(parquet::ParquetVersion::PARQUET_1_0);
    std::shared_ptr<parquet::WriterProperties> writer_props = builder.build();
    arrow::Status write_status = parquet::arrow::WriteTable(
        *arrow_table,
        arrow::default_memory_pool(),
        datafile,
        1024,  // TODO: Is this a good chunk_size?
        writer_props
    );

    if (!write_status.ok()) {
        throw std::runtime_error("Error writing to Parquet: " + write_status.ToString());
    }

    // Create the table at the destination
    auto transaction = schemaEntry->catalog.GetCatalogTransaction(context.client);
    auto entry = schemaEntry->CreateTable(transaction, *info);

    // Initialize the GraalVM isolate
    graal_isolate_t* isolate = nullptr;
    graal_isolatethread_t* thread = nullptr;
    if (graal_create_isolate(nullptr, &isolate, &thread) != 0) {
        throw std::runtime_error("Failed to create GraalVM isolate");
    }

    string creds = table_credentials.client_id + ":" + table_credentials.client_secret;
    char *result = nullptr;
    try {
        result = append_to_table(
                thread,
                const_cast<char*>(table_credentials.endpoint.c_str()),
                const_cast<char*>(creds.c_str()),
                const_cast<char*>(schemaEntry->schema_data->iceberg_catalog.c_str()),
                const_cast<char*>(table_info->schema.c_str()),
                const_cast<char*>(table_info->table.c_str()),
                const_cast<char*>(datafile_filename.c_str()),
                chunk.size());

    } catch (...) {
        DropInfo drop_info;
        drop_info.type = CatalogType::TABLE_ENTRY;
        drop_info.catalog = info->base->catalog;
        drop_info.schema = table_info->schema;
        drop_info.name =  table_info->table;
        schemaEntry->DropEntry(context.client, drop_info);
    }

    // Clean up the GraalVM isolate
    if (graal_detach_thread(thread) != 0) {
        throw std::runtime_error("Failed to detach GraalVM thread");
    }

    if (result == nullptr) {
        throw std::runtime_error("Failed to append to table");
    }

    // Update the table entry with the new metadata location
    auto table_entry = dynamic_cast<ICTableEntry *>(entry.get());
    table_entry->table_data->metadata_location = result;

    // Set output chunk to empty
    chunk.SetCardinality(0);

    return duckdb::OperatorResultType::FINISHED;
}

} // namespace duckdb