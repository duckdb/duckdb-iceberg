
-- first copy the data files from the remote
-- you will need to manually copy the field id information.
copy (select * from 's3://path/to/data/file/remote.parquet') to 'path/to/data/file/local.parquet' (FIELD_IDS {<field_ids + partition_field_ids>});

-- then copy the manifest files you want
copy (
select 
	status, 
	snapshot_id, 
	sequence_number,
	file_sequence_number,
	data_file 
from 
	(select * from (
		select 
			* replace(struct_update(data_file, file_path := ('data/persistent/<iceberg_table_name>/data' || data_file.file_path[63:])) as data_file),
			unnest(data_file)
		from read_avro('s3://warehouse/default/path/to/manifest_file_name.avro')
	)
where file_path in (
	'path/to/remote/data/file.parquet',
)))
to 
'data/persistent/<iceberg_table_name>/metadata/manifest_file_name.avro' (FORMAT AVRO,
FIELD_IDS {
	'status': 0,
	'snapshot_id' : 1,
	'sequence_number' : 3,
	'file_sequence_number' : 4,
	'data_file': {
		'__duckdb_field_id': 2,
		'content': 134,
		'file_path' : 100,
		'file_format' : 101,
		'partition': {
			'__duckdb_field_id' : 102,
			'joined_month': 1000
		},
		'record_count' : 103,
		'file_size_in_bytes': 104,
		'column_sizes': {
			'__duckdb_field_id': 108,
			'key' : 117,
			'value': 118
		},
		'value_counts' : {
			'__duckdb_field_id' : 109,
			'key': 119,
			'value': 120
		},
		'null_value_counts': {
			'__duckdb_field_id' : 110,
			'key': 121,
			'value': 122
		},
		'nan_value_counts': {
			'__duckdb_field_id' : 137,
			'key': 138,
			'value': 139
		},
		'lower_bounds': {
			'__duckdb_field_id' : 125,
			'key': 126,
			'value': 127
		},
		'upper_bounds': {
			'__duckdb_field_id' : 128,
			'key': 129,
			'value': 130
		},
		'key_metadata': 131,
		'split_offsets': {
			'__duckdb_field_id': 132,
			'element': 133
		}, 
		'equality_ids': {
			'__duckdb_field_id': 135,
			'element': 136
		},
		'sort_order_id': 140,
		'referenced_data_file': 143

	}
});



-- then copy the manifest list
copy (
	select * replace('path/to/local/manifest/file_name' as manifest_path)
	from 
		read_avro('s3://path/to/manifest/list') where <some condition to select specific manifests>
) to 'data/persistent/<table_name>/metadata/manifest_list_local.avro'
(FORMAT AVRO, FIELD_IDS {
	'manifest_path': 500,
	'manifest_length': 501,
	'partition_spec_id': 502,
	'content': 517,
	'sequence_number': 515,
	'min_sequence_number': 516,
	'added_snapshot_id': 503,
	'added_files_count': 504,
	'existing_files_count': 505,
	'deleted_files_count': 506,
	'added_rows_count': 512,
	'existing_rows_count': 513,
	'deleted_rows_count': 514,
	'partitions': {
		'element': {
			'contains_null': 509,
			'contains_nan': 518,
			'lower_bound': 510,
			'upper_bound': 511,
			'__duckdb_field_id': 508
		},
		'__duckdb_field_id': 507
	},
	'key_metadata': 519
});


-- lastly you need to create a metadata.json. You can most likely get away with copying the first or second metadata.json
-- and changing the location of the manifest list. This is assuming the schema has not been changed.
