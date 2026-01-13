CREATE or REPLACE TABLE default.type_promotion_partitioned_table (
	id int
)
PARTITIONED BY (id)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);