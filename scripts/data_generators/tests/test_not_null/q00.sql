CREATE OR REPLACE TABLE default.test_not_null (
    id INT,
    name STRING NOT NULL,
    address STRUCT<
        street: STRING NOT NULL,
        city: STRING NOT NULL,
        zip: STRING NOT NULL
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);