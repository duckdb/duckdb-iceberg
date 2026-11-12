-- The table itself is created through the Iceberg Java API in __init__.py, because Spark SQL
-- has no UUID type. Spark accepts the string representation of a UUID when inserting.
INSERT INTO default.bucket_partitioned_uuid
VALUES
    (1,  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
    (2,  'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'),
    (3,  'c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33'),
    (4,  'd3eebc99-9c0b-4ef8-bb6d-6bb9bd380a44'),
    (5,  'e4eebc99-9c0b-4ef8-bb6d-6bb9bd380a55'),
    (6,  'f5eebc99-9c0b-4ef8-bb6d-6bb9bd380a66'),
    (7,  'a6eebc99-9c0b-4ef8-bb6d-6bb9bd380a77'),
    (8,  'b7eebc99-9c0b-4ef8-bb6d-6bb9bd380a88'),
    (9,  'c8eebc99-9c0b-4ef8-bb6d-6bb9bd380a99'),
    (10, 'd9eebc99-9c0b-4ef8-bb6d-6bb9bd380aaa'),
    (11, 'f79c3e09-677c-4bbd-a479-3f349cb785e7'),
    (12, '00000000-0000-0000-0000-000000000000'),
    (13, 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
    (14, NULL)
