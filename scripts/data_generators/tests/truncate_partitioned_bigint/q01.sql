INSERT INTO default.truncate_partitioned_bigint
VALUES
    (1,  struct(10000000000001), 'row_1'),
    (2,  struct(10000000000002), 'row_2'),
    (3,  struct(10000000000003), 'row_3'),
    (4,  struct(10001000000000), 'row_4'),
    (5,  struct(10001000001000), 'row_5'),
    (6,  struct(10001000002000), 'row_6'),
    (7,  struct(10001000003000), 'row_7'),
    (8,  struct(10002000000000), 'row_8'),
    (9,  struct(10002000001000), 'row_9')
