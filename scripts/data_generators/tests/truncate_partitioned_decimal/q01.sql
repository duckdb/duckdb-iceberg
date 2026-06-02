INSERT INTO default.truncate_partitioned_decimal
VALUES
    (1,  struct('10.65'::DECIMAL(10,2)), 'tiger_owl_horse'),
    (2,  struct('10.49'::DECIMAL(10,2)), 'tiger_owl_frog'),
    (3,  struct('10.75'::DECIMAL(10,2)), 'tiger_owl_goose'),
    (4,  struct('10.1'::DECIMAL(10,2)), 'happy_fun_times'),
    (5, NULL,  'null_row')
