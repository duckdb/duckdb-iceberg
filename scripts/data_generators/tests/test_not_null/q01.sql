INSERT INTO default.test_not_null VALUES (
    1,
    'Alice',
    NAMED_STRUCT('street', '123 Main St', 'city', 'Metropolis', 'zip', '12345'),
    ARRAY('123-456-7890', '987-654-3210'),
    MAP('age', '30', 'membership', 'gold')
);