INSERT INTO default.schema_evolve_struct VALUES
    (3, NAMED_STRUCT(
        'first_name', 'Charlie',
        'last_name', 'Brown',
        'email', 'charlie@example.com'),
        ARRAY(34, 65),
        MAP('theme', 104, 'theme_importance', 3, 'language', 13, 'language_importance', 2)
    ),
    (4, NAMED_STRUCT(
        'first_name', 'Diana',
        'last_name', 'Prince',
        'email', 'diana@example.com'),
        ARRAY(45, 93),
        MAP('theme', 12, 'theme_importance', 1, 'language', 213, 'language_importance', 3)
    );