INSERT INTO default.schema_evolve_struct VALUES
	(1, NAMED_STRUCT(
		'first_name', 'Alice',
		'last_name', 'Smith'),
		ARRAY(21, 42),
		MAP('theme', 131, 'language', 115)
	),
	(2, NAMED_STRUCT(
		'first_name', 'Bob',
		'last_name', 'Jones'),
		ARRAY(13, 24),
		MAP('theme', 1, 'language', 1337)
	);