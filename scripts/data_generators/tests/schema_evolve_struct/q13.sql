INSERT INTO default.schema_evolve_struct VALUES
	(8, NAMED_STRUCT(
		'given_name', 'Heidi',
		'email', 'heidi@example.com',
		'age', 30),
		ARRAY(1, 2, 3, 4, 5),
		MAP('setting', 245, 'value', 453, 'value_updated_at', 3253)
	);
