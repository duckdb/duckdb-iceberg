INSERT INTO default.schema_evolve_struct VALUES
	(7, NAMED_STRUCT(
		'given_name', 'Grace',
		'last_name', 'Hopper',
		'email', 'grace@example.com',
		'age', 9223372036854775806),
		ARRAY(300, 9223372036854775806, 1000),
		MAP('setting', 9223372036854775806, 'negative', -9223372036854775807)
	);
