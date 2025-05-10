INSERT INTO default.schema_evolve_struct VALUES
	(5, NAMED_STRUCT(
		'given_name', 'Eve',
		'last_name', 'Doe',
		'email', 'eve@example.com'),
		ARRAY(200, 300),
		MAP('setting', 234, 'value', 235, 'value_importance', 2)
	);