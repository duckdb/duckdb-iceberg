INSERT INTO default.schema_evolve_struct VALUES
	(6, NAMED_STRUCT(
		'given_name', 'Frank',
		'last_name', 'Miller',
		'email', 'frank@example.com',
		'age', 42),
		ARRAY(500, 501),
		MAP('setting', 324, 'value', 167, 'value_importance', 2, 'value_updated_at', 123213)
	);