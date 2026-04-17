INSERT INTO default.filtering_on_bounds_nested
VALUES 
    (
        2,
        struct(2, X'deadbef0', 2.0),
        struct(struct(2, X'deadbef0', 2.0)),
        array(struct(2, X'deadbef0', 2.0)),
        array(struct(struct(2, X'deadbef0', 2.0))),
        map(2, X'deadbef0'),
        map(2, struct(2, X'deadbef0', 2.0))
    )
;