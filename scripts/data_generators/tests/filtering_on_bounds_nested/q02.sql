INSERT INTO default.filtering_on_bounds_nested
VALUES 
    (
        3,
        struct(3, X'deadbef1', NULL),
        struct(struct(3, X'deadbef1', NULL)),
        array(struct(3, X'deadbef1', 3.0)),
        array(struct(struct(3, X'deadbef1', 3.0))),
        map(3, X'deadbef1'),
        map(3, struct(3, NULL, 3.0))
    ),
    (
        4,
        struct(4, X'deadbef2', 4.0),
        struct(struct(4, X'deadbef2', 4.0)),
        array(struct(4, X'deadbef2', 4.0)),
        array(struct(struct(4, X'deadbef2', 4.0))),
        map(4, X'deadbef2'),
        map(4, struct(4, X'deadbef2', 4.0))
    )
;