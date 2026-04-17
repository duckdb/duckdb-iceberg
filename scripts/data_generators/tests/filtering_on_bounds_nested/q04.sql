INSERT INTO default.filtering_on_bounds_nested
VALUES
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