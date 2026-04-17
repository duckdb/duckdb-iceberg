INSERT INTO default.filtering_on_bounds_nested
VALUES 
    (
        1,
        struct(1, X'deadbeef', 1.0),
        struct(struct(1, X'deadbeef', 1.0)),
        array(struct(1, X'deadbeef', 1.0)),
        array(struct(struct(1, X'deadbeef', 1.0))),
        map(1, X'deadbeef'),
        map(1, struct(1, X'deadbeef', 1.0))
    )
;