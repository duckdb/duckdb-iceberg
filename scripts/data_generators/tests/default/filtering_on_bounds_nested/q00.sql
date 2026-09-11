CREATE or REPLACE TABLE default.filtering_on_bounds_nested (
    int integer,
    struct struct<
        int integer,
        bytes binary,
        double double
    >,
    deepstruct struct<
        struct struct<
            int integer,
            bytes binary,
            double double
        >
    >,
    list array<
        struct<
            int integer,
            bytes binary,
            double double
        >
    >,
    deeplist array<
        struct<
            struct struct<
                int integer,
                bytes binary,
                double double
            >
        >
    >,
    map map<
        integer,
        binary
    >,
    deepmap map<
        integer,
        struct<
            int integer,
            bytes binary,
            double double
        >
    >
)
USING ICEBERG;
