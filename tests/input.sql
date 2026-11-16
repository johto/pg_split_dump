CREATE FUNCTION trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
RETURN NEW;
END
$$;

CREATE TABLE tbl_with_trigger_fn(
);

CREATE TRIGGER trigger
AFTER DELETE ON tbl_with_trigger_fn
FOR EACH ROW
EXECUTE FUNCTION trigger_fn();

CREATE TABLE tbl_check_constraints(
    a integer CHECK (a > 0),
    b integer,
    CONSTRAINT a_b CHECK (a > b)
);

CREATE TABLE tbl_partitioned(
    id integer NOT NULL,
    val integer,
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE INDEX ON tbl_partitioned (val);

CREATE TABLE tbl_part_a PARTITION OF tbl_partitioned FOR VALUES FROM (0) TO (100);

CREATE TABLE tbl_partitioned_b PARTITION OF tbl_partitioned FOR VALUES FROM (100) TO (200);

CREATE TABLE tbl_fk_to_partitioned(
    id integer REFERENCES tbl_partitioned
);
