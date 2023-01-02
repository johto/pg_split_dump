CREATE TABLE tbl_check_constraints(
    a integer CHECK (a > 0),
    b integer,
    CONSTRAINT a_b CHECK (a > b)
);

CREATE FUNCTION my_default_func()
RETURNS integer
IMMUTABLE
LANGUAGE sql
AS $$
SELECT 4
$$;

CREATE SEQUENCE custom_default_seq;

CREATE TABLE defaults(
    a integer DEFAULT 0,
    b integer DEFAULT my_default_func(),
    -- implicit DEFAULT
    c serial,
    d integer DEFAULT nextval('custom_default_seq')
);
