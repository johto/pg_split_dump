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
