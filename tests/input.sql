CREATE TABLE tbl_check_constraints(
    a integer CHECK (a > 0),
    b integer,
    CONSTRAINT a_b CHECK (a > b)
);
