CREATE TABLE public.tbl_check_constraints (
    a integer,
    b integer,
    CONSTRAINT a_b CHECK ((a > b)),
    CONSTRAINT tbl_check_constraints_a_check CHECK ((a > 0))
);

ALTER TABLE public.tbl_check_constraints OWNER TO postgres;

