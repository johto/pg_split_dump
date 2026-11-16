CREATE TABLE public.tbl_partitioned (
    id integer NOT NULL,
    val integer
)
PARTITION BY RANGE (id);

ALTER TABLE public.tbl_partitioned OWNER TO postgres;

ALTER TABLE ONLY public.tbl_partitioned
    ADD CONSTRAINT tbl_partitioned_pkey PRIMARY KEY (id);

CREATE INDEX tbl_partitioned_val_idx ON ONLY public.tbl_partitioned USING btree (val);

