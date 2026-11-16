CREATE TABLE public.tbl_part_a (
    id integer NOT NULL,
    val integer
);

ALTER TABLE public.tbl_part_a OWNER TO postgres;

ALTER TABLE ONLY public.tbl_part_a
    ADD CONSTRAINT tbl_part_a_pkey PRIMARY KEY (id);

CREATE INDEX tbl_part_a_val_idx ON public.tbl_part_a USING btree (val);

