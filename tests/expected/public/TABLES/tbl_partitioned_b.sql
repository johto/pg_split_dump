CREATE TABLE public.tbl_partitioned_b (
{%- if PG_MAJOR >= 18 %}
    id integer CONSTRAINT tbl_partitioned_id_not_null NOT NULL,
{%- else %}
    id integer NOT NULL,
{%- endif %}
    val integer
);

ALTER TABLE public.tbl_partitioned_b OWNER TO postgres;

ALTER TABLE ONLY public.tbl_partitioned_b
    ADD CONSTRAINT tbl_partitioned_b_pkey PRIMARY KEY (id);

CREATE INDEX tbl_partitioned_b_val_idx ON public.tbl_partitioned_b USING btree (val);

