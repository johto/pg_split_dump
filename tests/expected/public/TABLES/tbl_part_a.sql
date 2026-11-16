CREATE TABLE public.tbl_part_a (
{%- if PG_MAJOR >= 18 %}
    id integer CONSTRAINT tbl_partitioned_id_not_null NOT NULL,
{%- else %}
    id integer NOT NULL,
{%- endif %}
    val integer
);

ALTER TABLE public.tbl_part_a OWNER TO postgres;

ALTER TABLE ONLY public.tbl_part_a
    ADD CONSTRAINT tbl_part_a_pkey PRIMARY KEY (id);

CREATE INDEX tbl_part_a_val_idx ON public.tbl_part_a USING btree (val);

