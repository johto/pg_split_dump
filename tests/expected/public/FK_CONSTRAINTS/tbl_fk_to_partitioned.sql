ALTER TABLE ONLY public.tbl_fk_to_partitioned
    ADD CONSTRAINT tbl_fk_to_partitioned_id_fkey FOREIGN KEY (id) REFERENCES public.tbl_partitioned(id);

