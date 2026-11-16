ALTER TABLE ONLY public.tbl_partitioned ATTACH PARTITION public.tbl_partitioned_b FOR VALUES FROM (100) TO (200);

