ALTER TABLE ONLY public.tbl_partitioned ATTACH PARTITION public.tbl_part_a FOR VALUES FROM (0) TO (100);

