SET client_encoding = 'UTF8';

SET standard_conforming_strings = 'on';

SET check_function_bodies = false;

SELECT pg_catalog.set_config('search_path', '', false);

\ir public/TRIGGER_FUNCTIONS/trigger_fn.sql
\ir public/TABLES/tbl_check_constraints.sql
\ir public/TABLES/tbl_fk_to_partitioned.sql
\ir public/TABLES/tbl_partitioned.sql
\ir public/TABLES/tbl_part_a.sql
\ir public/TABLES/tbl_partitioned_b.sql
\ir public/TABLES/tbl_with_trigger_fn.sql
\ir public/PARTITION_ATTACHES/tbl_part_a.sql
\ir public/PARTITION_ATTACHES/tbl_partitioned_b.sql
\ir public/INDEX_ATTACHES/tbl_part_a_pkey.sql
\ir public/INDEX_ATTACHES/tbl_part_a_val_idx.sql
\ir public/INDEX_ATTACHES/tbl_partitioned_b_pkey.sql
\ir public/INDEX_ATTACHES/tbl_partitioned_b_val_idx.sql
\ir public/FK_CONSTRAINTS/tbl_fk_to_partitioned.sql
