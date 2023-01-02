SET client_encoding = 'UTF8';

SET standard_conforming_strings = 'on';

SET check_function_bodies = false;

SELECT pg_catalog.set_config('search_path', '', false);

\ir public/TRIGGER_FUNCTIONS/trigger_fn.sql
\ir public/TABLES/tbl_check_constraints.sql
\ir public/TABLES/tbl_with_trigger_fn.sql
