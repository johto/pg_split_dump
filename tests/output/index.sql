SET client_encoding = 'UTF8';

SET standard_conforming_strings = 'on';

SET check_function_bodies = false;

SELECT pg_catalog.set_config('search_path', '', false);

\ir public/FUNCTIONS/my_default_func.sql
\ir public/TABLES/defaults.sql
\ir public/TABLES/tbl_check_constraints.sql
