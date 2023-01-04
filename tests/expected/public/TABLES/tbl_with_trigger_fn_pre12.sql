-- !! VER < 12
-- !! LOC public/TABLES/tbl_with_trigger_fn.sql
CREATE TABLE public.tbl_with_trigger_fn (
);

ALTER TABLE public.tbl_with_trigger_fn OWNER TO postgres;

CREATE TRIGGER trigger AFTER DELETE ON public.tbl_with_trigger_fn FOR EACH ROW EXECUTE PROCEDURE public.trigger_fn();

