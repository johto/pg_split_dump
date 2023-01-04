-- !! VER >= 12
CREATE TABLE public.tbl_with_trigger_fn (
);

ALTER TABLE public.tbl_with_trigger_fn OWNER TO postgres;

CREATE TRIGGER trigger AFTER DELETE ON public.tbl_with_trigger_fn FOR EACH ROW EXECUTE FUNCTION public.trigger_fn();

