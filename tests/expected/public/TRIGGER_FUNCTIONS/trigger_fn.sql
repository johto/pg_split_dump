CREATE FUNCTION public.trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN NEW;
END
$$;

ALTER FUNCTION public.trigger_fn() OWNER TO postgres;

