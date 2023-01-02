CREATE FUNCTION public.my_default_func() RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT 4
$$;

ALTER FUNCTION public.my_default_func() OWNER TO postgres;

