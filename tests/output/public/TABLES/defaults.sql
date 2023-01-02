CREATE TABLE public.defaults (
    a integer DEFAULT 0,
    b integer DEFAULT public.my_default_func()
);

ALTER TABLE public.defaults OWNER TO postgres;

