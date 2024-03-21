CREATE OR REPLACE FUNCTION dbo.fun_make_date(
	year integer,
	month integer,
	day integer)
    RETURNS date
    LANGUAGE 'plpgsql'
    COST 4
    VOLATILE STRICT PARALLEL UNSAFE
AS $BODY$
BEGIN
SELECT format('%s-%s-%s'::text, year, month, day)::date;
END;
$BODY$;

ALTER FUNCTION dbo.fun_make_date(integer, integer, integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_make_date(integer, integer, integer) TO PUBLIC;