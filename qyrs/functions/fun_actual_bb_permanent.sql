CREATE OR REPLACE FUNCTION dbo.fun_actual_bb_permanent(
	series_id_in integer)
    RETURNS TABLE(bb_actual_tkr character varying)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 14

AS $BODY$
BEGIN

return query

SELECT c.bb_permanent_tkr FROM dbo.fut_contract as c
INNER JOIN dbo.fut_desc as f on f.series_id = c.series_id
WHERE f.series_id = series_id_in AND c.first_notice > current_date
ORDER BY c.first_notice;

END;
$BODY$;

ALTER FUNCTION dbo.fun_actual_bb_permanent(integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_actual_bb_permanent(integer) TO PUBLIC;