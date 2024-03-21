CREATE OR REPLACE FUNCTION dbo.fun_seasprd(
	series_id_in integer,
	start_dt date DEFAULT '2009-12-31'::date)
    RETURNS TABLE(expiry_month character varying, expiry_month_numeric integer, lb double precision, avg double precision, ub double precision, count_ bigint)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 6

AS $BODY$
BEGIN

return query

WITH aa AS (
SELECT  c.expiry_month, c.expiry_month_numeric,
-- d.px_date, ln(d.px_close), p.px_log_avg, c.first_notice, c.first_notice - d.px_date as term,
-- ((c.first_notice - d.px_date)::double precision / 365 ) -0.6 as h, s.px_log_slope,
ln(d.px_close) - p.px_log_avg - (((c.first_notice - d.px_date)::double precision / 365) -0.6) * s.px_log_slope as px_adj
FROM dbo.vw_fut_data d
INNER JOIN dbo.vw_fut_contract c ON c.contract_id = d.contract_id
INNER JOIN dbo.vw_fut_avg_log_px_all p ON p.px_date = d.px_date AND p.series_id = c.series_id
INNER JOIN dbo.vw_fut_slope_log_px_all s ON s.px_date = d.px_date AND s.series_id = c.series_id
WHERE c.series_id = series_id_in and d.px_date > start_dt AND c.first_notice - d.px_date < 456 -- (365+91)
AND c.first_notice - d.px_date > 0
)

SELECT aa.expiry_month::varchar, aa.expiry_month_numeric::integer, percentile_disc(0.05) within group (order by px_adj) lb,
AVG(px_adj) avg, percentile_disc(0.95) within group (order by px_adj) ub, COUNT(px_adj)::bigint as count_ FROM aa
GROUP BY aa.expiry_month, aa.expiry_month_numeric
ORDER BY aa.expiry_month;

END;
$BODY$;

ALTER FUNCTION dbo.fun_seasprd(integer, date)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_seasprd(integer, date) TO PUBLIC;