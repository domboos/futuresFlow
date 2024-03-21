CREATE OR REPLACE FUNCTION dbo.fun_actual_tkr(
	bb_root_in character varying,
	noof_tkr integer DEFAULT 2)
    RETURNS TABLE(bb_tkr character varying, ib_tkr character varying, bc_tkr character varying, tkr bigint, first_notice date, last_trade date, ib_root character varying, exchange character varying, exp_month character varying, last_update date, px_close double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 14

AS $BODY$
BEGIN

return query

SELECT c.bb_actual_tkr AS bb_tkr,
c.ib_tkr,
c.bc_tkr::varchar,
c.contract_id as tkr,
c.first_notice,
c.last_trade,
f.ib_root,
f.exchange_ib AS exchange,
CAST(CAST(c.expiry_year as TEXT) || LPAD(c.expiry_month_numeric::text,2,'0') AS VARCHAR(6)) as exp_month,
d.last_update,
d.px_close
FROM dbo.vw_fut_contract as c
INNER JOIN dbo.vw_fut_desc as f on f.series_id = c.series_id
LEFT JOIN dbo.vw_fut_data_last as d on d.contract_id = c.contract_id
WHERE f.bb_root = bb_root_in AND c.first_notice > current_date
ORDER BY c.first_notice
LIMIT (SELECT LENGTH(expiry_months)+noof_tkr FROM dbo.fut_desc WHERE bb_root = bb_root_in);

END;
$BODY$;

ALTER FUNCTION dbo.fun_actual_tkr(character varying, integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_actual_tkr(character varying, integer) TO PUBLIC;