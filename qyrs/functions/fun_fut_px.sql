CREATE OR REPLACE FUNCTION dbo.fun_fut_px(
	contract_num integer)
    RETURNS TABLE(series_id bigint, px_date date, px_close double precision, expiry date, contract_id bigint)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
      return query

SELECT t1.series_id,
    t1.px_date,
	t1.px_close,
	t1.expiry,
    t1.contract_id
   FROM ( SELECT con.series_id,
            dat.px_date,
            con.expiry,
            row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY con.expiry) AS contract_no,
            dat.px_close,
         	dat.contract_id
           FROM dbo.vw_fut_contract con
             JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
             JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
          WHERE (con.expiry - dat.px_date) >= 0::double precision) t1
    where t1.contract_no = contract_num;
      END;
$BODY$;

ALTER FUNCTION dbo.fun_fut_px(integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_fut_px(integer) TO PUBLIC;