CREATE OR REPLACE FUNCTION dbo.fun_fut_rolled_ret(
	contract_num integer)
    RETURNS TABLE(series_id bigint, px_date date, px_close_ret double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
      return query

SELECT t1.series_id,
    t1.px_date,
	t1.px_close_ret
   FROM ( SELECT con.series_id,
            dat.px_date,
            row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY con.expiry) AS contract_no,
            dat.px_close / NULLIF(lag(dat.px_close) OVER (PARTITION BY dat.contract_id ORDER BY dat.px_date), 0::double precision) - 1::double precision AS px_close_ret
           FROM dbo.vw_fut_contract con
             JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
             JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
          WHERE (con.expiry - dat.px_date) >= des.roll_offset) t1
    where t1.contract_no = contract_num;
      END;
$BODY$;

ALTER FUNCTION dbo.fun_fut_rolled_ret(integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_fut_rolled_ret(integer) TO PUBLIC;