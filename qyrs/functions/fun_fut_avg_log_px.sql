CREATE OR REPLACE FUNCTION dbo.fun_fut_avg_log_px(
	interp boolean)
    RETURNS TABLE(series_id bigint, px_date date, px_log_avg double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN

if interp then

return query

select t5.series_id,
	t5.px_date,
    t5.px_log_avg
from (
    SELECT t2.series_id,
    t2.px_date,
    sum(t2.px_log * t2.weight) AS px_log_avg,
    count(t2.px_log) = max(t2.num_expiry_months + 1) as check_count,
    bool_and(t2.check_month) as check_month
from (
SELECT t1.series_id,
    t1.px_date,
    t1.px_log,
    case when t1.contract_no = 1 then t1.interp_weight
    	when t1.contract_no = t1.num_expiry_months + 1 then 1::double precision / t1.num_expiry_months::double precision - t1.interp_weight
        else 1::double precision / t1.num_expiry_months::double precision end as weight,
    t1.num_expiry_months,
    first_value(t1.expiry_month) over (partition by t1.series_id, t1.px_date order by t1.contract_no) =
    last_value(t1.expiry_month) over (partition by t1.series_id, t1.px_date order by t1.contract_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as check_month
   FROM ( SELECT con.series_id,
            dat.px_date,
         	con.expiry_month,
    		des.num_expiry_months,
         	least(con.last_trade, con.first_delivery) - dat.px_date as days_to_maturity,
         	first_value(least(con.last_trade, con.first_delivery) - dat.px_date) over (partition by con.series_id, dat.px_date order by least(con.last_trade, con.first_delivery))::double precision / 365::double precision as interp_weight,
            row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY least(con.last_trade, con.first_delivery)) AS contract_no,
            ln(NULLIF(GREATEST(dat.px_close, 0::double precision), 0::double precision)) AS px_log
           FROM dbo.vw_fut_contract con
             JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
			  JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
          WHERE least(con.last_trade, con.first_delivery) >= dat.px_date
          and string_to_array(des.expiry_months, NULL) @> string_to_array(con.expiry_month, NULL)) t1
  WHERE t1.contract_no <= t1.num_expiry_months + 1) t2
  GROUP BY t2.series_id, t2.px_date) t5
  where t5.check_count
  and t5.check_month;

else

return query

select t4.series_id,
	t4.px_date,
    t4.px_log_avg
from (
    SELECT t3.series_id,
    t3.px_date,
    avg(t3.px_log) AS px_log_avg,
    count(t3.px_log) = max(t3.num_expiry_months) as check_flag
   FROM ( SELECT con.series_id,
            dat.px_date,
         des.num_expiry_months,
             row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY least(con.last_trade, con.first_delivery)) AS contract_no,
            ln(NULLIF(GREATEST(dat.px_close, 0::double precision), 0::double precision)) AS px_log
           FROM dbo.vw_fut_contract con
             JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
			JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
          WHERE least(con.last_trade, con.first_delivery) >= dat.px_date
            and string_to_array(des.expiry_months, NULL) @> string_to_array(con.expiry_month, NULL)) t3
  WHERE t3.contract_no <= t3.num_expiry_months
  GROUP BY t3.series_id, t3.px_date) t4
  where t4.check_flag;

end if;

END;
$BODY$;

ALTER FUNCTION dbo.fun_fut_avg_log_px(boolean)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_fut_avg_log_px(boolean) TO PUBLIC;