CREATE OR REPLACE FUNCTION dbo.fun_fut_slope_log_px(
	interp boolean)
    RETURNS TABLE(series_id bigint, px_date date, px_log_slope double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN

if interp then

return query

SELECT t2.series_id,
    t2.px_date,
    t2.px_log_slope
from (
    SELECT t1.series_id,
        t1.px_date,
        count(t1.contract_no) = 4 as check_count,
        sum(t1.px_log * (case when t1.contract_no = 1 then -t1.ratio_dtm
            when t1.contract_no = 2 then t1.ratio_dtm - 1::double precision
            when t1.contract_no = t1.num_expiry_months + 1 then t1.ratio_dtm
            else 1::double precision - (t1.ratio_dtm) end)) AS px_log_slope,
        bool_and(t1.check_month) as check_month
    FROM (
        select t11.series_id,
        t11.px_date,
        t11.contract_no,
        t11.px_log,
        t11.first_dtm / nullif(t11.second_dtm - t11.first_dtm, 0) as ratio_dtm,
        t11.num_expiry_months,
        (first_value(t11.expiry_month) over (partition by t11.series_id, t11.px_date order by t11.contract_no) =
         nth_value(t11.expiry_month, 3) over (partition by t11.series_id, t11.px_date order by t11.contract_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) and
        (nth_value(t11.expiry_month, 2) over (partition by t11.series_id, t11.px_date order by t11.contract_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) =
         nth_value(t11.expiry_month, 4) over (partition by t11.series_id, t11.px_date order by t11.contract_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as check_month
        from (
            SELECT con.series_id,
                dat.px_date,
            	con.expiry_month,
            	des.num_expiry_months,
                (first_value(least(con.last_trade, con.first_delivery) - dat.px_date) over (partition by con.series_id, dat.px_date order by least(con.last_trade, con.first_delivery)))::double precision as first_dtm,
                (nth_value(least(con.last_trade, con.first_delivery) - dat.px_date, 2) over (partition by con.series_id, dat.px_date order by least(con.last_trade, con.first_delivery) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))::double precision as second_dtm,
                row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY least(con.last_trade, con.first_delivery)) AS contract_no,
                ln(NULLIF(GREATEST(dat.px_close, 0::double precision), 0::double precision)) AS px_log
           	FROM dbo.vw_fut_contract con
            JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
            JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
            WHERE least(con.last_trade, con.first_delivery) >= dat.px_date
        	and string_to_array(des.expiry_months, NULL) @> string_to_array(con.expiry_month, NULL)) t11
      WHERE t11.contract_no in (1, 2, t11.num_expiry_months + 1, t11.num_expiry_months + 2) ) t1
    GROUP BY t1.series_id, t1.px_date) t2
where t2.check_count
  and t2.check_month;

else

return query

Select t4.series_id,
	t4.px_date,
    t4.px_log_slope
from (
    SELECT t3.series_id,
        t3.px_date,
        count(t3.contract_no) = 2 as check_count,
        sum(t3.px_log * (case when t3.contract_no = 1 then -1::double precision else 1::double precision end)) AS px_log_slope,
        bool_and(t3.check_month) as check_month
    from (
       	select t33.series_id,
            t33.px_date,
            t33.contract_no,
            t33.px_log,
            first_value(t33.expiry_month) over (partition by t33.series_id, t33.px_date order by t33.contract_no) =
            last_value(t33.expiry_month) over (partition by t33.series_id, t33.px_date order by t33.contract_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as check_month
       	FROM (
            SELECT con.series_id,
                dat.px_date,
                con.expiry_month,
            	des.num_expiry_months,
                row_number() OVER (PARTITION BY con.series_id, dat.px_date ORDER BY least(con.last_trade, con.first_delivery)) AS contract_no,
                ln(NULLIF(GREATEST(dat.px_close, 0::double precision), 0::double precision)) AS px_log
             FROM dbo.vw_fut_contract con
             JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
             JOIN dbo.vw_fut_desc des ON con.series_id = des.series_id
             WHERE least(con.last_trade, con.first_delivery) >= dat.px_date
        	and string_to_array(des.expiry_months, NULL) @> string_to_array(con.expiry_month, NULL)) t33
	    WHERE t33.contract_no in (1, t33.num_expiry_months + 1) ) t3
	  GROUP BY t3.series_id, t3.px_date) t4
  where t4.check_count
  and t4.check_month;

end if;

END;
$BODY$;

ALTER FUNCTION dbo.fun_fut_slope_log_px(boolean)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_fut_slope_log_px(boolean) TO PUBLIC;