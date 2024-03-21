CREATE OR REPLACE FUNCTION dbo.fun_fut_cmat_log_px(
	cmat_days integer)
    RETURNS TABLE(series_id bigint, px_date date, px_log_cmat double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
      return query
      select
t2.series_id,
t2.px_date,
sum(t2.px_log * t2.px_weight) as px_log_cmat
from
			(SELECT t1.series_id,
            t1.px_date,
            t1.days_below_cmat_min,
            t1.days_above_cmat_min,
            t1.px_log,
            COALESCE(1::double precision - (GREATEST(t1.days_below_cmat, t1.days_above_cmat) - 1)::double precision / NULLIF(t1.days_below_cmat_min + t1.days_above_cmat_min - 2, 0)::double precision, 1::double precision) AS px_weight
           FROM ( SELECT con.series_id,
                    con.contract_id,
                    dat.px_date,
                    con.expiry - dat.px_date AS days_to_expiry,
                    NULLIF(GREATEST(cmat_days + 1 - (con.expiry - dat.px_date), 0), 0) AS days_below_cmat,
                    NULLIF(GREATEST(con.expiry - dat.px_date - cmat_days + 1, 0), 0) AS days_above_cmat,
                    min(NULLIF(GREATEST(cmat_days + 1 - (con.expiry - dat.px_date), 0), 0)) OVER (PARTITION BY con.series_id, dat.px_date) AS days_below_cmat_min,
                    min(NULLIF(GREATEST(con.expiry - dat.px_date - cmat_days + 1, 0), 0)) OVER (PARTITION BY con.series_id, dat.px_date) AS days_above_cmat_min,
                    ln(NULLIF(GREATEST(dat.px_close, 0::double precision), 0::double precision)) AS px_log
                   FROM dbo.vw_fut_contract con
                     JOIN dbo.vw_fut_data dat ON con.contract_id = dat.contract_id
                  WHERE con.expiry >= dat.px_date) t1
          WHERE t1.days_below_cmat = t1.days_below_cmat_min OR t1.days_above_cmat = t1.days_above_cmat_min) t2
          WHERE t2.days_below_cmat_min IS NOT NULL AND t2.days_above_cmat_min IS NOT NULL
          GROUP BY t2.series_id, t2.px_date;
    END;
$BODY$;

ALTER FUNCTION dbo.fun_fut_cmat_log_px(integer)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.fun_fut_cmat_log_px(integer) TO PUBLIC;