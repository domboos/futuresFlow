CREATE MATERIALIZED VIEW cftc.vw_return_w
TABLESPACE pg_default
AS
 WITH hh AS (
         WITH __data AS (
                 SELECT data.px_id,
                    data.px_date,
                    data.qty,
                    to_char(data.px_date::timestamp with time zone, 'Dy'::text) AS to_char
                   FROM cftc.data
                  WHERE to_char(data.px_date::timestamp with time zone, 'Dy'::text) = 'Tue'::text
                )
         SELECT d1.bb_tkr,
            e1.px_date,
            log(e1.qty / lag(e1.qty, 1) OVER (PARTITION BY e1.px_id ORDER BY e1.px_date)) AS return1,
            log(e2.qty / lag(e2.qty, 1) OVER (PARTITION BY e2.px_id ORDER BY e2.px_date)) AS return2,
            (ot.noof_active_contracts / 12)::numeric * log(e1.qty / lag(e1.qty, 1) OVER (PARTITION BY e1.px_id ORDER BY e1.px_date)) AS return1_wgt,
            (ot.noof_active_contracts / 12)::numeric * log(e2.qty / lag(e2.qty, 1) OVER (PARTITION BY e2.px_id ORDER BY e2.px_date)) AS return2_wgt
           FROM __data e1
             JOIN __data e2 ON e1.px_date = e2.px_date
             JOIN cftc.fut_desc d1 ON e1.px_id = d1.px_id
             JOIN cftc.fut_desc d2 ON e2.px_id = d2.px_id AND d1.bb_tkr::text = d2.bb_tkr::text
             JOIN cftc.order_of_things ot ON d1.bb_tkr::text = ot.bb_tkr::text
          WHERE d1.data_type::text = 'px_last'::text AND d1.adjustment::text = 'by_ratio'::text AND d1.roll::text = 'active_futures'::text AND d2.data_type::text = 'px_last'::text AND d2.adjustment::text = 'by_ratio'::text AND d2.roll::text = 'active_futures_2'::text
        )
 SELECT hh.bb_tkr,
    hh.px_date,
    hh.return1,
    hh.return2,
    avg(hh.return1) OVER (PARTITION BY hh.px_date) AS avg_return1,
    avg(hh.return2) OVER (PARTITION BY hh.px_date) AS avg_return2,
    hh.return1 - avg(hh.return1) OVER (PARTITION BY hh.px_date) AS rel_return,
    hh.return1 - hh.return2 AS carry_return,
    hh.return1_wgt - hh.return2_wgt - (avg(hh.return1_wgt) OVER (PARTITION BY hh.px_date) - avg(hh.return2_wgt) OVER (PARTITION BY hh.px_date)) AS rel_carry_return
   FROM hh
  WHERE hh.px_date > '1997-12-31'::date AND hh.return1 IS NOT NULL
WITH DATA;

ALTER TABLE cftc.vw_return_w
    OWNER TO bood;