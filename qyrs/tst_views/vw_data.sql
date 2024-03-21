CREATE MATERIALIZED VIEW cftc.vw_data
TABLESPACE pg_default
AS
 WITH cot_raw AS (
         SELECT dat.px_id,
            dat.px_date,
                CASE
                    WHEN dat.px_date < mult.adj_date THEN dat.qty / mult.adj_factor::numeric
                    ELSE dat.qty
                END AS qty,
            cd.bb_tkr,
            cd.bb_ykey,
            cd.cot_type
           FROM cftc.data dat
             LEFT JOIN cftc.cot_desc cd ON dat.px_id = cd.cot_id
             LEFT JOIN cftc.fut_mult mult ON cd.bb_tkr::text = mult.bb_tkr::text AND cd.bb_ykey::text = mult.bb_ykey::text
        )
 SELECT cot_raw.px_id,
    cot_raw.px_date,
    cot_raw.qty
   FROM cot_raw
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    dat1.qty - dat2.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'net_managed_money'::text AND dat1.cot_type::text = 'net_non_commercials'::text AND cd.cot_type::text = 'net_other_reportables'::text
UNION ALL
 SELECT cd2.px_id,
    dat.px_date,
    dat.qty * mult.multiplier::numeric AS qty
   FROM cftc.data dat
     LEFT JOIN cftc.fut_desc cd ON dat.px_id = cd.px_id
     LEFT JOIN cftc.fut_desc cd2 ON cd.bb_tkr::text = cd2.bb_tkr::text AND cd.bb_ykey::text = cd2.bb_ykey::text
     LEFT JOIN cftc.fut_mult mult ON cd.bb_tkr::text = mult.bb_tkr::text AND cd.bb_ykey::text = mult.bb_ykey::text
  WHERE cd.roll::text = 'active_futures'::text AND cd.adjustment::text = 'none'::text AND cd.data_type::text = 'px_last'::text AND cd2.data_type::text = 'contract_size'::text AND cd2.roll::text = 'active_futures'::text AND cd2.adjustment::text = 'none'::text
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    dat1.qty + dat2.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'net_swap'::text AND dat1.cot_type::text = 'net_pump'::text AND cd.cot_type::text = 'net_commercials'::text AND (cd.bb_tkr::text = ANY (ARRAY['CO'::character varying::text, 'QS'::character varying::text]))
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    dat1.qty + dat2.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'long_swap'::text AND dat1.cot_type::text = 'long_pump'::text AND cd.cot_type::text = 'long_commercials'::text AND (cd.bb_tkr::text = ANY (ARRAY['CO'::character varying::text, 'QS'::character varying::text]))
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    dat1.qty + dat2.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'short_swap'::text AND dat1.cot_type::text = 'short_pump'::text AND cd.cot_type::text = 'short_commercials'::text AND (cd.bb_tkr::text = ANY (ARRAY['CO'::character varying::text, 'QS'::character varying::text]))
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    (- dat1.qty) - dat2.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'net_commercials'::text AND dat1.cot_type::text = 'net_non_commercials'::text AND cd.cot_type::text = 'net_non_reportables'::text
UNION ALL
 SELECT cd.cot_id AS px_id,
    dat1.px_date,
    (- dat1.qty) - dat2.qty - dat3.qty AS qty
   FROM cot_raw dat2
     LEFT JOIN cot_raw dat1 ON dat2.px_date = dat1.px_date AND dat2.bb_tkr::text = dat1.bb_tkr::text AND dat2.bb_ykey::text = dat1.bb_ykey::text
     LEFT JOIN cot_raw dat3 ON dat2.px_date = dat3.px_date AND dat2.bb_tkr::text = dat3.bb_tkr::text AND dat2.bb_ykey::text = dat3.bb_ykey::text
     LEFT JOIN cftc.cot_desc cd ON dat1.bb_tkr::text = cd.bb_tkr::text AND dat1.bb_ykey::text = cd.bb_ykey::text
  WHERE dat2.cot_type::text = 'net_swap'::text AND dat1.cot_type::text = 'net_pump'::text AND dat3.cot_type::text = 'net_non_commercials'::text AND cd.cot_type::text = 'net_non_reportables'::text AND (cd.bb_tkr::text = ANY (ARRAY['CO'::character varying::text, 'QS'::character varying::text]))
WITH DATA;

ALTER TABLE cftc.vw_data
    OWNER TO bood;