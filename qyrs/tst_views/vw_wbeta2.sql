CREATE MATERIALIZED VIEW cftc.vw_wbeta2
TABLESPACE pg_default
AS
 SELECT vw_beta.px_date,
    vw_beta.model_id,
    sum(vw_beta.qty * vw_beta.return_lag::numeric) / sum(abs(vw_beta.qty)) AS average
   FROM cftc.vw_beta
  GROUP BY vw_beta.px_date, vw_beta.model_id
WITH DATA;

ALTER TABLE cftc.vw_wbeta2
    OWNER TO bood;