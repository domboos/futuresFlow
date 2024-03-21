CREATE MATERIALIZED VIEW cftc.vw_beta_rank
TABLESPACE pg_default
AS
 SELECT vw_beta.px_date,
    vw_beta.model_id,
    vw_beta.return_lag,
    vw_beta.qty,
    rank() OVER (PARTITION BY vw_beta.px_date, vw_beta.model_id ORDER BY vw_beta.qty DESC) AS beta_rank
   FROM cftc.vw_beta
WITH DATA;

ALTER TABLE cftc.vw_beta_rank
    OWNER TO bood;