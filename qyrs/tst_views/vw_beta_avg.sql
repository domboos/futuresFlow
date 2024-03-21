CREATE MATERIALIZED VIEW cftc.vw_beta_avg
TABLESPACE pg_default
AS
 SELECT vw_beta.px_date,
    vw_beta.model_id,
    avg(vw_beta.qty) AS average
   FROM cftc.vw_beta
  GROUP BY vw_beta.px_date, vw_beta.model_id
WITH DATA;

ALTER TABLE cftc.vw_beta_avg
    OWNER TO bood;