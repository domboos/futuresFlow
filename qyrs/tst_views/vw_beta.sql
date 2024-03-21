CREATE MATERIALIZED VIEW cftc.vw_beta
TABLESPACE pg_default
AS
 SELECT beta.model_id,
    beta.px_date,
    beta.return_lag,
    beta.qty
   FROM cftc.beta
  WHERE (beta.model_id IN ( SELECT model_desc.model_id
           FROM cftc.model_desc
          WHERE model_desc.model_type_id = ANY (ARRAY[76::bigint, 82::bigint, 95::bigint, 100::bigint])))
WITH DATA;

ALTER TABLE cftc.vw_beta
    OWNER TO bood;