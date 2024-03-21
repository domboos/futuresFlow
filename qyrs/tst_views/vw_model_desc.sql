CREATE MATERIALIZED VIEW cftc.vw_model_desc
TABLESPACE pg_default
AS
 WITH h AS (
         SELECT forecast.model_id,
            max(forecast.px_date) AS max_date
           FROM cftc.forecast
          GROUP BY forecast.model_id
        )
 SELECT model.model_id,
    model.bb_tkr,
    model.bb_ykey,
    model.cot_type,
    model.cot_norm,
    model.est_window,
    model.lookback,
    model.diff,
    model.decay,
    model.gamma_type,
    model.gamma_para,
    model.naildown_value0,
    model.naildown_value1,
    model.regularization,
    model.alpha_type,
    model.alpha,
    model.fit_lag,
    h.max_date
   FROM cftc.model_desc model
     LEFT JOIN h ON model.model_id = h.model_id
WITH DATA;

ALTER TABLE cftc.vw_model_desc
    OWNER TO bood;