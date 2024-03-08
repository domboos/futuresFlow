
Optimized Momentum:
Hier würde ich als ersten Schritt mal 500 (oder so) Aktien nehmen und die Forecasting Regression schätzen (über 5 Jahre). Dann dasselbe nochmals, mit anderen 5 Jahren. Dann die Koeffizienten vergleichen. Das ganze mit
Unadjustierten Renditen  Normalisiert durch die Vola über den ganzen Zeitraum
Normalisiert mit einem einfachen Vol-Schätzer (exponentiell mit 0.99 decay).

## Todos Code/ Analysis and Docs: 
- [x] in-sample pooled OLS Results 
- [ ] in-sample Results Description of pooled OLS
- [x] pooled regularized OLS
- [x] create forecasting routine
- [ ] Create Model robustness test, mincer Zarnowitz and Diebold Mariano Tests 
- [ ] pooled regularized OLS
- [ ] analyze pooled OLS Results
- [ ] demean returns and estimate again
- [ ] reduce number of companies 
- [ ] vol adjusted returns over whole period
- [ ] normalized returns with simple vol estimator with 0.99 decay