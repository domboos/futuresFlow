from futures_flow.ridge.ridge_functions import *
from futures_flow.risky_times.seasonalVol import *
from futures_flow.private.engines import *

import pandas as pd
import sqlalchemy as sq
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from statsmodels.nonparametric.kernel_regression import KernelReg
import statsmodels.api as sm
from arch import arch_model

engine1 = dbo_engine()

# define the output frame

crops = ['wheat', 'corn', 'soybean']
oos_years = list(range(2010, 2024))
newyear = 2023
idx = pd.MultiIndex.from_product([crops, oos_years], names=['crop', 'year'])
results_oos = pd.DataFrame(index=idx, columns=['alpha_ols', 'alpha_wls2', 'alpha_wls3', 'r2_garch', 'r2_mcgarch',
                                               'r2_garch_mz', 'r2_mcgarch_mz'])

# get starting values
alpha_method = 'reuse' # 'estimate'
WLS = True
wSmooth = False
OLS = False
rw = True
cv_method = 'gcv'
alpha4_scale = 1 # scales the smoothing parameter in the last step
if cv_method == 'gcv':
    scale = 100000000000000
else:
    scale = 10000000000

path = "C:\\Users\\bood\\switchdrive\\Private\\Artikel\\Risky Times\\Data\\"
file = 'Seasonalvol_CMT.xlsx'
#filea = 'alphas.xlsx'

dummy = pd.read_excel(path + file, 'dummy', usecols="A:B").dropna()
dummy.Dates = pd.to_datetime(dummy.Dates).dt.date
dummy = dummy.set_index('Dates')

lags = 250
model = 1

addon = 0
if model == 0:
    events = ['wasde', 'stock']
elif model == 1:
    events = ['wasde', 'stock', 'cp']
elif model == 2:
    events = ['wasde', 'stock', 'pp_acr']
elif model == 3:
    events = ['wasde', 'stock', 'cp', 'pp_acr']
elif model == 4:
    events = ['wasde', 'cp', 'q1', 'q2', 'q3', 'q4']
elif model == 5:
    events = ['stock', 'cp']
elif model == 6:
    events = ['wasde', 'stock', 'cp', 'stock_1', 'cp_1']
    _shift = 1
    addon = 2
elif model == 7:
    events = ['wasde', 'stock', 'cp', 'stock_1', 'cp_1']
    _shift = -1
    addon = 2
elif model == 8:
    events = ['wasde', 'stock', 'cp', 'lag']
    _shift = 1
    addon = 1
elif model == 9:
    events = []
noof_events = len(events)
noof_events0 = noof_events - addon

_idx = ['o1', 'a1', 'b1', 's1', 'o2', 'a2', 'b2', 's2'] + events
_clm = pd.MultiIndex.from_product([crops, ['para', 'tstat']])
resultsGARCH = pd.DataFrame(index=_idx, columns=_clm)

# WLS
if WLS or OLS:
    alphas = pd.read_excel(path + "Results" + str(model) + "_wls.xlsx", "results", header=0, index_col=[0, 1], usecols="A:E")
    alphas.index.names = ['crop', 'oos_year']
# Weighted Smoothing
if wSmooth:
    alphasW = pd.read_excel(path + "Results" + str(model) + "_wsmooth.xlsx", "results", header=0, index_col=[0, 1], usecols="A:E")
    alphasW.index.names = ['crop', 'oos_year']

for a, b in results_oos.iterrows():
    crop = a[0]
    oos_year = a[1]
    print(crop + ": " + str(oos_year))

    # loading grain data and creating a numpy array with the day ot year
    grains = pd.read_excel(path + file, crop, skiprows=4, usecols="A:C").dropna()
    grains.Dates = pd.to_datetime(grains.Dates).dt.date
    grains = grains.set_index('Dates')
    grains.index = pd.to_datetime(grains.index)
    day_of_year = grains[['day_of_year']].to_numpy().astype(int)
    day_of_year = np.delete(day_of_year, 0, 0) # deletes first value
    print(day_of_year)
    grains.drop('day_of_year', axis=1, inplace=True)

    # calculation quared returns
    sqrt_ret = pd.DataFrame(index=grains.index)
    sqrt_ret.loc[:, 'r2'] = np.log(grains / grains.shift(1)) ** 2
    sqrt_ret = sqrt_ret.dropna()    # deletes first value

    # Garch
    ret = pd.DataFrame(index=grains.index)
    ret.loc[:, 'daily_return'] = np.log(grains / grains.shift(1))
    ret = ret.dropna()    # deletes first value
    garch_model = arch_model(ret.loc[ret.index.year < oos_year, 'daily_return']*100, p=1, q=1, mean='Zero',
                             rescale=False).fit()
    garch_para = garch_model.params[1] * np.power(np.ones(lags) * garch_model.params[2], np.arange(lags))

    # loading reporting dates
    events_df = pd.DataFrame()
    _counter = 0
    for event in events[:noof_events0]:
        _counter = _counter+1
        if event=='wasde':
            _dates = pd.read_excel(path + file, 'Wasde', usecols="A,B")
        elif event=='stock':
            _dates = pd.read_excel(path + file, 'Stock', usecols="A,B")
        elif event=='cp':
            if crop == 'wheat':
                _dates = pd.read_excel(path + file, 'cp_wheat', usecols="A,B")
            else:
                _dates = pd.read_excel(path + file, 'cp_corn_soy', usecols="A,B")
        elif event=='pp_acr':
            if crop=='wheat':
                _dates = pd.read_excel(path + file, 'pp_acr_wheat', usecols="A,B")
            else:
                _dates = pd.read_excel(path + file, 'pp_acr', usecols="A,B")
        elif event=='q1':
            _dates = pd.read_excel(path + file, 'q1', usecols="A,B")
        elif event=='q2':
            _dates = pd.read_excel(path + file, 'q2', usecols="A,B")
        elif event=='q3':
            _dates = pd.read_excel(path + file, 'q3', usecols="A,B")
        elif event=='q4':
            _dates = pd.read_excel(path + file, 'q4', usecols="A,B")
        _dates.Dates = pd.to_datetime(_dates.Dates).dt.date
        _dates = _dates.set_index('Dates')
        _dates.index = pd.to_datetime(_dates.index)
        if _counter == 1:
            events_df = _dates
        else:
            events_df = pd.concat([events_df, _dates], axis=1).fillna(0)
    sqrt_ret = pd.concat([sqrt_ret, events_df], axis=1).fillna(0)

    # now the lags...
    for event in events:
        if event=='wasde_1':
            sqrt_ret['wasde_1'] = sqrt_ret['wasde'].shift(_shift)
        if event == 'stock_1':
            sqrt_ret['stock_1'] = sqrt_ret['stock'].shift(_shift)
        if event == 'cp_1':
            sqrt_ret['cp_1'] = sqrt_ret['cp'].shift(_shift)
        if event == 'lag':
            sqrt_ret['lag'] = sqrt_ret['r2']
            sqrt_ret.loc[~sqrt_ret.index.isin(events_df.index), 'lag'] = 0
            sqrt_ret['lag'] = sqrt_ret['lag'].shift(_shift)
    sqrt_ret.fillna(0, inplace=True)

    # creating gamma, dummy index for seasonal model and numpy array of squared returns
    days = max(day_of_year)[0]
    print(days)
    x_plot = np.linspace(1, days, days)  # [:, np.newaxis]
    _Gamma0 = getGamma(days, gammatype='circ', regularization='d2',)
    Gamma0 = np.concatenate((_Gamma0, np.zeros((days, noof_events))), axis=1)
    idx_seas = (day_of_year == x_plot).astype(np.float32)
    idx_evnt = sqrt_ret.loc[:, events]
    idx = np.concatenate((idx_seas, idx_evnt), axis=1)
    mi1 = pd.MultiIndex.from_product([['seas'], [int(d) for d in x_plot]])
    mi2 = pd.MultiIndex.from_product([['evnt'], events])
    idx_df = pd.DataFrame(index=sqrt_ret.index, columns=mi1, data=idx_seas)
    idx_df[mi2] = idx_evnt
    y_fit = sqrt_ret.loc[sqrt_ret.index.year < oos_year, ['r2']]
    y_oos = sqrt_ret.loc[sqrt_ret.index.year == oos_year, ['r2']]
    x_fit = idx_df.loc[idx_df.index.year < oos_year, :]
    x_oos = idx_df.loc[idx_df.index.year == oos_year, :]
    x_ooss = x_oos.copy()
    if model != 9:
        x_ooss['evnt'] = 0
        x_oose = sm.add_constant(x_oos['evnt'])
        x_fite = sm.add_constant(x_fit['evnt'])
    else:
        x_oose = pd.DataFrame(data=np.ones(y_oos.shape), index=y_oos.index)
        x_fite = pd.DataFrame(data=np.ones(y_fit.shape), index=y_fit.index)

    # OLS with CV
    alpha1 = alphas.loc[a]['alpha_ols']
    if alpha_method == 'estimate':
        alpha1 = getAlpha(alpha_type=cv_method, y=y_fit, x=x_fit, gma=Gamma0, start=alpha1, scale=scale)[0]
    results_oos.loc[a]['alpha_ols'] = alpha1
    y_values = np.concatenate((y_fit, np.zeros((days, 1))), axis=0)
    x_values = np.concatenate((x_fit, Gamma0 * alpha1), axis=0)
    model1 = sm.OLS(y_values, x_values).fit()

    if wSmooth:
        # WLS based on OLS
        p = np.sqrt(model1.params[:-noof_events])
        _Gamma0 = getGamma(days, gammatype='circ', regularization='d2', weights=p/np.mean(p))
        Gamma0 = np.concatenate((_Gamma0, np.zeros((days, noof_events))), axis=1)
        y_values = np.concatenate((y_fit, np.zeros((days, 1))), axis=0)
        x_values = np.concatenate((x_fit, Gamma0 * alpha1), axis=0)
        model2 = sm.OLS(y_values, x_values).fit()

        # iterate WLS
        p = np.sqrt(model2.params[:-noof_events])
        _Gamma0 = getGamma(days, gammatype='circ', regularization='d2', weights=p/np.mean(p))
        Gamma0 = np.concatenate((_Gamma0, np.zeros((days, noof_events))), axis=1)
        alpha3 = alphasW.loc[a]['alpha_wls2']
        if alpha_method == 'estimate':
            alpha3 = getAlpha(alpha_type=cv_method, y=y_fit, x=x_fit, gma=Gamma0, start=alpha3,
                              scale=scale)[0]
        results_oos.loc[a]['alpha_wls2'] = alpha3
        y_values3 = np.concatenate((y_fit, np.zeros((days, 1))), axis=0)
        x_values3 = np.concatenate((x_fit, Gamma0 * alpha3), axis=0)
        model3 = sm.OLS(y_values3, x_values3).fit()

        # 2nd iteration of WLS
        p = np.sqrt(model3.params[:-noof_events])
        _Gamma0 = getGamma(days, gammatype='circ', regularization='d2', weights=p/np.mean(p))
        Gamma0 = np.concatenate((_Gamma0, np.zeros((days, noof_events))), axis=1)
        alpha4 = alphasW.loc[a]['alpha_wls3']
        if alpha_method == 'estimate':
            alpha4 = getAlpha(alpha_type=cv_method, y=y_fit, x=x_fit, gma=Gamma0, start=alpha4, scale=scale)[0]
        results_oos.loc[a]['alpha_wls3'] = alpha4
        y_values4 = np.concatenate((y_fit, np.zeros((days, 1))), axis=0)
        x_values4 = np.concatenate((x_fit, Gamma0 * alpha4), axis=0)
        model4 = sm.OLS(y_values4, x_values4).fit()
    elif WLS:
        # WLS based on OLS
        _predict = model1.predict(x_fit).values
        wls_weights = np.concatenate((np.reshape(_predict, (-1, 1)) / np.mean(_predict), np.ones((days, 1))), axis=0)
        model2 = sm.WLS(y_values, x_values, 1 / (wls_weights * wls_weights)).fit()

        # iterate WLS
        _predict = model2.predict(x_fit).values
        diagW = np.diag(np.mean(_predict) / _predict)
        alpha3 = alphas.loc[a]['alpha_wls2']
        if alpha_method == 'estimate':
            alpha3 = getAlpha(alpha_type=cv_method, y=diagW @ y_fit, x=diagW @ x_fit, gma=Gamma0, start=alpha3,
                              scale=scale)[0]
        results_oos.loc[a]['alpha_wls2'] = alpha3
        y_values3 = np.concatenate((diagW @ y_fit, np.zeros((days, 1))), axis=0)
        x_values3 = np.concatenate((diagW @ x_fit, Gamma0 * alpha3), axis=0)
        model3 = sm.OLS(y_values3, x_values3).fit()

        # 2nd iteration of WLS
        _predict = model3.predict(x_fit).values
        diagW = np.diag(np.mean(_predict) / _predict)
        alpha4 = alphas.loc[a]['alpha_wls3']
        if alpha_method == 'estimate':
            alpha4 = getAlpha(alpha_type=cv_method, y=diagW @ y_fit, x=diagW @ x_fit, gma=Gamma0, start=alpha4,
                              scale=scale)[0]
        results_oos.loc[a]['alpha_wls3'] = alpha4
        y_values4 = np.concatenate((diagW @ y_fit, np.zeros((days, 1))), axis=0)
        x_values4 = np.concatenate((diagW @ x_fit, Gamma0 * alpha4 * alpha4_scale), axis=0)
        model4 = sm.OLS(y_values4, x_values4).fit()
        _model5 = sm.OLS(y_fit, x_fite).fit()
        if model != 9:
            _temp = _model5.predict(x_fite).values
            model5 = sm.WLS(y_fit, x_fite, 1 / (_temp * _temp)).fit()
        else:
            model5 = _model5
    elif OLS: # OLS
        model4 = model1
        model5 = sm.OLS(y_fit, x_fite).fit()

    # FILTERING
    _predict_cldr_fit = model4.predict(x_fit).to_frame(name="daily_return")
    ret_mc_xcldr_fit = ret.loc[ret.index.year < oos_year, :] / _predict_cldr_fit**(1/2)
    mcgarch_xcldr_model = arch_model(ret_mc_xcldr_fit, p=1, q=1, mean='Zero', vol='GARCH', dist='normal').fit()
    mcgarch_xcldr_para = mcgarch_xcldr_model.params[1] * np.power(np.ones(lags) * mcgarch_xcldr_model.params[2],
                                                                  np.arange(lags))

    _predict_evnt_fit = model5.predict(x_fite).to_frame(name="daily_return")
    ret_mc_xevnt_fit = ret.loc[ret.index.year < oos_year, :] / _predict_evnt_fit**(1/2)
    mcgarch_xevnt_model = arch_model(ret_mc_xevnt_fit, p=1, q=1, mean='Zero', vol='GARCH', dist='normal').fit()
    mcgarch_xevnt_para = mcgarch_xevnt_model.params[1] * np.power(np.ones(lags) * mcgarch_xevnt_model.params[2],
                                                                  np.arange(lags))


    # PLOTS
    if oos_year == newyear:
        if crop == 'wheat':
            _i = 1
            _j = 2
            plt.subplots(figsize=(9,6))
        elif crop == 'corn':
            _i = 3
            _j = 4
        else:
            _i = 5
            _j = 6
        plt.subplot(3, 2, _i)
        if model != 9:
            _ols = 100 * np.sqrt(252 * model1.params[:-noof_events])
            _wls = 100 * np.sqrt(252 * model4.params[:-noof_events])
        else:
            _ols = 100 * np.sqrt(252 * model1.params)
            _wls = 100 * np.sqrt(252 * model4.params)
        plt.plot(_ols)
        plt.plot(_wls)
        if model == 1:
            _ols_df = pd.DataFrame(_ols)
            _wls_df = pd.DataFrame(_wls)
            _ols_df.to_excel(path + 'Season_' + crop + '_ols.xlsx', 'ols')
            _wls_df.to_excel(path + 'Season_' + crop + '_wls.xlsx', 'wls')
        #
        resultsGARCH.loc[['o1', 'a1', 'b1'], (crop, 'para')] = garch_model.params.values
        resultsGARCH.loc[['o1', 'a1', 'b1'], (crop, 'tstat')] = garch_model.tvalues.values
        resultsGARCH.loc[['o2', 'a2', 'b2'], (crop, 'para')] = mcgarch_xcldr_model.params.values
        resultsGARCH.loc[['o2', 'a2', 'b2'], (crop, 'tstat')] = mcgarch_xcldr_model.tvalues.values
        resultsGARCH.loc[['s1'], (crop, 'para')] = np.sqrt(252) * np.sqrt(garch_model.params[0] # * (1 - garch_model.para[2])
                                                           / (1 - garch_model.params[1] - garch_model.params[2]))
        resultsGARCH.loc[['s2'], (crop, 'para')] = np.sqrt(mcgarch_xcldr_model.params[0] # * (1 - mcgarch_xcldr_model.para[2])
                                                           / (1 - mcgarch_xcldr_model.params[1] - mcgarch_xcldr_model.params[2]))
        # das ist praktisch identisch zu tstat
        # _tstat = tstat(diagW @ x_fit, diagW @ y_fit, Gamma0 * alpha4, np.reshape(model4.params, (-1, 1)))
        # print(_tstat[-noof_events:])
        resultsGARCH.loc[events, (crop, 'para')] = 100 * np.sqrt(model4.params[-noof_events:])
        resultsGARCH.loc[events, (crop, 'tstat')] = model4.tvalues[-noof_events:]

        plt.subplot(3, 2, _j)
        plt.plot(garch_para[:50])
        plt.plot(mcgarch_xcldr_para[:50])
        plt.plot(mcgarch_xevnt_para[:50])
        #plt.legend(["raw", "add w/o evnt", "mult w/o seas", "mult w/0 cldr"])
    else:
        _predict_garch = garch_prdict(y_fit, y_oos, garch_para, omega=0.0001*garch_model.params[0]/(1-garch_model.params[2]))
        _predict_seas = model4.predict(x_ooss).to_frame(name="daily_return")

        _predict_cldr = model4.predict(x_oos).to_frame(name="daily_return")
        ret_mx_xcldr_oos = ret.loc[ret.index.year == oos_year, :] / _predict_cldr**(1/2)
        _predict_xcldr = garch_prdict(ret_mc_xcldr_fit**2, ret_mx_xcldr_oos**2, mcgarch_xcldr_para,
                                      omega=mcgarch_xcldr_model.params[0]/(1-mcgarch_xcldr_model.params[2]))
        _predict_mcgarch = _predict_cldr * _predict_xcldr

        _predict_evnt = model5.predict(x_oose).to_frame(name="daily_return")
        ret_mx_xevnt_oos = ret.loc[ret.index.year == oos_year, :] / _predict_evnt**(1/2)
        _predict_xevnt = garch_prdict(ret_mc_xevnt_fit**2, ret_mx_xevnt_oos**2, mcgarch_xevnt_para,
                                      omega=mcgarch_xevnt_model.params[0]/(1-mcgarch_xevnt_model.params[2]))
        _predict_mcgarch_evnt = _predict_evnt * _predict_xevnt

        r2_garch = sm.OLS(y_oos, sm.add_constant(_predict_garch)).fit()
        r2_mcgarch = sm.OLS(y_oos, sm.add_constant(_predict_mcgarch)).fit()

        results_oos.loc[a]['r2_garch'] = getR2(y_oos, _predict_garch) * 100
        results_oos.loc[a]['r2_mcgarch'] = getR2(y_oos, _predict_mcgarch) * 100
        results_oos.loc[a]['r2_garch_mz'] = r2_garch.rsquared * 100
        results_oos.loc[a]['r2_mcgarch_mz'] = r2_mcgarch.rsquared * 100

        # stack data
        if oos_year == oos_years[0]:
            if crop == 'corn':
                corn_all_garch = _predict_garch
                corn_all_mcgarch = _predict_mcgarch
                corn_all_cldr = _predict_cldr
                corn_all_seas = _predict_seas
                corn_all_evnt = _predict_evnt
                corn_all_mcevnt = _predict_mcgarch_evnt
                corn_oos = y_oos
            if crop == 'wheat':
                wheat_all_garch = _predict_garch
                wheat_all_mcgarch = _predict_mcgarch
                wheat_all_cldr = _predict_cldr
                wheat_all_seas = _predict_seas
                wheat_all_evnt = _predict_evnt
                wheat_all_mcevnt = _predict_mcgarch_evnt
                wheat_oos = y_oos
            if crop == 'soybean':
                soybean_all_garch = _predict_garch
                soybean_all_mcgarch = _predict_mcgarch
                soybean_all_cldr = _predict_cldr
                soybean_all_seas = _predict_seas
                soybean_all_evnt = _predict_evnt
                soybean_all_mcevnt = _predict_mcgarch_evnt
                soybean_oos = y_oos
        else:
            if crop == 'corn':
                corn_all_garch = pd.concat([corn_all_garch, _predict_garch])
                corn_all_mcgarch = pd.concat([corn_all_mcgarch, _predict_mcgarch])
                corn_all_cldr = pd.concat([corn_all_cldr, _predict_cldr])
                corn_all_seas = pd.concat([corn_all_seas, _predict_seas])
                corn_all_evnt = pd.concat([corn_all_evnt, _predict_evnt])
                corn_all_mcevnt = pd.concat([corn_all_mcevnt, _predict_mcgarch_evnt])
                corn_oos = pd.concat([corn_oos, y_oos])
            if crop == 'wheat':
                wheat_all_garch = pd.concat([wheat_all_garch, _predict_garch])
                wheat_all_mcgarch = pd.concat([wheat_all_mcgarch, _predict_mcgarch])
                wheat_all_cldr = pd.concat([wheat_all_cldr, _predict_cldr])
                wheat_all_seas = pd.concat([wheat_all_seas, _predict_seas])
                wheat_all_evnt = pd.concat([wheat_all_evnt, _predict_evnt])
                wheat_all_mcevnt = pd.concat([wheat_all_mcevnt, _predict_mcgarch_evnt])
                wheat_oos = pd.concat([wheat_oos, y_oos])
            if crop == 'soybean':
                soybean_all_garch = pd.concat([soybean_all_garch, _predict_garch])
                soybean_all_mcgarch = pd.concat([soybean_all_mcgarch, _predict_mcgarch])
                soybean_all_cldr = pd.concat([soybean_all_cldr, _predict_cldr])
                soybean_all_seas = pd.concat([soybean_all_seas, _predict_seas])
                soybean_all_evnt = pd.concat([soybean_all_evnt, _predict_evnt])
                soybean_all_mcevnt = pd.concat([soybean_all_mcevnt, _predict_mcgarch_evnt])
                soybean_oos = pd.concat([soybean_oos, y_oos])

plt.show()
print(results_oos.to_string())
print(results_oos[['r2_garch', 'r2_mcgarch']].to_latex(float_format="{:0.2f}".format))

print(resultsGARCH.to_latex(float_format="{:0.3f}".format))
print(resultsGARCH.to_latex(float_format="{:0.2f}".format))

## Structuring the output and writing stuff to Excel
corn_all_garch.columns = ['garch']
wheat_all_garch.columns = ['garch']
soybean_all_garch.columns = ['garch']

corn_all_mcgarch.columns = ['mcgarch']
wheat_all_mcgarch.columns = ['mcgarch']
soybean_all_mcgarch.columns = ['mcgarch']

corn_all_seas.columns = ['seas']
wheat_all_seas.columns = ['seas']
soybean_all_seas.columns = ['seas']

corn_all_cldr.columns = ['cldr']
wheat_all_cldr.columns = ['cldr']
soybean_all_cldr.columns = ['cldr']

corn_all_evnt.columns = ['evnt']
wheat_all_evnt.columns = ['evnt']
soybean_all_evnt.columns = ['evnt']

corn_oos.columns = ['realized']
wheat_oos.columns = ['realized']
soybean_oos.columns = ['realized']

corn = pd.concat([corn_oos, corn_all_mcgarch, corn_all_garch, corn_all_seas, corn_all_cldr, corn_all_evnt], axis=1)
wheat = pd.concat([wheat_oos, wheat_all_mcgarch, wheat_all_garch, wheat_all_seas, wheat_all_cldr, wheat_all_evnt], axis=1)
soybean = pd.concat([soybean_oos, soybean_all_mcgarch, soybean_all_garch, soybean_all_seas, soybean_all_cldr, soybean_all_evnt], axis=1)

if alpha_method == 'estimate':
    if WLS:
        corn.to_excel(path + 'SeasonalvolC' + str(model) + '_wls.xlsx', 'oos_data')
        wheat.to_excel(path + 'SeasonalvolW' + str(model) + '_wls.xlsx', 'oos_data')
        soybean.to_excel(path + 'SeasonalvolS' + str(model) + '_wls.xlsx', 'oos_data')
        results_oos.to_excel(path + 'Results' + str(model) + '_wls.xlsx', 'results')
    elif wSmooth:
        corn.to_excel(path + 'SeasonalvolC' + str(model) + '_wsmooth.xlsx', 'oos_data')
        wheat.to_excel(path + 'SeasonalvolW' + str(model) + '_wsmooth.xlsx', 'oos_data')
        soybean.to_excel(path + 'SeasonalvolS' + str(model) + '_wsmooth.xlsx', 'oos_data')
        results_oos.to_excel(path + 'Results' + str(model) + '_wsmooth.xlsx', 'results')

# mz-regressions for corn
r2_garch_corn = sm.OLS(corn['realized'], sm.add_constant(corn['garch'])).fit()
r2t_garch_corn = sm.OLS(corn['realized']-corn['garch'].values, sm.add_constant(corn['garch'])).fit()
r2_mcgarch_corn = sm.OLS(corn['realized'], sm.add_constant(corn['mcgarch'])).fit()
r2t_mcgarch_corn = sm.OLS(corn['realized']-corn['mcgarch'].values, sm.add_constant(corn['mcgarch'])).fit()
r2_xxx_corn = sm.OLS(corn['realized'], sm.add_constant(corn[['garch', 'mcgarch']])).fit()
print(r2_garch_corn.summary())
print(r2t_garch_corn.summary())
print(r2_mcgarch_corn.summary())
print(r2t_mcgarch_corn.summary())
print(r2_xxx_corn.summary())

# mz regressions for wheat
r2_garch_wheat = sm.OLS(wheat['realized'], sm.add_constant(wheat['garch'])).fit()
r2t_garch_wheat = sm.OLS(wheat['realized']-wheat['garch'].values, sm.add_constant(wheat['garch'])).fit()
r2_mcgarch_wheat = sm.OLS(wheat['realized'], sm.add_constant(wheat['mcgarch'])).fit()
r2t_mcgarch_wheat = sm.OLS(wheat['realized']-wheat['mcgarch'].values, sm.add_constant(wheat['mcgarch'])).fit()
r2_xxx_wheat = sm.OLS(wheat['realized'], sm.add_constant(wheat[['garch', 'mcgarch']])).fit()
print(r2_garch_wheat.summary())
print(r2t_garch_wheat.summary())
print(r2_mcgarch_wheat.summary())
print(r2t_mcgarch_wheat.summary())
print(r2_xxx_wheat.summary())

# mz regressions for soy
r2_garch_soybean = sm.OLS(soybean['realized'], sm.add_constant(soybean['garch'])).fit()
r2t_garch_soybean = sm.OLS(soybean['realized']-soybean['garch'].values, sm.add_constant(soybean['garch'])).fit()
r2_mcgarch_soybean = sm.OLS(soybean['realized'], sm.add_constant(soybean['mcgarch'])).fit()
r2t_mcgarch_soybean = sm.OLS(soybean['realized']-soybean['mcgarch'].values, sm.add_constant(soybean['mcgarch'])).fit()
r2_xxx_soybean = sm.OLS(soybean['realized'], sm.add_constant(soybean[['garch', 'mcgarch']])).fit()
print(r2_garch_soybean.summary())
print(r2t_garch_soybean.summary())
print(r2_mcgarch_soybean.summary())
print(r2t_mcgarch_soybean.summary())
print(r2_xxx_soybean.summary())

# structuring the output of mz regressions and printing
_idx = ['alpha_garch', 'beta_garch', 'alpha_mcgarch', 'beta_mcgarch', 'alpha', 'beta2_garch', 'beta2_mcgarch']
_clm = pd.MultiIndex.from_product([crops, ['para', 'tstat']])
resultsMZ = pd.DataFrame(index=_idx, columns=_clm)
print(r2_garch_wheat.params.values)
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('wheat', 'para')] = r2_garch_wheat.params.values
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('wheat', 'tstat')] = r2t_garch_wheat.tvalues.values
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('corn', 'para')] = r2_garch_corn.params.values
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('corn', 'tstat')] = r2t_garch_corn.tvalues.values
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('soybean', 'para')] = r2_garch_soybean.params.values
resultsMZ.loc[['alpha_garch', 'beta_garch'], ('soybean', 'tstat')] = r2t_garch_soybean.tvalues.values

resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('wheat', 'para')] = r2_mcgarch_wheat.params.values
resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('wheat', 'tstat')] = r2t_mcgarch_wheat.tvalues.values
resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('corn', 'para')] = r2_mcgarch_corn.params.values
resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('corn', 'tstat')] = r2t_mcgarch_corn.tvalues.values
resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('soybean', 'para')] = r2_mcgarch_soybean.params.values
resultsMZ.loc[['alpha_mcgarch', 'beta_mcgarch'], ('soybean', 'tstat')] = r2t_mcgarch_soybean.tvalues.values

resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('wheat', 'para')] = r2_xxx_wheat.params.values
resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('wheat', 'tstat')] = r2_xxx_wheat.tvalues.values
resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('corn', 'para')] = r2_xxx_corn.params.values
resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('corn', 'tstat')] = r2_xxx_corn.tvalues.values
resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('soybean', 'para')] = r2_xxx_soybean.params.values
resultsMZ.loc[['alpha', 'beta2_garch', 'beta2_mcgarch'], ('soybean', 'tstat')] = r2_xxx_soybean.tvalues.values

print(resultsMZ)
_resultsMZ = resultsMZ*1000000
print(_resultsMZ.to_string())
print(resultsMZ.to_latex(float_format="{:0.2f}".format))


# structuring output for R2 etc. full sample
_idx = ['MSE', 'R2', 'MAE', 'GMLE']
_clm = pd.MultiIndex.from_product([crops, ['garch', 'mcgarch']])
resultsPredAll = pd.DataFrame(index=_idx, columns=_clm)
resultsPredNv = pd.DataFrame(index=_idx, columns=_clm)
resultsPredEv = pd.DataFrame(index=_idx, columns=_clm)

resultsPredAll.loc['MSE', ('wheat', 'garch')] = getMSE(wheat[['realized']], wheat[['garch']]) * 10000000
resultsPredAll.loc['MSE', ('wheat', 'mcgarch')] = getMSE(wheat[['realized']], wheat[['mcgarch']]) * 10000000
resultsPredAll.loc['MSE', ('corn', 'garch')] = getMSE(corn[['realized']], corn[['garch']]) * 10000000
resultsPredAll.loc['MSE', ('corn', 'mcgarch')] = getMSE(corn[['realized']], corn[['mcgarch']]) * 10000000
resultsPredAll.loc['MSE', ('soybean', 'garch')] = getMSE(soybean[['realized']], soybean[['garch']]) * 10000000
resultsPredAll.loc['MSE', ('soybean', 'mcgarch')] = getMSE(soybean[['realized']], soybean[['mcgarch']]) * 10000000

resultsPredAll.loc['R2', ('wheat', 'garch')] = getR2(wheat[['realized']], wheat[['garch']]) * 100
resultsPredAll.loc['R2', ('wheat', 'mcgarch')] = getR2(wheat[['realized']], wheat[['mcgarch']]) * 100
resultsPredAll.loc['R2', ('corn', 'garch')] = getR2(corn[['realized']], corn[['garch']]) * 100
resultsPredAll.loc['R2', ('corn', 'mcgarch')] = getR2(corn[['realized']], corn[['mcgarch']]) * 100
resultsPredAll.loc['R2', ('soybean', 'garch')] = getR2(soybean[['realized']], soybean[['garch']]) * 100
resultsPredAll.loc['R2', ('soybean', 'mcgarch')] = getR2(soybean[['realized']], soybean[['mcgarch']]) * 100

resultsPredAll.loc['MAE', ('wheat', 'garch')] = getMAE(wheat[['realized']], wheat[['garch']]) * 10000
resultsPredAll.loc['MAE', ('wheat', 'mcgarch')] = getMAE(wheat[['realized']], wheat[['mcgarch']]) * 10000
resultsPredAll.loc['MAE', ('corn', 'garch')] = getMAE(corn[['realized']], corn[['garch']]) * 10000
resultsPredAll.loc['MAE', ('corn', 'mcgarch')] = getMAE(corn[['realized']], corn[['mcgarch']]) * 10000
resultsPredAll.loc['MAE', ('soybean', 'garch')] = getMAE(soybean[['realized']], soybean[['garch']]) * 10000
resultsPredAll.loc['MAE', ('soybean', 'mcgarch')] = getMAE(soybean[['realized']], soybean[['mcgarch']]) * 10000

resultsPredAll.loc['GMLE', ('wheat', 'garch')] = getGMLE(wheat[['realized']], wheat[['garch']]) 
resultsPredAll.loc['GMLE', ('wheat', 'mcgarch')] = getGMLE(wheat[['realized']], wheat[['mcgarch']]) 
resultsPredAll.loc['GMLE', ('corn', 'garch')] = getGMLE(corn[['realized']], corn[['garch']]) 
resultsPredAll.loc['GMLE', ('corn', 'mcgarch')] = getGMLE(corn[['realized']], corn[['mcgarch']]) 
resultsPredAll.loc['GMLE', ('soybean', 'garch')] = getGMLE(soybean[['realized']], soybean[['garch']]) 
resultsPredAll.loc['GMLE', ('soybean', 'mcgarch')] = getGMLE(soybean[['realized']], soybean[['mcgarch']]) 

# structuring output for R2 etc. non event days
wheat_nv = wheat.loc[~wheat.index.isin(events_df.index), :]
corn_nv = corn.loc[~corn.index.isin(events_df.index), :]
soybean_nv = soybean.loc[~soybean.index.isin(events_df.index), :]

resultsPredNv.loc['MSE', ('wheat', 'garch')] = getMSE(wheat_nv[['realized']], wheat_nv[['garch']]) * 10000000
resultsPredNv.loc['MSE', ('wheat', 'mcgarch')] = getMSE(wheat_nv[['realized']], wheat_nv[['mcgarch']]) * 10000000
resultsPredNv.loc['MSE', ('corn', 'garch')] = getMSE(corn_nv[['realized']], corn_nv[['garch']]) * 10000000
resultsPredNv.loc['MSE', ('corn', 'mcgarch')] = getMSE(corn_nv[['realized']], corn_nv[['mcgarch']]) * 10000000
resultsPredNv.loc['MSE', ('soybean', 'garch')] = getMSE(soybean_nv[['realized']], soybean_nv[['garch']]) * 10000000
resultsPredNv.loc['MSE', ('soybean', 'mcgarch')] = getMSE(soybean_nv[['realized']], soybean_nv[['mcgarch']]) * 10000000

resultsPredNv.loc['R2', ('wheat', 'garch')] = getR2(wheat_nv[['realized']], wheat_nv[['garch']]) * 100
resultsPredNv.loc['R2', ('wheat', 'mcgarch')] = getR2(wheat_nv[['realized']], wheat_nv[['mcgarch']]) * 100
resultsPredNv.loc['R2', ('corn', 'garch')] = getR2(corn_nv[['realized']], corn_nv[['garch']]) * 100
resultsPredNv.loc['R2', ('corn', 'mcgarch')] = getR2(corn_nv[['realized']], corn_nv[['mcgarch']]) * 100
resultsPredNv.loc['R2', ('soybean', 'garch')] = getR2(soybean_nv[['realized']], soybean_nv[['garch']]) * 100
resultsPredNv.loc['R2', ('soybean', 'mcgarch')] = getR2(soybean_nv[['realized']], soybean_nv[['mcgarch']]) * 100

resultsPredNv.loc['MAE', ('wheat', 'garch')] = getMAE(wheat_nv[['realized']], wheat_nv[['garch']]) * 10000
resultsPredNv.loc['MAE', ('wheat', 'mcgarch')] = getMAE(wheat_nv[['realized']], wheat_nv[['mcgarch']]) * 10000
resultsPredNv.loc['MAE', ('corn', 'garch')] = getMAE(corn_nv[['realized']], corn_nv[['garch']]) * 10000
resultsPredNv.loc['MAE', ('corn', 'mcgarch')] = getMAE(corn_nv[['realized']], corn_nv[['mcgarch']]) * 10000
resultsPredNv.loc['MAE', ('soybean', 'garch')] = getMAE(soybean_nv[['realized']], soybean_nv[['garch']]) * 10000
resultsPredNv.loc['MAE', ('soybean', 'mcgarch')] = getMAE(soybean_nv[['realized']], soybean_nv[['mcgarch']]) * 10000

resultsPredNv.loc['GMLE', ('wheat', 'garch')] = getGMLE(wheat_nv[['realized']], wheat_nv[['garch']]) 
resultsPredNv.loc['GMLE', ('wheat', 'mcgarch')] = getGMLE(wheat_nv[['realized']], wheat_nv[['mcgarch']]) 
resultsPredNv.loc['GMLE', ('corn', 'garch')] = getGMLE(corn_nv[['realized']], corn_nv[['garch']]) 
resultsPredNv.loc['GMLE', ('corn', 'mcgarch')] = getGMLE(corn_nv[['realized']], corn_nv[['mcgarch']]) 
resultsPredNv.loc['GMLE', ('soybean', 'garch')] = getGMLE(soybean_nv[['realized']], soybean_nv[['garch']]) 
resultsPredNv.loc['GMLE', ('soybean', 'mcgarch')] = getGMLE(soybean_nv[['realized']], soybean_nv[['mcgarch']]) 

# structuring output for R2 etc. event days
wheat_ev = wheat.loc[wheat.index.isin(events_df.index), :]
corn_ev = corn.loc[corn.index.isin(events_df.index), :]
soybean_ev = soybean.loc[soybean.index.isin(events_df.index), :]

resultsPredEv.loc['MSE', ('wheat', 'garch')] = getMSE(wheat_ev[['realized']], wheat_ev[['garch']]) * 1000000
resultsPredEv.loc['MSE', ('wheat', 'mcgarch')] = getMSE(wheat_ev[['realized']], wheat_ev[['mcgarch']]) * 1000000
resultsPredEv.loc['MSE', ('corn', 'garch')] = getMSE(corn_ev[['realized']], corn_ev[['garch']]) * 1000000
resultsPredEv.loc['MSE', ('corn', 'mcgarch')] = getMSE(corn_ev[['realized']], corn_ev[['mcgarch']]) * 1000000
resultsPredEv.loc['MSE', ('soybean', 'garch')] = getMSE(soybean_ev[['realized']], soybean_ev[['garch']]) * 1000000
resultsPredEv.loc['MSE', ('soybean', 'mcgarch')] = getMSE(soybean_ev[['realized']], soybean_ev[['mcgarch']]) * 1000000

resultsPredEv.loc['R2', ('wheat', 'garch')] = getR2(wheat_ev[['realized']], wheat_ev[['garch']]) * 100
resultsPredEv.loc['R2', ('wheat', 'mcgarch')] = getR2(wheat_ev[['realized']], wheat_ev[['mcgarch']]) * 100
resultsPredEv.loc['R2', ('corn', 'garch')] = getR2(corn_ev[['realized']], corn_ev[['garch']]) * 100
resultsPredEv.loc['R2', ('corn', 'mcgarch')] = getR2(corn_ev[['realized']], corn_ev[['mcgarch']]) * 100
resultsPredEv.loc['R2', ('soybean', 'garch')] = getR2(soybean_ev[['realized']], soybean_ev[['garch']]) * 100
resultsPredEv.loc['R2', ('soybean', 'mcgarch')] = getR2(soybean_ev[['realized']], soybean_ev[['mcgarch']]) * 100

resultsPredEv.loc['MAE', ('wheat', 'garch')] = getMAE(wheat_ev[['realized']], wheat_ev[['garch']]) * 1000
resultsPredEv.loc['MAE', ('wheat', 'mcgarch')] = getMAE(wheat_ev[['realized']], wheat_ev[['mcgarch']]) * 1000
resultsPredEv.loc['MAE', ('corn', 'garch')] = getMAE(corn_ev[['realized']], corn_ev[['garch']]) * 1000
resultsPredEv.loc['MAE', ('corn', 'mcgarch')] = getMAE(corn_ev[['realized']], corn_ev[['mcgarch']]) * 1000
resultsPredEv.loc['MAE', ('soybean', 'garch')] = getMAE(soybean_ev[['realized']], soybean_ev[['garch']]) * 1000
resultsPredEv.loc['MAE', ('soybean', 'mcgarch')] = getMAE(soybean_ev[['realized']], soybean_ev[['mcgarch']]) * 1000

resultsPredEv.loc['GMLE', ('wheat', 'garch')] = getGMLE(wheat_ev[['realized']], wheat_ev[['garch']])
resultsPredEv.loc['GMLE', ('wheat', 'mcgarch')] = getGMLE(wheat_ev[['realized']], wheat_ev[['mcgarch']])
resultsPredEv.loc['GMLE', ('corn', 'garch')] = getGMLE(corn_ev[['realized']], corn_ev[['garch']])
resultsPredEv.loc['GMLE', ('corn', 'mcgarch')] = getGMLE(corn_ev[['realized']], corn_ev[['mcgarch']])
resultsPredEv.loc['GMLE', ('soybean', 'garch')] = getGMLE(soybean_ev[['realized']], soybean_ev[['garch']])
resultsPredEv.loc['GMLE', ('soybean', 'mcgarch')] = getGMLE(soybean_ev[['realized']], soybean_ev[['mcgarch']])

# structuring output for R2 etc. OLS
# rerun with OLS = True

#
_idx = ['R2', 'R2_', 'R2__', 'MAE', 'MAE_']
_clm = pd.MultiIndex.from_product([crops, ['cldr', 'seas']])
resultsPredWLS = pd.DataFrame(index=_idx, columns=_clm)


resultsPredWLS.loc['R2', ('wheat', 'seas')] = getR2(wheat[['realized']], wheat[['seas']]) * 100
resultsPredWLS.loc['R2', ('wheat', 'cldr')] = getR2(wheat[['realized']], wheat[['cldr']]) * 100
resultsPredWLS.loc['R2', ('corn', 'seas')] = getR2(corn[['realized']], corn[['seas']]) * 100
resultsPredWLS.loc['R2', ('corn', 'cldr')] = getR2(corn[['realized']], corn[['cldr']]) * 100
resultsPredWLS.loc['R2', ('soybean', 'seas')] = getR2(soybean[['realized']], soybean[['seas']]) * 100
resultsPredWLS.loc['R2', ('soybean', 'cldr')] = getR2(soybean[['realized']], soybean[['cldr']]) * 100

resultsPredWLS.loc['R2_', ('wheat', 'seas')] = getR2(wheat_nv[['realized']], wheat_nv[['seas']]) * 100
resultsPredWLS.loc['R2_', ('wheat', 'cldr')] = getR2(wheat_ev[['realized']], wheat_ev[['cldr']]) * 100
resultsPredWLS.loc['R2_', ('corn', 'seas')] = getR2(corn_nv[['realized']], corn_nv[['seas']]) * 100
resultsPredWLS.loc['R2_', ('corn', 'cldr')] = getR2(corn_ev[['realized']], corn_ev[['cldr']]) * 100
resultsPredWLS.loc['R2_', ('soybean', 'seas')] = getR2(soybean_nv[['realized']], soybean_nv[['seas']]) * 100
resultsPredWLS.loc['R2_', ('soybean', 'cldr')] = getR2(soybean_ev[['realized']], soybean_ev[['cldr']]) * 100

resultsPredWLS.loc['R2__', ('wheat', 'seas')] = getR2(wheat_nv[['realized']], wheat_nv[['evnt']]) * 100
resultsPredWLS.loc['R2__', ('wheat', 'cldr')] = getR2(wheat[['realized']], wheat[['evnt']]) * 100
resultsPredWLS.loc['R2__', ('corn', 'seas')] = getR2(corn_nv[['realized']], corn_nv[['evnt']]) * 100
resultsPredWLS.loc['R2__', ('corn', 'cldr')] = getR2(corn[['realized']], corn[['evnt']]) * 100
resultsPredWLS.loc['R2__', ('soybean', 'seas')] = getR2(soybean_nv[['realized']], soybean_nv[['evnt']]) * 100
resultsPredWLS.loc['R2__', ('soybean', 'cldr')] = getR2(soybean[['realized']], soybean[['evnt']]) * 100

resultsPredWLS.loc['MAE', ('wheat', 'seas')] = getMAE(wheat[['realized']], wheat[['seas']]) * 100
resultsPredWLS.loc['MAE', ('wheat', 'cldr')] = getMAE(wheat[['realized']], wheat[['cldr']]) * 100
resultsPredWLS.loc['MAE', ('corn', 'seas')] = getMAE(corn[['realized']], corn[['seas']]) * 100
resultsPredWLS.loc['MAE', ('corn', 'cldr')] = getMAE(corn[['realized']], corn[['cldr']]) * 100
resultsPredWLS.loc['MAE', ('soybean', 'seas')] = getMAE(soybean[['realized']], soybean[['seas']]) * 100
resultsPredWLS.loc['MAE', ('soybean', 'cldr')] = getMAE(soybean[['realized']], soybean[['cldr']]) * 100

resultsPredWLS.loc['MAE_', ('wheat', 'seas')] = getMAE(wheat_nv[['realized']], wheat_nv[['seas']]) * 100
resultsPredWLS.loc['MAE_', ('wheat', 'cldr')] = getMAE(wheat_ev[['realized']], wheat_ev[['cldr']]) * 100
resultsPredWLS.loc['MAE_', ('corn', 'seas')] = getMAE(corn_nv[['realized']], corn_nv[['seas']]) * 100
resultsPredWLS.loc['MAE_', ('corn', 'cldr')] = getMAE(corn_ev[['realized']], corn_ev[['cldr']]) * 100
resultsPredWLS.loc['MAE_', ('soybean', 'seas')] = getMAE(soybean_nv[['realized']], soybean_nv[['seas']]) * 100
resultsPredWLS.loc['MAE_', ('soybean', 'cldr')] = getMAE(soybean_ev[['realized']], soybean_ev[['cldr']]) * 100

# Pringing...
print(resultsPredAll)
print(resultsPredNv)
print(resultsPredEv)
print(resultsPredWLS)
print(resultsPredAll.to_latex(float_format="{:0.2f}".format))
print(resultsPredNv.to_latex(float_format="{:0.2f}".format))
print(resultsPredEv.to_latex(float_format="{:0.2f}".format))