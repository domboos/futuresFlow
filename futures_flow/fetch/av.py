# import toolboxes
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import io
import statsmodels as sm




def header_string(header, crop, wtype):
    if header == 'crop':
        header = crop
    elif header in ('wtype', 'type'):
        header = wtype
    header = header.replace(" ", "_").replace(",", "").replace(".", "").replace(")", "").replace("(", "").replace("&",
                                                                                                                "and")
    header_str = " AS " + header + " "
    return header_str


# ----------------------------------------------------------------------------------------------------------------------
def gets(engine, series_id, tab, type, start_dt='1900-01-01', end_dt='2100-01-01', constr=None):

    if constr is None:
        constr = ''
    else:
        constr = ' AND ' + constr

    h_1 = " WHERE px_date >= '" + str(start_dt) +  "' AND px_date <= '" + str(end_dt) + "' AND series_id ="
    h_2 = str(series_id[0]) + constr + " order by px_date"
    fut = pd.read_sql_query('SELECT px_date, ' + type + ' FROM ' + tab + h_1 + h_2, engine, index_col='px_date')
    for s in series_id[1:]:
        h_2 = str(s) + constr + " order by px_date"
        futs = pd.read_sql_query('SELECT px_date, ' + type + ' FROM ' + tab + h_1 + h_2, engine, index_col='px_date')
        fut = fut.merge(futs,'outer',left_index=True,right_index=True)
    return fut


# ----------------------------------------------------------------------------------------------------------------------
def dyn_wgt(wgt, ret, reb_dt, rr=1, lag=1):
    # wgt periodic weight vector, ret: returns, rr: rebalance ratio, lag
    wr = pd.concat([wgt.shift(lag+1),ret.fillna(0),reb_dt],axis=1)
    # after shift wgt/w dates are not the date when they are known anymore as in the usual convention
    T = wr.shape[0]
    N = int((wr.shape[1]-1)/2)
    w = wr.iloc[:,0:N]
    w = w.fillna(method = 'pad')  # pad
    w = w.fillna(0)               # if nan at the beginning set to 0
    r = wr.iloc[:,N:2*N]
    d = wr.iloc[:,2*N]
    print(N)
    for n in range(0,N):
        for t in range(1,T-1):
            if d.iloc[t]==False:
                w.iloc[t+1, n ] = w.iloc[t, n ]*(1+r.iloc[t,n])
            else:
                w.iloc[t+1, n ] = rr * w.iloc[t+1, n ] + (1-rr) * w.iloc[t, n ]*(1+r.iloc[t,n])
    return (w*r.values).sum(axis=1)

# ----------------------------------------------------------------------------------------------------------------------
# LOADING WASDE DATA
# ----------------------------------------------------------------------------------------------------------------------


def getw(engine, wtype, crop='Total Grains', region='World', ptype='projected', unit=None, header='wtype',
         start_dt='1900-01-01', end_dt='2100-01-01', show_marketing_yr=False):

    if unit is None:
        unit_str = ' '
    else:
        unit_str = "AND unit = '" + unit + "' "

    if show_marketing_yr:
        myr_str = ", marketing_yr "
    else:
        myr_str = ' '

    if ptype == 'all':
        ptype_str = " AND ptype <> 'previous projected' "
    elif ptype == 'all_fc':
        ptype_str = " AND ptype in ('projected', 'estimated') "
    else:
        ptype_str = " AND ptype = '" + ptype + "' "

    # define column header
    header_str = header_string(header, crop, wtype)

    w = pd.read_sql_query("SELECT DISTINCT release_dt, qty " + header_str + myr_str + " FROM dbo.wasde_data dt "
                          "INNER JOIN dbo.wasde_desc dc ON dc.wasde_id = dt.wasde_id "
                          "WHERE type = '" + wtype + "' "
                          "AND crop = '" + crop + "' "
                          "AND region = '" + region + "' "
                          "AND release_dt >= '" + start_dt + "' "
                          "AND release_dt <= '" + end_dt + "' "
                          + ptype_str + unit_str +
                          "ORDER BY release_dt ",
                          engine, index_col='release_dt', parse_dates=['release_dt'])

    return w


# ----------------------------------------------------------------------------------------------------------------------
def val(engine, series, w, ma_length=2600, start_dt='1900-01-01', end_dt='2100-01-01',
        eql_ratio=None, min_periods_ma=252):
    # todo: pull more data
    # todo: include VAL_ma in the output
    val = gets(engine, series, 'dbo.vw_fut_avg_log_px_all', 'px_log_avg', start_dt, end_dt)
    val.index = pd.to_datetime(val.index)
    VAL = val.rmul(w, axis=1).sum(axis=1, skipna=False)  # skipna = False set the row to zero if any element ins nan
    if eql_ratio is None:
        VAL_ma = VAL.rolling(ma_length, min_periods=min_periods_ma).mean()
        VAL = VAL.sub(VAL_ma)
    else:
        VAL = VAL.sub(eql_ratio)
    VAL = VAL.to_frame('valu')
    return VAL


def roll(engine, series, w, start_dt='1900-01-01', end_dt='2100-01-01'):
    slp = gets(engine, series, 'dbo.vw_fut_slope_log_px_all', 'px_log_slope', start_dt, end_dt)
    slp = slp.rmul(w, axis=1)
    SLP = slp.sum(axis=1, skipna=False)
    roll = -SLP.to_frame('roll')
    return roll

# ----------------------------------------------------------------------------------------------------------------------
# LOADING LONG TERM USDA FORECASTS
# ----------------------------------------------------------------------------------------------------------------------


def getlt(engine, start_yr=1900, end_yr=2100, usda_id=None, crop=None, type=None, region='United States', category=None,
          unit=None, xref_table='wasde_desc', usda_id_xref=None, marketing_yr=False, header='type', lead=0,
          include_release_dt='not', include_marketing_yr=True):

    # find the wasde_id if not given
    if usda_id is None:
        if usda_id_xref is None:
            if xref_table == 'wasde_desc':
                qry = "SELECT wasde_id FROM dbo.wasde_desc WHERE crop='" + str(crop) + "' AND type='" + str(type) \
                      + "' AND region='" + str(region) + "'"
            else:
                qry = "SELECT wasde_id FROM dbo.wasde_desc WHERE crop='" + str(crop) + "' AND type='" + str(type) \
                      + "' AND region='" + str(region) + "'"

            if unit is not None:
                qry = qry + "' AND unit='" + str(unit) + "' "

            if category is not None:
                qry = qry + "' AND  category='" + str(category) + "' "

            if include_marketing_yr in ('as_index', 'as_column'):
                myr_str = ", marketing_yr "
            else:
                myr_str = ''

            print(qry)
            usda_id_xref = pd.read_sql_query(qry, engine).iloc[0, 0]

        qry2 = "SELECT usda_id FROM dbo.usda_lt_proj_desc WHERE usda_id_xref=" + str(usda_id_xref)
        usda_id = pd.read_sql_query(qry2, engine).iloc[0, 0]

    # define column header
    header_str = header_string(header, crop, type)

    # querying data and return data
    if marketing_yr:
        data_qry = "SELECT year_projected, qty " + header_str + " FROM dbo.usda_lt_proj_data where usda_id = " \
                   + str(usda_id) + " AND year_projected >= " + str(start_yr) + " AND year_projected <= " \
                   + str(end_yr) + " AND marketing_yr ='" + marketing_yr + "' order by year_projected"
    else:
        data_qry = "SELECT year_projected, qty " + header_str + myr_str \
                   + " FROM dbo.usda_lt_proj_data where usda_id = " + str(usda_id) + " AND year_projected >= " \
                   + str(start_yr) + " AND year_projected <= " + str(end_yr) \
                   + " AND (left(marketing_yr,4)::bigint)=year_projected+" + str(lead) \
                   + " order by year_projected, marketing_yr"

    series = pd.read_sql_query(data_qry, engine, index_col='year_projected')

    # adding released and make the index pretty
    if include_release_dt in ('as_index', 'as_column'):
        rd = pd.read_sql_query('SELECT * FROM dbo.usda_lt_proj_release_dt', engine, index_col='year_projected')
        series = series.merge(rd, left_index=True, right_index=True)

    if include_release_dt == 'as_index' and include_release_dt in ('as_column', 'not'):
        series = series.reset_index().set_index('release_dt').drop(columns='year_projected')

    if include_release_dt == 'as_index' and include_release_dt == 'as_index':
        series = series.reset_index().set_index(['marketing_yr', 'release_dt']).drop(columns='year_projected')

    return series

# ----------------------------------------------------------------------------------------------------------------------


def getpc(engine, start_yr=1900, end_yr=2100, usda_id=None, crop=None, type=None, region='United States',
          header='type'):
    # unit, category and region are in the desc table but they are not used as type and crop defines the wasde_id

    if usda_id is None:
        qry_id = "SELECT usda_id FROM dbo.usda_pcost_desc WHERE type = '" + type + "' AND crop = '" + crop + \
                 "' AND region = '" + region + "'"
        usda_id = pd.read_sql_query(qry_id, engine).iloc[0, 0]

    # define column header
    header_str = header_string(header, crop, type)

    qry = "SELECT year, qty " + header_str + " FROM dbo.usda_pcost_data WHERE usda_id = " + str(usda_id) + \
          " AND year >= " + str(start_yr) + " AND year <= " + str(end_yr)

    return pd.read_sql_query(qry, engine, index_col='year')


# ----------------------------------------------------------------------------------------------------------------------
def getc(engine, crop, ctype, category=None, region='United States', unit=None, start_dt='1900-01-01',
         end_dt='2100-01-01', include_release_dt=False):

    if unit is None:
        unit_str = ''
    else:
        unit_str = "AND unit = '" + unit + "' "

    if category is None:
        cat_str = ''
    else:
        cat_str = "AND category = '" + category + "' "

    if include_release_dt:
        rd_str = ", release_dt"
    else:
        rd_str = ''

    # todo use header_string function
    name_str = " AS " + ctype.replace(" ","_").replace(",","").replace(".","").replace(")","").replace("(","") + " "

    w = pd.read_sql_query("SELECT DISTINCT date, qty" + name_str + rd_str + " FROM dbo.usda_condition_data dt "
                          "INNER JOIN dbo.usda_condition_desc_region r ON dt.usda_id = r.usda_id "
                          "INNER JOIN dbo.usda_condition_desc dc ON dc.usda_desc_id = r.usda_desc_id "
                          "WHERE type = '" + ctype + "' "
                          "AND crop = '" + crop + "' "
                          "AND region = '" + region + "' "
                          "AND date >= '" + start_dt + "' "
                          "AND date <= '" + end_dt + "' " + unit_str + cat_str +
                          "ORDER BY date",
                          engine, index_col='date', parse_dates=['date'])

    return w


# ----------------------------------------------------------------------------------------------------------------------
def getf(engine, crop, type, term, start_dt='1900-01-01', end_dt='2100-01-01', header='type'):

    # todo use header_string function
    name_str = header_string(header, crop, type)

    w = pd.read_sql_query("SELECT release_dt, qty" + name_str + " FROM dbo.fndmtl_data dt " +
                          "INNER JOIN dbo.fndmtl_desc dc ON dc.fndmtl_id = dt.fndmtl_id " +
                          "WHERE ftype = '" + type + "' " +
                          "AND cmdty = '" + crop + "' " +
                          "AND term = " + str(term) +
                          " AND release_dt >= '" + start_dt +
                          "' AND release_dt <= '" + end_dt +
                          "' ORDER BY release_dt",
                          engine, index_col='release_dt', parse_dates=['release_dt'])

    return w
