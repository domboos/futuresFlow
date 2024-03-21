import pandas as pd
import numpy as np
import io
import sqlalchemy
from sodapy import Socrata


def getcot(client, engine, commodities, group, side='long', options=False, crop_year='all', start_dt=None,
           end_dt=None, identifier='CFTC_Contract_Market_Code', bb_ykey='COMDTY', new_name=None):

    """
    commodities: as cftc identifier
    group: group of trader (cit, (non_)commercials, (non_)commercials_excit, managed_money, non_reportables,
                            open_interest, other_reportables, pump, swap, tot_reportables)
    side: long, short, spreading, net, number (for open_interest)
    options: true (includes futures and options) false (futures only)
    crop_year: current crop year (old), all the others (oth) and all
    identifier: can use bloomberg code

    Returns
    Pandas dataframe
    -------

    """

    if group in {'commercials_excit', 'non_commercials_excit', 'cit'}:
        # Supplemental - CIT: https://dev.socrata.com/foundry/publicreporting.cftc.gov/4zgm-a668
        source = "4zgm-a668"
    elif group in {'commercials', 'non_commercials', 'reportables', 'non_reportables', 'open_interest'}:
        if options:
            # Legacy - Combined: https://dev.socrata.com/foundry/publicreporting.cftc.gov/jun7-fc8e
            source = "jun7-fc8e"
        else:
            # Legacy - Futures Only: https://dev.socrata.com/foundry/publicreporting.cftc.gov/6dca-aqww
            source = "6dca-aqww"
    else:
        if options:
            # Disaggregated - Combined: https://dev.socrata.com/foundry/publicreporting.cftc.gov/kh3c-gbw2
            source = "kh3c-gbw2"
        else:
            #Disaggregated - Futures Only: https://dev.socrata.com/foundry/publicreporting.cftc.gov/72hh-3qpy
            source = "72hh-3qpy"

    if crop_year in ('oth', 'OTH', 'Other', 'OTHER'):
        crop_year = 'other'
    if crop_year in ('All', 'ALL'):
        crop_year = 'all'
    if crop_year in ('Old', 'OLD'):
        crop_year = 'old'

    if isinstance(commodities, str):
        commodities = [commodities]

    add_str = ""
    if start_dt is not None:
        add_str = " AND report_date_as_yyyy_mm_dd >= '" + start_dt + "' "

    df_out = pd.DataFrame()

    for commodity in commodities:
        if side == 'net':
            long = getcot(client, engine, commodity, group, 'long', options=options, crop_year=crop_year,
                          start_dt=start_dt, end_dt=end_dt, identifier=identifier, bb_ykey=bb_ykey)
            short = getcot(client, engine, commodity, group, 'short', options=options, crop_year=crop_year,
                           start_dt=start_dt, end_dt=end_dt, identifier=identifier, bb_ykey=bb_ykey)
            print(short)
            df_out = pd.merge(df_out, long-short, how='outer', left_index=True, right_index=True)
        else:
            _qry_gs = "SELECT category FROM dbo.cot_category WHERE ls='" + side + "' AND crop='" \
                      + crop_year + "' AND trader_group='" + group + "'"

            group_side = pd.read_sql(_qry_gs, con=engine).iloc[0,0]
            group_side = group_side.lower()
            print("loading " + group_side + " of " + commodity + "...")

            data = client.get(source,
                              select="report_date_as_yyyy_mm_dd as report_date, " + group_side,
                              where="cftc_contract_market_code='" + commodity + "'" + add_str,
                              order="report_date_as_yyyy_mm_dd")

            data_df = pd.DataFrame.from_records(data)
            data_df = data_df.astype({group_side: int})
            data_df['report_date'] = pd.to_datetime(data_df['report_date']).dt.date

            if new_name is None:
                renames = {group_side: commodity}
            else:
                renames = {group_side: new_name}

            data_df.rename(columns=renames, inplace=True)


            df_out = pd.merge(df_out, data_df.set_index('report_date'), how='outer', left_index=True, right_index=True)

    return df_out

