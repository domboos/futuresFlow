from futures_flow.private.engines import *

dicc = {'US TOTAL': 'United States',
        'ALABAMA': 'Alabama',
        'ALASKA': 'Alaska',
        'ARIZONA': 'Arizona',
        'ARKANSAS': 'Arkansas',
        'CALIFORNIA': 'California',
        'COLORADO': 'Colorado',
        'CONNECTICUT': 'Connecticut',
        'DELAWARE': 'Delaware',
        'FLORIDA': 'Florida',
        'GEORGIA': 'Georgia',
        'IDAHO': 'Idaho',
        'ILLINOIS': 'Illinois',
        'INDIANA': 'Indiana',
        'IOWA': 'Iowa',
        'KANSAS': 'Kansas',
        'KENTUCKY': 'Kentucky',
        'LOUISIANA': 'Louisiana',
        'MAINE': 'Maine',
        'MARYLAND': 'Maryland',
        'MASSACHUSETTS': 'Massachusetts',
        'MICHIGAN': 'Michigan',
        'MINNESOTA': 'Minnesota',
        'MISSISSIPPI': 'Mississippi',
        'MISSOURI': 'Missouri',
        'MONTANA': 'Montana',
        'NEBRASKA': 'Nebraska',
        'NEVADA': 'Nevada',
        'NEW HAMPSHIRE': 'New Hampshire',
        'NEW JERSEY': 'New Jersey',
        'NEW MEXICO': 'New Mexico',
        'NEW YORK': 'New York',
        'NORTH CAROLINA': 'North Carolina',
        'NORTH DAKOTA': 'North Dakota',
        'OHIO': 'Ohio',
        'OKLAHOMA': 'Oklahoma',
        'OREGON': 'Oregon',
        'OTHER STATES': 'Other States',
        'PENNSYLVANIA': 'Pennsylvania',
        'RHODE ISLAND': 'Rhode Island',
        'SOUTH CAROLINA': 'South Carolina',
        'SOUTH DAKOTA': 'South Dakota',
        'TENNESSEE': 'Tennessee',
        'TEXAS': 'Texas',
        'UTAH': 'Utah',
        'VERMONT': 'Vermont',
        'VIRGINIA': 'Virginia',
        'WASHINGTON': 'Washington',
        'WEST VIRGINIA': 'West Virginia',
        'WISCONSIN': 'Wisconsin',
        'WYOMING': 'Wyoming'}

# speed up db
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert
# -------------------

engine = dbo_engine()

def parse_inv_date(row):
    return datetime.strptime('01' + row['end_code'] + row['year'],'%d%m%Y')

def parse_inv_release_date(row):
    return datetime.strptime(row['load_time'],'%Y-%m-%d %H:%M:%S')

# import further libraries
import pandas as pd
from datetime import datetime
from nass import USDAApi
#https://github.com/brianmwadime/nass-usda
api = USDAApi('2DA9757E-A6EB-3628-A529-DB7B4183EA40')

years = range(2022, 2024)

#flag_hist = False  # load all historic data
flag_desc = False  # load desc table

if flag_desc:
    file = "G:\\Geteilte Ablagen\\Avena\\05 Database\\nass\\load.xlsx"
    print(pd.read_excel(file, 'desc').to_string())
    pd.read_excel(file, 'desc')\
        .to_sql('usda_condition_desc', engine, schema='dbo', if_exists='append', index=False)

desc = pd.read_sql_query("SELECT usda_desc_id, description FROM dbo.usda_condition_desc", engine)

# data loaded since 1990

for year in years:
    print(year)
    loadedData = pd.read_sql_query("SELECT usda_id, date FROM dbo.usda_condition_data WHERE date_part('year', date) in "
                                   + "(" + str(year-1) + "," + str(year) + "," + str(year+1) + ")",
                                   engine, parse_dates=['date'])

    loadedGrp = pd.read_sql_query("SELECT de.usda_desc_id, de.description, count(de.usda_desc_id) "
        + " FROM dbo.usda_condition_desc de "
        + " inner join dbo.usda_condition_desc_region re on re.usda_desc_id = de.usda_desc_id "
        + " inner join dbo.usda_condition_data da on da.usda_id = re.usda_id "
        + " WHERE date_part('year', date) = " + str(year)
        + " group by de.usda_desc_id, de.description", engine, parse_dates=['date'])

    print(loadedGrp)


    for idx, d in desc.iterrows():
        if d.description not in loadedGrp['description'].values:
            q = api.query().filter('short_desc', d.description).filter('year', year)\
                .filter('agg_level_desc', ("STATE", "NATIONAL"))

            c = q.count()
            pd.set_option('display.max_columns', 50)
            print(d.description + ': ' + str(c) + ' data points in ' + str(year))

            if q.count() >= 50000:
                warn = 'Too much data requested for short_desc: ' + d.description
                print(warn)

            if c > 0:
                try:
                    r = q.execute()
                except:
                    warn = 'No data retrieved for short_desc: ' + d.description
                    print(warn)

                print(pd.DataFrame(r))
                df = pd.DataFrame(r).replace(dicc)[['Value', 'load_time', 'location_desc', 'week_ending']]
                df.columns = ['qty', 'release_dt', 'region', 'date']
                df['qty'] = df['qty'].str.replace(',', '') #.astype(int)
                df['date'] = pd.to_datetime(df['date'], errors='coerce', format='%Y-%m-%d')
                print(df.dtypes)
                df['date'] = df['date'].fillna(df['release_dt'])
                df['usda_desc_id'] = d.usda_desc_id
                df['date'] = pd.to_datetime(df['date'])

                print(df.to_string())

                # check for new entries into usda_condition_desc_region
                loadedRegions = pd.read_sql_query("SELECT usda_desc_id, usda_id, region FROM dbo.usda_condition_desc_region "
                                                  "WHERE  usda_desc_id = " + str(d.usda_desc_id), engine)
                newDataRegions = df[['usda_desc_id', 'region']].drop_duplicates()
                newRegions = pd.merge(right=loadedRegions, left=newDataRegions,
                                      on=['usda_desc_id', 'region'], how='left', indicator=True)
                newRegions = newRegions.loc[newRegions._merge.isin(['left_only']), ['usda_desc_id', 'region']]

                if len(newRegions.index) > 0:
                    newRegions.to_sql('usda_condition_desc_region', engine, schema='dbo', if_exists='append', index=False)
                    print(str(len(newRegions.index)) + " new lines loaded into desc_region table")
                    loadedRegions = pd.read_sql_query("SELECT usda_desc_id, usda_id, region FROM dbo.usda_condition_desc_region "
                                                      "WHERE  usda_desc_id = " + str(d.usda_desc_id), engine)

                newData = pd.merge(right=df, left=loadedRegions,
                                   on=['usda_desc_id', 'region'])[['qty', 'release_dt', 'date', 'usda_id']]
                newData = pd.merge(right=loadedData, left=newData, on=['date', 'usda_id'], how='left', indicator=True)

                newData = newData.loc[newData._merge.isin(['left_only']), ['qty', 'release_dt', 'date', 'usda_id']]

                print('--- loading --------- .....')
                print(newData)
                newData.drop_duplicates().to_sql('usda_condition_data', engine, schema='dbo', if_exists='append', index=False)
