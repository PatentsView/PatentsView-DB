import sys
import pymysql
import configparser
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from lib.utilities import get_connection_string, get_current_config

def set_dataframe(startyear, endyear, engine):
    # cursor = con.cursor()
    # con.execute("SET NAMES utf8")
    query = '''
             SELECT persistent_location_id AS id, city, state, country, latitude AS lat, longitude AS lon, patents, assignees, inventors
             FROM location l
             LEFT JOIN (
             SELECT location_id, count(DISTINCT pa.assignee_id) AS assignees
             FROM patent_assignee pa
             INNER JOIN patent p
             ON pa.patent_id = p.patent_id
             WHERE `year` >=  %(start)s
             GROUP BY location_id
             ) a
             ON l.location_id = a.location_id
             LEFT JOIN (
             SELECT location_id, count(DISTINCT pi.inventor_id) AS inventors
             FROM patent_inventor pi
             INNER JOIN patent p
             ON pi.patent_id = p.patent_id
             WHERE `year` >=   %(start)s
             GROUP BY location_id
             ) i
             ON l.location_id = i.location_id
             LEFT JOIN (
             SELECT location_id, sum(num_patents) AS patents
             FROM location_year ly
             WHERE `year` >= %(start)s
             GROUP BY location_id
             ) p
             ON l.location_id = p.location_id
             WHERE (inventors IS NOT NULL
             OR assignees IS NOT NULL)
             AND city IS NOT NULL
             ORDER BY city'''
    data = pd.read_sql(query, engine, params={"start": startyear})

    # con.execute("SHOW VARIABLES LIKE 'character_set_%'")
    # for row in cursor:
    #     print(row)
    print(data.head())
    return data


def cleaning(data):
    notNL = data[(data.city.str.contains('^\\d.*'))]
    notNL = notNL[notNL.country != "NL"]
    notNL_list = list(notNL.groupby("city").size().index)
    data1 = data[~data.city.str.contains('.*"".*')]
    data1 = data1[~data1.city.str.contains('^#.*')]
    data1 = data1[~data1.city.str.contains('^\\(.*')]
    data1 = data1[~data1.city.str.contains('^".*')]
    data1 = data1[~data1.city.isin(notNL_list)]
    data1 = data1[data1.city.str.len() != 1]
    data1 = data1[data1.country.str.len() == 2]
    data1 = data1[data1.country.str.isupper()]
    data1 = data1[data1.city != data1.state]
    data1 = data1[data1.city != data1.country]
    data1["city"] = data1["city"].str.replace('"', "")
    data1["city"] = data1["city"].str.replace(r'^`', "")
    # data1 = data1[~data1["state"].isnull()]
    return data1


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description='Process table parameters')
#     parser.add_argument(
#         '-c',
#         type=str,
#         nargs=1,
#         help='File containing database config in INI format')
#     parser.add_argument(
#         '-d',
#         type=str,
#         nargs=1,
#         help='Destination directory for data output')
#
#     parser.add_argument(
#         '-e',
#         type=str,
#         nargs=1,
#         help='Environment Type')
#
#     args = parser.parse_args()
#     config = configparser.ConfigParser()
#     config.read(args.c[0])
#
#     access_user = config["cred_database"]["mysql_db_user"]
#     access_passwd = config["cred_database"]["mysql_db_password"]
#     access_mydb = config["cred_database"]["mysql_db_host"]
#     access_port = config["cred_database"]["mysql_db_port"]
#     access_db = config["cred_database"]["mysql_db_name"]
#     datadir = args.d[0]
#     environment_type = "DEV"
#     if args.e[0] == 1:
#         environment_type = "PROD"
def run_location_flatfile(**kwargs):
    go_live_path = "/project/go_live"
    config = get_current_config('granted_patent', **{
        "execution_date": kwargs['execution_date']
    })
    engine = create_engine(get_connection_string(config))
    root_engine = create_engine(get_connection_string(config, database="app_database"))

    start_end = pd.read_sql("select `key`, `value` from config where `key` in ('webtool_location_start_year', 'webtool_location_end_year')", con=root_engine)
    start = list(start_end[start_end['key'] =='webtool_location_start_year']['value'])[0]
    end = list(start_end[start_end['key'] =='webtool_location_end_year']['value'])[0]

    locdata = set_dataframe(start, end, engine)
    locdata1 = cleaning(locdata)

    # Create Dataframes of "State+Country Codes.xlsx"
    code_file = datadir + "/State+Country Codes.xlsx"
    countryNames = pd.read_excel(
        datadir + "/State+Country Codes.xlsx", \
        sheet_name="Country", header=None, names=["code", "countryName"])
    countryNames["code"] = np.where(countryNames["code"].isnull(), "NA", countryNames["code"])
    stateNames = pd.read_excel(code_file, sheet_name="State", header=None, names=["stateName", "code"])
    addon = pd.DataFrame([["District of Columbia", "DC"]], columns=["stateName", "code"])
    # stateNames = stateNames.append(addon, ignore_index=True)
    stateNames = pd.concat([stateNames, addon], ignore_index=True)

    # Join "State+Country Codes.xlsx" to locdata1
    locdata2 = pd.merge(locdata1, countryNames, left_on="country", right_on="code", how="left")
    locdata2 = locdata2.drop("code", axis=1)
    locdata2 = pd.merge(locdata2, stateNames, left_on="state", right_on="code", how="left")
    locdata2 = locdata2.drop("code", axis=1)
    locdata2["country"] = np.where(locdata2["countryName"].isnull(), locdata2.country, locdata2.countryName)
    locdata2["state"] = np.where(locdata2["stateName"].isnull(), locdata2.state, locdata2.stateName)
    locdata2 = locdata2.drop(["countryName", "stateName"], axis=1)
    locdata2["patents"].fillna(0, inplace=True)
    locdata2["patents"] = locdata2["patents"].astype("int64")
    ranked = locdata2[["id", "patents"]].sort_values("patents", ascending=False).reset_index()
    ranked = ranked.drop("index", axis=1)
    payload1 = ranked[:1000]
    payload2 = ranked[1000:]
    payload1.to_csv(datadir + "/location_data_payload1.csv", sep=" ", header=False, encoding="utf-8", index=False)
    payload2.to_csv(datadir + "/location_data_payload2.csv", sep=" ", header=False, encoding="utf-8", index=False)
