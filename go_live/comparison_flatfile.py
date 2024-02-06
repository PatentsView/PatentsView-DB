import configparser
import pandas as pd
import numpy as np
import json
import datetime
import argparse
from sqlalchemy import create_engine
from lib.utilities import get_connection_string, get_current_config
from lib.configuration import get_unique_connection_string

def set_dataframe(query, con):
    cursor = con.cursor()
    data = pd.read_sql(query, con)
    con.rollback()
    return data


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
#     go_live_path = args.d[0]
#     environment_type = "DEV"
#     if args.e[0] == 1:
#         environment_type = "PROD"

def run_comparison_flatfile(**kwargs):
    go_live_path = "/project/go_live"
    config = get_current_config('granted_patent', schedule='quarterly', **{
        "execution_date": kwargs['execution_date']
    })
    engine = create_engine(get_connection_string(config, database='REPORTING_DATABASE'))
    ## Countries
    countryI = pd.read_sql_table('webtool_comparison_countryI', con=engine)
    countryI_sector = pd.read_sql_table('webtool_comparison_countryIsector', engine)


    # Complete()
    countryI1 = countryI_sector.set_index(["country", "year", "sector_title"])
    mux = pd.MultiIndex.from_product([countryI1.index.levels[0], countryI1.index.levels[1], countryI1.index.levels[2]],
                                     names=["country", "year", "sector_title"])
    countryI1 = countryI1.reindex(mux, fill_value=0).reset_index()

    # Join countryI1 with countryI
    countryI1 = pd.merge(countryI1, countryI, how="left", on=["country", "year"])

    # Change NaN to 0
    countryI1["invCount"] = np.where(countryI1["invCount"].isnull(), 0, countryI1["invCount"])
    countryI1["invCount"] = countryI1["invCount"].astype("int64")

    startYear = 1976
    countryI1 = countryI1[countryI1["year"] >= startYear]

    countryIA = pd.read_sql_table('webtool_comparison_countryIA', engine)
    countryA_sector = pd.read_sql_table('webtool_comparison_countryIAsector', engine)

    # Right join with countryI1
    countryA1 = pd.merge(countryA_sector, countryI1.iloc[:, 0:3].drop_duplicates().reset_index().drop("index", axis=1),
                         on=["country", "year", "sector_title"], how="right")
    countryA1["assiSubCount"] = np.where(countryA1["assiSubCount"].isnull(), 0, countryA1["assiSubCount"])

    # Complete()
    countryA1 = countryA1.set_index(["country", "year", "sector_title"])
    mux = pd.MultiIndex.from_product([countryA1.index.levels[0], countryA1.index.levels[1], countryA1.index.levels[2]],
                                     names=["country", "year", "sector_title"])
    countryA1 = countryA1.reindex(mux, fill_value=0).reset_index()

    # Left join with countryIA
    countryA1 = pd.merge(countryA1, countryIA, on=["country", "year"], how="left")

    # Filter based on startYear
    countryA1 = countryA1[countryA1["year"] >= startYear]

    # Change NaN to 0
    countryA1["assiCount"] = np.where(countryA1["assiCount"].isnull(), 0, countryA1["assiCount"])
    countryA1["assiCount"] = countryA1["assiCount"].astype("int64")
    countryA1["assiSubCount"] = countryA1["assiSubCount"].astype("int64")

    # Import state and country table
    countryNames = pd.read_excel(
        go_live_path + "/State+Country Codes.xlsx", engine="openpyxl",
        sheet_name="Country", header=None, names=["code", "countryName"])
    countryNames["code"] = np.where(countryNames["code"].isnull(), "NA", countryNames["code"])

    countries = pd.merge(countryI1, countryA1, how="inner", on=["country", "year", "sector_title"])
    countries = pd.merge(countries, countryNames, left_on="country", right_on="code", how="inner").drop(
        ["country", "code"], axis=1).sort_values(["countryName", "year", "sector_title"],
                                                 ascending=[True, True, True]).reset_index().drop("index", axis=1)

    stateI = pd.read_sql_table('webtool_comparison_stateI', engine)
    stateI_sector = pd.read_sql_table('webtool_comparison_stateIsector', engine)

    # Complete()
    stateI1 = stateI_sector.set_index(["state", "year", "sector_title"])
    mux = pd.MultiIndex.from_product([stateI1.index.levels[0], stateI1.index.levels[1], stateI1.index.levels[2]],
                                     names=["state", "year", "sector_title"])
    stateI1 = stateI1.reindex(mux, fill_value=0).reset_index()

    # Left join stateI
    stateI1 = pd.merge(stateI1, stateI, on=["state", "year"], how="left")
    stateI1 = stateI1[stateI1["year"] >= startYear]

    # Change NaN to 0
    stateI1["invCount"] = np.where(stateI1["invCount"].isnull(), 0, stateI1["invCount"])
    stateI1["invCount"] = stateI1["invCount"].astype("int64")

    stateA = pd.read_sql_table('webtool_comparison_stateA', engine)
    stateA_sector = pd.read_sql_table('webtool_comparison_stateAsector', engine)

    # Right join stateI1

    stateA1 = pd.merge(stateA_sector, stateI1.iloc[:, 0:3].drop_duplicates().reset_index().drop("index", axis=1),
                       on=["state", "year", "sector_title"], how="right")

    stateA1["assiSubCount"] = np.where(stateA1["assiSubCount"].isnull(), 0, stateA1["assiSubCount"])

    # Complete()
    stateA1 = stateA1.set_index(["state", "year", "sector_title"])
    mux = pd.MultiIndex.from_product([stateA1.index.levels[0], stateA1.index.levels[1], stateA1.index.levels[2]],
                                     names=["state", "year", "sector_title"])
    stateA1 = stateA1.reindex(mux, fill_value=0).reset_index()

    # Left join with countryIA
    stateA1 = pd.merge(stateA1, stateA, on=["state", "year"], how="left")

    # Filter based on startYear
    stateA1 = stateA1[stateA1["year"] >= startYear]

    # Change NaN to 0
    stateA1["assiCount"] = np.where(stateA1["assiCount"].isnull(), 0, stateA1["assiCount"])
    stateA1["assiCount"] = stateA1["assiCount"].astype("int64")
    stateA1["assiSubCount"] = stateA1["assiSubCount"].astype("int64")

    stateNames = pd.read_excel(go_live_path + "/State+Country Codes.xlsx", sheet_name="State", header=None,
                               names=["stateName", "code"])
    addon = pd.DataFrame([["District of Columbia", "DC"]], columns=["stateName", "code"])
    # stateNames = stateNames.append(addon, ignore_index=True)
    stateNames = pd.concat([stateNames, addon], ignore_index=True)

    states = pd.merge(stateI1, stateA1, on=["state", "year", "sector_title"], how="inner")
    states = pd.merge(states, stateNames, left_on="state", right_on="code", how="inner").drop(["state", "code"],
                                                                                              axis=1).sort_values(
        ["stateName", "year", "sector_title"], ascending=[True, True, True]).reset_index().drop("index", axis=1)

    wipoI = pd.read_sql_table('webtool_comparison_wipoI', engine)

    wipoI1 = wipoI[wipoI["year"] >= startYear]

    # Complete()
    wipoI1["key"] = wipoI1["sector_title"] + "/" + wipoI1["field_title"]

    wipoI1 = wipoI1.set_index(["key", "year"])
    mux = pd.MultiIndex.from_product([wipoI1.index.levels[0], wipoI1.index.levels[1]], names=["key", "year"])
    wipoI1 = wipoI1.reindex(mux, fill_value=0).drop(["sector_title", "field_title"], axis=1).reset_index()
    # Divide "key" and reset sector_title and field title
    wipoI1["sector_title"] = wipoI1["key"].str.extract("(.*)/.*", expand=True)
    wipoI1["field_title"] = wipoI1["key"].str.extract(".*/(.*)", expand=True)
    wipoI1 = wipoI1.drop("key", axis=1)

    # Compute summation of each sector each year.
    invCount = wipoI1.groupby(["sector_title", "year"]).agg({"invSubCount": "sum"}).reset_index()
    invCount.columns = ["sector_title", "year", "invCount"]
    invCount["invCount"] = invCount["invCount"].astype("int64")

    wipoI1 = pd.merge(wipoI1, invCount, on=["sector_title", "year"], how="left").sort_values(
        ["sector_title", "year", "field_title"], ascending=[True, True, True]).reset_index().drop("index", axis=1)

    wipoA = pd.read_sql_table('webtool_comparison_wipoA', engine)

    wipoA1 = wipoA[wipoA["year"] >= startYear]

    # Complete()
    wipoA1["key"] = wipoA1["sector_title"] + "/" + wipoA1["field_title"]

    wipoA1 = wipoA1.set_index(["key", "year"])
    mux = pd.MultiIndex.from_product([wipoA1.index.levels[0], wipoA1.index.levels[1]], names=["key", "year"])
    wipoA1 = wipoA1.reindex(mux, fill_value=0).drop(["sector_title", "field_title"], axis=1).reset_index()
    # Divide "key" and reset sector_title and field title
    wipoA1["sector_title"] = wipoA1["key"].str.extract("(.*)/.*", expand=True)
    wipoA1["field_title"] = wipoA1["key"].str.extract(".*/(.*)", expand=True)
    wipoA1 = wipoA1.drop("key", axis=1)

    # Compute summation of each sector each year.
    assiCount = wipoA1.groupby(["sector_title", "year"]).agg({"assiSubCount": "sum"}).reset_index()
    assiCount.columns = ["sector_title", "year", "assiCount"]
    assiCount["assiCount"] = assiCount["assiCount"].astype("int64")

    wipoA1 = pd.merge(wipoA1, assiCount, on=["sector_title", "year"], how="left").sort_values(
        ["sector_title", "year", "field_title"], ascending=[True, True, True]).reset_index().drop("index", axis=1)

    wipo = pd.merge(wipoI1, wipoA1, on=["sector_title", "year", "field_title"], how="outer")
    wipo[["invSubCount", "invCount", "assiSubCount", "assiCount"]] = np.where(wipo.filter(regex=".*Count").isnull(), 0,
                                                                              wipo[["invSubCount", "invCount",
                                                                                    "assiSubCount", "assiCount"]])
    wipo = wipo.sort_values(["sector_title", "field_title", "year"], ascending=[True, True, True])

    # Make nested lists of dataframes to combine into JSON
    countries1 = countries[["countryName", "year", "invCount", "assiCount"]].drop_duplicates()

    countrycounts1 = countries1.groupby(["countryName"])["invCount"].apply(list).reset_index()
    countrycounts2 = countries1.groupby(["countryName"])["assiCount"].apply(list).reset_index()

    countrycounts = pd.merge(countrycounts1, countrycounts2, on=["countryName"], how="inner")
    countrycounts.columns = ["countryName", "inventorCounts", "assigneeCounts"]

    # Combine inventorCounts and assigneeCounts, by countries+sector titles

    countriesList1 = countries.groupby(["countryName", "sector_title"])["invSubCount"].apply(list).reset_index()
    countriesList2 = countries.groupby(["countryName", "sector_title"])["assiSubCount"].apply(list).reset_index()

    countriesList = pd.merge(countriesList1, countriesList2, on=["countryName", "sector_title"], how="inner")
    countriesList.columns = ["countryName", "sector_title", "inventorCounts", "assigneeCounts"]

    # Combine sector_title, inventorCounts and assigneeCounts as a new column techSectors, by countries
    countrynames = list(countries.groupby("countryName").size().index)

    countriesList_f = {}
    countriesList_f["countryName"] = {}
    countriesList_f["techSectors"] = {}


    def combine_rows(index, new_series, series_to_combine, i):
        if index not in new_series:
            new_series[index] = {}
            new_series[index]["id"] = []
            new_series[index]["inventorCounts"] = []
            new_series[index]["assigneeCounts"] = []

        new_series[index]["id"].append(series_to_combine["sector_title"][i])
        new_series[index]["inventorCounts"].append(series_to_combine["inventorCounts"][i])
        new_series[index]["assigneeCounts"].append(series_to_combine["assigneeCounts"][i])
        return new_series


    for i in range(countriesList.shape[0]):
        key_index = str(countrynames.index(countriesList["countryName"][i]))

        countriesList_f["countryName"][key_index] = countriesList["countryName"][i]

        countriesList_f["techSectors"] = combine_rows(key_index, countriesList_f["techSectors"], countriesList, i)

    countriesList = pd.DataFrame(countriesList_f)

    countriesList = pd.merge(countriesList, countrycounts, on="countryName", how="inner")
    countriesList = countriesList[["countryName", "inventorCounts", "assigneeCounts", "techSectors"]]
    countriesList.columns = ["id", "inventorCounts", "assigneeCounts", "techSectors"]

    # Convert to pretty json
    countries_tojson = []
    for i in range(countriesList.shape[0]):
        countries_tojson.append({})
        countries_tojson[i]["id"] = countriesList["id"][i]
        countries_tojson[i]["inventorCounts"] = countriesList["inventorCounts"][i]
        countries_tojson[i]["assigneeCounts"] = countriesList["assigneeCounts"][i]
        countries_tojson[i]["techSectors"] = []
        for s in range(len(countriesList["techSectors"][i]["id"])):
            countries_tojson[i]["techSectors"].append({})
            countries_tojson[i]["techSectors"][s]["id"] = countriesList["techSectors"][i]["id"][s]
            countries_tojson[i]["techSectors"][s]["inventorCounts"] = countriesList["techSectors"][i]["inventorCounts"][
                s]
            countries_tojson[i]["techSectors"][s]["assigneeCounts"] = countriesList["techSectors"][i]["assigneeCounts"][
                s]

    states1 = states[["stateName", "year", "invCount", "assiCount"]].drop_duplicates()

    statecounts1 = states1.groupby(["stateName"])["invCount"].apply(list).reset_index()
    statecounts2 = states1.groupby(["stateName"])["assiCount"].apply(list).reset_index()

    statecounts = pd.merge(statecounts1, statecounts2, on=["stateName"], how="inner")
    statecounts.columns = ["stateName", "inventorCounts", "assigneeCounts"]

    # Combine inventorCounts and assigneeCounts, by countries+sector titles

    statecounts1 = states.groupby(["stateName", "sector_title"])["invSubCount"].apply(list).reset_index()
    statecounts2 = states.groupby(["stateName", "sector_title"])["assiSubCount"].apply(list).reset_index()

    statesList = pd.merge(statecounts1, statecounts2, on=["stateName", "sector_title"], how="inner")
    statesList.columns = ["stateName", "sector_title", "inventorCounts", "assigneeCounts"]

    # Combine sector_title, inventorCounts and assigneeCounts as a new column techSectors, by countries
    statenames = list(states.groupby("stateName").size().index)

    statesList_f = {}
    statesList_f["stateName"] = {}
    statesList_f["techSectors"] = {}

    for i in range(statesList.shape[0]):
        key_index = str(statenames.index(statesList["stateName"][i]))

        statesList_f["stateName"][key_index] = statesList["stateName"][i]

        statesList_f["techSectors"] = combine_rows(key_index, statesList_f["techSectors"], statesList, i)

    statesList = pd.DataFrame(statesList_f)

    statesList = pd.merge(statesList, statecounts, on="stateName", how="inner")
    statesList = statesList[["stateName", "inventorCounts", "assigneeCounts", "techSectors"]]
    statesList.columns = ["id", "inventorCounts", "assigneeCounts", "techSectors"]

    # Convert to pretty json
    states_tojson = []
    for i in range(statesList.shape[0]):
        states_tojson.append({})
        states_tojson[i]["id"] = statesList["id"][i]
        states_tojson[i]["inventorCounts"] = statesList["inventorCounts"][i]
        states_tojson[i]["assigneeCounts"] = statesList["assigneeCounts"][i]
        states_tojson[i]["techSectors"] = []
        for s in range(len(statesList["techSectors"][i]["id"])):
            states_tojson[i]["techSectors"].append({})
            states_tojson[i]["techSectors"][s]["id"] = statesList["techSectors"][i]["id"][s]
            states_tojson[i]["techSectors"][s]["inventorCounts"] = statesList["techSectors"][i]["inventorCounts"][s]
            states_tojson[i]["techSectors"][s]["assigneeCounts"] = statesList["techSectors"][i]["assigneeCounts"][s]

    wipo1 = wipo[["sector_title", "year", "invCount", "assiCount"]].drop_duplicates()

    wipocounts1 = wipo1.groupby(["sector_title"])["invCount"].apply(list).reset_index()
    wipocounts2 = wipo1.groupby(["sector_title"])["assiCount"].apply(list).reset_index()

    wipocounts = pd.merge(wipocounts1, wipocounts2, on=["sector_title"], how="inner")
    wipocounts.columns = ["sector_title", "inventorCounts", "assigneeCounts"]

    # Combine inventorCounts and assigneeCounts, by countries+sector titles

    wipocounts1 = wipo.groupby(["field_title", "sector_title"])["invSubCount"].apply(list).reset_index()
    wipocounts2 = wipo.groupby(["field_title", "sector_title"])["assiSubCount"].apply(list).reset_index()

    wipoList = pd.merge(wipocounts1, wipocounts2, on=["field_title", "sector_title"], how="inner")
    wipoList.columns = ["field_title", "sector_title", "inventorCounts", "assigneeCounts"]

    # # Combine sector_title, inventorCounts and assigneeCounts as a new column techSectors, by countries
    wiponames = list(wipo.groupby("sector_title").size().index)

    wipoList_f = {}
    wipoList_f["sector_title"] = {}
    wipoList_f["techFields"] = {}


    def combine_rows2(index, new_series, series_to_combine, i):
        if index not in new_series:
            new_series[index] = {}
            new_series[index]["id"] = []
            new_series[index]["inventorCounts"] = []
            new_series[index]["assigneeCounts"] = []

        new_series[index]["id"].append(series_to_combine["field_title"][i])
        new_series[index]["inventorCounts"].append(series_to_combine["inventorCounts"][i])
        new_series[index]["assigneeCounts"].append(series_to_combine["assigneeCounts"][i])
        return new_series


    for i in range(wipoList.shape[0]):
        key_index = str(wiponames.index(wipoList["sector_title"][i]))

        wipoList_f["sector_title"][key_index] = wipoList["sector_title"][i]

        wipoList_f["techFields"] = combine_rows2(key_index, wipoList_f["techFields"], wipoList, i)

    wipoList = pd.DataFrame(wipoList_f)

    wipoList = pd.merge(wipoList, wipocounts, on="sector_title", how="inner")
    wipoList = wipoList[["sector_title", "inventorCounts", "assigneeCounts", "techFields"]]
    wipoList.columns = ["id", "inventorCounts", "assigneeCounts", "techFields"]

    # Convert to pretty json
    wipo_tojson = []
    for i in range(wipoList.shape[0]):
        wipo_tojson.append({})
        wipo_tojson[i]["id"] = wipoList["id"][i]
        wipo_tojson[i]["inventorCounts"] = wipoList["inventorCounts"][i]
        wipo_tojson[i]["assigneeCounts"] = wipoList["assigneeCounts"][i]
        wipo_tojson[i]["techFields"] = []
        for s in range(len(wipoList["techFields"][i]["id"])):
            wipo_tojson[i]["techFields"].append({})
            wipo_tojson[i]["techFields"][s]["id"] = wipoList["techFields"][i]["id"][s]
            wipo_tojson[i]["techFields"][s]["inventorCounts"] = wipoList["techFields"][i]["inventorCounts"][s]
            wipo_tojson[i]["techFields"][s]["assigneeCounts"] = wipoList["techFields"][i]["assigneeCounts"][s]

    output = {}
    output["startYear"] = int(startYear)
    output["endYear"] = countryI.year.max()

    output["countries"] = countries_tojson
    output["states"] = states_tojson
    output["techSectors"] = wipo_tojson

    f = open(go_live_path + "/comparison_data.json", "w",
             encoding="utf-8")
    f.write(json.dumps(output, default=str))
    f.close()

if __name__ == "__main__":
    run_comparison_flatfile(**{
        "execution_date": datetime.date(2023, 10, 1)
    })