import sys
import pymysql
import configparser
import argparse
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine
from lib.utils import get_connection_string

def set_dataframe(minyear, con):
    # cursor = con.cursor()
    query = '''
            SELECT p.patent_id, title, timescited, sector_title, field_title, i.persistent_inventor_id,
            CONCAT(i.name_first,' ',i.name_last) AS invname, i.num_patents AS invnumpat, il.country AS invcountry,
            il.state AS invstate, a.persistent_assignee_id, CONCAT_WS(' ',organization, a.name_first,a.name_last) AS assiname,
            a.num_patents AS assinumpat, al.country AS assicountry, al.state AS assistate
            FROM (
                  SELECT patent_id, title, num_times_cited_by_us_patents AS timescited
                  FROM patent
                  WHERE `year` >= %(min)s
                  ORDER BY timescited DESC
                  LIMIT 100
) AS p
                  LEFT JOIN patent_inventor AS pi ON p.patent_id = pi.patent_id
                  LEFT JOIN patent_assignee AS pa ON p.patent_id = pa.patent_id
                  LEFT JOIN wipo AS w ON p.patent_id = w.patent_id
                  LEFT JOIN wipo_field AS wf ON w.field_id = wf.id
                  LEFT JOIN inventor  AS i ON pi.inventor_id = i.inventor_id
                  LEFT JOIN assignee AS a ON pa.assignee_id = a.assignee_id
                  LEFT JOIN location AS il ON pi.location_id = il.location_id
                  LEFT JOIN location AS al ON pa.location_id = al.location_id'''
    data = pd.read_sql(query, con, params={"min": minyear})
    return data


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Process table parameters')
    parser.add_argument(
        '-c',
        type=str,
        nargs=1,
        help='File containing database config in INI format')
    parser.add_argument(
        '-d',
        type=str,
        nargs=1,
        help='Destination directory for data output')

    parser.add_argument(
        '-e',
        type=str,
        nargs=1,
        help='Environment Type')

    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.c[0])
    datadir = args.d[0]
    environment_type = "DEV"
    if args.e[0] == 1:
        environment_type = "PROD"

    engine = create_engine(get_connection_string(config, database="cred_database"))
    root_engine = create_engine(get_connection_string(config, database="app_database"))


    start_end = pd.read_sql("select `key`, `value` from config where `key` = 'webtool_relationship_start_year'", con=root_engine)
    minyear = list(start_end[start_end['key'] =='webtool_relationship_start_year']['value'])[0]
    print(minyear)
    pia = set_dataframe(minyear, engine)
    print("The original data has {} rows and {} columns.".format(pia.shape[1], pia.shape[0]))

    # Import state and country names
    countryNames = pd.read_excel(
        datadir + "/State+Country Codes.xlsx", \
        sheet_name="Country", header=None, names=["code", "countryName"])
    countryNames["code"] = np.where(countryNames["code"].isnull(), "NA", countryNames["code"])
    stateNames = pd.read_excel(datadir + "/State+Country Codes.xlsx", sheet_name="State", header=None,
                               names=["stateName", "code"])
    addon = pd.DataFrame([["District of Columbia", "DC"]], columns=["stateName", "code"])
    # stateNames = stateNames.append(addon, ignore_index=True)
    stateNames = pd.concat([stateNames, addon], ignore_index=True)

    pia_1 = pia[["patent_id", "title", "timescited", "field_title", "persistent_inventor_id", "persistent_assignee_id",
                 "assicountry", "invcountry", "assistate", "invstate"]]
    pia_1.columns = ["id", "description", "numCitations", "techField", "persistent_inventor_id",
                     "persistent_assignee_id",
                     "assicountry", "invcountry", "assistate", "invstate"]

    # Gather()
    c1 = pia_1[["assicountry"]].unstack().reset_index().drop("level_1", axis=1)
    c1.columns = ["countryentity", "country"]

    c2 = pia_1[["invcountry"]].unstack().reset_index().drop("level_1", axis=1)
    c2.columns = ["countryentity", "country"]

    s1 = pia_1[["assistate"]].unstack().reset_index().drop("level_1", axis=1)
    s1.columns = ["stateentity", "state"]

    s2 = pia_1[["invstate"]].unstack().reset_index().drop("level_1", axis=1)
    s2.columns = ["stateentity", "state"]

    com1 = c1.join(s1, how="outer")
    com1 = com1.join(pia_1, how="outer")

    com2 = c2.join(s1, how="outer")
    com2 = com2.join(pia_1, how="outer")

    com3 = c1.join(s2, how="outer")
    com3 = com3.join(pia_1, how="outer")

    com4 = c2.join(s2, how="outer")
    com4 = com4.join(pia_1, how="outer")

    # patents = com1.append(com2, ignore_index=True).append(com3, ignore_index=True).append(com4, ignore_index=True).drop(
    #     ["assicountry", "invcountry", "assistate", "invstate"], axis=1)
    patents = pd.concat([com1, com2, com3, com4], ignore_index=True).drop(
        ["assicountry", "invcountry", "assistate", "invstate"], axis=1)

    # Create USstates dataframe
    USstates = patents["state"].unique()
    USstates = pd.DataFrame(USstates, columns=["state"])
    USstates = pd.merge(USstates, stateNames, left_on=USstates.state, right_on=stateNames.code).sort_values("stateName",
                                                                                                            ascending=True).reset_index().drop(
        ["index", "key_0", "code"], axis=1)
    USstates = USstates[~USstates["state"].isnull()]
    USstates["stateIndex"] = USstates.index

    # Create countries dataframe
    countries = patents["country"].unique()
    countries = pd.DataFrame(countries, columns=["country"])
    countries = countries[~countries["country"].isnull()].reset_index().drop("index", axis=1)
    countries = pd.merge(countries, countryNames, how="left", left_on=countries.country,
                         right_on=countryNames.code).sort_values("countryName", ascending=True).reset_index().drop(
        ["index", "key_0", "code"], axis=1)
    countries = countries[~countries["country"].isnull()]
    countries["countryIndex"] = countries.index

    # Create inventors dataframe
    inventors = pia[["persistent_inventor_id", "invname", "invnumpat"]]
    inventors.columns = ["id", "name", "numPatents"]
    inventors = inventors[~inventors["id"].isnull()].drop_duplicates().sort_values("name",
                                                                                   ascending=True).reset_index().drop(
        "index", axis=1)
    inventors["inventorIndex"] = inventors.index

    # Create assignees dataframe
    assignees = pia[["persistent_assignee_id", "assiname", "assinumpat"]]
    assignees.columns = ["id", "name", "numPatents"]
    assignees["name.Upper"] = assignees["name"].str.upper()
    assignees = assignees[~assignees["id"].isnull()].drop_duplicates().sort_values("name.Upper",
                                                                                   ascending=True).reset_index().drop(
        "index", axis=1)
    assignees = assignees.drop("name.Upper", axis=1)
    assignees["numPatents"] = assignees["numPatents"].astype("int64")
    assignees["assigneeIndex"] = assignees.index

    # Create fields dataframe
    fields = pd.read_sql("SELECT * FROM wipo_field", engine)
    fields["id"] = fields["id"].astype("str")
    fields = fields.rename(columns={"id": "fieldId"})

    # Combine rows with the same sector_title, the same as fieldsNested dataframe in R codes
    fieldsNested_name = list(fields.groupby("sector_title").size().index)
    tmp = {}
    for i in range(fields.shape[0]):
        if fields.sector_title[i] not in tmp:
            tmp[fields.sector_title[i]] = {"id": [], "name": []}
        tmp[fields.sector_title[i]]["id"].append(fields["fieldId"][i])
        tmp[fields.sector_title[i]]["name"].append(fields["field_title"][i])

    # Convert the fieldsNested to json format for output
    tech_json = {}
    tech_json["techSectors"] = []
    for sector_id in range(len(tmp)):
        tech_json["techSectors"].append({})
        tech_json["techSectors"][sector_id]["name"] = list(tmp.keys())[sector_id]
        tech_json["techSectors"][sector_id]["techFields"] = []
        for i in range(len(tmp[list(tmp.keys())[sector_id]]["id"])):
            tech_json["techSectors"][sector_id]["techFields"].append({})
            tech_json["techSectors"][sector_id]["techFields"][i]["id"] = tmp[list(tmp.keys())[sector_id]]["id"][i]
            tech_json["techSectors"][sector_id]["techFields"][i]["name"] = tmp[list(tmp.keys())[sector_id]]["name"][i]


    # Update patents dataframe by merging other dataframes, save as patents1
    patents1 = pd.merge(patents, inventors[["id", "inventorIndex"]], how="left",
                        left_on=patents["persistent_inventor_id"], right_on=inventors["id"]).drop(["id_y", "key_0"],
                                                                                                  axis=1)
    patents1 = pd.merge(patents1, assignees[["id", "assigneeIndex"]], how="left",
                        left_on=patents1["persistent_assignee_id"], right_on=assignees["id"]).drop(["id", "key_0"],
                                                                                                   axis=1)
    patents1 = pd.merge(patents1, fields[["fieldId", "field_title"]], how="left", left_on=patents1["techField"],
                        right_on=fields["field_title"]).drop(["field_title", "key_0"], axis=1)
    patents1 = pd.merge(patents1, USstates[["state", "stateIndex"]], on="state", how="left")
    patents1 = pd.merge(patents1, countries[["country", "countryIndex"]], on="country", how="left")
    patents1 = patents1.rename(columns={"id_x": "id"})

    patents1 = patents1[
        ["description", "id", "numCitations", "inventorIndex", "assigneeIndex", "fieldId", "countryIndex",
         "stateIndex"]].sort_values(["numCitations", "id"],
                                    ascending=[False, False]).drop_duplicates().reset_index().drop("index", axis=1)

    patents1["addon"] = patents1[["description", "id", "numCitations"]].apply(lambda x: "/".join(x.astype(str)), axis=1)

    key = list(patents1["addon"].unique())
    key2 = {}
    i = 0
    for k in key:
        key2[k] = i
        i += 1
    patents1["addon_index"] = patents1["addon"].apply(lambda x: key2[x])

    # Update patents1 dataframe by combining rows with the same [id, description, numCitations]
    p = {}
    p["id"] = {}
    p["description"] = {}
    p["numCitations"] = {}
    p["inventors"] = {}
    p["assignees"] = {}
    p["techFields"] = {}
    p["states"] = {}
    p["countries"] = {}


    def combine_rows(index, new_series, series_to_combine, i):
        if index not in new_series:
            new_series[index] = []
        if series_to_combine[i] not in new_series[index]:
            if not np.isnan(series_to_combine[i]):
                new_series[index].append(int(series_to_combine[i]))
        return new_series


    def combine_rows2(index, new_series, series_to_combine, i):
        if index not in new_series:
            new_series[index] = []
        if series_to_combine[i] not in new_series[index]:
            new_series[index].append(series_to_combine[i])
        return new_series


    for i in range(patents1.shape[0]):
        key_index = str(patents1["addon_index"][i])

        p["id"][key_index] = patents1["id"][i]
        p["description"][key_index] = patents1["description"][i]
        p["numCitations"][key_index] = int(patents1["numCitations"][i])

        p["inventors"] = combine_rows(key_index, p["inventors"], patents1["inventorIndex"], i)
        p["assignees"] = combine_rows(key_index, p["assignees"], patents1["assigneeIndex"], i)
        p["techFields"] = combine_rows2(key_index, p["techFields"], patents1["fieldId"], i)
        p["states"] = combine_rows(key_index, p["states"], patents1["stateIndex"], i)
        p["countries"] = combine_rows(key_index, p["countries"], patents1["countryIndex"], i)

    patents1 = pd.DataFrame(p).sort_values("id").reset_index().drop("index", axis=1)

    # output json file
    output = {}

    patents_json = json.loads(patents1.to_json(orient="records"))
    output["patents"] = patents_json

    inventors_json = json.loads(inventors[["id", "name", "numPatents"]].to_json(orient="records"))
    output["inventors"] = inventors_json

    assignees_json = json.loads(assignees[["id", "name", "numPatents"]].to_json(orient="records"))
    output["assignees"] = assignees_json

    output["techSectors"] = tech_json["techSectors"]

    USstates = USstates.rename(columns={"stateName": "name"})
    states_json = json.loads(USstates[["name"]].to_json(orient="records"))
    output["states"] = states_json

    countries = countries.rename(columns={"countryName": "name"})
    countries_json = json.loads(countries[["name"]].to_json(orient="records"))
    output["countries"] = countries_json

    f = open(datadir + "/relationship_data.json", "w",
             encoding="utf-8")
    f.write(json.dumps(output, indent=4))
    f.close()
