import ast
import codecs
import csv
import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from QA.government_interest.GovtInterestTester import GovtInterestTester
from lib.configuration import get_connection_string, get_current_config, get_version_indicator


def upload_new_orgs(post_manual, engine):
    data = pd.read_csv(post_manual + '/new_organizations.csv')
    data.fillna("", inplace=True)
    ex = data.values.tolist()
    cursor = engine.connect()
    # this is because the auto increment isn't working for some reason
    id_code = cursor.execute("select max(organization_id) from government_organization;")
    max_id = [i for i in id_code][0][0]
    print(max_id)

    for i in range(len(ex)):
        data = cursor.execute("select * from government_organization where name ='" + ex[i][0] + "'")
        results = [i for i in data]
        if len(results) < 1:  # so if the organization is really not there
            org_id = max_id + i + 1  # adding one so we start with the next id after the original max
            row = str(tuple([int(org_id)] + [str(item) for item in ex[i]]))
            cursor.execute(
                "insert into government_organization (organization_id, name, level_one, level_two, level_three) values {0};".format(
                    row))


def row_lookup(row):
    if row['match'] is not np.nan and row['match'] != '':
        return row['match']
    elif row['non_government'] is not np.nan and row['non_government'] != '':
        return 'non_government'
    elif row['new'] is not np.nan and row['new'] != '':
        return row['new']


def create_dict(pre_manual, post_manual, persistent_files):
    data = []
    # this deals with the encoding issues we get from the QA people opening it in excel (I think thats what causes the problem)
    with open('{}/to_check_checked.csv'.format(post_manual), 'rb') as myfile:
        rows = csv.reader(codecs.iterdecode(myfile, 'utf-8', errors='ignore'))
        for row in rows:
            data.append(row)
    lookup_data = pd.DataFrame(data)
    lookup_data.columns = lookup_data.iloc[0]
    lookup_data = lookup_data.reindex(lookup_data.index.drop(0))

    lookup_data["clean"] = lookup_data.apply(lambda row: row_lookup(row), axis=1)
    lookup_data = lookup_data[['organization', 'clean']].dropna()
    lookup_data = lookup_data.rename(columns={'organization': 'original'})

    existing_lookup = pd.read_csv(persistent_files + '/existing_orgs_lookup.csv')
    existing_lookup = existing_lookup[['original', 'clean']].dropna()

    existing_plus_manual = pd.concat([lookup_data, existing_lookup])
    existing_plus_manual = existing_plus_manual.drop_duplicates()
    # TODO: uncomment this
    existing_plus_manual.to_csv(persistent_files + '/existing_orgs_lookup.csv', index=False)

    # automatically matched ones are not added to the persistent lookup
    # this is to prevent errors propagating and being hard to fix
    auto_matched = pd.read_csv('{}/automatically_matched.csv'.format(post_manual))
    auto_matched = auto_matched.rename(columns={'organization': 'original', 'solid': 'clean'})
    all_lookup = pd.concat([existing_plus_manual, auto_matched])

    # Creating a dictionary with key = raw.organization name and value = cleansed.organization
    original = [item.upper() for item in all_lookup['original']]
    dict_clean_org = dict(zip(original, all_lookup['clean']))
    return dict_clean_org


def lookup_raw_org(row, dict_clean_org, missed_orgs):
    if row['gi_statement'] is np.nan:
        return np.nan
    else:
        raw_org_list = row['orgs']
        if not raw_org_list is np.nan:
            orgs = raw_org_list.upper().split('|')
            looked_up = set()
            for org in orgs:
                if org in dict_clean_org.keys():
                    looked_up_org = dict_clean_org[org]
                    if not 'non_government' in looked_up_org:  # this is because of issues iwth spaces
                        for sub_org in looked_up_org.split('|'):
                            looked_up.add(sub_org.strip(' '))
                else:
                    missed_orgs.append(org)
            # we ONLY want 'United States Government' if there are no other orgs
            if len(looked_up) > 1 and "United States Government" in looked_up:
                looked_up.remove("United States Government")
            elif len(looked_up) == 0:
                looked_up = {"United States Government"}
        else:
            looked_up = {"United States Government"}
        set_org = ("|".join(looked_up).strip(' \t\n\r'))
        return set_org


def process_NER(pre_manual, post_manual, dict_clean_org):
    NER_results = pd.read_csv('{}/NER_output.csv'.format(pre_manual))
    missed_orgs = []
    NER_results['looked_up'] = NER_results.apply(lookup_raw_org, axis=1, args=(dict_clean_org, missed_orgs), )
    pd.DataFrame(missed_orgs).to_csv('{}/missed_orgs.csv'.format(post_manual))
    NER_results.to_csv('{}/lookedup_NER_output.csv'.format(post_manual))
    return NER_results


def readOrgs(db_cursor):
    org_dict = {}
    orgs = db_cursor.execute('SELECT organization_id, name FROM government_organization;')
    for row in orgs:
        org_dict[row[1].upper()] = int(row[0])
    return org_dict


def push_orgs(looked_up_data, org_id_mapping, config, version_indicator,database='TEMP_UPLOAD_DB'):
    missed = {}
    engine = create_engine(get_connection_string(config, database=database))
    post_manual = '{}/government_interest/post_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    engine.execute('set foreign_key_checks=0')
    for idx, row in looked_up_data.iterrows():
        patent_id = row['patent_id']
        gi_statement = row['gi_statement']
        if row['looked_up'] is not np.nan:
            orgs = row['looked_up'].split('|')
            # initialize set to track all orgs for each gi patent
            all_orgs = set()
            for org in orgs:
                if org.upper() in org_id_mapping.keys():
                    org_id = org_id_mapping[org.upper()]
                    all_orgs.add(org_id)
                else:
                    missed[patent_id] = org
            for org_id in list(all_orgs):
                query = "INSERT IGNORE INTO patent_govintorg (patent_id, organization_id, version_indicator) VALUES ('{}', '{}', '{}');".format(
                    patent_id, org_id, version_indicator)
                cursor = engine.connect()
                cursor.execute(query)
                cursor.close()
        if row['contracts'] is not np.nan:
            contracts = ast.literal_eval(row['contracts'])
            # contracts = list(set(row['contracts'].split('|')))
            for contract_award_no in contracts:
                if contract_award_no is not None:
                    query = "INSERT IGNORE INTO patent_contractawardnumber (patent_id, contract_award_number, version_indicator) values ('{}', '{}', '{}')".format(
                        patent_id, contract_award_no, version_indicator)
                    cursor = engine.connect()
                    cursor.execute(query)
                    cursor.close()
    missed_org_list = list(set(missed.keys()))
    missed_org_count = [missed[item] for item in missed_org_list]
    total_missed_orgs = pd.DataFrame(missed_org_list, missed_org_count)
    total_missed_orgs.to_csv('{}/incorrect_clean_orgs.csv'.format(post_manual))


def process_post_manual(doctype='granted_patent',database='TEMP_UPLOAD_DB', **kwargs):
    config = get_current_config(type=doctype, **kwargs)
    persistent_files = config['FOLDERS']['PERSISTENT_FILES']
    pre_manual = '{}/government_interest/pre_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    post_manual = '{}/government_interest/post_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    # engine = create_engine(get_connection_string(config, database=database))
    full_db_engine = create_engine(get_connection_string(config, 'RAW_DB'))
    # upload the new government organization we manually identified
    # upload_new_orgs(post_manual, engine)
    version_indicator = config['DATES']['END_DATE']

    # make and update the dictionary mapping original to clean org name
    dict_clean_org = create_dict(pre_manual, post_manual, persistent_files)

    missed_orgs = []  # global variable to hold the organizations we miss
    # create a dataframe with looked up organizations and contract award numbers
    looked_up = process_NER(pre_manual, post_manual, dict_clean_org)

    # get the mapping of orgs to org_ids
    org_id_mapping = readOrgs(full_db_engine)

    # push the mappings into the db
    push_orgs(looked_up, org_id_mapping, config, version_indicator, database=database)


def qc_gi(doctype='granted_patent',**kwargs):
    config = get_current_config(type=doctype, **kwargs)
    qc = GovtInterestTester(config)
    qc.runTests()


if __name__ == '__main__':
    from dateutil import rrule

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.date(2020, 12, 29), until=datetime.date(2021, 3, 23)):
        process_post_manual(**{
            "execution_date": dt
        })
        break
