import datetime
import os
import re

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
# Requires: solid matched organizations dataframe
# Modifies: list of solid matches
# Effects: if solid match has non_government as extra tag, removes non_government tag
from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_current_config
from lib.utilities import better_title


def non_gov_check(matched_df):
    # loop through matched_df rows
    for index, row in matched_df.iterrows():
        solid_values = row['solid'].split('|')
        # check list of solid matches
        if len(solid_values) > 1 and 'non_government' in solid_values:
            # if solid match found and non_government, remove non_government
            solid_values.remove('non_government')
            matched_df.at[index, 'solid'] = '|'.join(solid_values)

    return matched_df


def clean_matchlist(match_list):
    if len(match_list) == 0:
        match_list = np.nan
    else:
        match_list = '|'.join(match_list)
    return match_list


def fuzzy_match(org, govt_acc_dict, existing_lookup):
    all_possible_long = [item.strip() for item in govt_acc_dict.values()] + [item.strip() for item in
                                                                             existing_lookup.values()]
    solid_matches = []
    possible_matches = []
    if len(org.split(' ')) >= 3:
        results = process.extract(org, all_possible_long, scorer=fuzz.partial_ratio)
    else:
        results = process.extract(org, all_possible_long, scorer=fuzz.ratio)

    first_result = True
    for result in results:
        score = result[1]
        matched_org = result[0]
        len_dif = abs(len(org) - len(matched_org)) / len(org)
        if len_dif < 2:
            if score > 90:  # small acronyms match too many things
                if matched_org in ["Army", "Air Force",
                                   "Navy"]:  # special procesing to avoid losing information on longer military division
                    if len(matched_org) <= len(org) + 4:
                        solid_matches.append(matched_org)
                    else:
                        possible_matches.append(matched_org)
                else:
                    solid_matches.append(matched_org)
            elif score > 60:
                possible_matches.append(matched_org)
        elif len_dif > 2 and score > 90 and len(org) > 4:
            possible_matches.append(matched_org)
    return solid_matches, possible_matches


def match(org, govt_acc_dict, existing_lookup):
    solid_matches = []
    possible_matches = []

    for acronym, name in govt_acc_dict.items():
        if re.search(r'\b' + acronym + r'\b', org):
            solid_matches.append(name)
        if name in org:
            solid_matches.append(name)
    for existing, clean in existing_lookup.items():
        if (' ' + existing.lower() + ' ') in org.lower() or (' ' + clean.lower() + ' ') in (' ' + org.lower() + ' '):
            solid_matches.append(clean)
    solid_fuzzy, possible_fuzzy = fuzzy_match(org, govt_acc_dict, existing_lookup)
    solid_matches.extend(solid_fuzzy)
    # convert to set to get rid of duplicates
    solid_matches = list(set(solid_matches))
    possible_matches = list(set(possible_matches))
    # only get possible matches if there aren't solid matches
    if len(solid_matches) < 1:
        possible_matches.extend(possible_fuzzy)
    # if there are too many solid matches, put them in possible for review
    if len(solid_matches) > 2:
        possible_matches.extend(solid_matches)
        solid_matches = []
    # switch empty lists for nan and join them with '|'
    solid_matches = clean_matchlist(solid_matches)
    possible_matches = clean_matchlist(possible_matches)
    return solid_matches, possible_matches


def get_data(persistent_files, pre_manual):
    existing_lookup_data = pd.read_csv("{}/existing_orgs_lookup.csv".format(persistent_files)).dropna()
    existing_lookup = dict(zip(existing_lookup_data['original'], existing_lookup_data['clean']))

    government_orgs = pd.read_csv("{}/list_of_government_agencies.csv".format(persistent_files))
    govt_acc_dict = dict(zip([item.strip() for item in government_orgs["Acronym"]],
                             [better_title(item.strip()) for item in government_orgs["Long_form"]]))

    # #Input from NER round
    orgs = pd.read_csv("{}/distinct_orgs.txt".format(pre_manual), delimiter="\t")

    gov_to_skip = ['Government', 'US Government', 'U.S. Government', 'United States Government']
    # there is only one column in orgs file
    organizations = [item for item in orgs.iloc[:, 0] if not item in gov_to_skip]

    return existing_lookup, govt_acc_dict, organizations


def perform_lookups(existing_lookup, govt_acc_dict, organizations, manual_inputs):
    all_solid = []
    all_possible = []
    for org in organizations:  # kinda slow, ~7 minutes for 6 months of data
        solid_for_org, possible_for_org = match(org, govt_acc_dict, existing_lookup)
        all_solid.append(solid_for_org)
        all_possible.append(possible_for_org)
    results = pd.DataFrame([organizations, all_solid, all_possible]).T
    results.columns = ['organization', 'solid', 'possible']
    matched = results[~pd.isnull(results['solid'])][['organization', 'solid']]

    # Need to remove non_government from solid matched orgs (avoid post_manual.py issues)
    matched = non_gov_check(matched)
    matched.to_csv('{}/automatically_matched.csv'.format(manual_inputs), index=False)

    to_check = results[~pd.isnull(results['possible'])][['organization', 'possible']]
    to_check['match'] = ''
    to_check['new'] = ''
    to_check['non_government'] = ''
    to_check.to_csv('{}/to_check.csv'.format(manual_inputs), index=False)


def get_orgs(db_con, manual_inputs):
    raw = pd.read_sql("select * from government_organization", db_con)
    raw.to_csv(manual_inputs + "/government_organization.csv", index=False)


def process_ner_to_manual(dbtype='granted_patent', **kwargs):
    config = get_current_config(type=dbtype, **kwargs)
    persistent_files = config['FOLDERS']['PERSISTENT_FILES']
    pre_manual = '{}/government_interest/pre_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    manual_inputs = '{}/government_interest/manual_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
    post_manual = '{}/government_interest/post_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    for path in [pre_manual, manual_inputs, post_manual]:
        if not os.path.exists(path):
            os.makedirs(path)

    existing_lookup, govt_acc_dict, organizations = get_data(persistent_files, pre_manual)
    perform_lookups(existing_lookup, govt_acc_dict, organizations, manual_inputs)
    engine = create_engine(get_connection_string(config, 'RAW_DB'))
    get_orgs(engine, manual_inputs)


if __name__ == '__main__':
    process_ner_to_manual( **{
            "execution_date": datetime.date(2020, 12, 22)
            })
