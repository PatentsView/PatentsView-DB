import csv
import datetime
import re
import time
import pandas as pd
from sqlalchemy import create_engine
import scipy
from functools import partial

from tqdm import tqdm
tqdm.pandas()
from rapidfuzz import fuzz
import  multiprocess as mp
from multiprocess import Pool
import pymysql.cursors
from joblib import Parallel, delayed
from collections import defaultdict


import pickle
# from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from itertools import combinations

from QA.post_processing.AssigneePostProcessing import AssigneePostProcessingQC
from QA.post_processing.AssigneePostProcessingPhase2 import AssigneePostProcessingQCPhase2

from lib.configuration import get_connection_string, get_current_config, get_disambig_config
from updater.post_processing.create_lookup import load_lookup_table


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
        wkfolder=update_config['FOLDERS']['WORKING_FOLDER'],
        disamb_file="assignee_disambiguation.tsv")
    disambig_output = pd.read_csv(
        disambig_output_file,
        sep="\t",
        chunksize=300000,
        header=None,
        quoting=csv.QUOTE_NONE,
        names=['uuid', 'organization_id', 'person_id', 'organization', 'name'])
    count = 0
    engine.execute("TRUNCATE TABLE assignee_disambiguation_mapping")
    for disambig_chunk in disambig_output:
        disambig_chunk = disambig_chunk.assign(
            assignee_id=disambig_chunk.organization_id)
        disambig_chunk.assignee_id.fillna(disambig_chunk.person_id, inplace=True)
        disambig_chunk.drop(["organization_id", "person_id"], axis=1, inplace=True)
        count += disambig_chunk.shape[0]
        engine.connect()
        start = time.time()
        disambig_chunk[["uuid", "assignee_id"]].to_sql(name='assignee_disambiguation_mapping',
                                                       con=engine,
                                                       if_exists='append',
                                                       index=False,
                                                       method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
            end - start, 3),
            cnt=count))
        engine.dispose()


def update_rawassignee_for_type(update_config, database='RAW_DB', uuid_field='uuid'):
    ass_mapp_disambig = update_config["DISAMBIG_TABLES"]["ASSIGNEE"]
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = f"""
        UPDATE rawassignee ra left join {ass_mapp_disambig} adm
            on adm.uuid = ra.{uuid_field} 
        set  ra.assignee_id = adm.assignee_id
    """
    print(update_statement)
    engine.execute(update_statement)


def generate_disambiguated_assignees(update_config, engine, limit, offset):
    assignee_core_template = """
        SELECT assignee_id
        from disambiguated_assignee_ids order by assignee_id
        limit {limit} offset {offset}
    """

    assignee_data_template = """
        SELECT ra.assignee_id, ra.type, ra.name_first, ra.name_last, ra.organization, p.date as patent_date
        from rawassignee ra
                 join patent p on p.id = ra.patent_id
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra.assignee_id
        where ra.assignee_id is not null
        UNION 
        SELECT ra2.assignee_id, ra2.type, ra2.name_first, ra2.name_last, ra2.organization, a.date as patent_date
        from {pregrant_db}.rawassignee ra2
                 join {pregrant_db}.application a on a.document_number = ra2.document_number
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra2.assignee_id
        where ra2.assignee_id is not null;
    """
    assignee_core_query = assignee_core_template.format(limit=limit,
                                                        offset=offset)
    assignee_data_query = assignee_data_template.format(
        assign_core_query=assignee_core_query,
        pregrant_db=update_config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'])

    ## Query takes less than 2 minutes to run
    current_assignee_data = pd.read_sql_query(sql=assignee_data_query, con=engine)
    return current_assignee_data


def create_assignee(update_config):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    version_indicator = update_config['DATES']['END_DATE']
    suffix = update_config['DATES']['END_DATE']
    target_table = "assignee_{suffix}".format(suffix=suffix)
    drop_sql = f"""drop table if exists {target_table}"""
    print(drop_sql)
    engine.execute(drop_sql)
    create_sql = """
    CREATE TABLE `{target_table}` (
    `id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` int(4) DEFAULT NULL,
  `name_first` varchar(96) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(96) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `version_indicator` date NOT NULL DEFAULT '{version_indicator}',
  `created_date` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `assignee_version_indicator_index` (`version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """.format(version_indicator=version_indicator, target_table=target_table)
    # view_query = """
    # CREATE OR REPLACE SQL SECURITY INVOKER VIEW assignee as select * from {target_table}
    # """.format(target_table=target_table)
    engine.execute(create_sql)
    # engine.execute(view_query)
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_assignee_data = generate_disambiguated_assignees(
            update_config,
            engine,
            limit,
            offset)
        if current_assignee_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        step_time = time.time() - start
        canonical_assignments = assignee_reduce(current_assignee_data).rename({
            "assignee_id": "id"
        }, axis=1)
        canonical_assignments = canonical_assignments.assign(version_indicator=version_indicator)
        canonical_assignments.to_sql(name=target_table, con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def precache_assignees_ids(config):
    suffix = config['DATES']['END_DATE']
    query_list = []
    drop_query = f"""drop table if exists disambiguated_assignee_ids_{suffix} """
    query_list.append(drop_query)
    create_query = f"""
        CREATE TABLE disambiguated_assignee_ids_{suffix} (assignee_id varchar(256),  PRIMARY KEY (`assignee_id`))
        """.format(suffix=suffix)
    query_list.append(create_query)
    view_query = f"""
        CREATE OR REPLACE SQL SECURITY INVOKER VIEW disambiguated_assignee_ids as select assignee_id from disambiguated_assignee_ids_{suffix}
        """.format(suffix=suffix)
    query_list.append(view_query)
    assignee_cache_query = f"""
        INSERT IGNORE INTO disambiguated_assignee_ids_{suffix} (assignee_id)
        SELECT assignee_id
        from patent.rawassignee
        UNION
        SELECT assignee_id
        from pregrant_publications.rawassignee;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'], suffix=suffix)
    query_list.append(assignee_cache_query)
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    for query in query_list:
        print(query)
        engine.execute(query)


def assignee_reduce(assignee_data):
    assignee_key_fields = ['assignee_id', 'type', 'name_first', 'name_last', 'organization']
    assignee_data['help'] = assignee_data.groupby(assignee_key_fields)['assignee_id'].transform('count')
    out = assignee_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
        'assignee_id', keep='first').drop(
        ['help', 'patent_date'], 1)
    return out


def update_granted_rawassignee(**kwargs):
    config = get_disambig_config(schedule='quarterly', **kwargs)
    update_rawassignee_for_type(config, database='RAW_DB', uuid_field='uuid')


def update_pregranted_rawassignee(**kwargs):
    config = get_disambig_config(schedule='quarterly',**kwargs)
    update_rawassignee_for_type(config, database='PGPUBS_DATABASE', uuid_field='id')


def precache_assignees(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    precache_assignees_ids(config)


def create_canonical_assignees(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    create_assignee(config)


import csv
import datetime
import re
import time
import pandas as pd
from sqlalchemy import create_engine

from tqdm import tqdm
tqdm.pandas()
from thefuzz import fuzz
import  multiprocess as mp
from multiprocess import Pool
import pymysql.cursors

import pickle
# from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from itertools import combinations

from QA.post_processing.AssigneePostProcessing import AssigneePostProcessingQC
from QA.post_processing.AssigneePostProcessingPhase2 import AssigneePostProcessingQCPhase2

from lib.configuration import get_connection_string, get_current_config, get_disambig_config
from updater.post_processing.create_lookup import load_lookup_table


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
        wkfolder=update_config['FOLDERS']['WORKING_FOLDER'],
        disamb_file="assignee_disambiguation.tsv")
    disambig_output = pd.read_csv(
        disambig_output_file,
        sep="\t",
        chunksize=300000,
        header=None,
        quoting=csv.QUOTE_NONE,
        names=['uuid', 'organization_id', 'person_id', 'organization', 'name'])
    count = 0
    engine.execute("TRUNCATE TABLE assignee_disambiguation_mapping")
    for disambig_chunk in disambig_output:
        disambig_chunk = disambig_chunk.assign(
            assignee_id=disambig_chunk.organization_id)
        disambig_chunk.assignee_id.fillna(disambig_chunk.person_id, inplace=True)
        disambig_chunk.drop(["organization_id", "person_id"], axis=1, inplace=True)
        count += disambig_chunk.shape[0]
        engine.connect()
        start = time.time()
        disambig_chunk[["uuid", "assignee_id"]].to_sql(name='assignee_disambiguation_mapping',
                                                       con=engine,
                                                       if_exists='append',
                                                       index=False,
                                                       method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
            end - start, 3),
            cnt=count))
        engine.dispose()


def update_rawassignee_for_type(update_config, database='RAW_DB', uuid_field='uuid'):
    ass_mapp_disambig = update_config["DISAMBIG_TABLES"]["ASSIGNEE"]
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = f"""
        UPDATE rawassignee ra left join {ass_mapp_disambig} adm
            on adm.uuid = ra.{uuid_field} 
        set  ra.assignee_id = adm.assignee_id
    """
    print(update_statement)
    engine.execute(update_statement)


def generate_disambiguated_assignees(update_config, engine, limit, offset):
    assignee_core_template = """
        SELECT assignee_id
        from disambiguated_assignee_ids order by assignee_id
        limit {limit} offset {offset}
    """

    assignee_data_template = """
        SELECT ra.assignee_id, ra.type, ra.name_first, ra.name_last, ra.organization, p.date as patent_date
        from rawassignee ra
                 join patent p on p.id = ra.patent_id
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra.assignee_id
        where ra.assignee_id is not null
        UNION 
        SELECT ra2.assignee_id, ra2.type, ra2.name_first, ra2.name_last, ra2.organization, a.date as patent_date
        from {pregrant_db}.rawassignee ra2
                 join {pregrant_db}.application a on a.document_number = ra2.document_number
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra2.assignee_id
        where ra2.assignee_id is not null;
    """
    assignee_core_query = assignee_core_template.format(limit=limit,
                                                        offset=offset)
    assignee_data_query = assignee_data_template.format(
        assign_core_query=assignee_core_query,
        pregrant_db=update_config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'])

    current_assignee_data = pd.read_sql_query(sql=assignee_data_query, con=engine)
    return current_assignee_data


def create_assignee(update_config):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    version_indicator = update_config['DATES']['END_DATE']
    suffix = update_config['DATES']['END_DATE']
    target_table = "assignee_{suffix}".format(suffix=suffix)
    drop_sql = f"""drop table if exists {target_table}"""
    print(drop_sql)
    engine.execute(drop_sql)
    create_sql = """
    CREATE TABLE `{target_table}` (
    `id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` int(4) DEFAULT NULL,
  `name_first` varchar(96) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(96) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `version_indicator` date NOT NULL DEFAULT '{version_indicator}',
  `created_date` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `assignee_version_indicator_index` (`version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """.format(version_indicator=version_indicator, target_table=target_table)
    # view_query = """
    # CREATE OR REPLACE SQL SECURITY INVOKER VIEW assignee as select * from {target_table}
    # """.format(target_table=target_table)
    engine.execute(create_sql)
    # engine.execute(view_query)
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_assignee_data = generate_disambiguated_assignees(
            update_config,
            engine,
            limit,
            offset)
        if current_assignee_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        step_time = time.time() - start
        canonical_assignments = assignee_reduce(current_assignee_data).rename({
            "assignee_id": "id"
        }, axis=1)
        canonical_assignments = canonical_assignments.assign(version_indicator=version_indicator)
        canonical_assignments.to_sql(name=target_table, con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def precache_assignees_ids(config):
    suffix = config['DATES']['END_DATE']
    query_list = []
    drop_query = f"""drop table if exists disambiguated_assignee_ids_{suffix} """
    query_list.append(drop_query)
    create_query = f"""
        CREATE TABLE disambiguated_assignee_ids_{suffix} (assignee_id varchar(256),  PRIMARY KEY (`assignee_id`))
        """.format(suffix=suffix)
    query_list.append(create_query)
    view_query = f"""
        CREATE OR REPLACE SQL SECURITY INVOKER VIEW disambiguated_assignee_ids as select assignee_id from disambiguated_assignee_ids_{suffix}
        """.format(suffix=suffix)
    query_list.append(view_query)
    assignee_cache_query = f"""
        INSERT IGNORE INTO disambiguated_assignee_ids_{suffix} (assignee_id)
        SELECT assignee_id
        from patent.rawassignee
        UNION
        SELECT assignee_id
        from pregrant_publications.rawassignee;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'], suffix=suffix)
    query_list.append(assignee_cache_query)
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    for query in query_list:
        print(query)
        engine.execute(query)


def assignee_reduce(assignee_data):
    assignee_key_fields = ['assignee_id', 'type', 'name_first', 'name_last', 'organization']
    assignee_data['help'] = assignee_data.groupby(assignee_key_fields)['assignee_id'].transform('count')
    out = assignee_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
        'assignee_id', keep='first').drop(
        ['help', 'patent_date'], 1)
    return out


def update_granted_rawassignee(**kwargs):
    config = get_disambig_config(schedule='quarterly', **kwargs)
    update_rawassignee_for_type(config, database='RAW_DB', uuid_field='uuid')


def update_pregranted_rawassignee(**kwargs):
    config = get_disambig_config(schedule='quarterly',**kwargs)
    update_rawassignee_for_type(config, database='PGPUBS_DATABASE', uuid_field='id')


def precache_assignees(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    precache_assignees_ids(config)


def create_canonical_assignees(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    create_assignee(config)


# def additional_post_processing(**kwargs):
#     start = time.time()
#     config = get_current_config(schedule='quarterly', **kwargs)
#     suffix = config['DATES']['END_DATE']
#     assignee_table = f"assignee_{suffix}"
#     engine = create_engine(get_connection_string(config, "RAW_DB"))
#     #engine.execute("Drop table if exists assignee_reassignment_final")
#     assignee_data = pd.read_sql_table(assignee_table, con=engine)
#     # assignee_data = assignee_data.sample(frac=0.2, replace=True, random_state=1)
#     assignee_data = assignee_data.assign(gp=assignee_data.organization.str.lower().str[0:4])
#     unique_assignees = assignee_data.gp.unique()
#     gp_all_distances = []
#     for ass in unique_assignees:
#     # for ass in ['sony']:
#         temp_raw_data = assignee_data[assignee_data['gp']==ass]
#         unique_orgs = temp_raw_data.organization.unique()
#         new_dataframe_for_combos = pd.DataFrame(unique_orgs, columns=["organization"])
#         temp = generate_combinations(new_dataframe_for_combos)
#         gp_all_distances.append(temp)
#
#     documents = collect_training(gp_all_distances)
#     tfidf_vectorizer = CountVectorizer(ngram_range=(2, 10), lowercase=True, analyzer='char_wb')
#     tfidf_model = tfidf_vectorizer.fit(documents)
#     remap = remapping(gp_all_distances, tfidf_model)
#     connections = build_network_graph(remap)
#     clusters = build_clusters(connections)
#     melted_assignment = pd.DataFrame.from_dict(clusters, orient='index').reset_index().melt(id_vars='index')
#     final_assignment = melted_assignment[~melted_assignment.value.isna()]
#     final_assignment['value'].str.len().max()
#     final_assignment = final_assignment.rename(columns={'index': 'cluster', 'value': 'original_org_name'})
#     final_assignment.to_sql(name='assignee_reassignment_final', con=engine)
#     #

def process_gp(ass, assignee_data, manager_dict):
    temp_raw_data = assignee_data[assignee_data['gp'] == ass]
    unique_orgs = temp_raw_data['organization'].unique()
    new_dataframe_for_combos = pd.DataFrame(unique_orgs, columns=["organization"])
    result = generate_combinations(new_dataframe_for_combos)
    #result = generate_combinations_parallel(new_dataframe_for_combos)
    manager_dict[ass] = result

def additional_post_processing(**kwargs):
    start = time.time()
    config = get_current_config(schedule='quarterly', **kwargs)
    suffix = config['DATES']['END_DATE']
    assignee_table = f"assignee_{suffix}"
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    engine.execute("Drop table if exists assignee_reassignment_final")
    assignee_data = pd.read_sql_table(assignee_table, con=engine)
    assignee_data = assignee_data.assign(gp=assignee_data.organization.str.lower().str[0:4])
    unique_assignees = assignee_data.gp.unique()

    parallelism = int(config["PARALLELISM"]["parallelism"])
    # Create a manager to share data between processes
    with mp.Manager() as manager:
        # Create a dictionary to store results shared between processes
        shared_results = manager.dict()
        # Create a pool of processes
        with mp.Pool() as pool:
            # Apply the process_gp function to each unique assignee in parallel
            pool.starmap(process_gp, [(ass, assignee_data, shared_results ) for ass in unique_assignees])
        # Combine the results from all processes
        gp_all_distances = [shared_results[ass] for ass in unique_assignees]
    # record end time
    end = time.time()
    print(f"The total time to process all the distances is {end - start}")

    documents = collect_training_parallel(gp_all_distances, parallelism)
    tfidf_vectorizer = CountVectorizer(ngram_range=(2, 10), lowercase=True, analyzer='char_wb')
    tfidf_model = tfidf_vectorizer.fit(documents)
    remap = remapping(gp_all_distances, tfidf_model)
    connections = build_network_graph(remap)
    clusters = build_clusters(connections)
    melted_assignment = pd.DataFrame.from_dict(clusters, orient='index').reset_index().melt(id_vars='index')
    final_assignment = melted_assignment[~melted_assignment.value.isna()]
    final_assignment['value'].str.len().max()
    final_assignment = final_assignment.rename(columns={'index': 'cluster', 'value': 'original_org_name'})
    final_assignment.to_sql(name='assignee_reassignment_final', con=engine)


def additional_post_processing_update_queries(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    suffix = config['DATES']['END_DATE']
    ass_table = f"assignee_{suffix}"
    adm_table = f"assignee_disambiguation_mapping_{suffix}"
    query_list = []
    db_list = ["patent", "pregrant_publications"]
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    for db in db_list:
        query_0 = f"alter table {db}.{adm_table} add index assignee_id (assignee_id)"
        query_list.append(query_0)
        if db == 'patent':
            query_1 = f"alter table patent.{ass_table} add index id (id)"
            query_list.append(query_1)
            query_2 = f"alter table patent.{ass_table} add index organization (organization)"
            query_list.append(query_2)
            query_3 = f" alter table patent.assignee_reassignment_final add index `cluster` (`cluster`)"
            query_list.append(query_3)
            query_4 = f"alter table patent.assignee_reassignment_final add  index `original_org_name` (`original_org_name`)"
            query_list.append(query_4)
        query_5 = f"""
        update {db}.{adm_table} adm
            join patent.{ass_table} a on a.id = adm.assignee_id
            join patent.assignee_reassignment_final arr on arr.`original_org_name` collate utf8mb4_bin = a.organization
            join patent.{ass_table} a2 on arr.`cluster` collate utf8mb4_bin = a2.organization
        set adm.assignee_id=a2.id;
        """
        query_list.append(query_5)
        query_6 = f"""
        update {db}.rawassignee r
            join patent.{ass_table} a on a.id = r.assignee_id
            join patent.assignee_reassignment_final arr on a.organization = arr.`original_org_name` collate utf8mb4_bin  
            join patent.{ass_table} a2 on arr.`cluster` collate utf8mb4_bin = a2.organization
        set r.assignee_id=a2.id;
            """
        query_list.append(query_6)
    for q in query_list:
        print(q)
        engine.execute(q)

def build_network_graph(remapping):
    import networkx as nx
    G = nx.Graph()
    G.add_edges_from(remapping)
    # print(len(G.nodes))
    return nx.connected_components(G)

# def build_clusters(connections):
#     clusters = {}
#     for component in connections:
#         top_org = None
#         for node in component:
#             if top_org is None:
#                 top_org = node
#                 clusters[top_org] = []
#             else:
#                 clusters[top_org].append(node)
#     return clusters

def build_clusters(connections):
    clusters = defaultdict(list)
    for component in connections:
        if isinstance(component, set):
            component = list(component)  # Convert set to list
        if component:  # Ensure component is not empty
            top_org = component[0]  # Assigning the first node as the top_org
            clusters[top_org].extend(component[1:])  # Appending the rest of the nodes to the cluster
    return clusters

def remapping(gp_all_distances, tfidf_model):
    remapping = []
    # Remove empty dictionaries using list comprehension
    filtered_list = [d for d in gp_all_distances if d]
    for record in tqdm(filtered_list, desc='Assignee Remapping', position=0, leave=True, miniters=100000, maxinterval=1200):
        for top_org_idx in record:
            org_std_lower = str(record[top_org_idx]['org_std']).lower()
            for comparison in record[top_org_idx]['comparison']:
                X = tfidf_model.transform([org_std_lower,str(comparison['org_std']).lower()])

                rowsum = X.sum(axis=1)
                ratio = rowsum[0, 0] / rowsum[1, 0]
                if 0.85 < ratio < 1.15:
                    remapping.append([str(record[top_org_idx]['org']), str(comparison['org'])])
    return remapping

def collect_documents(record):
    documents = []
    for top_org_idx in record:
        for comparison in record[top_org_idx]['comparison']:
            documents.append(str(record[top_org_idx]['org_std']))
            documents.append(str(comparison['org_std']))
    return documents


def collect_training_parallel(gp_all_distances, num_processes=None):
    with Pool() as pool:
        results = pool.map(collect_documents, gp_all_distances)

    # Flatten the list of lists returned by Pool.map
    documents = [item for sublist in results for item in sublist]

    return documents

# def collect_training(gp_all_distances):
#     documents = []
#     for record in gp_all_distances:
#         for top_org_idx in record:
#             for comparison in record[top_org_idx]['comparison']:
#                 documents.append(str(record[top_org_idx]['org_std']))
#                 documents.append(str(comparison['org_std']))
#     return documents

def compute_overlap(a_b, gp_org_std, gp_org):
    a, b = a_b
    top_org_std = gp_org_std[a]
    comparison_org_std = gp_org_std[b]

    overlap = max(fuzz.token_set_ratio(top_org_std, comparison_org_std),
                  fuzz.ratio(top_org_std, comparison_org_std),
                  fuzz.partial_ratio(top_org_std, comparison_org_std),
                  fuzz.partial_token_set_ratio(top_org_std, comparison_org_std))

    if overlap > 95:
        return {'a': a, 'b': b, 'top_org': gp_org[a], 'comparison_org': gp_org[b], 'overlap': overlap}
    else:
        return None
# def generate_combinations_parallel(gp):
#     distances = {}
#     if gp.shape[0] > 5000 or gp.shape[0] < 2:
#         return distances
#
#     gp['org_std'] = gp['organization'].apply(lambda org: re.sub('[^A-Za-z0-9]+', '', str(org)))
#     gp_index = gp.index
#     gp_org_std = gp['org_std'].values
#     gp_org = gp['organization'].values
#
#     x = list(combinations(gp_index, 2))
#     l = len(x)
#
#     with Pool() as pool:
#         results = list(tqdm(pool.imap(partial(compute_overlap, gp_org_std=gp_org_std, gp_org=gp_org), x), total=l))
#
#     for result in results:
#         if result is not None:
#             a = result['a']
#             if a not in distances:
#                 distances[a] = {'org': result['top_org'], 'org_std': gp_org_std[a], 'comparison': []}
#             distances[a]['comparison'].append(
#                 {'org': result['comparison_org'], 'org_std': gp_org_std[result['b']], 'comparison_idx': result['b'], 'overlap': result['overlap']})
#     return distances
def generate_combinations(gp):
    distances = {}
    if gp.shape[0] > 5000 or gp.shape[0] < 2:
        return distances

    # Precompute normalized organization names
    gp['org_std'] = gp['organization'].apply(lambda org: re.sub('[^A-Za-z0-9]+', '', str(org)))
    # Extract required columns
    gp_index = gp.index
    gp_org_std = gp['org_std'].values
    gp_org = gp['organization'].values

    # Compute combinations
    x = list(combinations(gp_index, 2))
    l = len(x)
    for a, b in tqdm(x, total=l):
        top_org_std = gp_org_std[a]
        comparison_org_std = gp_org_std[b]

        overlap = max(fuzz.token_set_ratio(top_org_std, comparison_org_std),
                      fuzz.ratio(top_org_std, comparison_org_std),
                      fuzz.partial_ratio(top_org_std, comparison_org_std),
                      fuzz.partial_token_set_ratio(top_org_std, comparison_org_std))
        if overlap > 95:
            if a not in distances:
                distances[a] = {'org': gp_org[a], 'org_std': top_org_std, 'comparison': []}
            distances[a]['comparison'].append(
                {'org': gp_org[b], 'org_std': comparison_org_std, 'comparison_idx': b, 'overlap': overlap})
    return distances

# def generate_combinations(gp):
#     distances = {}
#     # print(gp.gp)
#     if gp.shape[0] > 5000 or gp.shape[0] < 2:
#         return distances
#     x = list(combinations(gp.index, 2))
#     l = len([y for y in x])
#     for a, b in tqdm(x, total=l):
#         top_org = gp.loc[a, 'organization']
#         comparison_org = gp.loc[b, 'organization']
#         top_org_std = re.sub('[^A-Za-z0-9]+', '', str(top_org))
#         comparison_org_std = re.sub('[^A-Za-z0-9]+', '', str(comparison_org))
#         overlap = max(fuzz.token_set_ratio(top_org_std, comparison_org_std),
#                       fuzz.ratio(top_org_std, comparison_org_std),
#                       fuzz.partial_ratio(top_org_std, comparison_org_std),
#                       fuzz.partial_token_set_ratio(top_org_std, comparison_org_std))
#         if overlap > 95:
#             if a not in distances:
#                 distances[a] = {'org': top_org, 'org_std': top_org_std, 'comparison': []}
#             distances[a]['comparison'].append(
#                 {'org': comparison_org, 'org_std': comparison_org_std, 'comparison_idx': b, 'overlap': overlap})
#     return distances

# def load_granted_lookup(**kwargs):
#     config = get_current_config(schedule='quarterly', **kwargs)
#     load_lookup_table(update_config=config, database='RAW_DB', parent_entity='patent',
#                       parent_entity_id='patent_id', entity='assignee',
#                       include_location=True, location_strict=False)

def load_granted_location_assignee(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='RAW_DB', parent_entity='location',
                      parent_entity_id='location_id', entity='assignee',
                      include_location=True, location_strict=True)

# def load_pregranted_lookup(**kwargs):
#     config = get_current_config(schedule='quarterly', **kwargs)
#     load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='publication',
#                       parent_entity_id='document_number', entity="assignee",
#                       include_location=True)

def load_pregranted_location_assignee(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='location',
                      parent_entity_id='location_id', entity='assignee',
                      include_location=True, location_strict=True)

def create_patent_assignee(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    q_list = []
    engine = create_engine(get_connection_string(config, 'RAW_DB'))
    q0 = """
    truncate table patent_assignee"""
    q_list.append(q0)
    q1 = """
INSERT IGNORE INTO patent_assignee 
(patent_id, assignee_id, sequence, location_id, version_indicator) 
SELECT patent_id, et.assignee_id, et.sequence,  location_id, et.version_indicator 
    from rawassignee et left join rawlocation rl on rl.id = et.rawlocation_id 
where assignee_id is not null;
    """
    q_list.append(q1)
    for q in q_list:
        print(q)
        engine.execute(q)

def create_publication_assignee(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    q_list = []
    engine = create_engine(get_connection_string(config, 'PGPUBS_DATABASE'))
    q0 = """
    truncate table publication_assignee"""
    q_list.append(q0)
    q1 = """
INSERT IGNORE INTO publication_assignee 
(document_number, assignee_id, sequence, location_id, version_indicator) 
SELECT document_number, et.assignee_id, et.sequence, location_id,  et.version_indicator 
    from rawassignee et left join rawlocation rl on rl.id = et.rawlocation_id 
where assignee_id is not null; 
    """
    q_list.append(q1)
    for q in q_list:
        print(q)
        engine.execute(q)

# def post_process_assignee(**kwargs):
#     update_granted_rawassignee(**kwargs)
#     update_pregranted_rawassignee(**kwargs)
#     precache_assignees(**kwargs)
#     create_canonical_assignees(**kwargs)
#     create_patent_assignee(**kwargs)
#     create_publication_assignee(**kwargs)

def check_largest_clusters(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    engine = create_engine(get_connection_string(config, 'RAW_DB'))
    cluster_query = """
select assignee_id, count(*) as records
from rawassignee
group by 1
order by 2 desc
limit 25;
    """
    df = pd.read_sql(cluster_query, con=engine)
    print(df)
    if df['records'][0] > 200000:
        raise Exception(f"ASSIGNEE DISAMBIGUATION OVER-CLUSTERED")
    if df['records'][0] < 50000:
        raise Exception(f"ASSIGNEE DISAMBIGUATION UNDER-CLUSTERED")


def additional_post_processing_assignee(**kwargs):
    additional_post_processing(**kwargs)
    ##No real improvement can be made in runtime
    additional_post_processing_update_queries(**kwargs)
    ##No real improvement can be made in runtime
    precache_assignees(**kwargs)
    ##No real improvement can be made in runtime
    create_canonical_assignees(**kwargs)
    ##No improvement can be made in runtime
    check_largest_clusters(**kwargs)

def post_process_qc(**kwargs):
    config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    qc = AssigneePostProcessingQC(config)
    qc.run_assignee_disambig_tests()

def post_process_assignee_qc_pgpubs(**kwargs):
    config = get_current_config('pgpubs', schedule='quarterly', **kwargs)
    qc = AssigneePostProcessingQC(config)
    qc.run_assignee_disambig_tests()

def post_process_assignee_patent_phase2_qc(**kwargs):
    config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    qc = AssigneePostProcessingQCPhase2(config)
    qc.runDisambiguationTests()

def post_process_assignee_pgpubs_phase2_qc(**kwargs):
    config = get_current_config('pgpubs', schedule='quarterly', **kwargs)
    qc = AssigneePostProcessingQCPhase2(config)
    qc.runDisambiguationTests()

if __name__ == '__main__':
    date = datetime.date(2023, 12, 31)
    # check_largest_clusters(**{
    #     "execution_date": date
    # })
    # post_process_qc(**{
    #     "execution_date": date
    # })

    # additional_post_processing(**{
    #     "execution_date": date })
