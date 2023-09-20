# import code
# import pandas as pd
# import numpy as np
# import re
# from itertools import chain
# from sqlalchemy import create_engine
# from datetime import date
# from os.path import exists
#
# from lib.configuration import get_current_config
#
# # the lists of tables that this code can attribute gender values to
# {
#     'pgpubs'          : ['rawassignee', 'rawinventor', 'us_parties'],
#     'granted_patents' : ['assignee', 'inventor', 'lawyer', 'non_inventor_applicant', 'rawassignee', 'rawexaminer', 'rawinventor', 'rawlawyer']
# }
#
# th = { # score threshholds pulled out for tuning and testing - would consider making an argument for the attribute methods
#     'comin' : 0.8, # single country minimum
#     'grmed' : 0.8, # country group median minimum
#     'grmin' : 0.4,  # country group individual minimum
#     'ambig' : 0.4,  # minimum to allow matching attribution from middle name
# }
#
# def load_reference_data(config):
#     """
#     reads and reshapes gender-name-country reference data from the resources folder
#     :param config: config object containing the resources folder location
#     :returns dictionary with the following key-value pairs:
#         namedata: a dataframe containing the proportion of male and female occurrances of a given name within a given country
#         known_countries: a list of the country codes that appear in the WGND data
#         wgnd_group_countries: a dictionary mapping a language code to the list of associated country codes
#         wgnd_country_langs: a dictionary mapping a country code to the list of associated language codes
#     """
#     gender_source = f"{config['FOLDERS']['resources_folder']}/gender_references/wgnd_cleaned.csv"
#     lang_group_source = f"{config['FOLDERS']['resources_folder']}/gender_references/wgnd_2_0_code-langcode.csv"
#
#     namedata = pd.read_csv(gender_source, keep_default_na=False)
#     wgnd_langs = pd.read_csv(lang_group_source, keep_default_na=False)
#
#     known_countries = namedata['country'].unique()
#
#     wgnd_group_countries = {}
#     for grp in wgnd_langs['langcode'].unique():
#         wgnd_group_countries[grp] = list(wgnd_langs.loc[wgnd_langs['langcode']==grp,'code'])
#
#     wgnd_country_langs = {}
#     for co in wgnd_langs['code'].unique():
#         wgnd_country_langs[co] = list(wgnd_langs.loc[wgnd_langs['code']==co,'langcode'])
#
#     co_to_rel_cos = {}
#     for co in known_countries:
#         langs = wgnd_country_langs[co] if co in wgnd_country_langs else []
#         relcos = list(set(chain(*[wgnd_group_countries[lang] for lang in langs])))
#         relcos = [c for c in relcos if c != co]
#         co_to_rel_cos[co] = relcos
#
#     # likely to be unused due to efficiency of join for exact country matches
#     # codata = {co: namedata.query("country == @co") for co in known_countries}
#
#     refdata = {'namedata': namedata,
#                'known_countries': known_countries,
#                'wgnd_group_countries': wgnd_group_countries,
#                'wgnd_country_langs': wgnd_country_langs,
#                'co_to_rel_cos' : co_to_rel_cos
#             #    'codata' : codata
#                }
#     return(refdata)
#
# def retrieve_table(table, engine, config, chunksize = None):
#     """
#     retrieves and outputs a given number of rows of a table from the production database
#     :param table: string name of table to query
#     :param engine: sqlalchemy engine for database connection
#     :param config: config object containing date and database information
#     :param chunksize: number of rows to retrieve from the table at one time. If None, retrieve all rows.
#     """
#     if table in ['assignee', 'lawyer', 'rawassignee', 'rawlawyer', 'us_parties', 'non_inventor_applicant']:
#         orgcon = "organization IS NULL"
#         # alternatively, could accept entries with org names and automatically attribute gender=None
#     else:
#         orgcon = None
#
#     if (False): #false as placeholder - will add condition to restrict to specific quarter
#         start_date = config['DATES']['START_DATE']
#         end_date = config['DATES']['END_DATE']
#         vicon = f"version_indicator > '{start_date}' AND version_indicator < '{end_date}'"
#     else:
#         vicon = None
#
#     if orgcon is not None or vicon is not None:
#         wherecon = "WHERE " + ' AND '.join([con for con in [orgcon,vicon] if con is not None])
#     else:
#         wherecon = ''
#
#     if chunksize is not None:
#         cycles = 0
#         while True:
#             sql = f"""
#             SELECT id, name_first, country
#             FROM {table}
#             {wherecon}
#             LIMIT {chunksize} OFFSET {chunksize * cycles}
#             """
#             res = pd.read_sql(sql, con=engine)
#             if res.shape[0] == 0: break
#             yield(res)
#             cycles +=1
#     else:
#         sql = f"""
#         SELECT id, name_first, country
#         FROM {table}
#         {wherecon}
#         """
#         res = pd.read_sql(sql, con=engine)
#         yield(res)
#
# def code_table(data, refdata, two_tier_merge = True, code_status = True):
#     """
#     attributes to each record in a table containing first names and countries of residence
#     a label of male, female, or unattributable based on provided reference data.
#     :param data: table containing at least columns 'name_first' and 'country'
#     :param refdata: sqlalchemy engine for database connection
#     :param two_tier_merge: boolean determining whether perform a second join for second given names
#         where first names were recognized but low-confidence for their country of residence.
#         improves speed for large tables.
#     :param code_status: boolean determining whether to assign and return attribution status
#     :returns column 'male_flag' containing attributions, and, if code_status == True,
#         column 'attribution_status' containing information on the basis for the attribution.
#     """
#
#     dcopy = data.copy()
#     dcopy['male_flag'] = None
#     dcopy.loc[:,'name_first'] = dcopy['name_first'].str.replace(pat="'", repl='') # remove apostrophes - e.g. "a'isha" -> "aisha"
#     dcopy.loc[:,'name_split'] = dcopy['name_first'].str.lower().str.split(pat='[^\w]+') # lowercase, clean, and separate names
#     nullspots = dcopy['name_split'].isnull()
#     dcopy.loc[:,'name_split'].loc[nullspots] = [[]]*nullspots.sum() #fill in nulls with empty lists
#     dcopy.loc[:,'country'] = dcopy['country'].str.upper().str.replace(pat='[^\w]+', repl='', regex=True) #clean country code
#     dcopy.loc[:,'matchname'] = [l[0] if len(l)>0 else None for l in dcopy['name_split']] # pull out primary name to join for quick matching
#
#     dcopy = dcopy.merge(refdata['namedata'], how='left', left_on=['matchname','country'], right_on = ['name','country'])
#     # dcopy.loc[:,'male_flag'] = [1 if p >= th['comin'] else None for p in dcopy['perc_M']]
#     dcopy.loc[dcopy['perc_M'] >= th['comin'] , 'male_flag'] = 1
#     dcopy.loc[dcopy['perc_F'] >= th['comin'] , 'male_flag'] = 0
#     if code_status:
#         dcopy.loc[dcopy['male_flag'].notnull(), 'attribution_status'] = 11
#
#     if two_tier_merge:
#         # do a second round of exact country-name matches where the first name was found but with ambiguous results
#         # may be slower or faster depending on size of dataset/chunk
#         dcopy.loc[:,'matchname2'] = [l[1] if len(l)>1 else None for l in dcopy['name_split']] # get secodnary name if available
#         dcopy = dcopy.merge(refdata['namedata'], how='left', left_on=['matchname2','country'], right_on = ['name','country'], suffixes=('_1','_2'))
#         name2spots = ((dcopy['perc_M_1'].notnull()) & (dcopy['matchname2'].notnull()) & (dcopy['perc_F_1'] < th['comin']) & (dcopy['perc_M_1'] < th['comin']))
#         dcopy.loc[(name2spots & (dcopy['perc_M_2'] >= th['comin']) & (dcopy['perc_M_1'] >= th['ambig'])), 'male_flag'] = 1
#         dcopy.loc[(name2spots & (dcopy['perc_F_2'] >= th['comin']) & (dcopy['perc_F_1'] >= th['ambig'])), 'male_flag'] = 0
#         if code_status:
#             dcopy.loc[(name2spots & (dcopy['perc_M_2'] >= th['comin']) & (dcopy['perc_M_1'] >= th['ambig'])), 'attribution_status'] = 21
#             dcopy.loc[(name2spots & (dcopy['perc_F_2'] >= th['comin']) & (dcopy['perc_F_1'] >= th['ambig'])), 'attribution_status'] = 21
#
#     notdone = dcopy['male_flag'].isnull()
#     if code_status:
#         dcopy.loc[notdone, ['male_flag','attribution_status']] = dcopy.loc[notdone].apply(attribute_rowwise_clean, axis=1, args=(refdata, code_status)).tolist()
#         return(dcopy[['male_flag','attribution_status']])
#     else:
#         dcopy.loc[notdone, 'male_flag'] = dcopy.loc[notdone].apply(attribute_rowwise_clean, axis=1, args=(refdata, code_status))
#         return(dcopy['male_flag'])
#
# def attribute_rowwise_unclean(row, refdata, code_status=True):
#     """
#     wrapper for attribute_single_name that extracts the given name and country from a row of a dataframe (or a dictionary)
#     :param row: a dictionary or series containing keys 'name_first' and 'country'
#     :param refdata: dictionary of dataframes and lists containing cleaned and reshaped name-gender-country data from WIPO's WGND
#     :param code_status: boolean determining whether to assign and return attribution status
#     """
#     return(attribute_single_name(row['name_first'], row['country'], refdata, code_status))
#
# def attribute_rowwise_clean(row, refdata, code_status=True):
#     """
#     wrapper for attribute_split_name for pd.apply() requiring that the required cleaning already be applied over the dataframe
#     :param row: a dictionary or series containing keys 'name_split' and 'country'
#     :param refdata: dictionary of dataframes and lists containing cleaned and reshaped name-gender-country data from WIPO's WGND
#     :param code_status: boolean determining whether to assign and return attribution status
#     """
#     return(attribute_split_name(row['name_split'], row['country'], refdata, code_status))
#
# def attribute_single_name(given_name, residence, refdata, code_status=True):
#     """
#     wrapper for attribute_split_name that cleans and splits a single given name value
#     wrapped by attribute_by_row for bulk attributions
#     :param names: ordered list of cleaned lowercase given names for a single individual
#     :param residence: string country code of individual's country of residence
#     :param refdata: dictionary of dataframes and lists containing cleaned and reshaped name-gender-country data from WIPO's WGND
#     :param code_status: boolean determining whether to assign and return attribution status
#     """
#     #split and clean name - needs to account for NULL/NaN/None
#     if pd.isna(given_name):
#         given_name = ''
#         names = []
#     else:
#         given_name = given_name.lower()
#         names = re.split('[^\w]+',given_name)
#     if pd.isna(residence):
#         residence = None
#     else:
#         residence = residence.upper()
#         residence=re.sub('[^\w]','',residence)
#
#     return(attribute_split_name(names, residence, refdata, code_status))
#
#
# def attribute_split_name(names, residence, refdata, code_status=True):
#     """
#     attributes a gender flag value of 1(M), 0(F), or None(X) to a given name(s)
#     recurses to successive names if the first name is unknown to WIPO
#     wrapped by attribute_single_name for unsplit names
#     :param names: ordered list of cleaned lowercase given names for a single individual
#     :param residence: string country code of individual's country of residence
#     :param refdata: dictionary of dataframes and lists containing cleaned and reshaped name-gender-country data from WIPO's WGND
#     :param code_status: boolean determining whether to assign and return attribution status
#     """
#     # introduce indicator for names with narrow majorities
#     # majority_memory = None
#     # alternative to majority memory - boolean arguments allowMale/allowFemale (default TRUE)
#     # give some flexibility in androgynous names -e.g. set threshold at .4 or .33 instead of .5
#     # allows names like Riley and Robin to go either way depending on middle name(s) instead of being locked to one
#     allowM, allowF = True, True
#
#     # can't attribute empty list
#     if len(names) == 0: return([None,99] if code_status else None) # 'X'
#
#     for i, name in enumerate(names):
#         if len(name) < 2: continue # can't attribute empty string, and shouldn't attribute initials
#         matches = refdata['namedata'].query("name == @name")
#         if matches.shape[0] == 0:
#             continue
#         # Case 1 - name found for country
#         submatch = matches[matches['country']==residence]
#         if submatch.shape[0] > 0: #should get either 1 or 0 matches
#             if allowM and submatch.iat[0,3] >= th['comin']:
#                 if code_status:
#                     return [1, 11 if i==0 else 21]
#                 return(1) # M
#             if allowF and submatch.iat[0,2] >= th['comin']:
#                 if code_status:
#                     return [0, 11 if i==0 else 21]
#                 return(0) # F
#             if i == 0:
#                 if submatch.iat[0,3] < th['ambig']:
#                     allowM = False
#                 if submatch.iat[0,2] < th['ambig']:
#                     allowF = False
#             continue
#         # Case 2 - name found in ling group
#         related_countries = refdata['co_to_rel_cos'][residence] if residence in refdata['co_to_rel_cos'] else []
#         submatch = matches[[c in related_countries for c in matches['country']]]
#         if submatch.shape[0] > 0:
#             # attribution requires all matching scores meet minimum value and have median value above threshhold
#             if allowM and all(submatch['perc_M'] >= th['grmin']) and (np.median(submatch['perc_M']) >= th['grmed']):
#                 if code_status:
#                     return [1, 12 if i==0 else 22]
#                 return(1) # M
#             if allowF and all(submatch['perc_F'] >= th['grmin']) and (np.median(submatch['perc_F']) >= th['grmed']):
#                 if code_status:
#                     return [0, 12 if i==0 else 22]
#                 return(0) # F
#             if i == 0:
#                 if any(submatch['perc_M'] < th['ambig']):
#                     allowM = False
#                 if any(submatch['perc_F'] < th['ambig']):
#                     allowF = False
#             continue
#         else:
#         # Case 3 - name found outside ling group
#             # attribution requires all matching scores meet minimum value and have median value above threshhold
#             if allowM and all(matches['perc_M'] >= th['grmin']) and (np.median(matches['perc_M']) >= th['grmed']):
#                 if code_status:
#                     return [1, 13 if i==0 else 23]
#                 return(1) # M
#             if allowF and all(matches['perc_F'] >= th['grmin']) and (np.median(matches['perc_F']) >= th['grmed']):
#                 if code_status:
#                     return [0, 13 if i==0 else 23]
#                 return(0) # F
#             if i == 0:
#                 if any(submatch['perc_M'] < th['ambig']):
#                     allowM = False
#                 if any(submatch['perc_F'] < th['ambig']):
#                     allowF = False
#     # if there was no general consensus with high confidence
#     return([None,99] if code_status else None) # X
#
# def update_attribution_table(attributed, engine):
#     """
#     update the gender value in the database for the current set of rows
#     :param attributed: dataframe with gender-attributed names
#     :param table: string name of table to update
#     :param engine: sqlalchemy engine for database connection
#     """
#     attributed[['id','male_flag']].to_sql('temp_gender', if_exists='append', con=engine) # upload attributed data
#
# def update_main_table(table, temp_table_name, engine, code_status = True):
#     """
#     updates main table with gender attributions by joining to the corresponding attribution table
#     :param table: string name of table to update
#     :param engine: sqlalchemy engine for database connection
#     :param code_status: boolean determining whether to assign attribution status code
#     """
#     update_sql = f"""
#     UPDATE TABLE {table} main
#     LEFT JOIN {temp_table_name} t ON (main.id = t.id)
#     SET main.male_flag = t.male_flag
#     {', main.attribution_status = t.attribution_status' if code_status else ''}
#     """
#     if code_status:
#         update_sql = update_sql + f""";
#         UPDATE TABLE {table} main
#         SET main.attribution_status = 98
#         WHERE main.attribution_status IS NULL
#         """
#     engine.execute(update_sql)
#
#
# # Old Codes:
# # 1: attribution successful - discontinued
# # 99: attribution unsuccessful - maintained
# # 98: attribution not attempted (yet) - maintained but should be unused
#
# # New Codes:
# # Digit 1: success
# # Digit 2: scope/detail
#
# # dictionary provided for reference
# new_attribution_status_codes = {
#     # 1X: successful attribution based on First Name
#     11: 'attributed based on first name and country of residence',
#     12: 'attributed based on first name and countries linguistically related to country of residence',
#     13: 'attributed based on first name in countries not linguistically related to country of residence',
#     # 2X: successful based on given name after first
#     21: 'attributed based on given name after first and country of residence',
#     22: 'attributed based on given name after first and countries linguistically related to country of residence',
#     23: 'attributed based on given name after first in countries not linguistically related to country of residence',
#     # 9X: No attribution
#     99: 'attribution unsuccessful (name not recognized or ambiguous)',
#     98: '[unused but reserved based on previous use]',
#     97: 'identified as organization, not individual'
#     }
#
#
# def table_genderize(dbtype = 'granted_patent',code_status=True):
#     """
#     reads the config file, and calls above functions to attribute and record gender for the names in the tables listed in the config file
#     :param dbtype: string indicating whether to attribute to granted or pregrant tables
#     :param code_status: boolean determining whether to assign and return attribution status
#     """
#     config = get_current_config(type=dbtype, **{'execution_date' : date.today()})
#
#     tablestring = config['GENDER'][f'{dbtype}_tables']
#     tables_to_attribute = re.split('[^\w]+', tablestring)
#
#     database = '{}'.format(config['GENDER'][f'{dbtype}_database'])
#     host = '{}'.format(config['DATABASE_SETUP']['HOST'])
#     user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
#     password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
#     port = '{}'.format(config['DATABASE_SETUP']['PORT'])
#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4')
#
#     chunksize = int(config['GENDER']['attribution_chunksize'])
#
#     refdata = load_reference_data(config)
#
#     for table in tables_to_attribute:
#         logpath = f"{config['GENDER']['log_location']}/{table}_gender_attribution_log_{config['DATES']['END_DATE']}"
#         temp_table_name = f"{table}_attribution_{config['DATES']['END_DATE']}"
#         # engine.execute(f"DROP TABLE IF EXISTS {temp_table_name}"") # start fresh each time task needs running
#         engine.execute(f"""
#             CREATE TABLE IF NOT EXISTS {temp_table_name}
#             (id VARCHAR(),
#             male_flag int,
#             attribution_status int DEFAULT 98)""")
#
#         for tablechunk in retrieve_table(table, engine, config, chunksize=chunksize):
#             if code_status:
#                 tablechunk.loc[:,['male_flag', 'attribution_status']] = code_table(tablechunk, refdata, code_status)
#                 # if not exists(logpath):
#                 #     tablechunk[:0].to_csv(logpath,index=False,header=True)
#                 tablechunk.to_csv(logpath,mode='a',index=False,header=False)
#                 tablechunk[['id','male_flag','attribution_status']].to_sql('temp_gender', if_exists='append', con=engine) # upload attributed data
#
#             else:
#                 tablechunk.loc[:,'male_flag'] = code_table(tablechunk, refdata, code_status)
#                 # if not exists(logpath):
#                 #     tablechunk[:0].to_csv(logpath,index=False,header=True)
#                 tablechunk.to_csv(logpath,mode='a',index=False,header=False)
#                 tablechunk[['id','male_flag']].to_sql('temp_gender', if_exists='append', con=engine) # upload attributed data
#         update_main_table(table,temp_table_name,engine,code_status)
#
#
# def confirm_table_formatting(dbtype = 'granted_patent', if_missing='raise'):
#     """
#     checks columns and formatting of tbales
#     :param dbtype: string indicating whether to attribute to granted or pregrant tables
#     :param if_missing: string determining behavior if table is not formatted properly. 'raise' or 'fix'
#     """
#     config = get_current_config(type=dbtype, **{'execution_date' : date.today()})
#
#     tablestring = config['GENDER'][f'{dbtype}_tables']
#     tables_to_attribute = re.split('[^\w]+', tablestring)
#
#     database = '{}'.format(config['GENDER'][f'{dbtype}_database'])
#     host = '{}'.format(config['DATABASE_SETUP']['HOST'])
#     user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
#     password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
#     port = '{}'.format(config['DATABASE_SETUP']['PORT'])
#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4')
#
#     for table in tables_to_attribute:
#         infosql = f"""
#         SELECT *
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE upper(TABLE_SCHEMA) = '{database.upper()}'
#         AND upper(TABLE_NAME) = '{table.upper()}'
#         """
#         table_info = pd.read_sql(infosql, con=engine)
#
#         if 'MALE_FLAG' not in table_info['COLUMN_NAME'].str.upper().values:
#             if if_missing == 'fix':
#                 engine.execute(f"ALTER TABLE {table} ADD male_flag int DEFAULT NULL")
#                 table_info = pd.read_sql(infosql, con=engine)
#             else:
#                 raise Exception(f"table {database}.{table} missing field 'MALE_FLAG'")
#
#         assert table_info.loc[table_info['COLUMN_NAME'] == 'MALE_FLAG', 'DATA_TYPE']=='int', \
#         f"'MALE_FLAG' column is wrong type. Type is {table_info.loc[table_info['COLUMN_NAME'] == 'MALE_FLAG', 'DATA_TYPE']}. Should be int."
#         assert table_info.loc[table_info['COLUMN_NAME'] == 'MALE_FLAG', 'IS_NULLABLE']=='YES', "'MALE_FLAG' column is not nullable."
#
#         if 'ATTRIBUTION_STATUS' not in table_info['COLUMN_NAME'].str.upper().values:
#             if if_missing == 'fix':
#                 engine.execute(f"ALTER TABLE {table} ADD attribution_status int DEFAULT NULL")
#                 table_info = pd.read_sql(infosql, con=engine)
#             else:
#                 raise Exception(f"table {database}.{table} missing field 'ATTRIBUTION_STATUS'")
#
#         assert table_info.loc[table_info['COLUMN_NAME'] == 'ATTRIBUTION_STATUS', 'DATA_TYPE']=='int', \
#         f"'ATTRIBUTION_STATUS' column is wrong type. Type is {table_info.loc[table_info['COLUMN_NAME'] == 'ATTRIBUTION_STATUS', 'DATA_TYPE']}. Should be int."
#         assert table_info.loc[table_info['COLUMN_NAME'] == 'ATTRIBUTION_STATUS', 'IS_NULLABLE']=='YES', "'ATTRIBUTION_STATUS' column is not nullable."
#
# def QA_gender_attribution(dbtype = 'granted_patent'):
#     """
#     checks for correct application of gender attribution
#     :param dbtype: string indicating whether attribution was performed on granted or pregrant tables
#     """
#     config = get_current_config(type=dbtype, **{'execution_date' : date.today()})
#
#     tablestring = config['GENDER'][f'{dbtype}_tables']
#     tables_to_attribute = re.split('[^\w]+', tablestring)
#
#     database = '{}'.format(config['GENDER'][f'{dbtype}_database'])
#     host = '{}'.format(config['DATABASE_SETUP']['HOST'])
#     user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
#     password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
#     port = '{}'.format(config['DATABASE_SETUP']['PORT'])
#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4')
#
#     for table in tables_to_attribute:
#         #confirm all male_flag in (1,0,NULL)
#         a = pd.read_sql(f"SELECT COUNT(*) FROM {table} WHERE male_flag IS NOT NULL AND male_flag NOT IN (1,0)", con=engine).iat[0,0]
#         assert a == 0, f"{a} unexpected values found in column 'MALE_FLAG' in table {table}."
#
#         #confirm all attribution_status in (1,99,98)
#         b = pd.read_sql(f"SELECT COUNT(*) FROM {table} WHERE attribution_status NOT IN (1,99,98)", con=engine).iat[0,0]
#         assert b == 0, f"{b} unexpected values found in column 'ATTRIBUTION_STATUS' in table {table}."
#
#         #confirm all attributions have status=1
#         c = pd.read_sql(f"SELECT COUNT(*) FROM {table} WHERE male_flag IN (1,0) AND attribution_status <> 1", con=engine).iat[0,0]
#         #confirm all non-attributions have status in (99,98)
#         d = pd.read_sql(f"SELECT COUNT(*) FROM {table} WHERE male_flag IS NULL AND attribution_status NOT IN (99,98)", con=engine).iat[0,0]
#
#         assert c == 0 and d == 0, f"{c+d} disallowed 'MALE_FLAG' and 'ATTRIBUTION_STATUS' combinations detected in table {table}."
#
#
# if __name__ == "__main__":
#     table_genderize()
#
# ######## OLD CODE #############
#
#     #### prototype, inefficient
# # def attribute_split_name(names, residence, refdata):
# #     """
# #     attributes a gender flag value of 1(M), 0(F), or None(X) to a given name(s)
# #     recurses to successive names if the first name is unknown to WIPO
# #     wrapped by attribute_single_name for unsplit names
# #     :param names: ordered list of cleaned lowercase given names for a single individual
# #     :param residence: string country code of individual's country of residence
# #     :param refdata: dictionary of dataframes and lists containing cleaned and reshaped name-gender-country data from WIPO's WGND
# #     """
# #     # can't attribute empty list
# #     if len(names) == 0: return(None) # 'X'
#
# #     # introduce indicator for names with narrow majorities
# #     majority_memory = None
# #     if residence in refdata['known_countries']:
# #         # CASE 1. country of residence has name:
# #         for i, name in enumerate(names):
# #             # match = refdata['codata'][residence].query(f'name=="{name}"') # this is much faster, but made unnecessary by the speed of joining on name and country
# #             match = refdata['namedata'].query(f'name=="{name}" and country=="{residence}"')
# #             #expect either one or zero rows found
# #             if match.shape[0] > 0: # name-country pair found
# #                 if match.iloc[0]['perc_M'] >= th['comin'] and majority_memory in (None,'M'):
# #                     return(1) # M
# #                 elif match.iloc[0]['perc_F'] >= th['comin'] and majority_memory in (None,'F'):
# #                     return(0) # F
# #                 elif majority_memory is None:
# #                     if match.iloc[0]['perc_M'] > th['ambig']:
# #                         majority_memory = 'M'
# #                     elif match.iloc[0]['perc_F'] > th['ambig']:
# #                         majority_memory = 'F'
# #             else: # name-country pair not found
# #                 if i == 0: break #skip to next case if first name not found
# #             if i == len(names)-1: # final given name after attempt to attribute
# #                 return(None) # X
# #         # CASE 2. name exists in WGND in linguistically related countries but not for country of residence
# #         for i, name in enumerate(names):
# #             langs = refdata['wgnd_country_langs'][residence]
# #             related_countries = set(chain(*[refdata['wgnd_group_countries'][lang] for lang in langs]))
# #             matches = refdata['namedata'].query(f"name == '{name}' and country in {list(related_countries)}")
# #             if matches.shape[0] > 0:
# #                 # attribution requires all matching scores meet minimum value and have median value above threshhold
# #                 if all(matches['perc_M'] > th['grmin']) and (np.median(matches['perc_M']) >= th['grmed']) and majority_memory in (None,'M'):
# #                     return(1) # M
# #                 elif all(matches['perc_F'] > th['grmin']) and (np.median(matches['perc_F']) >= th['grmed']) and majority_memory in (None,'F'):
# #                     return(0) # F
# #                 elif majority_memory is None:
# #                     if np.median(matches['perc_M']) > th['ambig']:
# #                         majority_memory = 'M'
# #                     elif np.median(matches['perc_F']) > th['ambig']:
# #                         majority_memory = 'F'
# #             else: # name not found within linguistic group
# #                 if i == 0: break # skip to next case if first name not found
# #             if i == len(names)-1: # final given name after attempt to attribute
# #                 return(None) # X
# #     for i, name in enumerate(names):
# #         # CASE 3. country not recognized/provided OR first name exists in WGND only outside of linguistic group
# #         matches = refdata['namedata'].query(f"name == '{name}'")
# #         if matches.shape[0] > 0:
# #             # attribution requires all matching scores meet minimum value and have median value above threshhold
# #             if all(matches['perc_M'] > th['grmin']) and (np.median(matches['perc_M']) >= th['grmed']) and majority_memory in (None,'M'):
# #                 return(1) # M
# #             elif all(matches['perc_F'] > th['grmin']) and (np.median(matches['perc_F']) >= th['grmed']) and majority_memory in (None,'F'):
# #                 return(0) # F
# #             elif majority_memory is None:
# #                 if np.median(matches['perc_M']) > th['ambig']:
# #                     majority_memory = 'M'
# #                 elif np.median(matches['perc_F']) > th['ambig']:
# #                     majority_memory = 'F'
# #         # if first name does not exist in WGND, recurse with remaining names (if any):
# #         elif i == 0 and len(names)>1:
# #             return(attribute_split_name(names[1:], residence, refdata))
# #     # if there was no general consensus with high confidence
# #     return(None) # X
