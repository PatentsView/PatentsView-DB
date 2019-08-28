#Counts QA
import pandas as pd
import os
import configparser
project_home = os.environ['PACKAGE_HOME']
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
from Development.helpers import general_helpers
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['NEW_QA_LOC']


#Counts- creating count description
def create_count_description(old_db_count, new_db_count, table, min_inc = 1.01, max_inc = 1.1):
    unchanging = ['cpc_subsection', 'wipo_field', 'nber_category', 'nber', 'cpc_group',
                  'nber_subcategory', 'mainclass', 'mainclass_current', 'subclass', 'subclass_current']
    slight_changes = ['cpc_subgroup']
    if not table in unchanging + slight_changes:
        if new_db_count < old_db_count:
            return "Problem: New table has fewer rows than old table "
        elif new_db_count == old_db_count:
            return "Problem: No new entries"
        elif new_db_count > old_db_count*max_inc:
            return "Problem: Too many new entries"
        elif new_db_count < old_db_count*min_inc:
            return "Problem: Too few new entries"
        elif old_db_count*min_inc < new_db_count < old_db_count*max_inc:
            return "No Problem!"
        else:
            return "Check the logic!"
    elif table in unchanging:
        if new_db_count == old_db_count:
            return "No Problem!"
        else:
            return "Problem: Number of rows in unchanging table changed"
    elif table in slight_changes:
        if old_db_count*.9 < new_db_count < old_db_count*1.1:            return "No Problem!"
        else: 
            return "Problem: Number of rows in slightly changing table changed"
        
#Counts- Get counts
def get_counts(previous_qa_loc, new_qa_loc, new_database):
    counts = pd.read_csv('{}/01_counts.csv'.format(previous_qa_loc))
    conn = engine.connect()
    conn.execute('use {}'.format(new_database))
    new_counts = []
    for table in counts['Table']:
       # print table
        var = conn.execute('select count(*) from {}'.format(table))
        count = [row[0] for row in var][0]
        new_counts.append(count)
    conn.close()
    return new_counts
#Counts- write to file
def make_file(new_counts, previous_qa_loc, new_qa_loc, new_database):
    counts = pd.read_csv('{}/01_counts.csv'.format(previous_qa_loc))
    counts[new_database] = new_counts
    del counts['Description']
    #the last row of the table is now the most recent previous database!
    counts['Description'] = counts.apply(lambda row: create_count_description(row[counts.columns[-3]], row[new_database],row['Table']), axis=1)
    cols = list(counts.columns)
    cols.pop(cols.index('Comments'))
    cols += ['Comments']
    data = counts[cols]
    counts.to_csv('{}/01_counts.csv'.format(new_qa_loc), index = False)
    


if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_csv("{}/01_counts.csv".format(previous_qa_loc))
    new_counts = get_counts(previous_qa_loc, new_qa_loc, new_database)
    make_file(new_counts,previous_qa_loc, new_qa_loc, new_database)