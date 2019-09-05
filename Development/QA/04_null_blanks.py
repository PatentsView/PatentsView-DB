#Count Null and Blank QA
import pandas as pd
import os
import configparser
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['NEW_QA_LOC']


def count_null_and_blank(new_database, previous_qa_loc):
    tc = pd.read_csv('{}/04_null_blanks.csv'.format(previous_qa_loc))
    table_col_dict = zip(tc['Table'], tc['Column'])
    results = []
    for table, col in table_col_dict:
        try:
            conn = engine.connect()
            var = conn.execute("select count(*) from {0}.{1} where `{2}` is null or `{2}` = '';".format(new_database, table, col))
            counts = [row[0] for row in var][0]
            conn.close()
            results.append(counts)
            print ("{0}.{1}: {2}".format(table, col, counts))
        except:
            results.append("Error: Problem with {0}.{1}".format(table, col))
            print ("Error: Problem with {0}.{1}".format(table, col) )
    return results
def increase_10_percent_desc(newdb_count, olddb_count, accept_inc):
    try:
        int(newdb_count)
        int(olddb_count)
        if newdb_count < olddb_count:
            return "Problem: Less nulls in current update than previous update."
        elif newdb_count > (olddb_count * accept_inc):
            return "Problem: Too many new nulls."
        elif olddb_count <= newdb_count <= olddb_count * accept_inc:
            return "No Problem!"
        else:
            return "Check the logic!"
    except:
        return "Problem : {}" .format(newdb_count)
def null_to_file(newdb_results,previous_qa_loc, new_qa_loc, new_database):
    df = pd.read_csv('{}/04_null_blanks.csv'.format(previous_qa_loc))
    df_2 = pd.DataFrame(newdb_results)
    df_2.rename(columns={0:new_database}, inplace = True)
    df_3 = pd.concat([df, df_2], axis = 1)
    df_3.drop(['Description'], inplace = True, axis = 1)
    df_3['Description'] = df_3.apply(lambda row: increase_10_percent_desc(row[df_3.columns[-1]], row[df_3.columns[-2]], 1.1), axis=1)
    df_3.to_csv('{}/04_null_blanks.csv'.format(new_qa_loc), index = False)


if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_csv("{}/04_null_blanks.csv".format(previous_qa_loc))
    null_results = count_null_and_blank(new_database, previous_qa_loc)
    null_to_file(null_results, previous_qa_loc, new_qa_loc, new_database)
