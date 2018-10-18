import MySQLdb
import os
import csv
import sys
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

def get_tables(db_con, output_folder):
    tables = ['patent', 'cpc_current','ipcr','nber','rawassignee','rawinventor','uspc_current','rawlawyer']
    for t in tables:
        print("Exporting table {}".format(t))
        col_data = db_con.execute('show columns from {}'.format(t)) #eventually add funcitonality to check that the columns getting exported are exactly the same
        cols = [c[0] for c in col_data]
        if t in ['rawinventor', 'patent']:
           #this removes the new rule_47 column in inentor and withdrawn in patent
            cols = cols[:-1]
        if t == 'rawlawyer':
             cols = cols[:8]
        col_string= ", ".join(cols)
        existing_data = db_con.execute("select {} from {};".format(col_string, t))
        outp = csv.writer(open('{}/{}.tsv'.format(output_folder, t),'w'),delimiter='\t')
        outp.writerow(cols)
        outp.writerows(existing_data)
     #rawlocation done separately because it is a specially query
    outp = csv.writer(open('{}/rawlocation.tsv'.format(output_folder),'w'),delimiter='\t')
    existing_data = db_con.execute('select id,location_id_transformed as location_id,city,state,country_transformed as country from rawlocation')
    outp.writerow(['id','location_id','city','state','country'])
    outp.writerows(existing_data)

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')
    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['OLD_DB'])
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_inputs')
    if not os.path.exists(disambig_folder):
        os.makedirs(disambig_folder)
    get_tables(db_con, disambig_folder)
    os.system('scp -i "PatentsView-DB/Development/PV_Apache_Solr.pem" {}/*.tsv centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/data'.format(disambig_folder))
