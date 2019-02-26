import MySQLdb
import os
import re
import csv
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers


def get_tables(db_con, output_folder):
    tables = ['patent', 'cpc_current','ipcr','nber','rawassignee','rawinventor','uspc_current','rawlawyer']
    #regex for removing inventor name details
    regex_keys = [r"\,\sdeceased",r"\,\sadministrator", r"\,\sexecutor", 
             r"\,\slegal.+",r"\,\spersonal.+" ]
    regex = re.compile("(%s)" % "|".join(regex_keys))

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
        if table == 'rawinventor':
            for_output = []
            for row in existing_data:
                data = [item for item in row]
                data[5] = re.sub(regex, '', row[5])
                for_output.append(data)

            outp.writerows(for_output)
        else:
            outp.writerows(existing_data)
        
     #rawlocation done separately because it is a specially query
    outp = csv.writer(open('{}/rawlocation.tsv'.format(output_folder),'w'),delimiter='\t')
    existing_data = db_con.execute('select id,location_id_transformed as location_id,city,state,country_transformed as country from rawlocation')
    outp.writerow(['id','location_id','city','state','country'])
    outp.writerows(existing_data)

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')
    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_inputs')
    
    if not os.path.exists(disambig_folder):
        os.makedirs(disambig_folder)
    get_tables(db_con, disambig_folder)