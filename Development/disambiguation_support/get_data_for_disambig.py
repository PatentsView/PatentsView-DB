import MySQLdb
import os
import re
import csv
import sys
import json
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers


def get_tables(db_con, output_folder, status_file):

    try:
        current_status = json.load(open(status_file))

    except OSError as e:
        print(e)
        current_status = {}


    tables = ['patent', 'cpc_current','ipcr','nber','rawassignee','rawinventor','uspc_current', 'rawlawyer', 'rawlocation']
    #regex for removing inventor name details
    regex_keys = [r"\,\sdeceased",r"\,\sadministrator", r"\,\sexecutor", 
             r"\,\slegal.+",r"\,\spersonal.+" ]
    regex = re.compile("(%s)" % "|".join(regex_keys))

    for t in tables:
        print("Exporting table {}".format(t))

        # check status file
        if t in current_status and current_status[t] == 1:
            continue

        # rawlocation done separately because it is a special query
        if t == 'rawlocation':

            # if this fails, it's okay to not have entry with value 0, will be re-processed anyways
            outp = csv.writer(open('{}/rawlocation.tsv'.format(output_folder),'w'),delimiter='\t')
            existing_data = db_con.execute('select id,location_id_transformed as location_id,city,state,country_transformed as country from rawlocation')
            outp.writerow(['id','location_id','city','state','country'])
            outp.writerows(existing_data)
            current_status[t] = 1
            json.dump(current_status, open(status_file, 'w'))
            db_con.dispose()

        else:

            try: 
                col_data = db_con.execute('show columns from {}'.format(t)) #eventually add funcitonality to check that the columns getting exported are exactly the same
                
                #this removes the new rule_47 column in inentor and withdrawn in patent
                cols = [c[0] for c in col_data]

                if t == 'rawinventor':
                    cols.remove('rule_47')
                    cols.remove('deceased')

                    # replace nulls with empty strings in mysql
                    idx_firstname = cols.index('name_first')
                    idx_lastname = cols.index('name_last')
                    cols[idx_firstname] = "coalesce(name_first, '') as name_first"
                    cols[idx_lastname] = "coalesce(name_last, '') as name_last"

                elif t == 'patent':
                    cols.remove('withdrawn')

                col_string= ", ".join(cols)
                existing_data = db_con.execute("select {} from {};".format(col_string, t))
                outp = csv.writer(open('{}/{}.tsv'.format(output_folder, t),'w'),delimiter='\t')
                outp.writerow(cols)

                if t == 'rawinventor':
                    for_output = []
                    for row in existing_data:
                        data = [item for item in row]
                        data[5] = re.sub(regex, '', row[5])
                        for_output.append(data)

                    outp.writerows(for_output)
                else:
                    outp.writerows(existing_data)

                # update status file to reflect tables already exported
                current_status[t] = 1
                json.dump(current_status, open(status_file, 'w'))
                db_con.dispose()

            # makes entry, but puts value as 0
            except Exception as e:
                current_status[t] = 0
                json.dump(current_status, open(status_file, 'w'))
                db_con.dispose()
                raise e



if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')
    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'], server_side_cursors=True)
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_inputs')
    status_file = project_home + '/Development/disambiguation_support/disambiginput_status.json'
    if not os.path.exists(disambig_folder):
        os.makedirs(disambig_folder)
    get_tables(db_con, disambig_folder, status_file)




