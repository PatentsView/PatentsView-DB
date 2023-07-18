import subprocess
import datetime
from lib.configuration import get_connection_string, get_current_config, get_unique_connection_string
import pymysql
from sqlalchemy import create_engine
import pandas as pd


def get_oldest_databases(config, db_type='granted_patent'):
    print(f"Running {db_type}")
    if db_type == 'granted_patent':
        q = """
select  min(table_schema)
from information_schema.tables
where left(table_schema, 6) ='upload'
"""
    elif db_type == 'pgpubs':
        q = """
        select  min(table_schema)
        from information_schema.tables
        where left(table_schema, 6) ='pgpubs'
        """
    else:
        "Database not Configured"
    print(q)
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                 user=config['DATABASE_SETUP']['USERNAME'],
                                 password=config['DATABASE_SETUP']['PASSWORD'],
                                 db=config['PATENTSVIEW_DATABASES']["PROD_DB"],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    try:
        if not connection.open:
            connection.connect()
        with connection.cursor() as generic_cursor:
            generic_cursor.execute(q)
            db = generic_cursor.fetchall()[0][0]
            print("--------------------------------------------------------------")
            print(f"Found Database {db} To Archive!")
            print("--------------------------------------------------------------")
            q2 = f"""
select table_name 
from information_schema.tables
where table_schema = '{db}'
            """
        print(q2)
        with connection.cursor() as generic_cursor2:
            generic_cursor2.execute(q2)
            table_list = generic_cursor2.fetchall()

    finally:
        if connection.open:
            connection.close()
    return db, table_list


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)

def backup_db(config, output_path, db):
    print("--------------------------------------------------------------")
    print("DUMPING DATABASE BACKUP")
    print("--------------------------------------------------------------")
    # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    defaults_file = "resources/sql.conf"
    # bash_command1 = f"mysqldump --defaults-file={defaults_file} --column-statistics=0  {db} > {output_path}/{db}_backup.sql"
    bash_command1 = f"mydumper --defaults-file={defaults_file} -B {db} -o {output_path}  -c --long-query-guard=9000000 -v 3"
    print(bash_command1)
    subprocess_cmd(bash_command1)


def backup_tables(db, output_path, table_list):
    # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    bash_command1 = f"mydumper -B {db} -T {table_list} -o {output_path}  -c --long-query-guard=9000000 -v 3"
    print(bash_command1)
    subprocess_cmd(bash_command1)


def upload_tables_for_testing(config, db, output_path, table_list):
    print("--------------------------------------------------------------")
    print("UPLOADING DATABASE BACKUPS FOR QA")
    print("--------------------------------------------------------------")
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    archive_db = f"archive_temp_{db}"
    q = f"create database {archive_db}"
    print(q)
    engine.execute(q)
    # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    defaults_file = "resources/sql.conf"
    if isinstance(table_list, str):
        for table in table_list.split(","):
            # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
            bash_command1 = f"gunzip -d {output_path}/{db}.{table}-schema.sql.gz"
            bash_command2 = f"gunzip -d {output_path}/{db}.{table}.sql.gz"
            bash_command3 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {output_path}/{db}.{table}-schema.sql"
            bash_command4 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {output_path}/{db}.{table}.sql"
            bash_command5 = f"gzip {output_path}/{db}.{table}.sql"
            bash_command6 = f"gzip {output_path}/{db}.{table}-schema.sql"
            for i in [bash_command1, bash_command2, bash_command3, bash_command4, bash_command5, bash_command6]:
                print(i)
                subprocess_cmd(i)
    else:
        for table in table_list:
            if table != None:
                bash_command1 = f"gunzip -d {output_path}/{db}.{table[0]}-schema.sql.gz"
                bash_command2 = f"gunzip -d {output_path}/{db}.{table[0]}.sql.gz"
                bash_command3 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {output_path}/{db}.{table[0]}-schema.sql"
                bash_command4 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {output_path}/{db}.{table[0]}.sql"
                bash_command5 = f"gzip {output_path}/{db}.{table[0]}.sql"
                bash_command6 = f"gzip {output_path}/{db}.{table[0]}-schema.sql"
                for i in [bash_command1, bash_command2, bash_command3, bash_command4, bash_command5, bash_command6]:
                    print(i)
                    subprocess_cmd(i)

def upload_tsv_backup_files(config, db, output_path, table_list):
    print("--------------------------------------------------------------")
    print("UPLOADING DATABASE BACKUPS FOR QA")
    print("--------------------------------------------------------------")
    defaults_file = "resources/sql.conf"
    if db == 'patent_text':
        pre = 'g_'
    else:
        pre = 'pg_'
    if isinstance(table_list, str):
        for table in table_list.split(","):
            # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
            bash_command1 = f"unzip {output_path}/{db}.{pre}{table}.tsv.zip"
            bash_command2 = f"mysql --defaults-file={defaults_file} --local-infile=1"
            bash_command3 = f"LOAD DATA LOCAL INFILE '{output_path}{pre}{table}.tsv' INTO TABLE {db}.{table} FIELDS TERMINATED BY '\t' ENCLOSED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
            bash_command4 = f"gzip {output_path}/{db}.{pre}{table}.sql"
            for i in [bash_command1, bash_command2, bash_command3, bash_command4]:
                print(i)
                subprocess_cmd(i)

def query_for_all_tables_in_db(connection_string, temp):
    print("--------------------------------------------------------------")
    print("GETTING A LIST OF ALL TABLES")
    print("--------------------------------------------------------------")
    engine = create_engine(connection_string)
    q = f"""
    select TABLE_NAME
    from information_schema.tables
    where TABLE_SCHEMA = '{temp}';
    """
    print(q)
    table_data_raw = pd.read_sql_query(sql=q, con=engine)
    return table_data_raw


def get_count_for_all_tables(connection_string, df, raise_exception=False):
    print("--------------------------------------------------------------")
    print("GETTING ROW COUNT FOR ALL TABLES")
    print("--------------------------------------------------------------")
    if isinstance(df, str):
        all_tables = df.split(",")
    else:
        all_tables = list(set(df['TABLE_NAME']))
    print(all_tables)
    engine = create_engine(connection_string)
    final_dataset_local = pd.DataFrame({})
    total = len(all_tables)
    counter = 1
    for t in all_tables:
        read_q = f"""
    select  "{t}" as tn, count(*)
    from {t};
    """
        print(read_q)
        table_vi = pd.read_sql_query(sql=read_q, con=engine)
        if raise_exception:
            num_rows = table_vi['count(*)'][0]
            print(f"{t} Has {num_rows} Rows")
            if num_rows == 0 or table_vi.empty == True:
                raise Exception(f"{t} IS EMPTY!!!! ")
        final_dataset_local = pd.concat([final_dataset_local, table_vi])
        print(f"We are {counter / total} % Done with the Queries for this DB")
        counter = counter + 1
    print(final_dataset_local)
    return final_dataset_local


def delete_databases(connection_string, db):
    engine = create_engine(connection_string)
    q = f"""
    SELECT table_schema AS "Database"
	, ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 1) AS "Size (GB)" 
FROM information_schema.tables 
where table_schema = '{db}'
GROUP BY table_schema
order by 2 desc; 
"""
    table_data_raw = pd.read_sql_query(sql=q, con=engine)
    table_data_raw['delete_query'] = "drop database " + table_data_raw['Database'] + ";"
    print(f"DELETING DATABASE {db}, Freeing-Up {table_data_raw['Size (GB)']} ")
    delete_db = table_data_raw['delete_query'][0]
    delete_archive_db = f"drop database archive_temp_{db}"
    for query in [delete_db, delete_archive_db]:
        print(query)
        engine.execute(query)


def delete_tables(connection_string, db, table_list):
    engine = create_engine(connection_string)
    total_size_freed = 0
    for table in table_list.split(","):
        q = f"""
        SELECT table_name AS "t_name"
        , ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 1) AS "Size (GB)" 
    FROM information_schema.tables 
    where table_schema = '{db}' and table_name = '{table}'
    GROUP BY table_schema
    order by 2 desc; """
        table_data_raw = pd.read_sql_query(sql=q, con=engine)
        delete_query = "drop table " + f"{db}.{table};"
        print(f"DELETING Table {db}.{table}, Freeing-Up {table_data_raw['Size (GB)']} GB")
        total_size_freed = total_size_freed+table_data_raw['Size (GB)']
        engine.execute(delete_query)
    delete_archive_db = f"drop database "f"archive_temp_{db}"
    print(delete_archive_db)
    engine.execute(delete_archive_db)
    print("--------------------------------------------------------------")
    print(f"TOTAL FREED SPACE {total_size_freed} GB!")
    print("--------------------------------------------------------------")


def compare_results_dfs(prod_count_df, backup_count_df):
    compare_df = pd.merge(prod_count_df, backup_count_df, on='tn')
    print("--------------------------------------------------------------")
    print("PRINT A COMPARISON DATAFRAME")
    print("--------------------------------------------------------------")
    print(compare_df)
    print("--------------------------------------------------------------")
    final = compare_df[compare_df['count(*)_x'] != compare_df['count(*)_y']]
    print("PRINTING A DF THAT SHOWS WHERE THE TWO DATASOURCES DIFFER -- EXPECTING A BLANK DF")
    print("--------------------------------------------------------------")
    print(final)
    print("--------------------------------------------------------------")
    if not final.empty:
        raise Exception("SOMETHING IS WRONG WITH THE BACKUP FILE !!!")
    else:
        print("The archived DB is identical to the current production DB -- YAY --- :D")


def clean_up_backups(db, output_path):
    print("--------------------------------------------------------------")
    print(f"CLEANING UP {output_path} DIRECTORY")
    print("--------------------------------------------------------------")
    bash_command1 = f"mkdir {output_path}/{db}"
    bash_command2 = f"mv {output_path}/{db}* {output_path}/{db}"
    for i in [bash_command1, bash_command2]:
        print(i)
        subprocess_cmd(i)

def run_database_archive(type):
    # LOOPING THROUGH MANY
    # for i in range(0, 16):
    #     print(f"We are on Iteration {i} of 16 or {i/16} %")
    # if type == 'patent' or type[:6] == 'upload':
    #     output_path = '/PatentDataVolume/DatabaseBackups/RawDatabase/patent_db_tables'
    # elif type == 'pregrant_publications' or type[:6] == 'pgpubs':
    #     output_path = "/PatentDataVolume/DatabaseBackups/PregrantPublications/pgpubs_db_tables"
    # else:
    #     raise NotImplementedError
    config = get_current_config(type=type, **{"execution_date": datetime.date(2022, 1, 1)})

    # Create Archive SQL FILE
    old_db, table_list = get_oldest_databases(config, db_type=type)

    if type == 'pgpubs':
        output_path = '/archive/PregrantPublications/pregrant_publications'
    else:
        output_path = '/archive/patent_'

    backup_db(config, output_path, old_db)
    upload_tables_for_testing(config, old_db, output_path, table_list)

    # QA Backup
    prod_connection_string = get_unique_connection_string(config, database=f"{old_db}", connection='DATABASE_SETUP')
    prod_data = query_for_all_tables_in_db(prod_connection_string, old_db)
    prod_count_df = get_count_for_all_tables(prod_connection_string, prod_data)

    backup_connection_string = get_unique_connection_string(config, database= f"archive_temp_{old_db}", connection='DATABASE_SETUP')
    backup_data = query_for_all_tables_in_db(backup_connection_string, f"archive_temp_{old_db}")
    backup_count_df = get_count_for_all_tables(backup_connection_string, backup_data)

    compare_results_dfs(prod_count_df, backup_count_df)

    # DELETE DB
    delete_databases(prod_connection_string, old_db)
    clean_up_backups(old_db, output_path)

def run_table_archive(config_db, table_list, output_path):
    # db = 'pregrant_publications'
    # NO SPACES ALLOWED IN TABLE_LIST
    config = get_current_config(type=config_db, **{"execution_date": datetime.date(2022, 1, 1)})
    db = config['PATENTSVIEW_DATABASES']["PROD_DB"]

    if table_list.isempty():
        raise Exception("Add Table List to DAG")
    # backup_tables(db,output_path, table_list)
    # table_list remains the same if you want to review all tables
    upload_tables_for_testing(config, db, output_path, table_list)
    # Compare archived DB to Original
    prod_connection_string = get_unique_connection_string(config, database=f"{db}", connection='DATABASE_SETUP')
    prod_count_df = get_count_for_all_tables(prod_connection_string, table_list)

    backup_connection_string = get_unique_connection_string(config, database= f"archive_temp_{db}", connection='DATABASE_SETUP')
    backup_count_df = get_count_for_all_tables(backup_connection_string, table_list)
    compare_results_dfs(prod_count_df, backup_count_df)
    delete_tables(prod_connection_string, db, table_list)


# if __name__ == '__main__':
    # type = 'pgpubs'
    # output_path ='/PatentDataVolume/DatabaseBackups/PregrantPublications'
    # config = get_current_config(type, **{"execution_date": datetime.date(2022, 1, 1)})
    # for i in range(1, 24):
    #     print("--------------------------------------------------------------")
    #     print(f"RUNNING ITERATION: {i}")
    #     print("--------------------------------------------------------------")
    #     run_database_archive(type=type)
        # run_table_archive(config)

if __name__ == '__main__':
    b_list = []
    for i in range(1976, 2022):
        temp = f'brf_sum_text_{i}'
        b_list.append(temp)
    dr_list = []
    for i in range(1976, 2022):
        temp = f'draw_desc_text_{i}'
        dr_list.append(temp)
    c_list = []
    for i in range(1976, 2022):
        temp = f'claims_{i}'
        c_list.append(temp)
    de_list = []
    for i in range(1976, 2022):
        temp = f'detail_desc_text_{i}'
        de_list.append(temp)
    tab_list = b_list + dr_list + c_list + de_list
    type = 'granted_patent'
    output_path = "/text_output/20220630/patent/download/"
    config = get_current_config(type, **{"execution_date": datetime.date(2022, 1, 1)})
    # upload_tsv_backup_files(config, output_path, 'patent_text', tab_list)
    upload_tsv_backup_files(config, output_path, 'patent_text', ['brf_sum_text_2022', 'draw_desc_text_2022', 'claims_2022', 'detail_desc_text_2022'])




