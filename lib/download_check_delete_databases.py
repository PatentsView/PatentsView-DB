import subprocess
import datetime
from lib.configuration import get_connection_string, get_current_config, get_unique_connection_string
import pymysql
from sqlalchemy import create_engine
import pandas as pd


def get_oldest_databases(config, db_type='patent'):
    print(f"Running {db_type}")
    if db_type == 'patent':
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
            print("--------------------------------")
            print(f"Found Database {db} To Archive!")
            print("--------------------------------")
    finally:
        if connection.open:
            connection.close()
    return db


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)

def backup_db(config, db):
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    if db[:6] == 'upload':
        output_path = '/PatentDataVolume/DatabaseBackups/RawDatabase/UploadBackups'
        # output_path = '/Users/bcard/db_backups'
    else:
        output_path = "/PatentDataVolume/DatabaseBackups/PregrantPublications/pregrant_publications"
    bash_command1 = f"mysqldump --defaults-file={defaults_file} --column-statistics=0  {db} > {output_path}/{db}_backup.sql"
    # bash_command1 = f"mydumper -B {db} -o {output_path}  -c --long-query-guard=9000000 -v 3"
    print(bash_command1)
    subprocess_cmd(bash_command1)


def backup_tables(db, table_list):
    # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    if db == 'patent':
        output_path = '/PatentDataVolume/DatabaseBackups/RawDatabase/patent_db_tables'
    elif db == 'pregrant_publications':
        output_path = "/PatentDataVolume/DatabaseBackups/PregrantPublications/pgpubs_db_tables"
    else:
        Exception("Database Not Configured")

    bash_command1 = f"mydumper -B {db} -T {table_list} -o {output_path}  -c --long-query-guard=9000000 -v 3"
    print(bash_command1)
    subprocess_cmd(bash_command1)


def upload_db_for_testing(config, db):
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    q = f"create database archive_check_{db}"
    print(q)
    engine.execute(q)
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    bash_command = f"mysql --defaults-file={defaults_file} -f archive_check_{db} < {db}_backup.sql"
    print(bash_command)
    subprocess_cmd(bash_command)


def upload_tables_for_testing(config, db, table_list):
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    archive_db = f"archive_check_{db}"
    q = f"create database {archive_db}"
    print(q)
    engine.execute(q)
    if db == 'patent':
        output_path = '/PatentDataVolume/DatabaseBackups/RawDatabase/patent_db_tables'
    elif db == 'pregrant_publications':
        output_path = "/PatentDataVolume/DatabaseBackups/PregrantPublications/pgpubs_db_tables"
    for table in table_list.split(","):
        # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
        bash_command1 = f"gunzip -d {output_path}/{db}.{table}-schema.sql.gz"
        bash_command2 = f"gunzip -d {output_path}/{db}.{table}.sql.gz"
        bash_command3 = f"mysql --defaults-file=resources/sql.conf -f {archive_db} < {output_path}/{db}.{table}-schema.sql"
        bash_command4 = f"mysql --defaults-file=resources/sql.conf -f {archive_db} < {output_path}/{db}.{table}.sql"
        bash_command5 = f"gzip {output_path}/{db}.{table}.sql"
        bash_command6 = f"gzip {output_path}/{db}.{table}-schema.sql"
        for i in [bash_command1, bash_command2, bash_command3, bash_command4, bash_command5, bash_command6]:
            print(i)
            subprocess_cmd(i)


def query_for_all_tables_in_db(connection_string, temp):
    engine = create_engine(connection_string)
    q = f"""
    select TABLE_NAME
    from information_schema.tables
    where TABLE_SCHEMA = '{temp}';
    """
    print(q)
    table_data_raw = pd.read_sql_query(sql=q, con=engine)
    return table_data_raw


def get_count_for_all_tables(connection_string, df):
    if type(df)==str:
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
        final_dataset_local = pd.concat([final_dataset_local, table_vi])
        print(f"We are {counter / total} % Done with the Queries for this DB")
        counter = counter + 1
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
    delete_archive_db = f"drop database "f"archive_check_{db}"
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
    delete_archive_db = f"drop database "f"archive_check_{db}"
    print(delete_archive_db)
    engine.execute(delete_archive_db)
    print("----------------------------------------")
    print(f"TOTAL FREED SPACE {total_size_freed} GB!")
    print("----------------------------------------")

def run_database_archive(config, type):
    # LOOPING THROUGH MANY
    # for i in range(0, 16):
    #     print(f"We are on Iteration {i} of 16 or {i/16} %")

    # Create Archive SQL FILE
    old_db = get_oldest_databases(config, db_type=type)
    backup_db(config, old_db)
    upload_db_for_testing(config, old_db)

    # Compare archived DB to Original
    prod_connection_string = get_unique_connection_string(config, database=f"{old_db}", connection='DATABASE_SETUP')
    prod_data = query_for_all_tables_in_db(prod_connection_string, old_db)
    prod_count_df = get_count_for_all_tables(prod_connection_string, prod_data)

    backup_connection_string = get_unique_connection_string(config, database= f"archive_check_{old_db}", connection='DATABASE_SETUP')
    backup_data = query_for_all_tables_in_db(backup_connection_string, f"archive_check_{old_db}")
    backup_count_df = get_count_for_all_tables(backup_connection_string, backup_data)
    breakpoint()
    compare_df = pd.merge(prod_count_df, backup_count_df, on='tn')
    print("PRINT A COMPARISON DATAFRAME")
    print(compare_df)
    print("--------------------------------------------------------------")
    final = compare_df[compare_df['count(*)_x'] != compare_df['count(*)_y']]
    print("PRINTING A DF THAT SHOWS WHERE THE TWO DATASOURCES DIFFER -- EXPECTING A BLANK DF")
    print(final)
    if not final.empty:
        raise Exception("SOMETHING IS WRONG WITH THE BACKUP FILE !!!")
    else:
        print("The archived DB is identical to the current production DB -- YAY --- :D")

    # DELETE DB
    delete_databases(prod_connection_string, old_db)


def run_table_archive(config):
    db = 'patent'
    # db = 'pregrant_publications'
    # NO SPACES ALLOWED IN TABLE_LIST
    table_list = "assignee_20210330,assignee_20210629,assignee_archive,lawyer_20210330"
    # backup_tables(db, table_list)
    # table_list remains the same if you want to review all tables
    upload_tables_for_testing(config, db, table_list)
    # Compare archived DB to Original
    prod_connection_string = get_unique_connection_string(config, database=f"{db}", connection='DATABASE_SETUP')
    prod_count_df = get_count_for_all_tables(prod_connection_string, table_list)

    backup_connection_string = get_unique_connection_string(config, database= f"archive_check_{db}", connection='DATABASE_SETUP')
    backup_count_df = get_count_for_all_tables(backup_connection_string, table_list)

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
    delete_tables(prod_connection_string, db, table_list)


if __name__ == '__main__':
    config = get_current_config('patent', **{"execution_date": datetime.date(2022, 1, 1)})
    run_database_archive(config, type='patent')
    # run_table_archive(config)
