import subprocess
import mysql.connector
import datetime
from lib.configuration import get_connection_string, get_current_config, get_unique_connection_string
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from dateutil.relativedelta import relativedelta
from QA.DatabaseTester import DatabaseTester

def get_oldest_table(config, table_type):
    current_db = config['PATENTSVIEW_DATABASES']["PROD_DB"]
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                 user=config['DATABASE_SETUP']['USERNAME'],
                                 password=config['DATABASE_SETUP']['PASSWORD'],
                                 db=current_db,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    table_type_length = len(table_type)
    q = f"""
    select  min(table_name)
    from information_schema.tables
    where table_schema = "{current_db}" and left(table_name, {table_type_length}) = '{table_type}' and table_type = "BASE TABLE"
    """
    print(q)
    try:
        if not connection.open:
            connection.connect()
        with connection.cursor() as generic_cursor:
            generic_cursor.execute(q)
            db = generic_cursor.fetchall()[0][0]
    finally:
        if connection.open:
            connection.close()
    return db, table_type_length


def get_oldest_database(config, db_type='granted_patent'):
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
        raise NotImplementedError(f"Database type {db_type} not Configured")
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
            q2 = f"""
select table_name 
from information_schema.tables
where table_schema = '{db}'
and table_type = 'BASE TABLE'
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

def find_data_collection_server_path(db_type, data_type):
    if db_type == 'pgpubs':
        if data_type == 'database':
            output_path = '/DatabaseBackups/PregrantPublications/pregrant_publications'
        elif data_type == 'table':
            output_path = '/DatabaseBackups/PregrantPublications/pgpubs_db_tables'
    elif db_type == 'granted_patent':
        if data_type == 'database':
            output_path = '/DatabaseBackups/patent_'
        elif data_type == 'table':
            output_path = '/DatabaseBackups/RawDatabase/patent_db_tables'
    return output_path


def backup_tables(db, output_path, table_list):
    defaults_file = "resources/sql.conf"
    bash_command1 = f"mydumper --defaults-file={defaults_file} -B {db} -T {table_list} -o {output_path}  -c --long-query-guard=9000000 -v 3"
    print(bash_command1)
    subprocess_cmd(bash_command1)
    from os import listdir
    from os.path import isfile, join
    files_in_output_path = [f for f in listdir(output_path) if isfile(join(output_path, f))]
    print(table_list)
    print(files_in_output_path)
    files_to_backup = [f for f in files_in_output_path if table_list in f and "schema" not in f ]
    file = files_to_backup[0].replace(".sql.gz", "")
    assert len(files_to_backup)>=1
    return file

def upload_tables_for_testing(config, db, output_path, table_list):
    print("--------------------------------------------------------------")
    print("UPLOADING DATABASE BACKUPS FOR QA")
    print("--------------------------------------------------------------")
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    archive_db = f"archive_temp_{db}"
    q = f"create database if not exists {archive_db}"
    print(q)
    engine.execute(q)
    # defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    defaults_file = "resources/sql.conf"
    if isinstance(table_list, str):
        for table in table_list.split(","):
            schema_file_path = f"{output_path}/{table}-schema.sql"
            sql_data_file_path = f"{output_path}/{table}.sql"
            bash_command1 = f"gunzip -d {schema_file_path}.gz"
            bash_command2 = f"gunzip -d {sql_data_file_path}.gz"
            bash_command3 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {schema_file_path}"
            bash_command4 = f"mysql --defaults-file={defaults_file} -f {archive_db} < {sql_data_file_path}"
            bash_command5 = f"gzip {sql_data_file_path}"
            bash_command6 = f"gzip {schema_file_path}"
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
        print(f"We are {counter / total:.2%} Done with the Queries for this DB")
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

def check_table_exists(config, database, table_name):
    q = f"""
SELECT * FROM information_schema.tables
WHERE table_name = '{table_name}' and table_schema = '{database}'
"""
    connection_string = get_connection_string(config, database='PROD_DB')
    engine = create_engine(connection_string)
    count_value = len(pd.read_sql_query(sql=q, con=engine))
    if count_value < 1:
        return False
    else:
        return True

def get_prior_quarter_start(date):
    current_quarter = (date.month - 1) // 3 + 1
    first_day_current_quarter = datetime.date(date.year, (current_quarter - 1) * 3 + 1, 1)
    first_day_last_quarter = first_day_current_quarter - datetime.timedelta(weeks=13)
    return first_day_last_quarter

def run_database_archive(type, output_override=None, **kwargs):
    config = get_current_config(type=type, **kwargs)
    db_type = "upload" if type == "granted_patent" else "pgpubs"
    # Create Archive SQL FILE
    old_db, table_list = get_oldest_database(config, db_type=type)
    oldest_db_date = datetime.datetime.strptime(old_db[7:], '%Y%m%d').date()

    first_day_last_quarter = get_prior_quarter_start(kwargs['execution_date'])
    if oldest_db_date < first_day_last_quarter:
        if output_override is not None:
            output_path = output_override
        else:
            output_path = find_data_collection_server_path(db_type=type, data_type="database")

        db_archive_list = pd.date_range(start=oldest_db_date, end=first_day_last_quarter, inclusive="left", freq="7D").tolist()
        db_archive_list_clean = [db_type + "_" + i.strftime('%Y%m%d') for i in db_archive_list]
        print(f"These databases will be archived:  {db_archive_list_clean}")

        for archive_database_candidate in db_archive_list_clean:
            backup_db(config, output_path, archive_database_candidate)
            upload_tables_for_testing(config, archive_database_candidate, output_path, table_list)

            # QA Backup
            prod_connection_string = get_unique_connection_string(config, database=f"{archive_database_candidate}", connection='DATABASE_SETUP')
            prod_data = query_for_all_tables_in_db(prod_connection_string, archive_database_candidate)
            prod_count_df = get_count_for_all_tables(prod_connection_string, prod_data)

            backup_connection_string = get_unique_connection_string(config, database=f"archive_temp_{archive_database_candidate}",
                                                                    connection='DATABASE_SETUP')
            backup_data = query_for_all_tables_in_db(backup_connection_string, f"archive_temp_{archive_database_candidate}")
            backup_count_df = get_count_for_all_tables(backup_connection_string, backup_data)

            compare_results_dfs(prod_count_df, backup_count_df)

            # DELETE DB
            delete_databases(prod_connection_string, archive_database_candidate)
            # clean_up_backups(old_db, output_path)
    else:
        print("Nothing to Archive this run")

def run_table_archive(type, tablename, **kwargs):
    """
    This function archives old tables in a database.

    Parameters:
    type (str): The type of the database.
    tablename (str): The name of the table to be archived. 
        for datestamped tables (e.g. inventor_YYYYMMDD), 
        this name should include the underscore, but exclude the date code.
    **kwargs: Additional keyword arguments for configuration.

    Raises:
    Exception: If no table name is provided.
    """
    config = get_current_config(type=type, **kwargs)
    # Get the production database from the configuration
    db = config['PATENTSVIEW_DATABASES']["PROD_DB"]
    # Raise an exception if no table name is provided
    if not tablename:
        raise Exception("Add Table List to DAG")
    output_path = find_data_collection_server_path(db_type=type, data_type="table")

    # Get the oldest table and its length
    oldest_table, length = get_oldest_table(config, tablename)
    # Convert the date string in the oldest table name to a date object
    print(f'this is the oldest table and length: {oldest_table}, {length}')
    oldest_table_date = datetime.datetime.strptime(oldest_table[length:], '%Y%m%d').date()
    first_day_last_quarter = get_prior_quarter_start(kwargs['execution_date']) - datetime.timedelta(weeks=1)
    if oldest_table_date < first_day_last_quarter:
        # Create a list of dates from the oldest table date to the start of the last quarter
        table_archive_list = pd.date_range(start=oldest_table_date, end=first_day_last_quarter, inclusive="left", freq="Q").tolist()
        # Append the date to the table name for each date in the list
        table_archive_list_clean = [tablename + i.strftime('%Y%m%d') for i in table_archive_list]
        print(f"These databases will be archived:  {table_archive_list_clean}")

        for archive_table_candidate in table_archive_list_clean:
            table_exists = check_table_exists(config, db, archive_table_candidate)
            if table_exists:
                # Backup the table
                file = backup_tables(db, output_path, archive_table_candidate)
                # Upload the table for testing
                upload_tables_for_testing(config, db, output_path, file)

                # Compare archived DB to Original
                prod_connection_string = get_unique_connection_string(config, database=f"{db}", connection='DATABASE_SETUP')
                prod_count_df = get_count_for_all_tables(prod_connection_string, archive_table_candidate)

                backup_connection_string = get_unique_connection_string(config, database= f"archive_temp_{db}", connection='DATABASE_SETUP')
                backup_count_df = get_count_for_all_tables(backup_connection_string, archive_table_candidate)

                # Compare the count dataframes of the production and backup databases
                compare_results_dfs(prod_count_df, backup_count_df)
                # Delete the table from the production database
                delete_tables(prod_connection_string, db, archive_table_candidate)
                
                print("---------------------------------------------------------------")
                print(f"FINISHED ARCHIVING {archive_table_candidate} !!!")
                print("---------------------------------------------------------------")
            else:
                continue


if __name__ == '__main__':
    type = 'pgpubs'
    # config = get_current_config(type, **{"execution_date": datetime.date(2023, 10, 1)})
    #run_database_archive(type=type, **{"execution_date": datetime.date(2023, 10, 1)})
    run_table_archive("pgpubs", "disambiguated_inventor_ids_", **{"execution_date": datetime.date(2023, 10, 1)})






