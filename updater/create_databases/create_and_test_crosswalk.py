import pymysql
from sqlalchemy import create_engine
from time import time
import json
from lib.configuration import get_connection_string, get_current_config

## Documentation drafted by Mintlify Doc Writer

def create_outer_patent_publication_crosswalk(**kwargs):
    """
    creates, configures, and populates the quarterly patent-publicatin crosswalk table.
    kwargs must include key 'execution_date' required for config
    """
    # in Spring 2023 run, total runtime was ~20 minutes. likely some room for improvement, but may not be a priority.
    # with modifications, runtime was about ~19 minutes - 8.4M rows
    config = get_current_config('pgpubs', schedule="quarterly", **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    end_date = config['DATES']["end_date"] # formatted '%Y-%m-%d'
    print(f"beginning creation of publication crosswalk for data ending {end_date}")

    table_create_query = f"""
    -- creating table...
    CREATE TABLE `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}`
    LIKE `pregrant_publications`.`granted_patent_crosswalk_template`
    """ 

    # maybe try skipping this, then setting uuids after population, then adding index?
    trigger_create_query = f"""
    -- configuring uuid generator...
    CREATE TRIGGER `pregrant_publications`.`before_insert_crosswalk_{end_date.replace('-','')}`
        BEFORE INSERT
        ON `granted_patent_crosswalk_{end_date.replace('-','')}`
        FOR EACH ROW SET new.id = uuid();
    """

    # Including commented conditions for excluding withdrawn patents in case we change course on their includion in the future.
    table_population_query = f"""
    -- populating table...
    INSERT INTO `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}`
    (`document_number`, `patent_id`, `application_number`, `g_version_indicator`, `pg_version_indicator`)

    SELECT 
        `q`.`document_number`
        ,`q`.`patent_id`
        ,`q`.`application_number`
        ,`q`.`g_version_indicator`
        ,`q`.`pg_version_indicator`
    FROM (
        SELECT 
            `b`.`document_number` AS `document_number`
            ,`d`.`id` AS `patent_id`
            ,`a`.`application_number` AS `application_number` 
            ,`d`.`version_indicator` AS `g_version_indicator`
            ,`b`.`version_indicator` AS `pg_version_indicator`
        FROM 
            `pregrant_publications`.`publication` `b` 
        LEFT JOIN `pregrant_publications`.`application` `a` 
            ON (`b`.`document_number` = `a`.`document_number`)
        LEFT JOIN `patent`.`application` `c` 
            ON (`a`.`application_number` = `c`.`number_transformed`)
        LEFT JOIN `patent`.`patent` `d` 
            ON (`c`.`patent_id` = `d`.`id` 
                AND `d`.`version_indicator` <= '{end_date}'
                -- AND `d`.`withdrawn` = 0
                )
        WHERE `b`.`version_indicator` <= '{end_date}'

        UNION

        SELECT 
            `b`.`document_number` AS `document_number`
            ,`d`.`id` AS `patent_id`
            ,`c`.`number_transformed` AS `application_number` 
            ,`d`.`version_indicator` AS `g_version_indicator`
            ,`b`.`version_indicator` AS `pg_version_indicator`
        FROM 
            `patent`.`patent` `d` 
        LEFT JOIN `patent`.`application` `c` 
            ON (`d`.`id` = `c`.`patent_id`)
        LEFT JOIN `pregrant_publications`.`application` `a` 
            ON (`c`.`number_transformed` = `a`.`application_number`)
        LEFT JOIN `pregrant_publications`.`publication` `b` 
            ON (`a`.`document_number` = `b`.`document_number`
                AND `b`.`version_indicator` <= '{end_date}'
                )
        WHERE `d`.`version_indicator` <= '{end_date}'
        -- AND `d`.`withdrawn` = 0
    ) `q`
    """

    h_pat_rmv_query = "DELETE FROM `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}` WHERE patent_id LIKE 'H%'"
    # currently H patents don't have corresponding application or publication data, and are the only patents that don't have either. 
    # They won't be useful records in the table until we eventually reparse their data.

    for query in [table_create_query, trigger_create_query, table_population_query, h_pat_rmv_query]:
        print(query)
        query_start_time = time()
        engine.execute(query)
        query_end_time = time()
        print("This query took:", query_end_time - query_start_time, "seconds")

    print(f"creation and population of granted_patent_crosswalk_{end_date.replace('-','')} is complete.")


def check_patent_id_formatting(table, engine):
    """
    checks the formatting of patent IDs in a given table and returns a pass or fail message.
    
    :param table: The name of the table being checked for patent ID formatting
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no rows with invalid patent_id formatting in the specified table, 
        or a string starting with "FAILED: " followed by the number of rows with invalid patent_id formatting.
    """
    # H patents should not be in the crosswalk table
    # expected patterns for patent_ids:
    # "PP[0-9]{4,5}"
    # "RE[0-9]{5}"
    # "(D|T)[0-9]{6}"
    # "[0-9]{7,8}"

    pat_id_formatting_qry = f"""
    SELECT COUNT(*) 
    FROM {table}
    WHERE patent_id IS NOT NULL
    AND NOT patent_id REGEXP "[0-9]{{7,8}}|(D|T)[0-9]{{6}}|RE[0-9]{{5}}|PP[0-9]{{4,5}}"
    """
    print(pat_id_formatting_qry)
    bad_pat_id_count = engine.execute(pat_id_formatting_qry).fetchone()[0]

    if bad_pat_id_count == 0:
        return "PASSED"
    else:
        return f"FAILED: {bad_pat_id_count} rows with invalid pgpub_id formatting"

def check_pgpub_id_formatting(table, engine):
    """
    checks the formatting of pgpub IDs in a given table and returns a pass or fail message.
    
    :param table: The name of the table being checked for pgpub ID formatting
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no rows with invalid pgpub ID formatting in the specified table, 
        or a string starting with "FAILED: " followed by the number of rows with invalid pgpub ID formatting.
    """
    # expected pattern for pgpub_ids:
    # (year)(7 digits) starting with year 2001

    pgp_id_formatting_query = f"""
    SELECT COUNT(*) 
    FROM {table}
    WHERE document_number IS NOT NULL
    AND NOT document_number REGEXP "20[012][0-9]{{8}}"
    """
    print(pgp_id_formatting_query)
    bad_pgp_id_count = engine.execute(pgp_id_formatting_query).fetchone()[0]

    if bad_pgp_id_count == 0:
        return "PASSED"
    else:
        return f"FAILED: {bad_pgp_id_count} rows with invalid pgpub_id formatting"

def check_application_id_formatting(table, engine):
    """
    checks the formatting of application IDs in a given table and returns a pass or fail message.
    
    :param table: The name of the table being checked for application ID formatting
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no rows with invalid application ID formatting in the specified table, 
        or a string starting with "FAILED: " followed by the number of rows with invalid application ID formatting.
    """
    # expected pattern for application ids:
    # 8 digits unpunctuated, or, for design patents, D followed by 7 digits
    # [D0-9][0-9]{7}
    app_id_formatting_query = f"""
    SELECT COUNT(*) 
    FROM {table}
    WHERE NOT application_number REGEXP "[D0-9][0-9]{{7}}"
    """
    bad_app_num_query = engine.execute(app_id_formatting_query).fetchone()[0]

    if bad_app_num_query == 0:
        return "PASSED"
    else:
        return f"FAILED: {bad_app_num_query} rows with invalid application number formatting"

def check_null_application_id(table, engine):
    """
    The function checks for null application numbers in a specified table and returns a pass or fail message.
    
    :param table: The name of the table in the pregrant_publications database that we want to check for null application numbers
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no rows with null application numbers in the specified table, or a string
        starting with "FAILED:" followed by the number of rows with null application numbers if there are any.
    """
    print("testing for null applicaiton_numbers")
    app_num_null_qry = f"SELECT COUNT(*) FROM `pregrant_publications`.`{table}` WHERE application_number IS NULL"
    print(app_num_null_qry)
    app_num_null_count = engine.execute(app_num_null_qry).fetchone()[0]

    if app_num_null_count == 0:
        return "PASSED"
    else:
        return f"FAILED: {app_num_null_count} rows with null application number"

def check_missing_pat_pub_ids(table, engine):
    """
    The function checks if a table has at least one patent or document ID and returns a pass or fail message.
    
    :param table: The name of the table in the pregrant_publications database that needs to be checked for missing patent or document IDs
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no rows with missing patent or document ids in the specified table, or
        or a string starting with "FAILED: " followed by the number of rows with missing ids if there are any.
    """
    print("testing for presence of at least one ID")
    no_id_qry = f"SELECT COUNT(*) FROM `pregrant_publications`.`{table}` WHERE patent_id IS NULL AND document_number IS NULL"
    print(no_id_qry)
    no_id_count = engine.execute(no_id_qry).fetchone()[0]

    if no_id_count == 0:
        return "PASSED"
    else:
        return f"FAILED: {no_id_count} rows missing both patent and document id"

def check_version_bounding(table, end_date, engine):
    """
    The function checks if version indicators in a given table are within valid date ranges.
    
    :param table: The name of the table being queried in the database
    :param end_date: The end date is a date value used as an upper bound for version indicators in a database table.
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no g_version_indicator or pg_version_indicator values out of the valid range, or a string starting
        with "FAILED:" indicating the number of g_version_indicator and pg_version_indicator values that are out of the valid range if there are any.
    """
    print("testing for correctly bounded version indicators")
    g_vi_bound_query = f"""
    SELECT COUNT(*) FROM `pregrant_publications`.`{table}` 
    WHERE g_version_indicator > '{end_date}'
    OR g_version_indicator < '1976-01-01'
    """
    pg_vi_bound_query = f"""
    SELECT COUNT(*) FROM `pregrant_publications`.`{table}` 
    WHERE pg_version_indicator > '{end_date}'
    OR pg_version_indicator < '2001-01-01'
    """
    print(g_vi_bound_query)
    g_vi_bound_count = engine.execute(g_vi_bound_query).fetchone()[0]
    print(pg_vi_bound_query)
    pg_vi_bound_count = engine.execute(pg_vi_bound_query).fetchone()[0]

    if g_vi_bound_count == 0 and pg_vi_bound_count == 0:
        return "PASSED"
    else:
        return f"FAILED: there are {g_vi_bound_count} g_version_indicator value out of valid range and {pg_vi_bound_count} pg_version_indicator values out of valid range."


def qc_crosswalk(**kwargs):
    """
    The function conducts a number of quality assurance tests on the granted patent crosswalk table.
    All tests are conducted regardless of the results of any individual test, and if any issues were identified,
    all failures are reported at the same time.
    kwargs must include key 'execution_date' required for config.
    """
    config = get_current_config('pgpubs', schedule="quarterly", **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    end_date = config['DATES']['END_DATE']
    table_name = f"granted_patent_crosswalk_{end_date.replace('-','')}"

    test_status = {}
    # check pgpub_id formatting (all numeric, correct length)
    test_status['pgpub_id'] = check_pgpub_id_formatting(table_name, engine)
    # check patent_id formatting (all valid prefixes or non-prefixed, prefixed IDs have no leading zero)
    test_status[' patent_id'] = check_patent_id_formatting(table_name, engine)
    # check application_number formatting (all numeric, correct length)
    test_status['application_id'] = check_application_id_formatting(table_name, engine)
    # check no application_number nulls
    test_status['app_id_nulls'] = check_null_application_id(table_name, engine)
    # all rows have either a pgpub_id or patent_id (or both)
    test_status['id_completeness'] = check_missing_pat_pub_ids(table_name, engine)
    # check all g_version_indicators and pg_version_indicators within date bounds
    test_status['version_bounding'] = check_version_bounding(table_name, end_date, engine)
    # currently no test for duplication:
        # patents_ids can have multiple valid document_numbers (unclear relationship currently) 
        # see example of application number 15/138490 in the uspto patent search - one matching patent, 4 matching document_numbers, all with the same title, inventors, continuity, etc.
        # document numbers should have only one current, unqithdrawn patent_id, but may have many withdrawn patent_ids.
        # application_numbers will be duplicated due to both above cases.
    
    # return all issues at once so they can be checked in a single debugging/correction session
    if not all([test_status[test] == 'PASSED' for test in test_status]):
        raise Exception(f"crosswalk QA testing failed. Test status:\n{json.dumps(test_status, indent=0)}")



if __name__ == "__main__":
    raise NotImplementedError