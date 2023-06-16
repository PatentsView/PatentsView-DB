import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from time import time
from datetime import datetime
import json
from lib.configuration import get_connection_string, get_current_config

## Documentation drafted by Mintlify Doc Writer

### granted_patent_crosswalk_template create syntax for reference
# CREATE TABLE `granted_patent_crosswalk_template` (
#   `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
#   `document_number` bigint(16) DEFAULT NULL,
#   `patent_id` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
#   `application_number` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
#   `g_version_indicator` date DEFAULT NULL,
#   `latest_pat_flag` tinyint(1) DEFAULT NULL,
#   `pg_version_indicator` date DEFAULT NULL,
#   `latest_pub_flag` tinyint(1) DEFAULT NULL,
#   `created_date` timestamp NULL DEFAULT current_timestamp(),
#   `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
#   PRIMARY KEY (`id`),
#   KEY `document_number` (`document_number`),
#   KEY `patent_id` (`patent_id`),
#   KEY `application_number` (`application_number`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

def create_outer_patent_publication_crosswalk(**kwargs):
    """
    creates, configures, and populates the quarterly patent-publicatin crosswalk table.
    kwargs must include key 'execution_date' required for config
    """
    # in Spring 2023 run, total creation runtime was ~20 minutes prior to adding currency flags. likely some room for improvement, but may not be a priority.
    config = get_current_config('pgpubs', schedule="quarterly", **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    end_date = config['DATES']["end_date"] 
    if len(end_date) == 8: # formatted '%Y%m%d'
        end_date = datetime.strptime(end_date,"%Y%m%d").strftime("%Y-%m-%d") # convert to the format MySQL will expect
    print(f"beginning creation of publication crosswalk for data ending {end_date}")

    query_dict = {}

    # currently will fail if the table for the current quarter already exists.
    # could consider dropping the table if it exists first, but for now let's require human intervention for re-run.
    query_dict['table_create_query'] = f"""
    -- creating crosswalk table...
    CREATE TABLE `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}`
    LIKE `pregrant_publications`.`granted_patent_crosswalk_template`
    """ 

    query_dict['trigger_create_query'] = f"""
    -- configuring uuid generator...
    CREATE TRIGGER `pregrant_publications`.`before_insert_crosswalk_{end_date.replace('-','')}`
        BEFORE INSERT
        ON `granted_patent_crosswalk_{end_date.replace('-','')}`
        FOR EACH ROW SET new.id = uuid();
    """

    # Including commented conditions for excluding withdrawn patents in case we change course on their includion in the future.
    query_dict['table_population_query'] = f"""
    -- populating crosswalk table...
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

    query_dict['h_pat_rmv_query'] = f"""
    -- removing H patents from crosswalk table...
    DELETE FROM `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}` 
    WHERE patent_id LIKE 'H%'
    """
    # currently H patents don't have corresponding application or publication data, and are the only patents that don't have either. 
    # They won't be useful records in the table until we eventually reparse their data.

    query_dict['latest_pub_create_query'] = f"""
    -- creating temp table of latest publications...
    CREATE TEMPORARY TABLE `pregrant_publications`.`temp_xwalk_pub_latest` (
        `application_number` varchar(16) DEFAULT NULL,
        `pg_max_vi` DATE DEFAULT NULL,
        PRIMARY KEY (`application_number`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    query_dict['latest_pub_populate_query'] = f"""
    -- populating temp table of latest publications...
    INSERT INTO `pregrant_publications`.`temp_xwalk_pub_latest`
    (application_number, pg_max_vi)
    SELECT application_number, MAX(pg_version_indicator)
    FROM `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}`
    GROUP BY 1
    """

    query_dict['latest_pat_create_query'] = f"""
    -- creating temp table of latest patents...
    CREATE TEMPORARY TABLE `pregrant_publications`.`temp_xwalk_pat_latest` (
        `application_number` varchar(16) DEFAULT NULL,
        `g_max_vi` DATE DEFAULT NULL,
        PRIMARY KEY (`application_number`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    query_dict['latest_pat_populate_query'] = f"""
    -- populating temp table of latest patents...
    INSERT INTO `pregrant_publications`.`temp_xwalk_pat_latest`
    (application_number, g_max_vi)
    SELECT application_number, MAX(g_version_indicator)
    FROM `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}`
    GROUP BY 1
    """

    query_dict['pat_flag_update_query'] = f"""
    -- setting latest patent flag...
    UPDATE `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}` xw
    LEFT JOIN `pregrant_publications`.`temp_xwalk_pat_latest` pat ON (xw.application_number = pat.application_number AND xw.g_version_indicator = pat.g_max_vi)
    SET xw.latest_pat_flag = CASE WHEN pat.g_max_vi IS NOT NULL THEN 1 ELSE 0 END
    """

    query_dict['pub_flag_update_query'] = f"""
    -- setting latest publication flag...
    UPDATE `pregrant_publications`.`granted_patent_crosswalk_{end_date.replace('-','')}` xw
    LEFT JOIN `pregrant_publications`.`temp_xwalk_pub_latest` pub ON (xw.application_number = pub.application_number AND xw.pg_version_indicator = pub.pg_max_vi)
    SET xw.latest_pat_flag = CASE WHEN pat.g_max_vi IS NOT NULL THEN 1 ELSE 0 END
    """

    #instantiating session to use temporary tables and cause rollback on error
    Session = sessionmaker(engine)
    with Session.begin() as session:
        for query_name in query_dict:
            print(f"running {query_name}:")
            query = query_dict[query_name]
            print(query)
            query_start_time = time()
            session.execute(query)
            query_end_time = time()
            t = query_end_time - query_start_time
            m,s = divmod(t, 60)
            print(f"This query took {m:02.0f}:{s:04.1f} (m:s)")
        print("committing SQL session...")

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

def check_application_number_formatting(table, engine):
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

def check_null_application_number(table, engine):
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
    intended to be used on granted_patent_crosswalk_{DATESTAMP} tables only
    
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
        return f"FAILED: there are {g_vi_bound_count} g_version_indicator values out of valid range and {pg_vi_bound_count} pg_version_indicator values out of valid range."


def check_unique_current_ids(table, engine):
    """
    Checks whether each applicaiton in the table has a unique record marked as current
    intended to be used on granted_patent_crosswalk_{DATESTAMP} tables only

    :param table: The name of the table in the pregrant_publications database that needs to be checked for duplicate current records
    :param engine: a SQLAlchemy engine object used to execute SQL queries
    :return: either "PASSED" if there are no application IDs with multiple records flagged as current in the specified table, or
        or a string starting with "FAILED: " followed by the number of application IDs with multiple records flagged as current.
    """

    print("testing that each applicaiton has a unique 'current' record")
    uniqueness_query = f"""
    SELECT COUNT(*) 
    FROM (
        SELECT application_number, patent_id, document_number, COUNT(*) 
        FROM `pregrant_publications`.`{table}`
        WHERE latest_pat_flag = 1
        AND latest_pub_flag = 1
        GROUP BY 1,2,3
        HAVING COUNT(*) > 1
    ) sq
    """
    print(uniqueness_query)
    duplication_count = engine.execute(uniqueness_query).fetchone()[0]

    if duplication_count == 0:
        return "PASSED"
    else:
        return f"FAILED: {duplication_count} application_numbers with multiple records flagged as current"


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
    test_status['pgpub_id_formatting'] = check_pgpub_id_formatting(table_name, engine)
    # check patent_id formatting (all valid prefixes or non-prefixed, prefixed IDs have no leading zero)
    test_status['patent_id_formatting'] = check_patent_id_formatting(table_name, engine)
    # check application_number formatting (all numeric, correct length)
    test_status['application_number_formatting'] = check_application_number_formatting(table_name, engine)
    # check no application_number nulls
    test_status['app_id_nulls'] = check_null_application_number(table_name, engine)
    # all rows have either a pgpub_id or patent_id (or both)
    test_status['id_completeness'] = check_missing_pat_pub_ids(table_name, engine)
    # check all g_version_indicators and pg_version_indicators within date bounds
    test_status['version_bounding'] = check_version_bounding(table_name, end_date, engine)
    # check uniqueness of all IDs where flagged as latest documents
    test_status['unique_current_ids'] = check_unique_current_ids(table_name, engine)
        # some applications will have multiple patent_ids and/or pgpub_ids due to re-publication, withdrawals, etc.
        # flags identify the most current patent and pgpub for any applicaiton within the date range of the update
    # return all issues at once so they can be checked in a single debugging/correction session
    print("QA tests complete. test results:")
    print(json.dumps(test_status, indent=0))
    if not all([test_status[test] == 'PASSED' for test in test_status]):
        failures = [test for test in test_status if test_status[test] != 'PASSED']
        raise Exception(f"crosswalk QA testing failed. Failed tests: {failures}")


if __name__ == "__main__":
    raise NotImplementedError
