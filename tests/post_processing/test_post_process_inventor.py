import pytest

from lib.configuration import get_config, get_connection_string


@pytest.fixture()
def config():
    config = get_config()
    config["DATABASE"]["NEW_DB"] = 'sarvo_test_db'
    yield config


@pytest.fixture(scope="module")
def clean_data():
    yield
    config = get_config()
    config["DATABASE"]["NEW_DB"] = 'sarvo_test_db'
    from sqlalchemy import create_engine
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    engine.execute("TRUNCATE TABLE inventor;")
    engine.execute("TRUNCATE TABLE disambiguated_inventor_ids;")


def pytest_generate_tests(metafunc):
    """
    Generate test cases from Excel files
    :param metafunc:
    """
    if "inventor_group" in metafunc.fixturenames:
        import pandas as pd
        test_cases = pd.read_excel('./PVDI_11_Inventor_Canonical_Test_Cases.xlsx', sheet_name=None)
        ids = []
        argnames = ["inventor_group", 'name_first', 'name_last']
        argvalues = []
        for name, sheet in test_cases.items():
            name_first = sheet.final_name_first.unique().tolist()[0]
            name_last = sheet.final_name_last.unique().tolist()[0]
            argvalues.append((sheet, name_first, name_last))
        metafunc.parametrize(argnames, argvalues, ids=ids)
    if metafunc.function.__name__ == 'test_inventor_generator':
        import pandas as pd
        from updater.post_processing.post_process_inventor import precache_inventors
        test_cases = pd.read_excel('./PVDI_11_Inventor_Generator_Test_Cases.xlsx')
        ids = []
        argnames = ["limit", "offset", "expected"]
        argvalues = []
        for idx, row in test_cases.iterrows():
            argvalues.append((row.lmt, row.offset, row.expected))
            ids.append(idx)
        metafunc.parametrize(argnames, argvalues, ids=ids)


class TestPostProcessInventor:
    def test_stopword_file(self, config):
        import os.path
        stop_word_file = "{folder}/post_processing/inventor_name_stopwords.txt".format(folder=config["FOLDERS"][
            "persistent_files"])
        assert (os.path.isfile(stop_word_file))
        from updater.post_processing.post_process_inventor import get_inventor_stopwords
        stopwords = get_inventor_stopwords(config)
        assert (len(stopwords) == 119)

    def test_inventor_clean(self, config, inventor_group, name_first, name_last):
        from updater.post_processing.post_process_inventor import inventor_clean, get_inventor_stopwords
        stopwords = get_inventor_stopwords(config)
        cleaned_inventor_group = inventor_clean(inventor_group, stopwords)
        assert (inventor_group.shape == cleaned_inventor_group.shape)

    def test_inventor_reducer(self, config, inventor_group, name_first, name_last):
        from updater.post_processing.post_process_inventor import inventor_reduce, inventor_clean, \
            get_inventor_stopwords
        stopwords = get_inventor_stopwords(config)
        cleaned_inventor_group = inventor_clean(inventor_group, stopwords)
        canonical_assignment = inventor_reduce(cleaned_inventor_group)
        assert (canonical_assignment.shape[0] == 1)
        comparison = cleaned_inventor_group.merge(canonical_assignment, on=['inventor_id', 'name_first', 'name_last'])
        assert (comparison.shape[0] >= 1)
        assert (name_first == canonical_assignment.name_first.tolist()[0])
        assert (name_last == canonical_assignment.name_last.tolist()[0])

    def test_inventor_precache(self, config):
        from updater.post_processing.post_process_inventor import precache_inventors
        from sqlalchemy import create_engine
        import pandas as pd

        count_query = """
        SELECT count(1) as `rows` from disambiguated_inventor_ids;
        """
        distinct_count_query = """
        SELECT count(distinct inventor_id) distinct_inventor_ids from rawinventor;
        """
        duplicate_query = """
        SELECT count(1) as duplicates
            from (SELECT inventor_id from disambiguated_inventor_ids group by inventor_id having count(1) > 1) x
        """

        engine = create_engine(get_connection_string(config, "NEW_DB"))
        pre_count = pd.read_sql_query(count_query, engine)
        assert (pre_count.rows.tolist()[0] == 0)
        precache_inventors(config)
        post_count = pd.read_sql_query(count_query, engine)
        distinct_count = pd.read_sql_query(distinct_count_query, engine)
        assert (post_count.rows.tolist()[0] == distinct_count.distinct_inventor_ids.tolist()[0])
        duplicate_count = pd.read_sql_query(duplicate_query, engine)
        assert (duplicate_count.duplicates.tolist()[0] == 0)

    @pytest.mark.dependency(depends=['test_inventor_precache'])
    def test_inventor_generator(self, config, limit, offset, expected):
        from updater.post_processing.post_process_inventor import generate_disambiguated_inventors
        from sqlalchemy import create_engine
        engine = create_engine(get_connection_string(config, "NEW_DB"))
        inventors = generate_disambiguated_inventors(engine, limit, offset)
        assert (len(inventors.inventor_id.unique()) == expected)
        # disambiguated id, name first, name last, patent date
        assert (inventors.shape[1] == 4)

    def test_inventor_table_generator(self, config):
        from updater.post_processing.post_process_inventor import create_inventor
        create_inventor(config)
        from sqlalchemy import create_engine
        import pandas as pd
        engine = create_engine(get_connection_string(config, "NEW_DB"))
        count_query = "SELECT count(1)  as `rows` from inventor"
        distinct_id_query = "SELECT count(distinct inventor_id) inventors from rawinventor"
        column_query = """
                SELECT COLUMN_NAME
                from information_schema.COLUMNS
                where TABLE_SCHEMA='{schema}'
                        and TABLE_NAME='inventor'""".format(schema=config["DATABASE"]["NEW_DB"])

        column_data = pd.read_sql_query(column_query, con=engine)
        expected_columns = ['id', 'name_first', 'name_last']
        actual_column_names = column_data.COLUMN_NAME.tolist()
        assert len(actual_column_names) == len(expected_columns)
        assert set(actual_column_names) == set(expected_columns)

        inv_count_data = pd.read_sql_query(count_query, con=engine)
        rinv_distinct_count_data = pd.read_sql_query(distinct_id_query, con=engine)
        inv_count = inv_count_data.rows.tolist()[0]
        rinv_distinct_counts = rinv_distinct_count_data.inventors.tolist()[0]
        assert inv_count == rinv_distinct_counts

        duplicate_query = "SELECT count(1) dups from (SELECT count(1)   from inventor group by id having count(1)>1)x"
        dup_count = pd.read_sql_query(duplicate_query, con=engine)
        assert (dup_count.dups.tolist()[0] == 0)
        engine.execute("TRUNCATE TABLE inventor")
