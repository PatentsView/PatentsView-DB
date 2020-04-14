import json
import os

from sqlalchemy import create_engine

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_config


class MergeTest(PatentDatabaseTester):
    def runTests(self):
        self.test_merge_status()
        super(MergeTest, self).runTests()

    def __init__(self, config):
        super().__init__(config, 'NEW_DB')
        self.table_config = {'application': {'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'id_transformed': {'data_type': 'varchar', 'null_allowed': True},
                                             'type': {'data_type': 'varchar', 'null_allowed': True},
                                             'number_transformed': {'data_type': 'varchar', 'null_allowed': True},
                                             'number': {'data_type': 'varchar', 'null_allowed': False},
                                             'series_code_transformed_from_type': {'data_type': 'varchar',
                                                                                   'null_allowed': True},
                                             'country': {'data_type': 'varchar', 'null_allowed': True},
                                             'id': {'data_type': 'varchar', 'null_allowed': False},
                                             'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True}},
                             'botanic': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                         'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                         'latin_name': {'data_type': 'varchar', 'null_allowed': True},
                                         'variety': {'data_type': 'varchar', 'null_allowed': True}},
                             'figures': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                         'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                         'num_figures': {'data_type': 'int', 'null_allowed': True},
                                         'num_sheets': {'data_type': 'int', 'null_allowed': True}}, 'foreigncitation': {
                'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                'number': {'data_type': 'varchar', 'null_allowed': False},
                'country': {'data_type': 'varchar', 'null_allowed': True},
                'uuid': {'data_type': 'varchar', 'null_allowed': False},
                'category': {'data_type': 'varchar', 'null_allowed': True},
                'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                'sequence': {'data_type': 'int', 'null_allowed': False}}, 'foreign_priority': {
                'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                'country': {'data_type': 'varchar', 'null_allowed': True},
                'sequence': {'data_type': 'int', 'null_allowed': False},
                'country_transformed': {'data_type': 'varchar', 'null_allowed': True},
                'kind': {'data_type': 'varchar', 'null_allowed': True},
                'number': {'data_type': 'varchar', 'null_allowed': False},
                'uuid': {'data_type': 'varchar', 'null_allowed': False}},
                             'government_interest': {'gi_statement': {'data_type': 'mediumtext', 'null_allowed': False},
                                                     'patent_id': {'data_type': 'varchar', 'null_allowed': False}},
                             'ipcr': {'ipc_version_indicator': {'data_type': 'date', 'null_allowed': True},
                                      'ipc_class': {'data_type': 'varchar', 'null_allowed': True},
                                      'classification_value': {'data_type': 'varchar', 'null_allowed': True},
                                      'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                      'sequence': {'data_type': 'int', 'null_allowed': False},
                                      'subclass': {'data_type': 'varchar', 'null_allowed': True},
                                      'classification_status': {'data_type': 'varchar', 'null_allowed': True},
                                      'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                      'main_group': {'data_type': 'varchar', 'null_allowed': True},
                                      'classification_data_source': {'data_type': 'varchar', 'null_allowed': True},
                                      'classification_level': {'data_type': 'varchar', 'null_allowed': True},
                                      'subgroup': {'data_type': 'varchar', 'null_allowed': True},
                                      'action_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                                      'section': {'data_type': 'varchar', 'null_allowed': False},
                                      'symbol_position': {'data_type': 'varchar', 'null_allowed': True}},
                             'mainclass': {'id': {'data_type': 'varchar', 'null_allowed': False}},
                             'non_inventor_applicant': {'fname': {'data_type': 'varchar', 'null_allowed': True},
                                                        'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                                        'organization': {'data_type': 'varchar', 'null_allowed': True},
                                                        'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                                        'sequence': {'data_type': 'int', 'null_allowed': False},
                                                        'rawlocation_id': {'data_type': 'varchar',
                                                                           'null_allowed': False},
                                                        'designation': {'data_type': 'varchar', 'null_allowed': True},
                                                        'lname': {'data_type': 'varchar', 'null_allowed': True},
                                                        'applicant_type': {'data_type': 'varchar',
                                                                           'null_allowed': True}},
                             'otherreference': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                                'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                                'text': {'data_type': 'text', 'null_allowed': False},
                                                'sequence': {'data_type': 'int', 'null_allowed': False}},
                             'patent': {'type': {'data_type': 'varchar', 'null_allowed': False},
                                        'title': {'data_type': 'mediumtext', 'null_allowed': False},
                                        'number': {'data_type': 'varchar', 'null_allowed': False},
                                        'kind': {'data_type': 'varchar', 'null_allowed': False},
                                        'country': {'data_type': 'varchar', 'null_allowed': False},
                                        'num_claims': {'data_type': 'int', 'null_allowed': False},
                                        'date': {'data_type': 'date', 'null_allowed': False, 'date_field': True},
                                        'filename': {'data_type': 'varchar', 'null_allowed': False},
                                        'id': {'data_type': 'varchar', 'null_allowed': False},
                                        'abstract': {'data_type': 'mediumtext', 'null_allowed': True},
                                        'withdrawn': {'data_type': 'int', 'null_allowed': True}},
                             'pct_data': {'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                          'kind': {'data_type': 'varchar', 'null_allowed': True},
                                          'rel_id': {'data_type': 'varchar', 'null_allowed': False},
                                          'doc_type': {'data_type': 'varchar', 'null_allowed': True},
                                          'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                                          '102_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                                          '371_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                                          'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                          'country': {'data_type': 'varchar', 'null_allowed': True}},
                             'rawassignee': {'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'name_last': {'data_type': 'varchar', 'null_allowed': True},
                                             'assignee_id': {'data_type': 'varchar', 'null_allowed': True},
                                             'organization': {'data_type': 'varchar', 'null_allowed': True},
                                             'rawlocation_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'sequence': {'data_type': 'int', 'null_allowed': False},
                                             'type': {'data_type': 'varchar', 'null_allowed': True},
                                             'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                             'name_first': {'data_type': 'varchar', 'null_allowed': True}},
                             'rawinventor': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                             'name_last': {'data_type': 'varchar', 'null_allowed': True},
                                             'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'sequence': {'data_type': 'int', 'null_allowed': False},
                                             'inventor_id': {'data_type': 'varchar', 'null_allowed': True},
                                             'rule_47': {'data_type': 'varchar', 'null_allowed': True},
                                             'rawlocation_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'deceased': {'data_type': 'varchar', 'null_allowed': True},
                                             'name_first': {'data_type': 'varchar', 'null_allowed': True}},
                             'rawlawyer': {'name_last': {'data_type': 'varchar', 'null_allowed': True},
                                           'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                           'organization': {'data_type': 'varchar', 'null_allowed': True},
                                           'lawyer_id': {'data_type': 'varchar', 'null_allowed': True},
                                           'country': {'data_type': 'varchar', 'null_allowed': True},
                                           'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                           'sequence': {'data_type': 'int', 'null_allowed': False},
                                           'name_first': {'data_type': 'varchar', 'null_allowed': True}},
                             'rawlocation': {'id': {'data_type': 'varchar', 'null_allowed': False},
                                             'country_transformed': {'data_type': 'varchar', 'null_allowed': True},
                                             'location_id': {'data_type': 'varchar', 'null_allowed': True},
                                             'location_id_transformed': {'data_type': 'varchar', 'null_allowed': True},
                                             'city': {'data_type': 'varchar', 'null_allowed': True},
                                             'state': {'data_type': 'varchar', 'null_allowed': True},
                                             'country': {'data_type': 'varchar', 'null_allowed': True}},
                             'rel_app_text': {'sequence': {'data_type': 'int', 'null_allowed': False},
                                              'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                              'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                              'text': {'data_type': 'mediumtext', 'null_allowed': False}},
                             'subclass': {'id': {'data_type': 'varchar', 'null_allowed': False}},
                             'usapplicationcitation': {'name': {'data_type': 'varchar', 'null_allowed': True},
                                                       'sequence': {'data_type': 'int', 'null_allowed': False},
                                                       'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                                       'kind': {'data_type': 'varchar', 'null_allowed': True},
                                                       'application_id_transformed': {'data_type': 'varchar',
                                                                                      'null_allowed': True},
                                                       'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                                       'number': {'data_type': 'varchar', 'null_allowed': True},
                                                       'number_transformed': {'data_type': 'varchar',
                                                                              'null_allowed': True},
                                                       'application_id': {'data_type': 'varchar',
                                                                          'null_allowed': False},
                                                       'country': {'data_type': 'varchar', 'null_allowed': True},
                                                       'date': {'data_type': 'date', 'null_allowed': True,
                                                                'date_field': True},
                                                       'category': {'data_type': 'varchar', 'null_allowed': True}},
                             'uspatentcitation': {'citation_id': {'data_type': 'varchar', 'null_allowed': False},
                                                  'category': {'data_type': 'varchar', 'null_allowed': True},
                                                  'date': {'data_type': 'date', 'null_allowed': True,
                                                           'date_field': True},
                                                  'sequence': {'data_type': 'bigint', 'null_allowed': False},
                                                  'name': {'data_type': 'varchar', 'null_allowed': True},
                                                  'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                                  'kind': {'data_type': 'varchar', 'null_allowed': True},
                                                  'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                                  'country': {'data_type': 'varchar', 'null_allowed': True}},
                             'uspc': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                      'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                      'mainclass_id': {'data_type': 'varchar', 'null_allowed': False},
                                      'subclass_id': {'data_type': 'varchar', 'null_allowed': False},
                                      'sequence': {'data_type': 'int', 'null_allowed': False}},
                             'usreldoc': {'status': {'data_type': 'varchar', 'null_allowed': True},
                                          'relkind': {'data_type': 'varchar', 'null_allowed': True},
                                          'sequence': {'data_type': 'int', 'null_allowed': False},
                                          'reldocno': {'data_type': 'varchar', 'null_allowed': False},
                                          'kind': {'data_type': 'varchar', 'null_allowed': True},
                                          'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                          'country': {'data_type': 'varchar', 'null_allowed': True},
                                          'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                          'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True},
                                          'doctype': {'data_type': 'varchar', 'null_allowed': True}},
                             'us_term_of_grant': {'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                                  'term_extension': {'data_type': 'varchar', 'null_allowed': True},
                                                  'lapse_of_patent': {'data_type': 'varchar', 'null_allowed': True},
                                                  'disclaimer_date': {'data_type': 'date', 'null_allowed': True,
                                                                      'date_field': True},
                                                  'term_disclaimer': {'data_type': 'varchar', 'null_allowed': True},
                                                  'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                                  'term_grant': {'data_type': 'varchar', 'null_allowed': True}}}

        self.config = config
        self.project_home = os.environ['PACKAGE_HOME']

    def test_merge_status(self):
        status_folder = '{}/{}'.format(self.project_home, 'create_databases')
        status_file = '{}/{}'.format(status_folder, 'merge_status.json')
        current_status = json.load(open(status_file))
        if sum(current_status.values()) < len(self.table_config):
            raise Exception("Some tables were not loaded")

    def test_patent_abstract_null(self, table):
        engine = create_engine(self.database_connection_string)
        count_query = "SELECT count(*) from {tbl} where abstract is null and type!='design' and type!='reissue'".format(
            tbl=table)
        count_cursor = engine.execute(count_query)
        count_value = count_cursor.fetchall()[0][0]
        if count_value != 0:
            raise Exception(
                "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. Count: {count}".format(
                    database=self.database_section , table=table,
                    count=count_value))


if __name__ == '__main__':
    config = get_config()
    mc = MergeTest(config)
    for table in mc.table_config:
        for field in mc.table_config[table]:
            if "date_field" in mc.table_config[table][field] and mc.table_config[table][field]["date_field"]:
                mc.assert_zero_dates(table, field)
