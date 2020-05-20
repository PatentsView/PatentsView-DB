import datetime
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
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
            'application': {'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'id_transformed': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'type': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                            'number_transformed': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'number': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'series_code_transformed_from_type': {'data_type': 'varchar', 'null_allowed': True,
                                                                  'category': False},
                            'country': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False}},
            'botanic': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                        'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                        'latin_name': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                        'variety': {'data_type': 'varchar', 'null_allowed': True, 'category': False}},
            'figures': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                        'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                        'num_figures': {'data_type': 'int', 'null_allowed': True, 'category': False},
                        'num_sheets': {'data_type': 'int', 'null_allowed': True, 'category': False}},
            'foreigncitation': {
                'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                'number': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                'category': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False}}, 'foreign_priority': {
                'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                'country_transformed': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                'kind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                'number': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False}}, 'government_interest': {
                'gi_statement': {'data_type': 'mediumtext', 'null_allowed': False, 'category': False},
                'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False}},
            'ipcr': {'ipc_version_indicator': {'data_type': 'date', 'null_allowed': True, 'category': False},
                     'ipc_class': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'classification_value': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                     'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                     'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                     'subclass': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'classification_status': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                     'main_group': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'classification_data_source': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'classification_level': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'subgroup': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                     'action_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                     'section': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                     'symbol_position': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'mainclass': {'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False}},
            'non_inventor_applicant': {'fname': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                       'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                       'organization': {'data_type': 'varchar', 'null_allowed': True,
                                                        'category': False},
                                       'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                       'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                                       'rawlocation_id': {'data_type': 'varchar', 'null_allowed': True,
                                                          'category': False},
                                       'designation': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                                       'lname': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                       'applicant_type': {'data_type': 'varchar', 'null_allowed': True,
                                                          'category': True}},
            'otherreference': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                               'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                               'text': {'data_type': 'text', 'null_allowed': False, 'category': False},
                               'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False}},
            'patent': {'type': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                       'title': {'data_type': 'mediumtext', 'null_allowed': False, 'category': False},
                       'number': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                       'kind': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                       'country': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                       'num_claims': {'data_type': 'int', 'null_allowed': False, 'category': False},
                       'date': {'data_type': 'date', 'null_allowed': False, 'date_field': True, 'category': False},
                       'filename': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                       'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                       'abstract': {'data_type': 'mediumtext', 'null_allowed': True, 'category': False},
                       'withdrawn': {'data_type': 'int', 'null_allowed': True, 'category': False}},
            'pct_data': {'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'kind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'rel_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'doc_type': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                         '102_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                         '371_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                         'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'rawassignee': {'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'name_last': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'assignee_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'organization': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'rawlocation_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                            'type': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                            'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'name_first': {'data_type': 'varchar', 'null_allowed': True, 'category': False}},
            'rawinventor': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'name_last': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                            'inventor_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'rule_47': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'rawlocation_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'deceased': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                            'name_first': {'data_type': 'varchar', 'null_allowed': True, 'category': False}},
            'rawlawyer': {'name_last': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                          'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                          'organization': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                          'lawyer_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                          'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                          'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                          'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                          'name_first': {'data_type': 'varchar', 'null_allowed': True, 'category': False}},
            'rawlocation': {'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'country_transformed': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                            'location_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'location_id_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                        'category': False},
                            'city': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'state': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'rel_app_text': {'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                             'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                             'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                             'text': {'data_type': 'mediumtext', 'null_allowed': False, 'category': False}},
            'subclass': {'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False}},
            'usapplicationcitation': {'name': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                      'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                                      'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                      'kind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                                      'application_id_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                                     'category': False},
                                      'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                      'number': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                      'number_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                             'category': False},
                                      'application_id': {'data_type': 'varchar', 'null_allowed': False,
                                                         'category': False},
                                      'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                                      'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True,
                                               'category': False},
                                      'category': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'uspatentcitation': {'citation_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                 'category': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                                 'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True,
                                          'category': False},
                                 'sequence': {'data_type': 'bigint', 'null_allowed': False, 'category': False},
                                 'name': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                 'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                 'kind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                                 'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                 'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'uspc': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                     'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                     'mainclass_id': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                     'subclass_id': {'data_type': 'varchar', 'null_allowed': False, 'category': True},
                     'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False}},
            'usreldoc': {'status': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'relkind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                         'reldocno': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'kind': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'country': {'data_type': 'varchar', 'null_allowed': True, 'category': True},
                         'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                         'date': {'data_type': 'date', 'null_allowed': True, 'date_field': True, 'category': False},
                         'doctype': {'data_type': 'varchar', 'null_allowed': True, 'category': True}},
            'us_term_of_grant': {'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                 'term_extension': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                 'lapse_of_patent': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                 'disclaimer_date': {'data_type': 'date', 'null_allowed': True, 'date_field': True,
                                                     'category': False},
                                 'term_disclaimer': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                 'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                 'term_grant': {'data_type': 'varchar', 'null_allowed': True, 'category': False}}}

        self.config = config
        self.project_home = os.environ['PACKAGE_HOME']

    def test_merge_status(self):
        status_folder = '{}/{}'.format(self.project_home, 'create_databases')
        status_file = '{}/{}'.format(status_folder, 'merge_status.json')
        current_status = json.load(open(status_file))
        if sum(current_status.values()) < len(self.table_config):
            raise Exception("Some tables were not loaded")

    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        count_query = "SELECT count(*) from {tbl} where abstract is null and type!='design' and type!='reissue'".format(
            tbl=table)
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                    "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. Count: {count}".format(
                        database=self.database_section, table=table,
                        count=count_value))


if __name__ == '__main__':
    config = get_config()
    mc = MergeTest(config)
