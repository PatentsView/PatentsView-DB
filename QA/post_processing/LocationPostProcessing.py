import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester


class LocationPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
            'rawlocation': {'fields': {'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                       'country_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                               'category': False},
                                       'location_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                       'location_id_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                                   'category': False},
                                       'city': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                       'state': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                                       'country': {'data_type': 'varchar', 'null_allowed': True, 'category': False}}},
            'location': {"fields": {"id": {"data_type": "varchar", "null_allowed": False, "category": False},
                                    "city": {"data_type": "varchar", "null_allowed": True, "category": False},
                                    "state": {"data_type": "varchar", "null_allowed": True, "category": False},
                                    "country": {"data_type": "varchar", "null_allowed": True, "category": True},
                                    "latitude": {"data_type": "float", "null_allowed": True, "category": False},
                                    "longitude": {"data_type": "float", "null_allowed": True, "category": False},
                                    "county": {"data_type": "varchar", "null_allowed": True, "category": False},
                                    "state_fips": {"data_type": "int", "null_allowed": True, "category": False},
                                    "county_fips": {"data_type": "int", "null_allowed": True, "category": False}},
                         'related_entities': [
                             {'table': 'location_assignee', 'source_id': 'id', 'destination_id': 'location_id'},
                             {'table': 'location_inventor', 'source_id': 'id', 'destination_id': 'location_id'}]}}

        self.disambiguated_data_fields = ['city', 'state', 'country']
        self.entity_table = 'rawlocation'
        self.entity_id = 'id'
        self.disambiguated_id = 'location_id'
        self.disambiguated_table = 'location'
        self.patent_exclusion_list.extend(['location', 'rawlocation'])

    def test_floating_entities(self, table=None, table_config=None):
        pass

    def load_top_n_patents(self):
        top_n_data = """
SELECT l.`id`,
       l.city,
       l.state,
       l.country,
       Count(pa.patent_id) + Count(pi.patent_id)
FROM   location l
       LEFT JOIN location_assignee la
              ON la.location_id = l.id
       LEFT JOIN `location_inventor` li
              ON li.location_id = l.id
       LEFT JOIN patent_assignee pa
              ON pa.assignee_id = la.assignee_id
       LEFT JOIN patent_inventor pi
              ON pi.patent_id = li.inventor_id
GROUP  BY l.`id`,
          l.city,
          l.state,
          l.country
ORDER  BY Count(pa.patent_id) + Count(pi.patent_id) DESC
LIMIT  100;
"""
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as top_cursor:
            top_cursor.execute(top_n_data)
            database_type, version = self.config["DATABASE"][self.database_section].split("_")
            for top_n_data_row in top_cursor:
                data_value = ", ".join([x if x is not None else '' for x in top_n_data_row[1:-1]])

                self.qa_data['DataMonitor_topnentities'].append(
                    {"database_type": database_type, 'entity_name': 'location',
                     "related_entity": 'patent',
                     'update_version': version, 'entity_value': data_value,
                     'related_entity_count': top_n_data_row[-1]})

    def runTests(self):
        self.load_top_n_patents()
        super(LocationPostProcessingQC, self).runTests()
