import datetime
import time

from QA.post_processing.DisambiguationTester import DisambiguationTester
from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_current_config


class LocationPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
                'rawlocation':       {
                        'fields': {
                                'id':                      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country_transformed':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'location_id':             {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'location_id_transformed': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'city':                    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'state':                   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'country':                 {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'location':          {
                        "fields":           {
                                "id":          {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "city":        {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "state":       {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "country":     {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     True
                                        },
                                "latitude":    {
                                        "data_type":    "float",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "longitude":   {
                                        "data_type":    "float",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "county":      {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "state_fips":  {
                                        "data_type":    "int",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "county_fips": {
                                        "data_type":    "int",
                                        "null_allowed": True,
                                        "category":     False
                                        }
                                },
                        'related_entities': [
                                {
                                        'table':          'location_assignee',
                                        'source_id':      'id',
                                        'destination_id': 'location_id'
                                        },
                                {
                                        'table':          'location_inventor',
                                        'source_id':      'id',
                                        'destination_id': 'location_id'
                                        }, {
                                        'table':          'rawlocation',
                                        'source_id':      'id',
                                        'destination_id': 'location_id'
                                        }]
                        },
                'location_assignee': {
                        "fields":           {
                                "location_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "assignee_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                },
                        'related_entities': [
                                {
                                        'table':          'location',
                                        'source_id':      'location_id',
                                        'destination_id': 'id'
                                        },
                                {
                                        'table':          'assignee',
                                        'source_id':      'assignee_id',
                                        'destination_id': 'id'
                                        }]
                        },
                'location_inventor': {
                        "fields":           {
                                "location_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "inventor_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        }
                                },
                        'related_entities': [
                                {
                                        'table':          'location',
                                        'source_id':      'location_id',
                                        'destination_id': 'id'
                                        },
                                {
                                        'table':          'inventor',
                                        'source_id':      'inventor_id',
                                        'destination_id': 'id'
                                        }]
                        },
                }

        self.disambiguated_data_fields = ['city', 'state', 'country']
        self.entity_table = 'rawlocation'
        self.entity_id = 'id'
        self.disambiguated_id = 'location_id'
        self.disambiguated_table = 'location'
        self.patent_exclusion_list.extend(['location', 'rawlocation','location_assignee','location_inventor'])

    # def test_floating_entities(self, table=None, table_config=None):
    #     pass

    def load_top_n_patents(self):
        chunk_size = 1000
        location_template = """
        SELEcT {select} from location order by id limit {limit} offset {offset}
        """
        offset = 0
        location_n_template = """
SELECT l.city,
       l.state,
       l.country,
       count(patent_id)
from ({data_query}) l
         join
     (SELECT *
      from (SELECT patent_id, location_id
            from patent_assignee
                     join ({core_query}) l on l.id = patent_assignee.location_id) pa
      union
      SELECT *
      from (SELECT patent_id, location_id
            from patent_inventor
                     join ({core_query}) l on l.id = patent_inventor.location_id) pi) ai
     on ai.location_id = l.id
group by l.id, l.city, l.state, l.country;
        """
        location_counts = {}
        while True:
            query_start = time.time()
            location_core_query = location_template.format(limit=chunk_size, offset=offset, select="id")
            location_data_query = location_template.format(limit=chunk_size, offset=offset,
                                                           select=", ".join(["id", "city", "state", "country"]))
            location_n_query = location_n_template.format(core_query=location_core_query,
                                                          data_query=location_data_query)

            if not self.connection.open:
                self.connection.connect()
            cursor_size = 0
            with self.connection.cursor() as top_cursor:
                top_cursor.execute(location_n_query)
                for top_n_data_row in top_cursor:
                    cursor_size += 1
                    location_counts[(top_n_data_row[0], top_n_data_row[1], top_n_data_row[2])] = top_n_data_row[-1]
            if cursor_size == 0:
                break
            offset += chunk_size
            print("Completed {core_query} in {duration} seconds".format(core_query=location_core_query,
                                                                        duration=time.time() - query_start))
        top_n_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:100]
        rank = 1
        for location in top_n_locations:
            data_value = ", ".join([x if x is not None else 'N/A' for x in location[0]])
            self.qa_data['DataMonitor_topnentities'].append(
                    {
                            "database_type":        self.database_type,
                            'entity_name':          'location',
                            "related_entity":       'patent',
                            'update_version':       self.version,
                            'entity_value':         data_value,
                            'related_entity_count': location[1],
                            'entity_rank':          rank
                            })
            rank += 1

    # def runTests(self):
    #     self.load_top_n_patents()
    #     super(LocationPostProcessingQC, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })
    config['DATES'] = {
            "START_DATE": '20201006',
            "END_DATE":   '20201229'
            }
    lc = LocationPostProcessingQC(config)
    lc.runTests()