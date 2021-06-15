from abc import ABC

from QA.PatentDatabaseTester import PatentDatabaseTester


class DisambiguationTester(PatentDatabaseTester, ABC):
    def __init__(self, config, database_section, start_date, end_date):
        super().__init__(config, database_section, start_date, end_date)

        self.entity_table = None
        self.entity_id = None

        self.disambiguated_id = None
        self.disambiguated_table = None
        self.disambiguated_data_fields = []

    def init_qa_dict(self):
        super(DisambiguationTester, self).init_qa_dict()
        self.extended_qa_data = {
                "DataMonitor_distinctidcount": [],
                'DataMonitor_topnentities':    []
                }
        self.qa_data.update(self.extended_qa_data)

    def test_floating_entities(self, table_name):
        if "patent_id" in self.table_config[table_name]["fields"]:
            super(DisambiguationTester, self).test_floating_entities(table_name)
        ratio_to_patent = None
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as count_cursor:
            entity_count_query = "SELECT count(distinct {disambiguated_id}) from {entity_table} et".format(
                    disambiguated_id=self.disambiguated_id, entity_table=self.entity_table)
            entity_row_query = "SELECT count(*) from {entity_table} et".format(
                    entity_id=self.disambiguated_id, entity_table=self.entity_table)
            count_cursor.execute(entity_count_query)
            entity_count_value = count_cursor.fetchall()[0][0]
            count_cursor.execute(entity_row_query)
            entity_rows_value = count_cursor.fetchall()[0][0]
            ratio_to_self = round((entity_rows_value * 1.0) / entity_count_value, 3)

            if "patent_id" in self.table_config[table_name]["fields"]:
                patent_count_query = "SELECT count(id) from patent"
                count_cursor.execute(patent_count_query)
                patent_count_value = count_cursor.fetchall()[0][0]
                ratio_to_patent = round((patent_count_value * 1.0) / entity_count_value, 3)

            self.qa_data['DataMonitor_distinctidcount'].append(
                    {
                            "database_type":      self.database_type,
                            'table_name':         self.entity_table,
                            "column_name":        self.disambiguated_id,
                            'update_version':     self.version,
                            'distinct_id_count':  entity_count_value,
                            'ratio_to_patent_id': ratio_to_patent,
                            'ratio_to_self':      ratio_to_self
                            })

    def top_n_generator(self, table_name=None):
        print("\tLoading Top N Entities for {table_name} in {db}".format(
                table_name=self.disambiguated_table,
                db=self.config["PATENTSVIEW_DATABASES"][self.database_section]))
        table_name = self.disambiguated_table
        if 'related_entities' in self.table_config[table_name]:
            related_table_configs = self.table_config[table_name]["related_entities"]
            for related_table_config in related_table_configs:
                print("\tLoading Top N Entities for {table_name} in {db} from {related_table}".format(
                        related_table=related_table_config['table'], table_name=self.disambiguated_table,
                        db=self.config["PATENTSVIEW_DATABASES"][
                            self.database_section]))
                self.load_top_entities(table_name, related_table_config)

    def load_top_entities(self, top_entities_for, top_entities_by):
        data_fields = ['id'] + self.disambiguated_data_fields
        prefixed_data_fields = ["dt.{df}".format(df=dfield) for dfield in data_fields]
        top_n_data_query = """
    SELECT {fields}, count(1)
    FROM   {top_entities_for} dt
    JOIN {top_entities_from} jt
    ON dt.{source_id} = jt.{destination_id}
    GROUP  BY {fields}
    ORDER  BY Count(1) DESC
    LIMIT  {n}
            """.format(n=100, fields=", ".join(prefixed_data_fields), top_entities_for=top_entities_for,
                       top_entities_from=top_entities_by["table"], source_id=top_entities_by["source_id"],
                       destination_id=top_entities_by['destination_id'])
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as top_cursor:
            top_cursor.execute(top_n_data_query)
            rank = 1
            for top_n_data_row in top_cursor:
                data_value = ", ".join([x if x is not None else '' for x in top_n_data_row[1:-1]])

                self.qa_data['DataMonitor_topnentities'].append(
                        {
                                "database_type":        self.database_type,
                                'entity_name':          top_entities_for,
                                "related_entity":       top_entities_by["table"],
                                'entity_rank':          rank,
                                'update_version':       self.version,
                                'entity_value':         data_value,
                                'related_entity_count': top_n_data_row[-1]
                                })
                rank += 1

    def test_invalid_id(self, table_name=None):
        print("\tTesting Invalid Disambiguation IDs {table_name} in {db}".format(
                table_name=self.disambiguated_table,
                db=self.config["PATENTSVIEW_DATABASES"][self.database_section]))
        invalid_query = """
SELECT count(1)
from {disambiguated_table} dt
         left join {entity_table} et
                   on et.{id_field} = dt.id
         left join {pregrant_db}.{entity_table} et2 on et2.{id_field} = dt.id
where et.{id_field} is null
  and et2.{id_field} is null;
        """.format(
                disambiguated_table=self.disambiguated_table, entity_table=self.entity_table,
                id_field=self.disambiguated_id, pregrant_db=self.config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'])
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(invalid_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value > 0:
                raise Exception(
                        "There are {id_field} in {disambiguated_table} table that are not in  {entity_table}".format(
                                disambiguated_table=self.disambiguated_table, entity_table=self.entity_table,
                                id_field=self.disambiguated_id))

    def test_related_floating_entities(self, table_name, table_config):
        pass

    def runTests(self):
        print("Beginning Disambiguation Specific Tests")
        self.test_invalid_id()
        self.top_n_generator(table_name=self.disambiguated_table)
        self.save_qa_data()
        self.init_qa_dict()
        super(DisambiguationTester, self).runTests()
