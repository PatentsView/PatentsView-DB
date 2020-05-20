from abc import ABC

import pymysql

from QA.PatentDatabaseTester import PatentDatabaseTester


class DisambiguationTester(PatentDatabaseTester, ABC):
    def __init__(self, config, database_section, start_date, end_date):
        super().__init__(config, database_section, start_date, end_date)
        self.extended_qa_data = {"DataMonitor_distinctidcount": []}
        self.qa_data.update(self.extended_qa_data)
        self.entity_table = None
        self.entity_id = None

        self.disambiguated_id = None
        self.disambiguated_table = None

    def test_floating_entities(self):
        if not self.connection.open:
            self.connection.connect()
        float_query = "SELECT count(distinct {disambiguated_id}) from {entity_table} et LEFT JOIN patent p on p.id = et.patent_id where p.id is null".format(
            disambiguated_id=self.disambiguated_id, entity_table=self.entity_table)
        import pprint
        pprint.pprint(float_query)
        total_float_count = 0
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(float_query)
            total_float_count = count_cursor.fetchall()[0][0]
        if total_float_count > 0:
            raise Exception(
                "Entity Table : {table} has patent IDs that are not in patent table. Count: {count}".format(
                    table=self.entity_table, count=total_float_count))

        patent_count_query = "SELECT count(id) from patent"
        entity_count_query = "SELECT count(distinct {disambiguated_id}) from {entity_table} et".format(
            disambiguated_id=self.disambiguated_id, entity_table=self.entity_table)
        entity_row_query = "SELECT count(*) from {entity_table} et".format(
            entity_id=self.disambiguated_id, entity_table=self.entity_table)

        with self.connection.cursor() as count_cursor:
            count_cursor.execute(entity_count_query)
            entity_count_value = count_cursor.fetchall()[0][0]

            count_cursor.execute(entity_row_query)
            entity_rows_value = count_cursor.fetchall()[0][0]

            count_cursor.execute(patent_count_query)
            patent_count_value = count_cursor.fetchall()[0][0]
            ratio_to_patent = round((patent_count_value * 1.0) / entity_count_value, 3)
            ratio_to_self = round((entity_rows_value * 1.0) / entity_count_value, 3)
            database_type, version = self.config["DATABASE"][self.database_section].split("_")
            self.qa_data['DataMonitor_distinctidcount'].append(
                {"database_type": database_type, 'table_name': self.entity_table, "column_name": self.disambiguated_id,
                 'update_version': version, 'distinct_id_count': entity_count_value,
                 'ratio_to_patent_id': ratio_to_patent, 'ratio_to_self': ratio_to_self})

    def test_invalid_id(self):
        invalid_query = "SELECT count(1) from {disambiguated_table} dt left join {entity_table} et on et.{id_field}=dt.id where et.{id_field} is null".format(
            disambiguated_table=self.disambiguated_table, entity_table=self.entity_table,
            id_field=self.disambiguated_id)
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

    def runTests(self):
        #self.test_floating_entities()
        self.test_invalid_id()
        super(DisambiguationTester, self).runTests()
