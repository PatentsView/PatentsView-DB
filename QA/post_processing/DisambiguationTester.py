from abc import ABC
from time import time

from QA.DatabaseTester import DatabaseTester


class DisambiguationTester(DatabaseTester):
    def __init__(self, config, database_section, start_date, end_date):
        super().__init__(config, database_section, start_date, end_date)

    def init_qa_dict_disambig(self):
        self.qa_data = {
            "DataMonitor_distinctidcount": [],
            'DataMonitor_topnentities': []
        }

    def save_qa_data(self):
        super(DisambiguationTester, self).save_qa_data()

    def test_floating_entities(self):
        ratio_to_patent = None
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as count_cursor:
            entity_count_query = "SELECT count(distinct {disambiguated_id}) from {entity_table} et".format(
                disambiguated_id=self.disambiguated_id, entity_table=self.entity_table)
            entity_row_query = "SELECT count(*) from {entity_table} et".format(
                entity_id=self.disambiguated_id, entity_table=self.entity_table)
            print(entity_count_query)
            count_cursor.execute(entity_count_query)
            entity_count_value = count_cursor.fetchall()[0][0]
            print(entity_row_query)
            count_cursor.execute(entity_row_query)
            entity_rows_value = count_cursor.fetchall()[0][0]
            ratio_to_self = round((entity_rows_value * 1.0) / entity_count_value, 3)
            if ratio_to_self < 3:
                raise Exception(f"DISAMBIGUATION MAY HAVE FAILED - Clustered id to raw id ratio is low")

            if "patent_id" in self.table_config[self.entity_table]["fields"]:
                patent_count_query = "SELECT count(id) from patent"
                count_cursor.execute(patent_count_query)
                patent_count_value = count_cursor.fetchall()[0][0]
                ratio_to_patent = round((patent_count_value * 1.0) / entity_count_value, 3)

            self.qa_data['DataMonitor_distinctidcount'].append(
                {
                    "database_type": self.database_type,
                    'table_name': self.entity_table,
                    "column_name": self.disambiguated_id,
                    'update_version': self.version,
                    'distinct_id_count': entity_count_value,
                    'ratio_to_patent_id': ratio_to_patent,
                    'ratio_to_self': ratio_to_self
                })

    def test_entity_id_updated(self):
        if {self.entity_table} in ['assignee', 'inventor']:
            for db, id in [['pregrant_publications', 'id'], ['patent', 'uuid']]:
                test_entity_id_updated_query = f"""
select count(*)
from {db}.{self.entity_table} a
    inner join {db}.{self.disambiguated_table}_disambiguation_mapping_{self.end_date} b on a.{id}=b.uuid 
where a.{self.disambiguated_id} != b.{self.disambiguated_id};"""
                print(test_entity_id_updated_query)
                if not self.connection.open:
                    self.connection.connect()
                with self.connection.cursor() as g_cursor:
                    g_cursor.execute(test_entity_id_updated_query)
                    count_not_updated = g_cursor.fetchall()[0][0]
                    if count_not_updated > 0:
                        raise Exception(f"ENTITY NOT UPDATED IN THE RAW TABLE")

    def top_n_generator(self, table):
        if 'related_entities' in self.table_config[table]:
            related_table_configs = self.table_config[table]["related_entities"]
            for related_table_config in related_table_configs:
                print(f"\t\t\tLoading Top N Entities for {self.database_section}.{self.disambiguated_table} from {related_table_config['related_table']}")
                self.load_top_entities(table, related_table_config)

    def load_top_entities(self, table_name, related_table_config):
        if 'patent' not in table_name:
            if 'location' in table_name:
                top_n_data_query = f"""
                SELECT concat(main.location_name, ', ', main.state, ', ', main.country) as location_name
                        , count(*)
                FROM  geo_data.curated_locations main
                    JOIN patent.{related_table_config["related_table"]} related 
                    ON main.uuid = related.{related_table_config['related_table_id']}
                GROUP  BY 1
                ORDER  BY 2 DESC
                LIMIT 100"""
            else:
                top_n_data_query = f"""
            SELECT {self.aggregator}
                    , count(*)
            FROM  {table_name} main
                JOIN {related_table_config["related_table"]} related ON main.{related_table_config["main_table_id"]} = related.{related_table_config['related_table_id']}
            GROUP  BY 1
            ORDER  BY 2 DESC
            LIMIT 100"""
            print(top_n_data_query)
            if not self.connection.open:
                self.connection.connect()
            with self.connection.cursor() as top_cursor:
                top_cursor.execute(top_n_data_query)
                rank = 1
                for top_n_data_row in top_cursor:
                    self.qa_data['DataMonitor_topnentities'].append(
                        {
                            "database_type": self.database_type,
                            'entity_name': table_name,
                            "related_entity": related_table_config["related_table"],
                            'entity_rank': rank,
                            'update_version': self.version,
                            'entity_value': top_n_data_row[0],
                            'related_entity_count': top_n_data_row[-1]
                        })
                    rank += 1


    def test_invalid_id(self):
        print(f"\tTesting Invalid Disambiguation IDs {self.disambiguated_table} in {self.database_section}")
        if self.entity_table == 'rawlawyer':
            invalid_query = f"""
            SELECT count(*)
            from {self.disambiguated_table} dt
                left join {self.entity_table} et on et.{self.disambiguated_id} = dt.id
            where et.{self.disambiguated_id} is null and et.version_indicator <= '{self.end_date}';
                    """
        else:
            invalid_query = f"""
            SELECT count(*)
            from {self.disambiguated_table} dt
                left join {self.entity_table} et on et.{self.disambiguated_id} = dt.id
                left join {self.config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE']}.{self.entity_table} et2 on et2.{self.disambiguated_id} = dt.id
            where et.{self.disambiguated_id} is null
                and et2.{self.disambiguated_id} is null
                and et.version_indicator <= '{self.end_date}'
                and et2.version_indicator <= '{self.end_date}';
            """
        print(invalid_query)
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(invalid_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value > 0:
                print(invalid_query)
                raise Exception(f"There are {self.disambiguated_id} in {self.disambiguated_table} table that are not in {self.entity_table}")

    def remove_blank_assignees(self, table):
        query = f"""
delete
from {table} 
where organization is null and name_first is null and name_last is null;
        """
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            num_affected_rows = cursor.rowcount
            print("Number of affected rows: ", num_affected_rows)

    def runTests(self):
        print("Beginning Disambiguation Specific Tests")
        self.test_entity_id_updated()
        self.init_qa_dict_disambig()
        self.test_invalid_id()
        self.test_floating_entities()
        for table in self.table_config:
            if "assignee_" in table and len(table) == 8:
                self.remove_blank_assignees(table)
            if "disambiguation" not in table and "patent_" not in table:
                print(f"\t\tBeginning Tests for {table}")
                self.top_n_generator(table)
                self.save_qa_data()
                self.init_qa_dict_disambig()


