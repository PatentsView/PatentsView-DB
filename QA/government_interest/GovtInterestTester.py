import datetime
import itertools

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config
import logging

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class GovtInterestTester(DatabaseTester):
    def __init__(self, config, database = 'TEMP_UPLOAD_DB', id_type = 'patent_id'):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES'][database], datetime.date(year=1976, month=1, day=1), end_date)
        self.id_type = id_type

    def init_qa_dict(self):
        super(GovtInterestTester, self).init_qa_dict()
        self.extended_qa_data = {
                "DataMonitor_govtinterestsampler": []
                }
        self.qa_data.update(self.extended_qa_data)

    def generate_govt_int_samples(self):
        print("\tGenerating samples for Govt Interest")
        if not self.connection.open:
            self.connection.connect()
        start_dt = datetime.datetime.strptime(self.config['DATES']['START_DATE'], '%Y%m%d')
        end_dt = datetime.datetime.strptime(self.config['DATES']['END_DATE'], '%Y%m%d')
        table_prefix = 'patent' if self.id_type == 'patent_id' else 'publication'
        base_id = 'id' if self.id_type == 'patent_id' else 'document_number'
        date_where = ["WHERE  p.date BETWEEN '{start_dt}' AND '{end_dt}'".format(start_dt=start_dt, end_dt=end_dt)]
        organization_wheres = {
                "All Patents":     "go.`name` NOT LIKE '%United States Government%' and go.`name` is not null",
                "US Govt Patents": "go.`name` LIKE '%United States Government%'",
                "No Organization": f"pg.`{self.id_type}` is null"
                }

        where_combinations = itertools.product(date_where, organization_wheres.keys())
        sampler_template = """
SELECT gi.{id_type},
       gi.gi_statement,
       go.organization_id, 
       go.name,
       pca.contract_award_number
FROM   `government_interest` gi
       JOIN {table_prefix} p
         ON p.{base_id} = gi.{id_type}
       LEFT JOIN `{table_prefix}_contractawardnumber` pca
         ON pca.{id_type} = gi.{id_type}
       LEFT JOIN `{table_prefix}_govintorg` pg
         ON pg.{id_type} = gi.{id_type}
       LEFT JOIN {raw_db}.government_organization go
         ON go.`organization_id` = pg.organization_id
           inner join 
           		(select p.{base_id} 
           		from {table_prefix} p
           		       Join `government_interest` gi on p.{base_id}=gi.{id_type}
           			   LEFT JOIN `{table_prefix}_govintorg` pg ON pg.{id_type} = p.{base_id}
           		       LEFT JOIN {raw_db}.`government_organization` go ON go.`organization_id` = pg.organization_id
           		{where_clause}
           		order by rand() 
           		limit 5) as rand_5_samples ON p.{base_id}=rand_5_samples.{base_id};       
        """

        for date_clause, where_combination_type in where_combinations:

            where_clause = "AND ".join([date_clause, organization_wheres[where_combination_type]])
            sampler_query = sampler_template.format(
                  where_clause=where_clause, 
                  raw_db=self.config['PATENTSVIEW_DATABASES']['RAW_DB'],
                  table_prefix = table_prefix,
                  id_type = self.id_type,
                  base_id = base_id)
            with self.connection.cursor() as gov_int_cursor:
                print(sampler_query)
                gov_int_cursor.execute(sampler_query)
                for gov_int_row in gov_int_cursor:
                    self.qa_data['DataMonitor_govtinterestsampler'].append({
                            'sample_type'           : where_combination_type,
                            "database_type"         : self.database_type,
                            'update_version'        : self.version,
                            self.id_type            : gov_int_row[0],
                            'gov_int_stmt'          : gov_int_row[1],
                            'contract_award_number' : gov_int_row[4],
                            'organization_id'       : gov_int_row[2],
                            'organization'          : gov_int_row[3]
                            })

    def run_GI_tests(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        self.generate_govt_int_samples()
        for table in self.table_config:
            self.load_table_row_count(table, where_vi=False)
            self.load_nulls(table, self.table_config[table], where_vi=False)
            self.test_blank_count(table, self.table_config[table], where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter / total_tables} %")
            for field in self.table_config[table]["fields"]:
                logger.info(f"==============================================================================")
                logger.info(f"\tBEGINNING TESTS FOR COLUMN: {table}.{field}")
                logger.info(f"==============================================================================")
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text', 'varchar']:
                    self.test_newlines(table, field, where_vi=False)
                self.test_null_byte(table, field, where_vi=False)
                self.save_qa_data()
                self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"==============================================================================")
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter / total_tables} %")
            logger.info(f"==============================================================================")


def begin_gi_test(config, database = 'TEMP_UPLOAD_DB', id_type = 'patent_id'):
    qc = GovtInterestTester(config, database = database, id_type = id_type)
    qc.runTests()


if __name__ == '__main__':
    dbtype = 'granted_patent'
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2023, 7, 25)
            })
    qc = GovtInterestTester(config, database="PROD_DB", id_type=('patent_id' if dbtype =='granted_patent' else 'document_number'))
    qc.run_GI_tests()

