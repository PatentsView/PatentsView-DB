import datetime
from lib.configuration import get_current_config
from QA.post_processing.DisambiguationTester import DisambiguationTester
import logging
from lib.configuration import get_connection_string, get_current_config, get_unique_connection_string
from sqlalchemy import create_engine
from time import time

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class InventorGenderPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, "gender_attribution", datetime.date(year=1976, month=1, day=1), end_date)

    def summarize_rawinventor_gender(self, config):
        cstr = get_unique_connection_string(config, connection='DATABASE_SETUP', database="gender_attribution")
        engine = create_engine(cstr)
        e_date = config["DATES"]["END_DATE"]
        # Takes ~10 minutes
        query1= f"""
create table gender_attribution.rawinventor_gender_{e_date}
select p.id as patent_id, r.inventor_id, p.date, g.gender_flag
from patent.rawinventor r 
    inner join gender_attribution.inventor_gender_{e_date} g on r.inventor_id = g.inventor_id
    inner join patent.patent p on r.patent_id=p.id;
"""
        query2= f"""
create table gender_attribution.rawinventor_gender_agg_{e_date}
select date
	, count(distinct inventor_id)  as unique_inventors
	, count(distinct patent_id) as unique_patents
	, sum(case when gender_flag = "M" then 1 else 0 end) as num_male_patent_inventors
	 , sum(case when gender_flag = "F" then 1 else 0 end) as num_female_patent_inventors
	 , sum(case when gender_flag = "U" then 1 else 0 end) as num_unatt_patent_inventors
from gender_attribution.rawinventor_gender_{e_date}
group by 1;
        """
        query3 = f"alter table gender_attribution.rawinventor_gender_agg_{e_date} add index `date` (`date`)"
        q_list = [query1,query2,query3]
        for query in q_list:
            logger.info(query)
            query_start_time = time()
            engine.execute(query)
            query_end_time = time()
            t = query_end_time - query_start_time
            m, s = divmod(t, 60)
            logger.info(f"This query took {m:02.0f}:{s:04.1f} (m:s)")

    def runInventorGenderTests(self):
        # self.summarize_rawinventor_gender(self.config)
        super(DisambiguationTester, self).runDisambiguationTests()

if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule="quarterly", **{
                    "execution_date": datetime.date(2023, 10, 1)
                                })
    qc = InventorGenderPostProcessingQC(config)
    qc.runInventorGenderTests()
