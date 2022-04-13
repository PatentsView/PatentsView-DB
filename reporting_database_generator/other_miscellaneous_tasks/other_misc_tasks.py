from lib.configuration import get_connection_string, get_current_config, get_version_indicator
import datetime

import pymysql
from sqlalchemy import create_engine
from time import time


def create_granted_patent_crosswalk(**kwargs):
    config = get_current_config('pgpubs', schedule="quarterly", **kwargs)
    sd = config['DATES']["start_date"]
    ed = config['DATES']["end_date"]
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    print("Creating Granted Patent Crosswalk Table")
    patent_cq_query = f"""
insert into granted_patent_crosswalk
	(id, document_number, patent_number, application_number, version_indicator)
select UUID(), b.document_number, d.id as patent_number, a.application_number, d.version_indicator
from pregrant_publications.application a
	inner join pregrant_publications.publication b on a.document_number=b.document_number
    inner join patent.application c on a.application_number=c.number_transformed
    inner join patent.patent d on c.patent_id = d.id
where d.version_indicator >= {sd} and d.version_indicator<= {ed}
        """
    print(patent_cq_query)
    query_start_time = time()
    engine.execute(patent_cq_query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")


def qa_granted_patent_crosswalk(**kwargs):
    config = get_current_config('pgpubs', schedule="quarterly", **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    sd = config['DATES']["start_date"]
    ed = config['DATES']["end_date"]
    engine = create_engine(cstr)
    patent_cq_query = f"""
    select count(*)
    from granted_patent_crosswalk
    where version_indicator >= {sd} and version_indicator<= {ed}
            """
    print(patent_cq_query)
    engine.execute(patent_cq_query)
    rows = engine.execute(patent_cq_query).fetchone()[0]
    if rows == 0:
        raise Exception("GRANTED PATENT CROSSWALK WAS NOT UPDATED FOR THE CURRENT TIMEFRAME")



if __name__ == "__main__":
    qa_granted_patent_crosswalk(**{
            "execution_date": datetime.date(2021, 10, 1)
            })