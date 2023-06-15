from lib.configuration import get_connection_string, get_current_config
import datetime

import pymysql
from sqlalchemy import create_engine
from time import time


def create_granted_patent_crosswalk(**kwargs):
    ## archival version used prior to 2022 decision to include records without both publication and patent
    config = get_current_config('pgpubs', schedule="weekly", **kwargs)
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


if __name__ == "__main__":
    create_granted_patent_crosswalk(**{
            "execution_date": datetime.date(2021, 10, 1)
            })