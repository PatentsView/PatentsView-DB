from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_current_config

def update_granted_patent_crosswalk(config):
    cstr = get_connection_string(config, 'PGPUBS_DATABASE')
    engine = create_engine(cstr)
    engine.execute(
        """
            INSERT IGNORE INTO pregrant_publications granted_patent_crosswalk (document_number, patent_number, application_number)
            SELECT a.document_number, p.patent_id AS patent_number, a.application_number
            FROM pregrant_publications.application a
            JOIN patent.application p
            WHERE a.application_number = p.number;
        """)


def begin_update_crosswalk(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    update_granted_patent_crosswalk(config)

def post_upload_database(**kwargs):
    config = get_current_config(**kwargs)
    qc = AppUploadTest(config)
    qc.runTests()


if __name__ == "__main__":
    begin_update_crosswalk(**{
        "execution_date": datetime.date(2020, 12, 17)
    })
