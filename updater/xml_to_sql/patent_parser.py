import datetime

from lib.configuration import get_current_config
from updater.xml_to_sql.parser import queue_parsers
from updater.xml_to_sql.post_processing import consolidate_granted_cpc, clean_rawlocation_plus_downstream
from lib.utilities import trim_whitespace
from lib.duckdb_sink import sink_enabled, export_parquet, parquet_dir, apply_uuid_triggers


def patent_sql_parser(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    queue_parsers(config, type='granted_patent')
    if sink_enabled(config):
        # DuckDB ports of the same three post-parse steps, then land parquet.
        from updater.xml_to_sql import post_processing_duckdb as ppd
        # stands in for the create_uuid_triggers task (MySQL-only)
        apply_uuid_triggers(config, ['main_cpc', 'further_cpc', 'rel_app_text'])
        ppd.consolidate_granted_cpc(config)
        ppd.trim_whitespace(config)
        ppd.clean_rawlocation_plus_downstream(config, applicant_table="non_inventor_applicant")
        written = export_parquet(config)
        print("wrote {} parquet files to {}".format(len(written), parquet_dir(config)))
        return
    consolidate_granted_cpc(config)
    trim_whitespace(config)
    clean_rawlocation_plus_downstream(config, applicant_table="non_inventor_applicant")


if __name__ == '__main__':
    # config = get_current_config('granted_patent', **{
    #         "execution_date": datetime.date(2020, 12, 29)
    #         })
    #
    # config['DATES'] = {
    #         "START_DATE": '20201006',
    #         "END_DATE":   '20201229'
    #         }
    # queue_parsers(config, type='granted_patent')
    patent_sql_parser(**{
            "execution_date": datetime.date(2023, 6, 13)
            })
