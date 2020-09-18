import pandas as pd
from sqlalchemy import create_engine
import csv
import time

from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_connection_string, get_config


def update_rawinventor(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    update_statement = "UPDATE rawinventor ri left join inventor_disambiguation_mapping idm on idm.uuid = ri.uuid set ri.inventor_id=idm.inventor_id "
    engine.execute(update_statement)


def create_inventor(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    inventors_name_with_count = pd.read_sql_query(
        "SELECT inventor_id, name_first, name_last, count(1) name_count from rawinventor where inventor_id is not null group by inventor_id, name_first, name_last;",
        con=engine)
    inventors_data = inventors_name_with_count.sort_values("name_count", ascending=False).groupby("inventor_id").head(
        1).reset_index(drop=True)
    inventors_data.drop("name_count", axis=1).rename({"inventor_id": "id"}, axis=1).to_sql(name='inventor', con=engine,
                                                                                           if_exists='append',
                                                                                           index=False)


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
        wkfolder=update_config['FOLDERS']['WORKING_FOLDER'], disamb_file="inventor_disambiguation.tsv")
    disambig_output = pd.read_csv(disambig_output_file, sep="\t", chunksize=300000, header=None, quoting=csv.QUOTE_NONE,
                                  names=['unknown_1', 'uuid', 'inventor_id', 'name_first', 'name_middle', 'name_last',
                                         'name_suffix'])
    count = 0
    for disambig_chunk in disambig_output:
        engine.connect()
        start = time.time()
        count += disambig_chunk.shape[0]
        disambig_chunk[["uuid", "inventor_id"
                        ]].to_sql(name='inventor_disambiguation_mapping',
                                  con=engine,
                                  if_exists='append',
                                  index=False,
                                  method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
            end - start, 3),
            cnt=count))
        engine.dispose()


def post_process_inventor(config):
    upload_disambig_results(config)
    update_rawinventor(config)
    create_inventor(config)


def post_process_qc(config):
    qc = InventorPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    #post_process_inventor(config)
    post_process_qc(config)
