import datetime
import os
import zipfile

import pandas as pd
from sqlalchemy import create_engine

from QA.collect_supplemental_data.WithdrawnTest import WithdrawnTest
from lib import xml_helpers
from lib.configuration import get_connection_string, get_current_config
from lib.utilities import download


def download_withdrawn_patent_numbers(destination_folder):
    """ Download and extract the list of withdrawn patents """

    # Download
    url = 'https://www.uspto.gov/sites/default/files/documents/withdrawn.zip'
    filepath = os.path.join(destination_folder, 'withdrawn.zip')
    print("Destination: {}".format(filepath))
    if not os.path.exists(destination_folder):
        os.mkdir(destination_folder)
    download(url=url, filepath=filepath)

    # Rename and unzip the contained textfile

    # Zip files should contain a single text file: withdrawnMMDDYYYY.txt
    z = zipfile.ZipFile(filepath)
    potential_files = [file for file in z.infolist()
                       if file.filename.startswith('withdrawn')
                       and file.filename.endswith('.txt')]

    # If the zip file doesn't match what we expect, raise an error
    assert (len(potential_files) == 1), \
        "Zero or multiple files found; unsure which to parse: " \
        "{}".format(potential_files)

    # Strip the date to make this filename consistent between updates
    withdrawn_patent_file = z.infolist()[0]
    withdrawn_patent_file.filename = 'withdrawn.txt'
    z.extract(withdrawn_patent_file, path=destination_folder)
    z.close()

    # Remove the original zip file
    print("Removing: {}".format(filepath))
    os.remove(filepath)


def load_withdrawn(update_config):
    engine = create_engine(get_connection_string(update_config, "TEMP_UPLOAD_DB"))
    withdrawn_folder = '{}/withdrawn'.format(update_config['FOLDERS']['WORKING_FOLDER'])

    withdrawn_file = '{}/withdrawn.txt'.format(withdrawn_folder)
    withdrawn_patents = []
    with open(withdrawn_file, 'r') as f:
        for line in f.readlines():
            if len(line.strip()) > 0:
                withdrawn_patents.append(xml_helpers.process_patent_numbers(line.strip('\n')))
    withdrawn_patents_frame = pd.DataFrame(withdrawn_patents)
    withdrawn_patents_frame.columns = ['patent_id']
    withdrawn_patents_frame.to_sql(con=engine, name="withdrawn_patents", if_exists='replace', index=False)


def update_withdrawn(update_config):
    # JOIN without index; Expected to be fast regardless
    update_query = """
UPDATE patent p join `{temp_upload_db}`.`withdrawn_patents` twp on twp.patent_id = p.id set p.withdrawn = 1;
    """.format(temp_upload_db=update_config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    engine = create_engine(get_connection_string(update_config, "PROD_DB"))
    engine.execute(update_query)


def process_withdrawn(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    withdrawn_folder = '{}/withdrawn'.format(config['FOLDERS']['WORKING_FOLDER'])
    download_withdrawn_patent_numbers(withdrawn_folder)
    load_withdrawn(update_config=config)
    update_withdrawn(update_config=config)


def post_withdrawn(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    qc = WithdrawnTest(config)
    qc.runTests()


if __name__ == '__main__':
    process_withdrawn(**{
            "execution_date": datetime.date(2020, 1, 5)
            })
    post_withdrawn(**{
            "execution_date": datetime.date(2020, 1, 5)
            })
