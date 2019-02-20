import os
import sys
import zipfile
sys.path.append('/project/Development')
from helpers import general_helpers,xml_helpers




def download_withdrawn_patent_numbers(destination_folder):
    """ Download and extract the list of withdrawn patents """

    # Download
    url = 'https://www.uspto.gov/sites/default/files/documents/withdrawn.zip'
    filepath = os.path.join(destination_folder, 'withdrawn.zip')
    print("Destination: {}".format(filepath))
    if not os.path.exists(destination_folder):
        os.mkdir(destination_folder)
    general_helpers.download(url=url, filepath=filepath)

    # Rename and unzip the contained textfile

    # Zip files should contain a single text file: withdrawnMMDDYYYY.txt
    z = zipfile.ZipFile(filepath)
    potential_files = [file for file in z.infolist()
                       if file.filename.startswith('withdrawn')
                       and file.filename.endswith('.txt')]

    # If the zip file doesn't match what we expect, raise an error
    assert (len(potential_files) == 1), \
        "Zero or multiple files found; unsure which to parse: "\
        "{}".format(potential_files)

    # Strip the date to make this filename consistent between updates
    withdrawn_patent_file = z.infolist()[0]
    withdrawn_patent_file.filename = 'withdrawn.txt'
    z.extract(withdrawn_patent_file, path=destination_folder)
    z.close()

    # Remove the original zip file
    print("Removing: {}".format(filepath))
    os.remove(filepath)

def update_withdrawn(engine, withdrawn_folder):
    
    withdrawn_file = '{}/withdrawn.txt'.format(withdrawn_folder)

    withdrawn_patents = []
    with open(withdrawn_file, 'r') as f:
        for line in f.readlines():
            withdrawn_patents.append(xml_helpers.process_patent_numbers(line.strip('\n')))

    for e, patent in enumerate(withdrawn_patents):
        db_con = engine.connect()
        query = "update patent set withdrawn = 1 where id = '{}'".format(patent)
        db_con.execute(query)
        db_con.close()
    
if __name__=='__main__':    
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')
    
    engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    
    withdrawn_folder = '{}/withdrawn'.format(config['FOLDERS']['WORKING_FOLDER'])
    download_withdrawn_patent_numbers(withdrawn_folder)
    
    update_withdrawn(engine, withdrawn_folder)
    

