import re
import os
import csv
import requests
from zipfile import ZipFile
from cpc_class_tables import write_csv
from clint.textui import progress


def parse_and_write_uspc(inputdir, outputdir):
    """ Parse and write USPC info to CSV tables """

    # Parse USPC information from text files
    uspc_applications = parse_uspc_applications(inputdir, 'uspc_applications.zip')
    write_csv(uspc_applications, outputdir,
              'USPC_application_classes_data.csv')

    uspc_patents = parse_uspc_patents(inputdir, 'uspc_patents.zip')
    write_csv(uspc_patents, outputdir,
              'USPC_application_classes_data.csv')


def download(url, outputdir, filename):
    """ Download data from a URL with a handy progress bar """

    r = requests.get(url, stream=True)
    with open(os.path.join(outputdir, filename), 'wb') as f:

        content_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024),
                                  expected_size=(content_length/1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()


def parse_uspc_applications(inputdir, zip_filename):
    """
    Parse USPC Application information from the USPTO MCF Application zip file

    Original Data:
        US20180027683A1361724000S
        US20180027707A1361752000S
        US20180042153P1PLT258000S
        US20180084553A1D14240000S
        US20180092099A1D14240000S

    Parsed Rows:

        Patent Number       Main Class      Subclass    Classification Type
        -------------------------------------------------------------------
        2018/20180027677,   29,             525.1,      2
        2018/20180027677,   312,            265.5,      3
        2018/20180027683,   29,             428,        1
        2018/20180027683,   211,            26,         2
        2018/20180027683,   312,            223.2,      3
    """
    zip = ZipFile(os.path.join(inputdir, zip_filename), 'r')

    # The zip file should contain a single text file like 'mcfappl[\d]+.zip'
    number_of_files_in_zip = len(zip.namelist())
    name_of_first_file_in_zip = zip.namelist()[0]
    assert(number_of_files_in_zip == 1 and
           re.search('mcfappl[\d]+\.txt$', name_of_first_file_in_zip))

    rows = []
    with zip.open(name_of_first_file_in_zip) as f:
        for classification in f:
            # TODO: Check with the team that this is correct
            row = parse_uspc_application(classification)
            rows.append(row)


def parse_uspc_application(app):
    """
    Parse USPC Application information from a USPTO MCF Application entry

    Original Data:
        US20010000001A1520001000P

    Parsed Row:
        2001/20010000001,520,1,0
    """
    # Patent Number
    patent_number = app[2:6] + '/' + app[2:13]

    # Main Class
    main_class = re.sub('^0+', '', app[15:18])

    # Subclass
    subclass = app[18:24]

    # Classification Type
    # TODO: Check the implementation of classification type, and what this
    # really represents. 0 is a placeholder for now.
    class_type = 0

    return [patent_number, main_class, subclass, class_type]


def parse_uspc_patents(inputdir):
    return [['place', 'holder']]
