import re
import os
from zipfile import ZipFile
import sys
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
sys.path.append('/project/Development')
from helpers import general_helpers


def parse_and_write_uspc(inputdir, outputdir):
    """ Parse and write USPC info to CSV tables """
    uspc_applications = parse_uspc_applications(inputdir,
                                                'uspc_applications.zip')
    general_helpers.write_csv(uspc_applications, outputdir,
              'USPC_application_classes_data.csv')

    uspc_patents = parse_uspc_patents(inputdir, 'uspc_patents.zip')
    general_helpers.write_csv(uspc_patents, outputdir,
              'USPC_patent_classes_data.csv')


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
    global found_patents
    found_patents = {}

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
            row = parse_uspc_application(classification.decode('utf-8'))
            rows.append(row)

    return rows


def parse_uspc_application(row):
    """
    Parse USPC Application information from a USPTO MCF Application entry
    Original Data:
        US20010000001A1520001000P
    Parsed Row:
        2001/20010000001,520,1,0
    """
    # Patent Number
    #print(row)
    patent_number = row[2:6] + '/' + row[2:13]

    # Main Class
    main_class = re.sub('^0+', '', row[15:18])

    # Subclass
    subclass = parse_subclass(row[18:-2])

    # Classification Type
    order = parse_order(class_type_char=row[-2],
                        patent_number=patent_number, primary_type_char='P')
    return [patent_number, main_class, subclass, order]


def parse_subclass(subclass):
    """ Parse subclass informatinon from the relevant subclass string """
    left = subclass[:3]
    right = subclass[3:]

    # TODO: Reorganize logic to be more readable
    if right == '000':
        subclass = left.lstrip('0')
    else:
        if right.isdigit():
            if left.isalpha():
                subclass = left.lstrip('0') + right.lstrip('0')
            else:
                if left[0].isalpha():
                    subclass = left.replace('0', '') + '.' + right
                else:
                    subclass = left.lstrip('0') + '.' + right.replace('0', '')
        else:
            if len(right.replace('0', '')) > 1:
                subclass = left.lstrip('0') + '.' + right.replace('0', '')
            else:
                subclass = left.lstrip('0') + right.replace('0', '')

    return subclass


def parse_uspc_patents(inputdir, zip_filename):
    """
    Parse USPC Patent information from the USPTO MCF Patent zip file
    Original Data:
        0000001295004000O
        0000001016100000X
        0000002057058490O
        0000003142042000O
        0000003142048000X
        0000003144012000X
    Parsed Rows:
        Patent Number       Main Class      Subclass    Classification Type
        -------------------------------------------------------------------
        2018/20180027677,   29,             525.1,      2
        2018/20180027677,   312,            265.5,      3
        2018/20180027683,   29,             428,        1
        2018/20180027683,   211,            26,         2
        2018/20180027683,   312,            223.2,      3
    """
    global found_patents
    found_patents = {}
    rows = []

    zip = ZipFile(os.path.join(inputdir, zip_filename), 'r')

    # The zip file should contain a single text file like 'mcfappl[\d]+.zip'
    number_of_files_in_zip = len(zip.namelist())
    name_of_first_file_in_zip = zip.namelist()[0]
    assert(number_of_files_in_zip == 1 and
           re.search('mcfpat[\d]+\.txt$', name_of_first_file_in_zip))

    with zip.open(name_of_first_file_in_zip) as f:
        for classification in f:
            row = parse_uspc_patent(classification.decode('utf-8'))
            rows.append(row)

    return rows


def parse_uspc_patent(row):
    """
    Parse USPC Patent information from a USPTO MCF Patent entry
    Original Data:
        0000001295004000O
    Parsed Row:
        0000001,295,4,0
    """
    # Patent Number
    patent_number = row[:7]

    # Main Class
    main_class = re.sub('^0+', '', row[7:10])

    # Subclass
    subclass = parse_subclass(row[10:-2])

    # Classification Type
    order = parse_order(class_type_char=row[-2],
                        patent_number=patent_number, primary_type_char='O')

    return [patent_number, main_class, subclass, order]


def parse_order(class_type_char, patent_number, primary_type_char):
    """ Parse classification order from row, using the global counter
    of found patents to increment order each time a patent is seen again. For
    example, a Primary and three Secondary USPC Classifications will have
    orders 0, 1, 2, and 3 respectively. (The Primary will always have order 0,
    and the Secondary will have orders incremented by the order of appearance)
    class_type_char: the USPC classification character
                        that indicates primary or secondary
    patent_number: 7 digit patent number parsed from the USPC classification
    primary_type_char: The character that represents primary
                        (typically 'O' for patents or 'P' for applications)
    """

    if class_type_char == primary_type_char:
        # Primary Patents -> class_type = 0
        class_type = str(0)
    else:
        # Secondary Patents -> class_type = 1, 2, 3, ... (incrementing)
        if patent_number in found_patents:
            found_patents[patent_number] += 1
        else:
            found_patents[patent_number] = 1
        class_type = str(found_patents[patent_number])

    return class_type

if __name__ == '__main__':

    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')

    inputdir = '{}/uspc_input'.format(config['FOLDERS']['WORKING_FOLDER'])
    outputdir = '{}/uspc_output'.format(config['FOLDERS']['WORKING_FOLDER'])

    print("Parsing and writing USPC Classification tables... \n")
    print("INPUT DIRECTORY: {}".format(inputdir))
    print("OUTPUT DIRECTORY: {}".format(outputdir))

    if not os.path.exists(inputdir):
         os.makedirs(inputdir)
    # Download USPC Patent Classifications
    uspc_patents_url = 'https://bulkdata.uspto.gov/data/patent/classification/mcfpat.zip'
    uspc_patents_filepath = os.path.join(inputdir, 'uspc_patents.zip')
    general_helpers.download(uspc_patents_url, uspc_patents_filepath)

    # Download USPC Application Classifications
    uspc_applications_url = 'https://bulkdata.uspto.gov/data/patent' \
                            '/classification/mcfappl.zip'
    uspc_applications_filepath = os.path.join(inputdir,
                                              'uspc_applications.zip')
    general_helpers.download(uspc_applications_url, uspc_applications_filepath)
    print('downloaded')
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    # Parse USPC Classifications and write to CSV
    parse_and_write_uspc(inputdir, outputdir)
