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
    uspc_applications = parse_uspc_applications(inputdir,
                                                'uspc_applications.zip')
    print(uspc_applications[:10])
    write_csv(uspc_applications, outputdir,
              'USPC_application_classes_data.csv')

    # print(uspc_patents[:10])
    uspc_patents = parse_uspc_patents(inputdir, 'uspc_patents.zip')
    write_csv(uspc_patents, outputdir,
              'USPC_patent_classes_data.csv')


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

    return rows

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
    # TODO: Implement the custom subclass parser
    subclass = parse_subclass(app[10:-2])
    # subclass = app[18:24]

    # Classification Type
    # TODO: Check the implementation of classification type, and what this
    # really represents. 0 is a placeholder for now.
    class_type = '0'

    return [patent_number, main_class, subclass, class_type]


def parse_subclass(subclass):
    """ Parse subclass informatinon from the relevant subclass string """
    left = subclass[:3]
    right = subclass[3:]
    path = -1

    if right == '000':
        subclass = left.lstrip('0')
        path = 0
    else:
        if right.isdigit():
            if left.isalpha():
                subclass = left.lstrip('0') + right.lstrip('0')
                path = 1
            else:
                if left[0].isalpha():
                    subclass = left.replace('0', '') + '.' + right
                    path = 2
                else:
                    subclass = left.lstrip('0') + '.' + right.replace('0', '')
                    path = 3
        else:
            if len(right.replace('0', '')) > 1:
                subclass = left.lstrip('0') + '.' + right.replace('0', '')
                path = 5
            else:
                subclass = left.lstrip('0') + right.replace('0', '')
                path = 4
    print("Path: {}".format(path))
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
    zip = ZipFile(os.path.join(inputdir, zip_filename), 'r')

    # The zip file should contain a single text file like 'mcfappl[\d]+.zip'
    number_of_files_in_zip = len(zip.namelist())
    name_of_first_file_in_zip = zip.namelist()[0]
    assert(number_of_files_in_zip == 1 and
           re.search('mcfpat[\d]+\.txt$', name_of_first_file_in_zip))

    rows = []
    with zip.open(name_of_first_file_in_zip) as f:
        for classification in f:
            # TODO: Check with the team that this is correct

            # Ensure the new parser returns the same results
            old = parse_uspc_patent_old(classification)
            new = parse_uspc_patent(classification)
            if old != new:
                print('Original: {}'.format(classification))
                print('Original subgroup: {}'.format(classification[10:-2]))
                print("Old: {}".format(old))
                print("New: {}".format(new))

            assert(old[:3] == new[:3])

            row = parse_uspc_patent(classification)
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
    # TODO: change to [10:16]
    subclass = parse_subclass(row[10:-2])


    # Classification Type
    # TODO: Check the implementation of classification type, and what this
    # really represents. 0 is a placeholder for now.
    class_type = '0'

    return [patent_number, main_class, subclass, class_type]


def parse_uspc_patent_old(i):
    """
    Old method of parsing USPC Patent info
    """
    pats = {}
    patentnum = i[:7]
    mainclass = re.sub('^0+','',i[7:10])
    subclass = i[10:-2]
    if subclass[3:] != '000':
        try:
            temp = int(subclass[3:])
            if re.search('[A-Z]{3}',subclass[:3]) is None:
                if re.search('^[A-Z]',subclass[:3]):
                    subclass = re.sub('0+','',subclass[:3])+'.'+subclass[3:]
                else:
                    subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
            else:
                subclass = re.sub('^0+','',subclass[:3])+re.sub('^0+','',subclass[3:])
        except:
            if len(re.sub('0+','',subclass[3:])) > 1:
                subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
            else:
                subclass = re.sub('^0+','',subclass[:3])+re.sub('0+','',subclass[3:])
    else:
        subclass = re.sub('^0+','',subclass[:3])
    if i[-2] == 'O':
        return [patentnum,mainclass,subclass,'0']
    else:
        try:
            gg = pats[patentnum]
            return [str(patentnum),mainclass,subclass,str(gg)]
            pats[patentnum]+=1
        except:
            pats[patentnum] = 2
            return [str(patentnum),mainclass,subclass,'1']
