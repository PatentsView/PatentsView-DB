import re
import os
import csv
import requests
from zipfile import ZipFile
from cpc_class_tables import write_csv
from clint.textui import progress


# def download_and_extract_uspc(outputdir):
#     """ Download and extract USPC Classification information to a directory """
#     for table_name in ['mcfpat.zip', 'mcfappl.zip']:
#         url = 'https://bulkdata.uspto.gov/'\
#                     'data/patent/classification/{}'.format(table_name)
#
#         print("Downloading: {}".format(url))
#         download(url, outputdir, table_name)
#
#         print("Unzipping: {}".format(table_name))
#         unzip(outputdir, table_name)


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


def unzip(dir, filename):
    zip = ZipFile(os.path.join(dir, filename), 'r').extractall(dir)


def parse_uspc_applications(inputdir, zip_filename):
    """ Parse USPC information from a USPC Applications zipfile """
    zip = ZipFile(os.path.join(inputdir, zip_filename), 'r')

    # The zip file should contain a single text file like 'mcfappl[\d]+.zip'
    number_of_files_in_zip = len(zip.namelist())
    name_of_first_file_in_zip = zip.namelist()[0]
    assert(number_of_files_in_zip == 1 and
           re.search('mcfappl[\d]+\.txt$', name_of_first_file_in_zip))

    with zip.open(name_of_first_file_in_zip) as f:
        for classification in f:
            # print(classification)


    return [['place', 'holder']]


def parse_uspc_patents(inputdir):
    return [['place', 'holder']]




def uspc_table(working_directory):
    import re,csv,os,urllib2,HTMLParser,zipfile
    from bs4 import BeautifulSoup as bs
    from datetime import date
    from zipfile import ZipFile

    import mechanize
    br = mechanize.Browser()
    paturl = 'https://bulkdata.uspto.gov/data/patent/classification/mcfpat.zip'
    appurl = 'https://bulkdata.uspto.gov/data/patent/classification/mcfappl.zip'

    os.mkdir(working_directory + "/uspc_inputs")
    inputs = working_directory + "/uspc_inputs/"
    output = working_directory + "/uspc_output/"
    os.mkdir(output)

    br.retrieve(paturl,os.path.join(inputs,'mcfpat.zip'))
    br.retrieve(appurl,os.path.join(inputs,'mcfappl.zip'))


    diri = os.listdir(inputs)
    for d in diri:
        # if re.search('mcfcls.*?zip',d):
        #     classindxfile = ZipFile(os.path.join(inputs, d),'r')
        if re.search('mcfpat.*?zip',d):
            patclassfile = ZipFile(os.path.join(inputs, d),'r')
        if re.search('mcfappl.*?zip',d):
            appclassfile = ZipFile(os.path.join(inputs, d), 'r')

    # #Classes Index File parsing for class/subclass text
    # classidx = classindxfile.open(classindxfile.namelist()[0]).read().split('\n')
    # data = {}
    # for n in range(len(classidx)):
    #     classname = re.sub('[\.\s]+$','',classidx[n][21:])
    #     mainclass = re.sub('^0+','',classidx[n][:3])
    #     if classidx[n][6:9] != '000':
    #         try:
    #             temp = int(classidx[n][6:9])
    #             if re.search('[A-Z]{3}',classidx[n][3:6]) is None:
    #                 if re.search('^[A-Z]',classidx[n][3:6]):
    #                     subclass = re.sub('0+','',classidx[n][3:6])+'.'+classidx[n][6:9]
    #                 else:
    #                     subclass = re.sub('^0+','',classidx[n][3:6])+'.'+re.sub('0+','',classidx[n][6:9])
    #             else:
    #                 subclass =re.sub('^0+','',classidx[n][3:6])+re.sub('^0+','',classidx[n][6:9])
    #         except:
    #             if len(re.sub('0+','',classidx[n][6:9])) > 1:
    #                 subclass = re.sub('^0+','',classidx[n][3:6])+'.'+re.sub('0+','',classidx[n][6:9])
    #             else:
    #                 subclass = re.sub('^0+','',classidx[n][3:6])+re.sub('0+','',classidx[n][6:9])
    #     else:
    #         subclass = re.sub('^0+','',classidx[n][3:6])
    #     if classidx[n][18:21] != '000':
    #         try:
    #             temp = int(classidx[n][18:21])
    #             if re.search('[A-Z]{3}',classidx[n][15:18]) is None:
    #                 if re.search('^[A-Z]',classidx[n][15:18]):
    #                     highersubclass = re.sub('0+','',classidx[n][15:18])+'.'+classidx[n][18:21]
    #                 else:
    #                     highersubclass = re.sub('^0+','',classidx[n][15:18])+'.'+re.sub('0+','',classidx[n][18:21])
    #             else:
    #                 highersubclass = re.sub('^0+','',classidx[n][15:18])+re.sub('^0+','',classidx[n][18:21])
    #         except:
    #             if len(re.sub('0+','',classidx[n][18:21])) > 1:
    #                 highersubclass = re.sub('^0+','',classidx[n][15:18])+'.'+re.sub('0+','',classidx[n][18:21])
    #             else:
    #                 highersubclass = re.sub('^0+','',classidx[n][15:18])+re.sub('0+','',classidx[n][18:21])
    #     else:
    #         highersubclass = re.sub('^0+','',classidx[n][15:18])

    #     try:
    #         gg = data[mainclass+' '+highersubclass]
    #         data[mainclass+' '+subclass] = classname+'-'+gg
    #     except:
    #         data[mainclass+' '+subclass] = classname



    # # Create subclass and mainclass tables out of current output
    # output = working_directory + "/uspc_output/"
    # os.mkdir(output)
    # outp1 = csv.writer(open(os.path.join(output,'mainclass.csv'),'wb'))
    # outp2 = csv.writer(open(os.path.join(output,'subclass.csv'),'wb'))
    # exist = {}
    # # print len(data)
    # # print data.keys()[1]
    # # print data.values()[1]
    # counter = 0
    # counter2 =0
    # for k,v in data.items():
    #     i = k.split(' ')+[v]
    #     if counter2 < 20:
    #         print i

    #     #try:
    #     if i[1] == '':
    #         outp1.writerow([i[0],i[2]])
    #         if counter < 10:
    #             print i
    #             print "and it is " + i[1]
    #             counter +=1
    #     else:
    #         try:
    #             gg = exist[i[0]+'/'+i[1]]
    #         except:
    #             exist[i[0]+'/'+i[1]] = 1
    #             outp2.writerow([i[0]+'/'+i[1],i[2]])
    #     # except:
    #     #     try:
    #     #         gg = exist[i[0]+'/'+i[1]]
    #     #     except:
    #     #         exist[i[0]+'/'+i[1]] = 1
    #     #         outp2.writerow([i[0]+'/'+i[1],i[2]])

    #Get patent-class pairs
    outp = csv.writer(open(os.path.join(output,'USPC_patent_classes_data.csv'),'wb'))
    pats = {}
    with patclassfile.open(patclassfile.namelist()[0]) as inp:
        for i in inp:
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
                outp.writerow([patentnum,mainclass,subclass,'0'])
            else:
                try:
                    gg = pats[patentnum]
                    outp.writerow([str(patentnum),mainclass,subclass,str(gg)])
                    pats[patentnum]+=1
                except:
                    pats[patentnum] = 2
                    outp.writerow([str(patentnum),mainclass,subclass,'1'])

    #Get application-class pairs
    outp = csv.writer(open(os.path.join(output,'USPC_application_classes_data.csv'),'wb'))
    pats = {}
    with appclassfile.open(appclassfile.namelist()[0]) as inp:
        for i in inp:
            patentnum = i[2:6]+'/'+i[2:13]
            mainclass = re.sub('^0+','',i[15:18])
            subclass = i[18:-2]
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
            if i[-2] == 'P':
                outp.writerow([patentnum,mainclass,subclass,'0'])
            else:
                try:
                    gg = pats[patentnum]
                    outp.writerow([str(patentnum),mainclass,subclass,str(gg)])
                    pats[patentnum]+=1
                except:
                    pats[patentnum] = 2
                    outp.writerow([str(patentnum),mainclass,subclass,'1'])
