import urllib
import zipfile
import os
from lxml import html
import lxml.html
import sys
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

def download_cpc_schema(destination_folder):
    """ Download and extract the most recent CPC Schema """

    # Find the correct CPC Schema url
    cpc_schema_url = find_cpc_schema_url()
    cpc_schema_zip_filepath = os.path.join(destination_folder,
                                           "CPC_Schema.zip")
    print(cpc_schema_url)
    # Download the CPC Schema zip file
    print("Destination: {}".format(cpc_schema_zip_filepath))
    general_helpers.download(url=cpc_schema_url, filepath=cpc_schema_zip_filepath)

    # Unzip the zip file
    print("Extracting contents to: {}".format(destination_folder))
    z = zipfile.ZipFile(cpc_schema_zip_filepath)
    z.extractall(destination_folder)
    z.close()

    # Remove the original zip file
    print("Removing: {}".format(cpc_schema_zip_filepath))
    os.remove(cpc_schema_zip_filepath)


def find_cpc_schema_url():
    """
    Search the CPC Scheme & Definition Page for the most recent CPC Scheme.
    This method is necessary because the schema zip file may change names, and
    multiple versions of the schema may be listed on the webpage.

    If there are multiple schema urls, sorting alphabetically ensures that the
    most recent schema is returned.
    """
    base_url = 'http://www.cooperativepatentclassification.org'
    page = urllib.request.urlopen(base_url + '/cpcSchemeAndDefinitions/Bulk.html')
    tree = html.fromstring(page.read())
    potential_links = []
    for link in tree.xpath('//a/@href'):
        if (link.lstrip(".").startswith("/cpc/bulk/CPCSchemeXML")
                and link.endswith(".zip")):
            potential_links.append(link.replace('..',''))

    # Since zip files are formatted CPCSchemeXMLYYYYMM.zip,
    # the last sorted url corresponds to the latest version
    return base_url + sorted(potential_links)[-1]


def download_cpc_grant_and_pgpub_classifications(destination_folder):
    """
    Download and extract the most recent CPC Master Classification Files (MCF)
    """
    # Find the correct CPC Grant and PGPub MCF urls
    cpc_grant_mcf_url, cpc_pgpub_mcf_url = find_cpc_grant_and_pgpub_urls()
    cpc_grant_zip_filepath = os.path.join(destination_folder,
                                          'CPC_grant_mcf.zip')
    cpc_pgpub_zip_filepath = os.path.join(destination_folder,
                                          'CPC_pgpub_mcf.zip')

    # Download and extract CPC Grant and PGPub classifications
    for (filepath, url) in [(cpc_grant_zip_filepath, cpc_grant_mcf_url),
                            (cpc_pgpub_zip_filepath, cpc_pgpub_mcf_url)]:

        # Download the files
        print("Destination: {}".format(filepath))
        general_helpers.download(url=url, filepath=filepath)

        # Rename and unzip zip files
        # Zip files contain a single folder with many subfiles. We just want
        # the contents, so rename the subfiles to ignore their container
        z = zipfile.ZipFile(filepath)
        text_files = [file for file in z.infolist()
                      if file.filename.endswith('.txt')]

        # For example, zip file contents ['foo/', 'foo/bar.txt, 'foo/baz.txt']
        # would be extracted as ['bar.txt', 'baz.txt'] (with 'foo/' ignored)
        for text_file in text_files:
            text_file.filename = text_file.filename.split('/')[-1]
            z.extract(text_file, path=destination_folder)
        z.close()

        # Remove the original zip file
        #print("Removing: {}".format(filepath))
        #os.remove(filepath)


def find_cpc_grant_and_pgpub_urls():
    """
    Search the CPC Bulk Data Storage System for the most recent CPC MCF .
    This method is necessary because the MCF zip file may change names, and
    multiple versions of the MCF file may be listed on the webpage.

    If there are multiple urls, sorting alphabetically ensures that the
    most recent version is returned.
    """
    base_url = 'https://bulkdata.uspto.gov/data/patent/classification/cpc/'
    page = urllib.request.urlopen(base_url)
    tree = html.fromstring(page.read())

    potential_grant_links = []
    potential_pgpub_links = []
    for link in tree.xpath('//a/@href'):
        if (link.startswith("US_Grant_CPC_MCF_Text")
                and link.endswith(".zip")):
            potential_grant_links.append(link)
        elif (link.startswith("US_PGPub_CPC_MCF_Text")
                and link.endswith(".zip")):
            potential_pgpub_links.append(link)

    # Since zip files are formatted Filename_YYYY-MM-DD.zip,
    # the last sorted url corresponds to the latest version
    latest_grant_link = base_url + sorted(potential_grant_links)[-1]
    latest_pgpub_link = base_url + sorted(potential_pgpub_links)[-1]

    return latest_grant_link, latest_pgpub_link


def download_ipc(destination_folder):
    """ Download and extract the most recent CPC to IPC Concordance """
    # Find the correct CPC to IPC Concordance
    ipc_url = find_ipc_url()
    print(ipc_url)
    print("___________")
    ipc_filepath = os.path.join(destination_folder, "ipc_concordance.txt")

    # Download the IPC text file
    print("Destination: {}".format(ipc_filepath))
    general_helpers.download(url=ipc_url, filepath=ipc_filepath)


def find_ipc_url():
    """ Find the url of the CPC to IPC concordance in text format """
    base_url = 'http://www.cooperativepatentclassification.org'
    page = urllib.request.urlopen(base_url + '/cpcConcordances')
    tree = html.fromstring(page.read())

    potential_links = []
    for link in tree.xpath('//a/@href'):
        print(link)
        if (link.lstrip('.').lstrip("/").startswith("cpcConcordances/CPCtoIPCtxt")
                and link.endswith(".txt")):
            potential_links.append(link)

    # There should be exactly one link to the CPC to IPC concordance.
    # Since files are not formatted nicely, we can't sort alphabetically to
    # determine the correct file. If multiple links found, raise an exception
    print(potential_links)
    assert (len(set(potential_links)) == 1), "Unsure which URL to use of: " \
                                             "{}".format(potential_links)
    return base_url + '/' + potential_links[0]


############################################
# TESTS
############################################

def find_cpc_schema_url_test():
    expected_url = 'http://www.cooperativepatentclassification.org/cpc/interleaved/CPCSchemeXML201808.zip'
    assert (find_cpc_schema_url() == expected_url)


def find_cpc_grant_and_pgpub_urls_test():
    expected_grant_url = 'https://bulkdata.uspto.gov/data/patent/classification/cpc/US_Grant_CPC_MCF_Text_2018-06-01.zip'
    expected_pgpub_url = 'https://bulkdata.uspto.gov/data/patent/classification/cpc/US_PGPub_CPC_MCF_Text_2018-06-01.zip'
    assert ((find_cpc_grant_and_pgpub_urls()) ==
            (expected_grant_url, expected_pgpub_url))


def find_ipc_url_test():
    expected_url = 'http://www.cooperativepatentclassification.org/cpcConcordances/CPCtoIPCtxtMay2018.txt'
    assert (find_ipc_url() == expected_url)


if __name__ == '__main__':
    """ Running this script will execute tests; importing it will not """

    import sys
    import datetime
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')
    # Find URLs correctly
    #TODO: update these to reflect most recent dates
    print(find_cpc_schema_url())
    #find_cpc_schema_url_test()

    print(find_cpc_grant_and_pgpub_urls())
    #find_cpc_grant_and_pgpub_urls_test()

    print(find_ipc_url())
    #find_ipc_url_test()

    destination_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
    # Download CPC data, and manually inspect output
    print(str(datetime.datetime.now()))
    download_cpc_schema(destination_folder)  # <1 min
    print(str(datetime.datetime.now()))
    download_cpc_grant_and_pgpub_classifications(destination_folder)  # few minutes
    print(str(datetime.datetime.now()))
    download_ipc(destination_folder)  # <1 min
    print(str(datetime.datetime.now()))


