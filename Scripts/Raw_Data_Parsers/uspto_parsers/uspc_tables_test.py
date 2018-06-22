import sys

from uspc_table import parse_and_write_uspc, download, unzip

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: python {} {} {}".format(sys.argv[0],
                                              "<INPUT DIR>", "<OUTPUT DIR>"))
        sys.exit(1)

    inputdir = sys.argv[1]
    outputdir = sys.argv[2]

    print("Parsing and writing USPC Classification tables... \n")
    print("INPUT DIRECTORY: {} \n".format(inputdir))
    print("OUTPUT DIRECTORY: {} \n".format(outputdir))


    # Download and extract tables
    # for table_name in ['mcfpat.zip', 'mcfappl.zip']:
    #     url = 'https://bulkdata.uspto.gov/'\
    #                 'data/patent/classification/{}'.format(table_name)
    #
    #     # Download tables
    #     print("Downloading: {}".format(url))
    #     download(url, inputdir, table_name)
    #
    #     # Unzip tables
    #     print("Unzipping: {}".format(table_name))
    #     unzip(inputdir, table_name)

    # uspc_patents_url = 'https://bulkdata.uspto.gov/data/patent/classification/mcfpat.zip'
    # uspc_applications_url = 'https://bulkdata.uspto.gov/data/patent/classification/mcfappl.zip'
    # download(uspc_patents_url, inputdir, 'uspc_patents.zip')
    # download(uspc_applications_url, inputdir, 'uspc_applications.zip')

    parse_and_write_uspc(inputdir, outputdir)
