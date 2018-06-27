import sys
import os
from uspc_table import parse_and_write_uspc, parse_uspc_patents
from parser_utils import download

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: python {} {} {}".format(sys.argv[0],
                                              "<INPUT DIR>", "<OUTPUT DIR>"))
        sys.exit(1)

    inputdir = sys.argv[1]
    outputdir = sys.argv[2]

    print("Parsing and writing USPC Classification tables... \n")
    print("INPUT DIRECTORY: {}".format(inputdir))
    print("OUTPUT DIRECTORY: {}".format(outputdir))

    # Download USPC Patent Classifications
    uspc_patents_url = 'https://bulkdata.uspto.gov/data/patent' \
                       '/classification/mcfpat.zip'
    uspc_patents_filepath = os.path.join(outputdir, 'uspc_patents.zip')
    download(uspc_patents_url, uspc_patents_filepath)

    # Download USPC Application Classifications
    uspc_applications_url = 'https://bulkdata.uspto.gov/data/patent' \
                            '/classification/mcfappl.zip'
    uspc_applications_filepath = os.path.join(outputdir,
                                              'uspc_applications.zip')
    download(uspc_applications_url, uspc_applications_filepath)

    # Parse USPC Classifications and write to CSV
    parse_and_write_uspc(inputdir, outputdir)
