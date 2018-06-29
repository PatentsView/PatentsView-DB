from cpc_parser import parse_pgpub_file, parse_grant_file, parse_and_write_cpc
from cpc_table import cpc_table

def parse_pgpub_file_test(problematic_file):
    results = parse_pgpub_file(problematic_file)
    print(len(results))


def parse_grant_file_test(problematic_file):
    results = parse_grant_file(problematic_file)
    print(len(results))


if __name__ == '__main__':

    import sys

    # pgpub_problematic_file = '/Users/jsennett/Documents/PatentsView/cpc_new/US_PGPub_CPC_MCF_20150200000.txt'
    # grant_problematic_file = '/Users/jsennett/Documents/PatentsView/cpc_new/US_Grant_CPC_MCF_8000000.txt'
    # parse_grant_file_test(grant_problematic_file)
    # parse_pgpub_file_test(pgpub_problematic_file)
    # parse_and_write_cpc(inputdir=sys.argv[1], outputdir=sys.argv[2])

    # Compare results to previous parser
    cpc_table('/Users/jsennett/Documents/PatentsView/cpc/archive')
