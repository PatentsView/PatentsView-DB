import argparse
from uspto_parsers import generic_parser_1976_2001,generic_parser_2002_2004

parser = argparse.ArgumentParser(description='This program is used to parse USPTO full-text patent grant data for 1976-2004.',epilog='(Example syntax: python parser_wrapper.py --input-dir "uspto_raw/1976-2001/" --output-dir "uspto_parsed/1976-2001/" --period 1)')
parser.add_argument('--input-dir',required=True,help='Full path to directory where all patent raw files are located (TXT or XML format; as downloaded from Google Patents or ReedTech).')
parser.add_argument('--output-dir',required=True,help='Full path to directory where to write all output csv files.')
parser.add_argument('--period',choices=['1','2'],required=True,help='Enter 1 for 1976-2001 or 2 for 2002-2004.')
params = parser.parse_args()

if int(params.period)==1:
    generic_parser_1976_2001.parse_patents(params.input_dir,params.output_dir)
else:
    generic_parser_2002_2004.parse_patents(params.input_dir,params.output_dir)




