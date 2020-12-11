# PatentsView
===========
## Setting up developer environment for Airflow Based Parser
See DEVELOPER.md

## Historic Parser Script

### 1976 -2004 Parser
#### Requirements
1. Python 2.7.8
2. MySQL-python 1.2.5

#### Steps
0. Download the zip file from [v0.1](https://github.com/PatentsView/PatentsView-DB/releases/tag/v0.1) release
1. Download the files to be processed to a directory. Note that you should use separate directories for the two periods; don't mix files in the same input directory.
2. Unzip the files.
3. Run the `parser_wrapper.py` script to parse the raw files into CSV files.
  1. Help for the this script can be obtained by: `python parser_wrapper.py --help`.
  2. A complete example call might look like this: `D:\PatentsView-DB\Scripts\Raw_Data_Parsers> python parser_wrapper.py --input-dir d:/data/smalltest/raw_1976-2001 --output-dir d:/data/smalltest/parsed_1976-2001 --period 1`
4. Repeat the prior step as needed to get all the desired files parsed.
5. Run the `parser_wrapper.py` script to load the data from the CSV files into a MySQL database.
  1. E.g.: `python parser_wrapper.py --mysql 1 --mysql-input-dir "uspto_parsed/1976-2001" --mysql-host name_or_address_of_host --mysql-username your_username --mysql-passwd your_password --mysql-dbname target_database`

### Classification File Parser
#### Requirements
1. Python 2.7.8
2. MySQL-python 1.2.5
3. Mechanize (installed using easy_install)

#### Steps
0. Download the zip file from [v0.1](https://github.com/PatentsView/PatentsView-DB/releases/tag/v0.1) release
1. Run the  parser_wrapper.py  script to parse the raw files into CSV files.
  1. Help for the this script can be obtained by:  python parser_wrapper.py --help .
  2. A complete example call might look like this:  D:\PatentsView-DB\Scripts\Raw_Data_Parsers> python parser_wrapper.py --uspc-create 1 --uspc-input-dir d:/data/smalltest/raw_class
2. Run the  parser_wrapper.py  script to load the data from the CSV files into a MySQL database.
  1. E.g.:  python parser_wrapper.py --uspc-upload 1 --uspc-upload-dir d:/data/smalltest/raw_class --mysql-host name_or_address_of_host --mysql-username your_username --mysql-passwd your_password --appdb target_apps_database --patdb target_grants_database  
3. Run the parser_wrapper.py script to load the CPC current class data into a MySQL database.
  1. E.g.: python parser_wrapper.py --cpc-upload 1 --cpc-upload-dir d:/data/smalltest/cpc_class --mysql-host name_or_address_of_host --mysql-username your_username --mysql-passwd your_password --appdb target_apps_database --patdb target_grants_database 
4. Run the nber_tables.sql statement to create NBER classification tables (nber_category, nber_subcategory, nber) - file in Create folder.
5. Run the parser_wrapper.py script to load the NBER class data into a MySQL database (available only for granted patents).
  1. E.g.: python parser_wrapper.py --nber-upload 1 --nber-upload-dir d:/data/smalltest/nber_class --mysql-host name_or_address_of_host --mysql-username your_username --mysql-passwd your_password --patdb target_grants_database 

