#Full Database Load Process

These are the instructions for creating a brand new patents DB and applications DB from scratch, so-called "full DB load". It will start with nothing but the code and result in fully populated databases or the raw data. See [some other page]() for creating the PatentsView DB from this raw DB.

Note that specific drives, directories, and databases names are used through this document. You can, of course, change those to anything you like as long as you make the appropriate changes to the working config files and use them consistently.

##Server Environment Setup
Check these two Github wiki pages for instructions on setting up the server(s) for processing:

- [PatentsProcessor setup](https://github.com/CSSIP-AIR/PatentsProcessor/wiki/Setup-Instructions)
- [InventorDisambiguator setup](https://github.com/CSSIP-AIR/InventorDisambiguator/wiki/Setup-Instructions)

These will include steps on installing the required software and getting the code from Github.

The last time this process was run, it was done on AWS servers with these general specs, but they were changed during the processing to handle the demands of the different processes.

- EC2: r3.2xlarge
- RDS: r3.xlarge, 2000 PIOPS


##Preparation
1. Check `process.cfg` for settings
	- `parse=initialload`
	- `clean=False`
	- `consolidate=False`
	- `doctype=all`
	- `datadir=F:/Patent Full Text/YYYY`
2. Check `lib\alchemy\config.ini` for DB names: `patent_20141215` and `app_20141215`
3. Create/empty `F:\Patent Full Text\1976-2001_parsed`
4. Create/empty `F:\Patent Full Text\2002-2004_parsed`
5. Create/empty `F:\CPC_class`
6. Create/empty `f:\USPC_class`
7. Create directories for the raw data, named like: `F:\Patent Full Text\YYYY` and `F:\Application Full Text\YYYY`, one per year to be processed.
8. Populate these directories with the bulk data files from Reed Tech.
	1. `http://patents.reedtech.com/pgrbft.php`
	2. `http://patents.reedtech.com/parbft.php`
	3. Note: I found it easiest to use the FireFox extension DownThemAll, downloading all files into a single dir, and then moving them to year-specific dirs.
	4. Note: Downloading from Reed Tech was much slower (like 60X) than from Google, except for the latest file which was pretty fast. So you could download what is available from Google, and then add from Reed Tech whatever Google didn't have. As of this writing (12/15/2014), Google only had data files up through 10/09/2014.
9. Create DBs. Created new DB (no schema), one for each year to be processed.

	Example SQL script:

		/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
		/*!40101 SET NAMES utf8 */;
		/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
		/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2001` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2002` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2003` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2004` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2005` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2006` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2007` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2008` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2009` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2010` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2011` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2012` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2013` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `app_20141215_2014` /*!40100 DEFAULT CHARACTER SET utf8 */;
		/* These next four (2001-2004) won't actually get populated, but the parser needs them to exist or it will fail. */
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2001` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2002` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2003` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2004` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2005` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2006` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2007` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2008` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2009` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2010` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2011` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2012` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2013` /*!40100 DEFAULT CHARACTER SET utf8 */;
		CREATE DATABASE IF NOT EXISTS `patent_20141215_2014` /*!40100 DEFAULT CHARACTER SET utf8 */;
		/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
		/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
		/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
10. Create clone directories `D:\PatentsProcessor_Pat_YYYY` and `D:\PatentsProcessor_App_YYYY`, one per year to be processed.

	The following commands can be put in a .bat file

		xcopy PatentsProcessor PatentsProcessor_App_2001 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2002 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2003 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2004 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2005 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2006 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2007 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2008 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2009 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2010 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2011 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2012 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2013 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_App_2014 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2005 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2006 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2007 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2008 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2009 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2010 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2011 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2012 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2013 /E /Q /I
		xcopy PatentsProcessor PatentsProcessor_Pat_2014 /E /Q /I
11. Edit the `process.cfg` and `config.ini` files in those cloned directories to reflect the correct directories and DBs for that specific year.
12. Create new DBs using the `Scripts\Create\patents.sql` script
	- `patent_20141215_1976-2001`
	- `patent_20141215_2002-2004`
13. Set MySQL `wait_timeout` variable to 172800 (48 hours) on the MySQL server. Default is 8 hours, which may cause a timeout when the location disambiguation is run.

## Part 1 - Parallel Groups A and B
Parallel Group A and Parallel Group B can be run simultaneously.
###Parallel Group A
1. Change dir to `d:\PatentsView-DB\Scripts\Raw_Data_Parsers`
2. `parser_wrapper.py --input-dir="f:\Patent Full Text\1976-2001" --output-dir="f:\Patent Full Text\1976-2001_parsed" --period 1`
	- 7.1h
3. `python parser_wrapper.py --mysql 1 --mysql-input-dir "f:\Patent Full Text\1976-2001_parsed" --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --mysql-dbname patent_20141215_1976-2001`
	- Failed with out of memory error on a 60G machine when trying to process the `claim.csv` file. It was 13G in size. Workaround:
		- Used GSplit to split the file into 100,000 rows each. `Named claim.csv.NNN.`
		- Moved the `claim.csv`, `application.csv`, and `rawlocation.csv` files to `Save` folder. The last two since they were already processed.
		- Temp mod on the `csv_to_mysql.py` code to comment out lines 53-55:
				
				diri.insert(0,'patent.csv')
				del diri[diri.index('rawlocation.csv')]
				diri.insert(2,'rawlocation.csv')

	- 34.2h

###Parallel Group B
1. Change dir to `d:\PatentsView-DB\Scripts\Raw_Data_Parsers`
2. `parser_wrapper.py --input-dir="f:\Patent Full Text\2002-2004" --output-dir="f:\Patent Full Text\2002-2004_parsed" --period 2`
	- 4.7h 
3. `python parser_wrapper.py --mysql 1 --mysql-input-dir "f:\Patent Full Text\2002-2004_parsed" --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --mysql-dbname patent_20141215_2002-2004`
	- 8.6h

##Part 2 - Parallel Paths C+
Do the following in each `PatentProcessor_XXX_YYYY` directory. On an 8 CPU machine, up to 7 can be run at a time. But keep in mind that the more that are run in parallel, the more hit on the DB server. The last time this was run, we started with anEC2 of r3.2xlarge and RDS of r3.xlarge with 2000 PIOPS. Partway through we changed the RDS to 3000 PIOPS.

`python start.py process.cfg`

- On average when run in parallel with 3-6 others in parallel on an 8 core machine, these would take about 34 hours for each patent year (2005-2014) and  20 hours for each app year (2001-2014). So, theoretically, if run parallel combined these would take about 4.5 days.
- Note: there were file format issues with the 2003 app files. Specifically:
	- Errored in pa030313.xml. The line at 3168412 had the end of one patent and the beginning of the next on the same line. Like below. So I manually split that line and reran.
			
		`</patent-application-publication><?xml version="1.0" encoding="UTF-8"?>`

	- Again in pa030320.xml at line 3919942, pa030424.xml, pa030501.xml
	- Malformed text in pa030501.xml for US20030082999A1. Removed that application.

## Part 3: Merging
After Part 1 and Part 2 above are both complete, you will have individually populated year-based raw databases without any post-processing. The next part is to merge the year-based databases into a single database.

1. Change RDS to 5000 PIOPS
2. Create target merged DBs and run `Scripts\Create` scripts to create schema
3. Change dir to `d:\PatentsView-DB\Scripts\Raw_Data_Parsers`
4. Merge the patent DBs: `parser_wrapper.py --merge-db 1 --sourcedb patent_20141215_1976-2001,patent_20141215_2002-2004,patent_20141215_2005,patent_20141215_2006,patent_20141215_2007,patent_20141215_2008,patent_20141215_2009,patent_20141215_2010,patent_20141215_2011,patent_20141215_2012,patent_20141215_2013,patent_20141215_2014 --targetdb patent_20141215 --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed>`
	- 16.0h
4. Merge the app DBs: `parser_wrapper.py --merge-db 1 --sourcedb app_20141215_2001,app_20141215_2002,app_20141215_2003,app_20141215_2004,app_20141215_2005,app_20141215_2006,app_20141215_2007,app_20141215_2008,app_20141215_2009,app_20141215_2010,app_20141215_2011,app_20141215_2012,app_20141215_2013,app_20141215_2014 --targetdb app_20141215 --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed>`
	- 17.0h

## Part 4: Get Current Class Data
1. Get the latest USPC current class assignments: `parser_wrapper.py --uspc-create 1 --uspc-input-dir f:\USPC_class`
	- 9m
2. Upload the USPC current classes: `parser_wrapper.py --uspc-upload 1 --uspc-upload-dir f:\USPC_class --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --patdb patent_20141215 --appdb app_20141215`
	- 4.2h
3. Upload the CPC current classes: `parser_wrapper.py --cpc-upload 1 --cpc-upload-dir f:\CPC_class --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --patdb patent_20141215 --appdb app_20141215`
	- 7.2h
4. Upload the NBER categories: `parser_wrapper.py --nber-upload 1 --nber-upload-dir f:\NBER_class --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --patdb patent_20141215`

## Part 5: Run Assignee, Lawyer, and Location Disambiguations
1. Change dir to `d:\PatentsProcessor`
2. Make sure the `config.ini` file points to `patent_20141215` and `app_20141215`
3. `python lib/lawyer_disambiguation.py grant`
	- 2.4h
	- Used 24G of memory
4. `python lib/assignee_disambiguation.py`
	- 9.4h
	- Used 51G of memory
5. `python lib/geoalchemy.py`
	- 16h patents
	- 14.3h apps
	- Used 74G of memory
	- Had to change the MySQL wait_timeout variable to 172800 (48 hours), since it was timing out after 8 hours. Would give a SQLAlchemy OperationalError exception stating "MYSQL server has gone away". The code was also changed to avoid this, and with the code change and the extended timeout it ran successfully; the success is uncertain if only one of those measures (code change and extended timeout) is in place.

## Part 6: Inventor Disambiguation
1. Change dir to `d:\PatentsProcessor`
2. `run_consolidation.bat`
	- 14.6h
3. Copy `disambiguator_November_22.tsv` from `d:\PatentsProcessor` to `d:\InventorDisambiguator`
4. Change dir to `d:\InventorDisambiguator`
5. `php Initialize_Input.php disambiguator_December_11.tsv`
	- 3m
6. `python Initialize_ID.py`
	- 22m
	- When using the PHP version got PHP Fatal error:  `Out of memory (allocated 1783627776) (tried to allocate 134217728 bytes) in D:\InventorDisambiguator\Initialize_ID.php on line 13 zend_mm_heap corrupted`
	- So decided to port the code to Python. Worked fine.
7. `Matrixify_Attributes.py`
	- 10m
	- The PHP version of this would fail with memory errors, so ported to Python.
8. Make sure `main.cpp` has been compiled as `disambig.exe` per the InventorDisambiguator instructions above.
9. `disambig d:\InventorDisambiguator`
	- 5.5h
	- 8G
10. Copy `_disambiguator_output_cpp.tsv` to `d:\PatentsProcessor`
11. Change dir to `d:\PatentsProcessor`
12. `integrate.py disambiguator_December_11.tsv _disambiguator_output_cpp.tsv`
	- 7.4h
	- 50G

## Part 7: Transformational script
1. Change dir to 'd:/PatentsView-DB/Scripts/Transform_script'
2. `python start.py --transform 1 --transform-upload-dir <upload-folder> --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --appdb <appdb> --patdb <patdb>`  
	 - Transforms existing data in Applications and Patent DB by adding `_transformed` columns
3. Run a SQL query on Application DB to create new `application_update` table: 'CREATE TABLE <appdb>.application_update LIKE <appdb>.application'
4. `python start.py --update-appnums 1 --appnums-upload-dir <upload-folder> --mysql-host <removed> --mysql-username <removed> --mysql-passwd <removed> --appdb <appdb>`
	- Creates a new updated table in Applications DB to populate number fields and granted flag
	   * When data is updated: need to recreate the crosswalk between <patdb>.application and <appdb>.application numbers              to get the granted flag.



##End Results:
Full populated and disambiguated patents and applications databases.
Total minimum processing time: 264 hours (11 days).
