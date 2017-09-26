README
======

#### StarCluster and Batch Parsing

We take advantage of the fantastic [StarCluster](http://star.mit.edu/cluster/) for batch processing of the Patent XML files. Defaults (such as sgeadmin) are assumed.

We use StarCluster mostly for distributed jobs.
The StarCluster machines are configured to replicate the environment necessary to parse 

  1. `python fetch_xml.py` Fetching the XML files from USPTO. For now this fetches files specified in `urls.pickle`. As of 8/1/2013, `urls.pickle` contains files from 2005-mid 2013
  2.  Login to the starcluster using root
  3.  Point the `config.ini` file so it points to the MySQL database
  4.  `cd /home/sgeadmin/patentprocessor/starcluster; sh load_pre.sh > ../tar/[num].log` execute the shell script
  5.  Transfer the tar files to a separate location (or server) to begin the MySQL ingestion process.
  6. Execute `build_tsv.py` and specify the location of the `tar.gz` files. This builds several text files which can be later ingested.
  7. Modify `config.ini` file and set the proper credentials to the desired database. `from lib import alchemy` so the schema is fully updated.
  8. Log into mysql. If it is a remote server, such as on Amazon RDS, `mysql -u [user] -p --local-infile=1 -h [db] [tbl]` and execute `source load.sql`. The default database is assumed to be `uspto_new` so if this should be something else, please make the appropriate adjustments.
