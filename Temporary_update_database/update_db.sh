
echo "---------------Parsing the New Data-------------------------"
# python run_parsers.py
echo "----------------Copying Persistent Files into Govt Int Working Dir -------"
tail -n +2 -q 2017_Oct/processed_gov/*.csv >> 2017_Oct/processed_gov/merged_csvs.csv
cp PersistentFiles/omitLocs.csv 2017_Oct/processed_gov/omitLocs.csv
echo " -------------Running the perl script for GI ----------------------"
perl Code/PatentsView-DB/Scripts/Government_Interest/govtinterest_v1.0.pl

echo "---------------Copying the old database--------------------"
echo "Need to update this to read from config file & add mysql to the Cygwin command prompt"
# mysqldump -u skelley -p actual_password -h pv3-ingestmysql.cckzcdkkfzqo.us-east-1.rds.amazonaws.com patent_20170808 | mysql -u skelley -p actual_password -h pv3-ingestmysql.cckzcdkkfzqo.us-east-1.rds.amazonaws.com patent_20171003
echo "-------------Merging Databases and Performing Post Processing-------------"
# python postprocess.py
echo "-----------Doing Assignee and Lawyer Disambiguation -----------"
# need to update path to entitiy files in assignee disambig __init__.py"
# cd Code/PatentsView-DB/Assignee_Lawyer_Disambiguation/
# python lib/assignee_disambiguation.py
# python lib/lawyer_disambiguation.py grant

echo "--- Location Disambiguation -------"
python/Code/PatentsView-DB/Location_Disambiguation/get_locs.py