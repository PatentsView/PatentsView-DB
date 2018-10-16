#!/bin/bash
#need to add this bit to rebuild after new pull
#ssh -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com "mvn clean package" &&
#echo "built" &&
#ssh -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; nohup sh scripts/db/start_mongo_server_aws.sh > mongo_start.out 2> mongo_start.err < /dev/null" &
#wait 30s to get database started
#sleep 30 &&
#echo "Starting disambiguation" &&
#ssh -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; bash run_inventor.sh" &&
#echo "Done inventor" &&
ssh -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; bash run_assignee.sh" &&
echo "Done assignee" &&
ssh -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; bash run_location.sh" &&
echo "Done location!"
