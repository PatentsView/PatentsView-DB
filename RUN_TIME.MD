#Run Time Instructions

This is a bit complicated for now, will streamline in the future

###Steps:
1) THIS CHANGES THE OLD DATABASE IN PLACE. So don't start it unless there is a current backup. Make sure that the isn't a database with the name of the temporary_upload database or the new database either.
2) For now, pull from 'update-db-testing' branch (will be merged into develop after it runs smoothly). 
3) You also need to make a mysql config file to run steps that pull directly from mysql. I'm going to figure out an improvement to this goign forward. Copy the Development/disambiguation_support/mysql_conf_template.conf file. 
3) You need to get the previous qa results and upload them into whatever folder you named in your OLD_QA_LOC spot. 
4) The airflow process doesn't start until manually triggered (this is so it doesn't restart if you have to rebuild and start again), so go to the web UI and start it. 
5) You are expecting gi_post_manual to fail because it relies on having uploaded the manually checked csv files. In Development/persistent_files/manual_lookup_instructions.txt there are instructions for how to do the look ups and what the files need to be called. After postprocess_NER finishes, use these instruction to create the lookups (make sure the files are named as described, have columns as described and are csv not excel files) then clear gi_post_manual to run it. 
6) You also need to get a private key added for the disambiguation server and give Sarah your IP so I can add it. This won't matter until you getto the upload_disambig_files step. There is also a wierd thing you need to do with logging into it from your docker container to add it to approved hosts. 
