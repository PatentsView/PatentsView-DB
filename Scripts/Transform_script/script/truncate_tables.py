import MySQLdb

def clean(host, user, password, db):
    mydb = MySQLdb.connect(host= host,
    user=user,
    passwd=password, db=db)
    cursor = mydb.cursor()
    tables = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 'lawyer', 'location_assignee', 'location_inventor', 'patent_assignee', 'patent_inventor', 'patent_lawyer', 'wipo'] 
    for table in tables:
    	cursor.execute("truncate table " + table + ";")
    	mydb.commit()
