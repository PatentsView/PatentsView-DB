import MySQLdb
import re,os

def merge_db(host,username,password,sourcedb,targetdb):
 
    dbnames = sourcedb.split(',')
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        charset='utf8',
        use_unicode=True)
    cursor = mydb.cursor()
    
    for d in dbnames:
        cursor.execute('SHOW TABLES FROM '+d)
        tables = [t[0] for t in cursor.fetchall()]
        for t in tables:
            cursor.execute('SELECT * from '+d+'.'+t)
            res = [f for f in cursor.fetchall()]
            for r in res:
                rr = [str(i) for i in r]
                try:
                    cursor.execute('INSERT INTO '+targetdb+'.'+t+' VALUES("'+'","'.join(rr).replace('"None"','NULL')+'")')
                except:
                    pass
            
        mydb.commit()