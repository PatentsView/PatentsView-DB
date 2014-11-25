import MySQLdb
import re,os

def merge_db_pats(host,username,password,sourcedb,targetdb):
 
    dbnames = sourcedb.split(',')
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        charset='utf8',
        use_unicode=True)
    
    cursor = mydb.cursor()
    
    primaries = ['application','mainclass','subclass','mainclass_current','subclass_current']
    for d in dbnames:
        cursor.execute('SET FOREIGN_KEY_CHECKS=0')
        cursor.execute('SHOW TABLES FROM `'+d+'`')
        tables = [t[0] for t in cursor.fetchall()]
        for t in tables:
            print d,t
            if t in primaries:
                cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * FROM `'+d+'`.'+t+' WHERE `'+d+'`.'+t+'.id NOT IN (SELECT id FROM `'+targetdb+'`.'+t+')')
            else:
                cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * from `'+d+'`.'+t)
            mydb.commit()
        cursor.execute('set foreign_key_checks=1')
