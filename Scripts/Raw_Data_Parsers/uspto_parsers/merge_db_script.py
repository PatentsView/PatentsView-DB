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
    
    primaries = ['mainclass','subclass','mainclass_current','subclass_current']
    for d in dbnames:
        cursor.execute('SET FOREIGN_KEY_CHECKS=0')
        cursor.execute('SHOW TABLES FROM `'+d+'`')
        tables = [t[0] for t in cursor.fetchall()]
        for t in tables:
            print d,t
            if t in primaries:
                try:
                    cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * FROM `'+d+'`.'+t+' WHERE `'+d+'`.'+t+'.id NOT IN (SELECT id FROM `'+targetdb+'`.'+t+')')
                except:
                    try:
                        cursor.execute('CREATE TABLE '+targetdb+'.'+t+' LIKE '+d+'.'+t)
                        mydb.commit()
                        cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * FROM `'+d+'`.'+t+' WHERE `'+d+'`.'+t+'.id NOT IN (SELECT id FROM `'+targetdb+'`.'+t+')')
                    except:
                        cursor.execute('SHOW COLUMNS FROM '+targetdb+'.'+t)
                        col1 = [(f[0],f[1]) for f in cursor.fetchall()]
                        cursor.execute('SHOW COLUMNS FROM '+d+'.'+t)
                        col2 = [(f[0],f[1]) for f in cursor.fetchall()]
                        if len(col1) > len(col2):
                            colstoadd = set.difference(set(col1),set(col2))
                        else:
                            colstoadd = set.difference(set(col2),set(col1))
                        for col in list(colstoadd):
                            try:
                                cursor.execute('ALTER TABLE '+targetdb+'.'+t+' ADD COLUMN '+col[0]+' '+col[1])
                                mydb.commit()
                            except:
                                pass
                        cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * FROM `'+d+'`.'+t+' WHERE `'+d+'`.'+t+'.id NOT IN (SELECT id FROM `'+targetdb+'`.'+t+')')
                        
                        
            else:
                try:
                    cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * from `'+d+'`.'+t)
                except:
                    try:
                        cursor.execute('CREATE TABLE '+targetdb+'.'+t+' LIKE '+d+'.'+t)
                        mydb.commit()
                        cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * from `'+d+'`.'+t)
                    except:
                        cursor.execute('SHOW COLUMNS FROM '+targetdb+'.'+t)
                        col1 = [(f[0],f[1]) for f in cursor.fetchall()]
                        cursor.execute('SHOW COLUMNS FROM '+d+'.'+t)
                        col2 = [(f[0],f[1]) for f in cursor.fetchall()]
                        if len(col1) > len(col2):
                            colstoadd = set.difference(set(col1),set(col2))
                        else:
                            colstoadd = set.difference(set(col2),set(col1))
                        for col in list(colstoadd):
                            try:
                                cursor.execute('ALTER TABLE '+targetdb+'.'+t+' ADD COLUMN '+col[0]+' '+col[1])
                                mydb.commit()
                            except:
                                pass
                        cursor.execute('INSERT INTO '+targetdb+'.'+t+' SELECT * from `'+d+'`.'+t)
            mydb.commit()
        cursor.execute('set foreign_key_checks=1')
