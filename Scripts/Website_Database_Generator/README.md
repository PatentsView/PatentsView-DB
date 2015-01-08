# PatentsView III Website Database Generator

## Requirements

* Developed and tested under Python 3.4 and MySQL 5.6.2
* MySQL Python Connector 2.0.2 (https://dev.mysql.com/downloads/connector/python/)

## Steps

**WARNING:** `generate_database.sql` drops the existing database (`PatentsView_20141215_dev` by default).

* Edit `generate_database.sql` and `add_full_text_indexes.sql` and replace `patent_20141215` with the name of the source database and `PatentsView_20141215_dev` with the name of the destination database to be created.  In the future, we might want to simplify this by working in the "current" database, but during development it was safer to retain explicit database references.

* Run `generate_database.sql`.  This will take a long time depending on server load and configuration.  The run that completed just prior to the creation of this README took approximately 10 hours.

* Run the UnencodeHTMLEntities Python script against the patent.title column.
```
python main.py <hostname> 3306 <username> <password> PatentsView_20141215_dev patent title patent_id
```
* Run the UnencodeHTMLEntities Python script against the patent.abstract column.
```
python main.py <hostname> 3306 <username> <password> PatentsView_20141215_dev patent abstract patent_id
```
* Run `add_full_text_indexes.sql`.  The most recent run took about 3 hours to complete.
