/*
    Merge together two disctinct MySQL tables
    mysqldump [options] uspto -T /var/lib/mysql/uspto
    mysql -u [user] -p --local-infile=1 -h [db] [tbl]
    
    READ THIS: http://dev.mysql.com/doc/refman/5.5/en/optimizing-innodb-bulk-data-loading.html
*/


SELECT "new base", NOW();
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';
SET innodb_lock_wait_timeout = 500;
SET autocommit=0; 

SELECT "patent";
LOAD DATA LOCAL INFILE 'new/patent.txt' INTO TABLE uspto_new.patent FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "rawlocation";
LOAD DATA LOCAL INFILE 'new/rawlocation.txt' IGNORE INTO TABLE uspto_new.rawlocation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "subclass";
LOAD DATA LOCAL INFILE 'new/subclass.txt' IGNORE INTO TABLE uspto_new.subclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "mainclass";
LOAD DATA LOCAL INFILE 'new/mainclass.txt' IGNORE INTO TABLE uspto_new.mainclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "application";
LOAD DATA LOCAL INFILE 'new/application.txt' INTO TABLE uspto_new.application FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "rawassignee";
LOAD DATA LOCAL INFILE 'new/rawassignee.txt' INTO TABLE uspto_new.rawassignee FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "rawinventor";
LOAD DATA LOCAL INFILE 'new/rawinventor.txt' INTO TABLE uspto_new.rawinventor FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "ipcr";
LOAD DATA LOCAL INFILE 'new/ipcr.txt' INTO TABLE uspto_new.ipcr FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "rawlawyer";
LOAD DATA LOCAL INFILE 'new/rawlawyer.txt' INTO TABLE uspto_new.rawlawyer FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "usreldoc";
LOAD DATA LOCAL INFILE 'new/usreldoc.txt' INTO TABLE uspto_new.usreldoc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "uspc";
LOAD DATA LOCAL INFILE 'new/uspc.txt' INTO TABLE uspto_new.uspc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

COMMIT;
SET autocommit=1; 
SET innodb_lock_wait_timeout = 50;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';
SELECT NOW();

/* ------------------------------- */

SELECT "new citatons", NOW();
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';
SET innodb_lock_wait_timeout = 500;
SET autocommit=0; 

SELECT "foreigncitation";
LOAD DATA LOCAL INFILE 'new/foreigncitation.txt' INTO TABLE uspto_new.foreigncitation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "otherreference";
LOAD DATA LOCAL INFILE 'new/otherreference.txt' INTO TABLE uspto_new.otherreference FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "usapplicationcitation";
LOAD DATA LOCAL INFILE 'new/usapplicationcitation.txt' INTO TABLE uspto_new.usapplicationcitation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
SELECT "uspatentcitation";
LOAD DATA LOCAL INFILE 'new/uspatentcitation.txt' INTO TABLE uspto_new.uspatentcitation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

COMMIT;
SET autocommit=1; 
SET innodb_lock_wait_timeout = 50;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';
SELECT NOW();

/* ------------------------------- */

SELECT "new claims", NOW();
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';
SET innodb_lock_wait_timeout = 500;
SET autocommit=0; 

SELECT "citation";
LOAD DATA LOCAL INFILE 'new/claim.txt' INTO TABLE uspto_new.claim FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

COMMIT;
SET autocommit=1; 
SET innodb_lock_wait_timeout = 50;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';
SELECT NOW();
