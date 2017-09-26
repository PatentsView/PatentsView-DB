#cd /home/sgeadmin/patentprocessor/starcluster; sh load_pre.sh > ../tar/2.log

cd /mnt/sgeadmin
for i in `ls *.xml`
    do echo $i
    cd /var/lib/mysql/uspto
    echo " - remove txt"
    rm *.txt

    cd /home/sgeadmin/patentprocessor
    echo " - drop database"
    mysql -root uspto < starcluster/load_drop.sql
    mysql -root uspto < starcluster/load_drop.sql

    cd /home/sgeadmin/patentprocessor
    echo " - python"
    python parse.py -p /mnt/sgeadmin -x $i
    echo " - mysqldump"
    mysqldump -root uspto -T /var/lib/mysql/uspto

    echo " - duplicate"
    cd /var/lib/mysql/uspto
    tar -czf $i.tar.gz *.txt
    mv $i.tar.gz /home/sgeadmin/patentprocessor/tar

done
