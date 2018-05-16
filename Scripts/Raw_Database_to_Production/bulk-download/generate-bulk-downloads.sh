#!/bin/bash
TABLE_FILE="$1"
DB="$2"
HOST="$3"
config="$4"


[[ $# -lt 3 ]] && echo "Usage: $(basename $0) <TABLE_FILE> <DATABASE> <HOST> (MYSQL_CONFIG_SUFFIX)" && exit 1

size=${#config}
configoption="--defaults-group-suffix=${config}"
if [ $size -le 0 ]; then
    echo -n "DB User: "
    read USER
    echo -n "DB Password: "
    read -s PASSWORD
    configoption="--user=$USER \
                  --password=$PASSWORD"
fi




# Option to debug--used internally
DEBUG="Yes"



numTables=$(wc -l < $TABLE_FILE)

while read table; do 

    # Set the SELECT statement that will create the table
    case "$table" in 
        # Tables with custom select statements
        ("application") selectStatement="SELECT id_transformed as id, patent_id, series_code_transformed_from_type as series_code, number_transformed as number, country, date from application" ;;
        ("usapplicationcitation") selectStatement="SELECT uuid, patent_id, application_id_transformed as application_id, date, name, kind, number_transformed as number, country, category, sequence from usapplicationcitation" ;;
        ("location_assignee") selectStatement="SELECT distinct rl.location_id, ri.assignee_id from rawassignee ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.assignee_id is not NULL and rl.location_id is not NULL" ;;
        ("location_inventor") selectStatement="SELECT rl.location_id, ri.inventor_id from rawinventor ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.inventor_id is not NULL and rl.location_id is not NULL" ;;
        ("rawlawyer") selectStatement="SELECT uuid, lawyer_id, patent_id, name_first, name_last, organization, country, sequence FROM rawlawyer" ;;
        ("rawlocation") selectStatement="SELECT id, location_id, city, state, country_transformed as country, location_id_transformed as latlong from rawlocation" ;;
        # Tables with standard select statements
        (*) selectStatement="SELECT * FROM $table"
    esac

    # To debug, limit tables to a single row.
    if [[ "$DEBUG" == "Yes" ]]; then
        selectStatement="$selectStatement LIMIT 1"
    fi

    # Handle the detail_desc_text table differently: we'll split it into 5 tables
    if [[ $table = 'detail_desc_text' ]]; then

        # Count how many rows are in detail_desc_text
        rows=$(mysql -s $configoption\
                        --host="$HOST" \
                        --database="$DB" \
                        --execute="SELECT count(*) FROM detail_desc_text; ")

        # Split the table into 5 tables using 5 SELECT statements with LIMIT and OFFSET parameters
        if [ "$rows" -ge 0 ]; then
            rowsPerTable=$(( $rows/5 + 5 ))
            for i in $(seq 1 5); do

                # To debug, limit tables to a single row.
                if [[ "$DEBUG" == "Yes" ]]; then
                    selectStatement="SELECT * FROM detail_desc_text LIMIT 1 OFFSET 0 "
                else
                    offset=$(( i*rowsPerTable - rowsPerTable ))
                    selectStatement="SELECT * FROM detail_desc_text LIMIT $rowsPerTable OFFSET $offset "
                fi

                echo "$selectStatement" | \
                    mysql "$configoption" \
                            --quick \
                          --host="$HOST" \
                          --database="$DB" > "${table}_${i}.tsv";
                zip -rm ${table}_${i}.tsv.zip ${table}_${i}.tsv
                echo "${table}_${i}.tsv.zip generated using: $selectStatement"
            done
        else
            echo "unable to determine # rows to generate detail_desc_text zip files"
        fi
    else
        echo "$selectStatement" | mysql "$configoption" \
                                        --quick \
                                        --host="$HOST" \
                                        --database="$DB" > "$table.tsv"; 
        zip -rm $table.tsv.zip $table.tsv 
        echo "$table.tsv.zip generated using: $selectStatement"
    fi

    # Output a status message
    ((tablesCompleted++))
    echo "(" $tablesCompleted " of " $numTables " tables downloaded)" 
done < $TABLE_FILE

