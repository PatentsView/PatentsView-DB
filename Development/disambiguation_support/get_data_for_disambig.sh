#!/bin/bash
CONF_FILE="$1"
OUTPUT_LOCATION="$2"


echo "SELECT uuid, patent_id, inventor_id, rawlocation_id, name_first, name_last, sequence from rawinventor limit 10"| mysql --defaults-file="$CONF_FILE"  --quick >"$OUTPUT_LOCATION/temp_raw.tsv"


# # mkdir -p $OUTPUT_LOCATION

# for table in rawinventor patent cpc_current ipcr nber rawassignee uspc_current rawlawyer rawlocation 
# do
#     echo "$table"
#     # Set the SELECT statement that will create the table
#     case "$table" in 
#         # Tables with custom select statements
#         ("rawinventor") selectStatement="SELECT uuid, patent_id, inventor_id, rawlocation_id, name_first, name_last, sequence from rawinventor" ;;
#         ("patent") selectStatement="SELECT id, type, number, country, date, abstract, title, kind, num_claims, filename from patent" ;;
#         ("rawlocation") selectStatement="SELECT select `id`,location_id_transformed as location_id,city,state,country_transformed as country from rawlocation" ;;
#         # Tables with standard select statements
#         (*) selectStatement="SELECT * FROM $table"
#     esac
    
#     echo "$selectStatement limit 10"
#     echo "$selectStatement limit 10" | mysql --defaults-file="$CONF_FILE" "$OUTPUT_LOCATION/$table.tsv"|| exit 1;

# #     echo "$selectStatement" | mysql --quick -u $USER -h $HOST -p \
# #                                     --database="$DB" >"$OUTPUT_LOCATION/$table.tsv" || exit 1;

# done


