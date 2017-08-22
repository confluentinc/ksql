#!/usr/bin/env bash

LOG_FILE=/tmp/ksql-connect.log
echo "PROCESSING UPLOAD "  > $LOG_FILE

echo "Loading Clickstream-Demo TABLES to Confluent-Connect => Elastic => Grafana datasource"
echo "Logging to:" $LOG_FILE

./elastic-dynamic-template.sh >> $LOG_FILE 2>&1




declare -a tables=('click_user_sessions_ts' 'user_ip_activity_ts' 'clickstream_status_codes_ts' 'enriched_error_codes_ts' 'errors_per_min_alert_ts' 'errors_per_min_ts' 'events_per_min_max_avg_ts' 'events_per_min_ts' 'pages_per_min_ts');
for i in "${tables[@]}"
do



    table_name=$i
    TABLE_NAME=`echo $table_name | tr '[a-z]' '[A-Z]'`

    echo "==================================================================" >> $LOG_FILE
    echo "Charting " $TABLE_NAME  >> $LOG_FILE
    echo "Charting " $TABLE_NAME


    ## Cleanup existing data

    # Elastic
    curl -X "DELETE" "http://localhost:9200/""$table_name" >> $LOG_FILE 2>&1

    # Connect
    curl -X "DELETE" "http://localhost:8083/connectors/es_sink_""$TABLE_NAME" >> $LOG_FILE 2>&1

#    # Grafana
    curl -X "DELETE" "http://localhost:3000/api/datasources/name/""$table_name"   --user admin:admin >> $LOG_FILE 2>&1

    # Wire in the new connection path
    echo "\n\nConnecting KSQL->Elastic->Grafana " "$table_name" >> $LOG_FILE 2>&1
    ./ksql-connect-es-grafana.sh "$table_name" >> $LOG_FILE 2>&1
done

echo "Navigate to http://localhost:3000/dashboard/db/click-stream-analysis"


# ========================
#   REST API Notes
# ========================
#
# Extract datasources from grafana
# curl -s "http://localhost:3000/api/datasources"  -u admin:admin|jq -c -M '.[]'
#
# Delete a Grafana DataSource
# curl -X "DELETE" "http://localhost:3000/api/datasources/name/

# List confluent connectors
# curl -X "GET" "http://localhost:8083/connectors"
#
# Delete a Confluent-Connector
# curl -X "DELETE" "http://localhost:8083/connectors/es_sink_PER_USER_KBYTES_TS"
#
# Delete an Elastic Index
# curl -X "DELETE" "http://localhost:9200/per_user_kbytes_ts"
#
