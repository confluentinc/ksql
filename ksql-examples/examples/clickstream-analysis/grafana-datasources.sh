#!/bin/bash

# Note: extract datasources from grafana
# curl -s "http://localhost:3000/api/datasources"  -u admin:admin|jq -c -M '.[]'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":10,"orgId":1,"name":"click_user_sessions_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"click_user_sessions_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":7,"orgId":1,"name":"clickstream_status_codes_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"clickstream_status_codes_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":8,"orgId":1,"name":"enriched_error_codes_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"enriched_error_codes_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":9,"orgId":1,"name":"errors_per_min_alert_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"errors_per_min_alert_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":6,"orgId":1,"name":"errors_per_min_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"errors_per_min_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":2,"orgId":1,"name":"events_per_min_max_avg_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"events_per_min_max_avg_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":3,"orgId":1,"name":"events_per_min_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"events_per_min_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'

curl -X "POST" "http://localhost:3000/api/datasources" \
	    -H "Content-Type: application/json" \
	     --user admin:admin \
	     -d $'{"id":4,"orgId":1,"name":"pages_per_min_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"pages_per_min_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'


#curl -X "POST" "http://localhost:3000/api/datasources" \
#	    -H "Content-Type: application/json" \
#	     --user admin:admin \
#	     -d $'{"id":4,"orgId":1,"name":"per_user_kbytes_ts","type":"elasticsearch","typeLogoUrl":"public/app/plugins/datasource/elasticsearch/img/elasticsearch.svg","access":"proxy","url":"http://localhost:9200","password":"","user":"","database":"per_user_kbytes_ts","basicAuth":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"}}'
#
#





