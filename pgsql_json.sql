-- Name: pgsql.pgsql_json
SELECT *
FROM short_view_for_zabbix
WHERE rounded_timestamp>=  date_round(NOW() - INTERVAL '5 minutes', INTERVAL '1 minute')
;

