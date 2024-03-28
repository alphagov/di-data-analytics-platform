INSERT INTO "environment-txma-stage"."auth_account_management" (
	event_id,
	component_id,
	user_user_id,
	timestamp,
	timestamp_formatted,
	event_timestamp_ms,
	event_timestamp_ms_formatted,
	extensions_notifyreference,
	extensions_zendeskticketnumber,
	extensions_suspiciousactivities,
	year,
	month,
	day,
	processed_date,
	event_name
)
SELECT
	event_id as event_id,
	component_id as component_id,
	user.user_id as user_user_id,
	timestamp as timestamp,
	timestamp_formatted as timestamp_formatted,
	event_timestamp_ms as event_timestamp_ms,
	event_timestamp_ms_formatted as event_timestamp_ms_formatted,
	case format('%s',cast("extensions"."notify_reference" as VARCHAR))
		when 'null' then null
	    else format('%s',cast("extensions"."notify_reference" as VARCHAR))
	end as extensions_notifyreference,
	case format('%s',cast("extensions"."zendesk_ticket_number" as VARCHAR))
		when 'null' then null
	    else format('%s',cast("extensions"."zendesk_ticket_number" as VARCHAR))
	end as extensions_zendeskticketnumber,
	case format('%s',cast("extensions"."suspicious_activities" as JSON))
		when 'null' then null
	    else format('%s',cast("extensions"."suspicious_activities" as JSON))
	end as 	extensions_suspiciousactivities,
	CAST(year as INT) as year,
	CAST(month as INT) as month,
	CAST(day as INT) as day,
	CAST(date_format(now(), '%Y%m%d') as VARCHAR) AS processed_date,
	event_name as event_name
FROM 
	"environment-txma-raw"."home_report_suspicious_activity"
WHERE
	CAST(concat(year, month, day) AS INT) > filter_value AND
	CAST(concat(year, month, day) AS INT) < CAST(date_format(now(), '%Y%m%d') as INT)
;