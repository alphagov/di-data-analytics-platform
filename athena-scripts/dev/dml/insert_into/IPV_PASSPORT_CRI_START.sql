INSERT INTO "dev-txma-stage"."ipv_passport" (
	event_id,
	client_id,
	component_id,
	user_govuk_signin_journey_id,
	user_user_id,
	timestamp,
	timestamp_formatted,
	extensions_evidence,
	extensions_iss,
	year,
	month,
	day,
	processed_date,
	event_name
)
SELECT
	event_id as event_id,
	'' as client_id,
	component_id as component_id,
	user_govuk_signin_journey_id as user_govuk_signin_journey_id,
	'' as user_user_id,
	timestamp as timestamp,
	timestamp_formatted as timestamp_formatted,
	'' as extensions_evidence,
	'' as extensions_iss,
	CAST(year as INT) as year,
	CAST(month as INT) as month,
	CAST(day as INT) as day,
	CAST(date_format(now(), '%Y%m%d') as INT) AS processed_date,
	event_name as event_name
FROM 
	"dev-txma-raw"."ipv_passport_cri_start";