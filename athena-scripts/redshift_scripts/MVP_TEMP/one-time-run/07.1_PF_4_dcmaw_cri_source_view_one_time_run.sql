--Product Family 4 -dcmaw_cri


—-source view

Create or replace view dev_conformed.v_stg_dcmaw_cri
AS
select DISTINCT 
Auth.product_family,
Auth.event_id,
Auth.client_id,
Auth.component_id,
Auth.user_govuk_signin_journey_id,
Auth.user_user_id,
Auth.timestamp,
Auth.timestamp_formatted,
null extensions_clientname,
Auth.processed_date,
Auth.event_name,
1 EVENT_COUNT,
Null REJECTION_REASON,
Null REASON,
Null NOTIFICATION_TYPE,
Null MFA_TYPE,
Null ACCOUNT_RECOVERY,
failedcheckdetails_biometricverificationprocesslevel FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
checkdetails_biometricverificationprocesslevel CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
strengthscore strength_score,
Null ADDRESSES_ENTERED,
activityhistoryscore ACTIVITY_HISTORY_SCORE,
Null IDENTITY_FRAUD_SCORE,
Null DECISION_SCORE,
Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
failedcheckdetails_checkmethod FAILED_CHECK_DETAILS_CHECK_METHOD,
Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
Null CHECK_DETAILS_KBV_QUALITY,
Null VERIFICATION_SCORE,
checkdetails_checkmethod CHECK_DETAILS_CHECK_METHOD,
Null Iss,
validityscore VALIDITY_SCORE,
type "TYPE",
BatC.product_family batch_product_family,
BatC.maxrundate,
ref.product_family ref_product_family,
ref.domain,
ref.sub_domain,
ref.other_sub_domain from 
( select * from 
    (SELECT
           'dcmaw_cri' Product_family 
            ,row_number() over (partition by event_id,timestamp_formatted order by cast (day as integer) desc) as row_num
            ,*
    FROM
                (with base_data as
            (SELECT
                event_id,
                event_name,
                client_id,
                component_id,
                "timestamp",
                timestamp_formatted,
                user_govuk_signin_journey_id,
                user_user_id,
                year,
                month,
                day,
                processed_date,
                extensions_evidence,
                nvl2(valid_json_data,valid_json_data.activityHistoryScore ,valid_json_data) AS activityhistoryscore,
                nvl2(valid_json_data,valid_json_data.checkdetails,valid_json_data) AS checkdetails,
                nvl2(valid_json_data,valid_json_data.failedcheckdetails,valid_json_data) AS failedcheckdetails,
                nvl2(valid_json_data,valid_json_data.strengthscore,valid_json_data) AS strengthscore,
                nvl2(valid_json_data,valid_json_data.type,valid_json_data) AS type,
                nvl2(valid_json_data,valid_json_data.validityscore,valid_json_data) AS validityscore    
                FROM (
                    SELECT
                        event_id,
                        event_name,
                        client_id,
                        component_id,
                        "timestamp",
                        timestamp_formatted,
                        user_govuk_signin_journey_id,
                        user_user_id,
                        year,
                        month,
                        day,
                        processed_date,
                        extensions_evidence,
                        case extensions_evidence != ''
                        and is_valid_json_array(extensions_evidence)
                        when true then json_parse(
                            json_extract_array_element_text(extensions_evidence, 0)
                        )
                        else null end as valid_json_data
                    FROM
                        "dev-redshift"."dev_txma_stage"."dcmaw_cri"
                        --where extensions_evidence != ''
                        --and event_id='f6eb0bef-98dc-4a71-ac33-d6bc1725f11d'
                )), level_1_data as
            (SELECT
                        event_id,
                        event_name,
                        client_id,
                        component_id,
                        "timestamp",
                        timestamp_formatted,
                        user_govuk_signin_journey_id,
                        user_user_id,
                        year,
                        month,
                        day,
                        processed_date,
                        activityhistoryscore,
                        strengthscore,
                        json_serialize(checkdetails) checkdetails_final,
                        json_serialize(failedcheckdetails) failedcheckdetails_final,
                        type,
                        validityscore
                    FROM
                        base_data
                        where json_serialize(failedcheckdetails) != ''
            ),level_2_data as
            (select 
                    event_id,
                    event_name,
                    client_id,
                    component_id,
                    "timestamp",
                    timestamp_formatted,
                    user_govuk_signin_journey_id,
                    user_user_id,
                    year,
                    month,
                    day,
                    processed_date,
                    activityhistoryscore,
                    strengthscore,
                    type,
                    validityscore,
                    case failedcheckdetails_final != ''
                        and is_valid_json_array(failedcheckdetails_final)
                        when true then json_parse(
                            json_extract_array_element_text(failedcheckdetails_final, 0)
                        )
                    else null end as valid_json_failedcheckdetails_data,
                    case checkdetails_final != ''
                        and is_valid_json_array(checkdetails_final)
                        when true then json_parse(
                            json_extract_array_element_text(checkdetails_final, 0)
                        )
                    else null end as valid_json_checkdetails_data  
            from level_1_data
            )
            select 
                event_id,
                event_name,
                client_id,
                component_id,
                "timestamp",
                timestamp_formatted,
                user_govuk_signin_journey_id,
                user_user_id,
                year,
                month,
                day,
                processed_date,
                activityhistoryscore,
                strengthscore,
                type,
                validityscore,
                nvl2(valid_json_failedcheckdetails_data,valid_json_failedcheckdetails_data.checkmethod,valid_json_failedcheckdetails_data) AS failedcheckdetails_checkmethod,
                nvl2(valid_json_failedcheckdetails_data,valid_json_failedcheckdetails_data.biometricverificationprocesslevel,valid_json_failedcheckdetails_data) AS        
                failedcheckdetails_biometricverificationprocesslevel,
                nvl2(valid_json_checkdetails_data,valid_json_checkdetails_data.checkmethod,valid_json_checkdetails_data) AS checkdetails_checkmethod,
                nvl2(valid_json_checkdetails_data,valid_json_checkdetails_data.biometricverificationprocesslevel,valid_json_checkdetails_data) AS      
                checkdetails_biometricverificationprocesslevel
            from  level_2_data 
    ) 
    )
    where  row_num=1  
    ) Auth
    join dev_conformed.BatchControl BatC
    On Auth.Product_family=BatC.Product_family
    and to_date(processed_date,'YYYYMMDD')  > NVL(MaxRunDate,null)
    join dev_conformed.REF_EVENTS ref
    on Auth.EVENT_NAME=ref.event_name
    with no schema binding;

 