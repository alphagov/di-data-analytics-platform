CREATE OR REPLACE PROCEDURE conformed.sp_conformed_stage_view_data_objects() 
AS $$ 
BEGIN 
/*
Name       Date         Notes
P Sodhi    15/09/2023   Update to ipv_cri_kbv view.
*/

    Create or replace view conformed.V_STG_auth_account_creation
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
    Auth.processed_date,
    Auth.event_name,
    1 EVENT_COUNT,
    Null REJECTION_REASON,
    Null REASON,
    Null NOTIFICATION_TYPE,
    Null MFA_TYPE,
    Null ACCOUNT_RECOVERY,
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    Null Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain from 
    ( select * from 
        (SELECT
        'auth_account_creation'Product_family 
        ,row_number() over (partition by event_id,timestamp_formatted order by cast (day as integer) desc) as row_num,*
    FROM
        "dap_txma_reporting_db"."dap_txma_stage"."auth_account_creation") 
        where  row_num=1 ) Auth
        join conformed.BatchControl BatC
        On Auth.Product_family=BatC.Product_family
        and to_date(processed_date,'YYYYMMDD')  > NVL(MaxRunDate,null)
        join conformed.REF_EVENTS ref
        on Auth.EVENT_NAME=ref.event_name
        with no schema binding;

    ---

    Create or replace view conformed.v_stg_auth_orchestration
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
    Auth.extensions_clientname,
    Auth.processed_date,
    Auth.event_name,
    1 EVENT_COUNT,
    Null REJECTION_REASON,
    Null REASON,
    Null NOTIFICATION_TYPE,
    Null MFA_TYPE,
    Null ACCOUNT_RECOVERY,
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    Null Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain from 
    ( select * from 
        (SELECT
            'auth_orchestration'Product_family 
                ,row_number() over (partition by event_id,timestamp_formatted order by cast (day as integer) desc) as row_num,*
        FROM
        "dap_txma_reporting_db"."dap_txma_stage"."auth_orchestration") 
        where  row_num=1  
        ) Auth
        join conformed.BatchControl BatC
        On Auth.Product_family=BatC.Product_family
        and to_date(processed_date,'YYYYMMDD')  > NVL(MaxRunDate,null)
        join conformed.REF_EVENTS ref
        on Auth.EVENT_NAME=ref.event_name
        with no schema binding;

    ---

    Create
or replace view conformed.v_stg_auth_account_user_login AS
select
    DISTINCT Auth.product_family,
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
    extensions_isnewaccount is_new_account,
    Null REJECTION_REASON,
    Null REASON,
    Null NOTIFICATION_TYPE,
    Null MFA_TYPE,
    Null ACCOUNT_RECOVERY,
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    Null Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    extensions_testuser test_user,
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain
from
    (
        select
            *
        from
            (
                SELECT
                    'auth_account_user_login' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    "dap_txma_reporting_db"."dap_txma_stage"."auth_account_user_login"
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    ---

Create
/*
Name       Date         Notes
P Sodhi    08/12/2023   Added processed date to row_number logic to avoid issue with migrated data.
*/
or replace view conformed.v_stg_dcmaw_cri AS
select
    DISTINCT Auth.product_family,
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
    extensions_previousgovuksigninjourneyid,
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
    ref.other_sub_domain
from
    (
        select
            *
        from
            (
                SELECT
                    'dcmaw_cri' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    (
                        with base_data as (
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
                                extensions_previousgovuksigninjourneyid,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.activityHistoryScore,
                                    valid_json_data
                                ) AS activityhistoryscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.checkdetails,
                                    valid_json_data
                                ) AS checkdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.failedcheckdetails,
                                    valid_json_data
                                ) AS failedcheckdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.strengthscore,
                                    valid_json_data
                                ) AS strengthscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.validityscore,
                                    valid_json_data
                                ) AS validityscore
                            FROM
                                (
                                    SELECT
                                        event_id,
                                        event_name,
                                        client_id,
                                        component_id,
                                        "timestamp",
                                        timestamp_formatted,
                                        user_govuk_signin_journey_id,
                                        FIRST_VALUE(extensions_previousgovuksigninjourneyid) OVER (
                                            PARTITION BY event_id
                                            ORDER BY
                                                event_id ROWS UNBOUNDED PRECEDING
                                        ) AS extensions_previousgovuksigninjourneyid,
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
                                        "dap_txma_reporting_db"."dap_txma_stage"."dcmaw_cri"
                                )
                        ),
                        level_1_data as (
                            SELECT
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                extensions_previousgovuksigninjourneyid,
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
                            where
                                json_serialize(failedcheckdetails) != ''
                        ),
                        level_2_data as (
                            select
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                extensions_previousgovuksigninjourneyid,
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
                            from
                                level_1_data
                        )
                        select
                            event_id,
                            event_name,
                            client_id,
                            component_id,
                            "timestamp",
                            timestamp_formatted,
                            user_govuk_signin_journey_id,
                            extensions_previousgovuksigninjourneyid,
                            user_user_id,
                            year,
                            month,
                            day,
                            processed_date,
                            activityhistoryscore,
                            strengthscore,
                            type,
                            validityscore,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.checkmethod,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_checkmethod,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.biometricverificationprocesslevel,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_biometricverificationprocesslevel,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.checkmethod,
                                valid_json_checkdetails_data
                            ) AS checkdetails_checkmethod,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.biometricverificationprocesslevel,
                                valid_json_checkdetails_data
                            ) AS checkdetails_biometricverificationprocesslevel
                        from
                            level_2_data
                    )
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    ---

    Create or replace view conformed.v_stg_auth_account_mfa
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
    extensions_notificationtype NOTIFICATION_TYPE,
    extensions_mfatype MFA_TYPE,
    extensions_accountrecovery ACCOUNT_RECOVERY,
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    Null Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain from 
    ( select * from 
        (SELECT
            'auth_account_mfa' Product_family 
                ,row_number() over (partition by event_id,timestamp_formatted order by cast (day as integer) desc) as row_num,*
        FROM
        "dap_txma_reporting_db"."dap_txma_stage"."auth_account_mfa") 
        where  row_num=1  
        ) Auth
        join conformed.BatchControl BatC
        On Auth.Product_family=BatC.Product_family
        and to_date(processed_date,'YYYYMMDD')  > NVL(MaxRunDate,null)
        join conformed.REF_EVENTS ref
        on Auth.EVENT_NAME=ref.event_name
        with no schema binding;

    ---

Create or replace view conformed.v_stg_auth_account_management AS
select
    DISTINCT Auth.product_family,
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
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    Null Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain,
                            nvl2(
                                    valid_json_data,
                                    valid_json_data.client_id,
                                    valid_json_data
                                ) AS sus_activity_client_id,
                                 nvl2(
                                    valid_json_data,
                                    valid_json_data.event_id,
                                    valid_json_data
                                ) AS sus_activity_event_id,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.event_type,
                                    valid_json_data
                                ) AS sus_activity_event_type,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.timestamp,
                                    valid_json_data
                                ) AS sus_activity_timestamp,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.session_id,
                                    valid_json_data
                                ) AS sus_activity_session_id
                                ,user_sessionid
                                ,event_timestamp_ms
                                ,event_timestamp_ms_formatted
                                ,extensions_notifyreference
                                ,extensions_zendeskticketnumber
from
    (
        select case extensions_suspiciousactivities != ''
                                        and is_valid_json_array(extensions_suspiciousactivities)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_suspiciousactivities, 0)
                                        )
                                        else null end as valid_json_data,
            *
        from
            (
                SELECT
                    'auth_account_management' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by processed_date desc,
                            cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    "dap_txma_reporting_db"."dap_txma_stage"."auth_account_management"
                --Where event_id='ec54faf9-67a9-418a-b7c7-afe8d9d7cb69'    
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    ---

    Create or replace view conformed.v_stg_ipv_cri_address
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
    Null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    extensions_addressesentered ADDRESSES_ENTERED,
    Null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    Null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    Null CHECK_DETAILS_CHECK_METHOD,
    extensions_iss Iss,
    Null VALIDITY_SCORE,
    Null "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain from 
    ( select * from 
        (SELECT
            'ipv_cri_address' Product_family 
                ,row_number() over (partition by event_id,timestamp_formatted order by cast (day as integer) desc) as row_num,*
        FROM
        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_address") 
        where  row_num=1  
        ) Auth
        join conformed.BatchControl BatC
        On Auth.Product_family=BatC.Product_family
        and to_date(processed_date,'YYYYMMDD')  > NVL(MaxRunDate,null)
        join conformed.REF_EVENTS ref
        on Auth.EVENT_NAME=ref.event_name
        with no schema binding;

    ---

    Create
/*
Name       Date         Notes
P Sodhi    08/12/2023   Added processed date to row_number logic to avoid issue with migrated data.
*/
or replace view conformed.v_stg_ipv_cri_driving_license AS
select
    DISTINCT Auth.product_family,
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
    extensions_iss Iss,
    validityscore VALIDITY_SCORE,
    type "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain
from
    (
        select
            *
        from
            (
                SELECT
                    'ipv_cri_driving_license' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    (
                        with base_data as (
                            SELECT
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                extensions_iss,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                user_user_id,
                                year,
                                month,
                                day,
                                processed_date,
                                extensions_evidence,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.activityHistoryScore,
                                    valid_json_data
                                ) AS activityhistoryscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.checkdetails,
                                    valid_json_data
                                ) AS checkdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.failedcheckdetails,
                                    valid_json_data
                                ) AS failedcheckdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.strengthscore,
                                    valid_json_data
                                ) AS strengthscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.validityscore,
                                    valid_json_data
                                ) AS validityscore
                            FROM
                                (
                                    SELECT
                                        event_id,
                                        event_name,
                                        client_id,
                                        component_id,
                                        "timestamp",
                                        extensions_iss,
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
                                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_driving_license"
                                )
                        ),
                        level_1_data as (
                            SELECT
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                user_user_id,
                                extensions_iss,
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
                            where
                                json_serialize(failedcheckdetails) != ''
                        ),
                        level_2_data as (
                            select
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                user_user_id,
                                extensions_iss,
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
                            from
                                level_1_data
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
                            extensions_iss,
                            processed_date,
                            activityhistoryscore,
                            strengthscore,
                            type,
                            validityscore,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.checkmethod,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_checkmethod,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.biometricverificationprocesslevel,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_biometricverificationprocesslevel,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.checkmethod,
                                valid_json_checkdetails_data
                            ) AS checkdetails_checkmethod,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.biometricverificationprocesslevel,
                                valid_json_checkdetails_data
                            ) AS checkdetails_biometricverificationprocesslevel
                        from
                            level_2_data
                    )
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    ---

Create
/*
Name       Date         Notes
P Sodhi    08/12/2023   Added processed date to row_number logic to avoid issue with migrated data.
*/
or replace view conformed.v_stg_ipv_cri_fraud AS
select
    DISTINCT Auth.product_family,
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
    identityfraudscore IDENTITY_FRAUD_SCORE,
    decisionscore DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    failedcheckdetails_checkmethod FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    checkdetails_checkmethod CHECK_DETAILS_CHECK_METHOD,
    iss Iss,
    validityscore VALIDITY_SCORE,
    type "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain
from
    (
        select
            *
        from
            (
                SELECT
                    'ipv_cri_fraud' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    (
                        with base_data as (
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
                                iss,
                                processed_date,
                                extensions_evidence,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.activityhistoryscore,
                                    valid_json_data
                                ) AS activityhistoryscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.checkdetails,
                                    valid_json_data
                                ) AS checkdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.failedcheckdetails,
                                    valid_json_data
                                ) AS failedcheckdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.decisionscore,
                                    valid_json_data
                                ) AS decisionscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.identityfraudscore,
                                    valid_json_data
                                ) AS identityfraudscore,
                            null as
                                strengthscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type,
                            null AS
                                validityscore
                            FROM
                                (
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
                                        extensions_iss as iss,
                                        case extensions_evidence != ''
                                        and is_valid_json_array(extensions_evidence)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_evidence, 0)
                                        )
                                        else null end as valid_json_data
                                    FROM
                                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_fraud"
                                )
                        ),
                        level_1_data as (
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
                                iss,
                                processed_date,
                                activityhistoryscore,
                                strengthscore,
                                identityfraudscore,
                                decisionscore,
                                json_serialize(checkdetails) checkdetails_final,
                                json_serialize(failedcheckdetails) failedcheckdetails_final,
                                type,
                                validityscore
                            FROM
                                base_data
                            where
                                json_serialize(failedcheckdetails) != ''
                        ),
                        level_2_data as (
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
                                iss,
                                processed_date,
                                activityhistoryscore,
                                strengthscore,
                                identityfraudscore,
                                decisionscore,
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
                            from
                                level_1_data
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
                            iss,
                            processed_date,
                            activityhistoryscore,
                            strengthscore,
                            identityfraudscore,
                            decisionscore,
                            type,
                            validityscore,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.checkmethod,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_checkmethod,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.biometricverificationprocesslevel,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_biometricverificationprocesslevel,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.checkmethod,
                                valid_json_checkdetails_data
                            ) AS checkdetails_checkmethod,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.biometricverificationprocesslevel,
                                valid_json_checkdetails_data
                            ) AS checkdetails_biometricverificationprocesslevel
                        from
                            level_2_data
                    )
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;
 

    ---

CREATE
OR REPLACE VIEW "conformed"."v_stg_ipv_journey" AS
/*  fix for DAC-2886 and DAC-2777
 */
 select
    DISTINCT Auth.product_family,
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
    REJECTION_REASON,
    REASON,
    HAS_mitigations,
    LEVEL_OF_CONFIDENCE,
    CI_FAIL,
    Null NOTIFICATION_TYPE,
    Null MFA_TYPE,
    Null ACCOUNT_RECOVERY,
    failedcheckdetails_biometricverificationprocesslevel FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    checkdetails_biometricverificationprocesslevel CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    Null ADDRESSES_ENTERED,
    activityhistoryscore ACTIVITY_HISTORY_SCORE,
    strengthscore strength_score,
    validityscore VALIDITY_SCORE,
    identityfraudscore IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    failedcheckdetails_kbvresponsemode FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    failedcheckdetails_checkmethod FAILED_CHECK_DETAILS_CHECK_METHOD,
    checkdetails_kbvresponsemode CHECK_DETAILS_KBV_RESPONSE_MODE,
    checkdetails_kbvquality CHECK_DETAILS_KBV_QUALITY,
    verificationscore VERIFICATION_SCORE,
    checkdetails_checkmethod CHECK_DETAILS_CHECK_METHOD,
    extensions_iss Iss,
    type "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain,
    gpg45_strength_score,
    gpg45_validity_score,
    gpg45_activity_score,
    gpg45_fraud_score,
    gpg45_verification_score,
    extensions_age,
    extensions_isukissued,
    extensions_successful,
    extensions_reproveidentity,
    event_timestamp_ms,
    event_timestamp_ms_formatted,
    extensions_mitigationtype,
    case when mitigatingcredentialissuer ='null'
    then NULL
    else 
    SPLIT_PART(mitigatingcredentialissuer, ',', 1) 
    end AS mitigatingcredentialissuer,
    --code,
    case when code1 ='null'
    then NULL
    else 
    code1
    end mitigating_code,
    --extensions_returncodes_code,
    --extensions_returncodes_issuer,
    extensions_returncodes_issuer_updated extensions_returncodes_issuer,
    extensions_returncodes_code_updated extensions_returncodes_code,
    extensions_journeytype
from
    (select
            *,
            --SUBSTRING(after_first_comma, POSITION(':' IN after_first_comma) + 1) AS mitigatingcredentialissuer,
            case when after_first_comma=''
            then null
            else
                replace(replace(replace(replace(replace(SUBSTRING(after_first_comma, POSITION(':' IN after_first_comma) + 1),'[',''),'"',''),']',''),'}',''),'\\','') 
            end mitigatingcredentialissuer,
            case when code_derived is not NULL
            then replace(json_serialize(code_derived),'"','')
            end code,
            case when code_before_comma is not NULL
            then replace(replace(replace(replace(code_before_comma,'"',''),'\\',''),'[',''),']','')
            end code1,
            case when extensions_returncodes_issuer='null' then NULL 
            ELSE
               SPLIT_PART(extensions_returncodes_issuer, ',', 1) 
            END   AS extensions_returncodes_issuer_updated,
            CASE when extensions_returncodes_code='null' then NULL 
            ELSE
               SPLIT_PART(extensions_returncodes_code, ',', 1) 
            END AS extensions_returncodes_code_updated
        from
            (
            select  'ipv_journey' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,
                            cast (day as integer) desc
                    ) as row_num,
                            event_id,
                            event_name,
                            client_id,
                            component_id,
                            "timestamp",
                            timestamp_formatted,
                            user_govuk_signin_journey_id,
                            extensions_iss,
                            verificationscore,
                            extensions_successful,
                            --extensions_experianiiqresponse,
                            user_user_id,
                            year,
                            month,
                            day,
                            processed_date,
                            validityscore,
                            identityfraudscore,
                            activityhistoryscore,
                            strengthscore,
                            REJECTION_REASON,
                            REASON,
                            HAS_mitigations,
                            LEVEL_OF_CONFIDENCE,
                            CI_FAIL,
                            gpg45_strength_score,
                            gpg45_validity_score,
                            gpg45_activity_score,
                            gpg45_fraud_score,
                            gpg45_verification_score,
                            extensions_age,
                            extensions_isukissued,
                            type,
                            failedcheckdetails_biometricverificationprocesslevel,
                              checkdetails_biometricverificationprocesslevel,
                              failedcheckdetails_checkmethod,
                             failedcheckdetails_kbvresponsemode,
                              checkdetails_checkmethod,
                             checkdetails_kbvquality,
                             checkdetails_kbvresponsemode,
                            extensions_reproveidentity,
                            event_timestamp_ms,
                            event_timestamp_ms_formatted,
                            extensions_mitigationtype,
                            code_derived,
                            SUBSTRING(json_serialize(code_derived), POSITION(',' IN json_serialize(code_derived)) + 1) AS after_first_comma1,  
                            SPLIT_PART(json_serialize(code_derived), ',', 1) AS code_before_comma , 
                            after_first_comma,
                            extensions_returncodes_code,
                            extensions_returncodes_issuer,
                            extensions_journeytype
                            from 
(
with base_data as (SELECT
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
                                extensions_successful,
                                --extensions_experianiiqresponse,
                                extensions_iss,
                                extensions_evidence,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.activityHistoryScore,
                                    valid_json_data
                                ) AS activityhistoryscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.identityfraudscore,
                                    valid_json_data
                                ) AS identityfraudscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.strengthscore,
                                    valid_json_data
                                ) AS strengthscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.validityscore,
                                    valid_json_data
                                ) AS validityscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.checkdetails,
                                    valid_json_data
                                ) AS checkdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.failedcheckdetails,
                                    valid_json_data
                                ) AS failedcheckdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.verificationscore,
                                    valid_json_data
                                ) AS verificationscore,
                                REJECTION_REASON,
                                REASON,
                                HAS_mitigations,
                                LEVEL_OF_CONFIDENCE,
                                CI_FAIL,
                                gpg45_strength_score,
                                gpg45_validity_score,
                                gpg45_activity_score,
                                gpg45_fraud_score,
                                gpg45_verification_score,
                                extensions_age,
                                extensions_isukissued,
                                extensions_reproveidentity,
                                event_timestamp_ms,
                                event_timestamp_ms_formatted,
                                extensions_mitigationtype,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.mitigations,
                                    valid_json_data
                                ) AS mitigations_extension_data,
                                extensions_returncodes ,
                                nvl2(
                                    valid_json_data_extensions_returncodes,
                                    valid_json_data_extensions_returncodes.code,
                                    valid_json_data_extensions_returncodes
                                ) AS extensions_returncodes_code,
                                 nvl2(
                                    valid_json_data_extensions_returncodes,
                                    valid_json_data_extensions_returncodes.issuers,
                                    valid_json_data_extensions_returncodes
                                ) AS extensions_returncodes_issuer,
                                extensions_journeytype
                            FROM
                                (
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
                                        extensions_iss,
                                        processed_date,
                                        extensions_rejectionreason REJECTION_REASON,
                                        extensions_reason REASON,
                                        extensions_hasmitigations HAS_mitigations,
                                        extensions_levelofconfidence LEVEL_OF_CONFIDENCE,
                                        extensions_cifail CI_FAIL,
                                        extensions_evidence,
                                        extensions_successful,
                                        --extensions_experianiiqresponse,
                                        case extensions_evidence != ''
                                        and is_valid_json_array(extensions_evidence)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_evidence, 0)
                                        )
                                        else null end as valid_json_data,
                                        extensions_gpg45scores,
                                        json_extract_path_text(
                                            extensions_gpg45scores,
                                            'evidences',
                                            '0',
                                            'strength'
                                        ) AS gpg45_strength_score,
                                        json_extract_path_text(
                                            extensions_gpg45scores,
                                            'evidences',
                                            '0',
                                            'validity'
                                        ) AS gpg45_validity_score,
                                        json_extract_path_text(extensions_gpg45scores, 'activity') AS gpg45_activity_score,
                                        json_extract_path_text(extensions_gpg45scores, 'fraud') AS gpg45_fraud_score,
                                        json_extract_path_text(extensions_gpg45scores, 'verification') AS gpg45_verification_score,
                                        extensions_age,
                                        extensions_isukissued,
                                        extensions_reproveidentity,
                                        event_timestamp_ms,
                                        event_timestamp_ms_formatted,
                                        extensions_mitigationtype,
                                        extensions_returncodes,
                                        case extensions_returncodes != '[]'
                                        and is_valid_json_array(extensions_returncodes)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_returncodes, 0)
                                        )
                                        else null end as valid_json_data_extensions_returncodes,
                                        extensions_journeytype
                                    FROM
                                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_journey"
                                )),
                        level_1_data as (
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
                                extensions_successful,
                                verificationscore,
                                identityfraudscore,
                                REJECTION_REASON,
                                REASON,
                                HAS_mitigations,
                                LEVEL_OF_CONFIDENCE,
                                CI_FAIL,
                                gpg45_strength_score,
                                gpg45_validity_score,
                                gpg45_activity_score,
                                gpg45_fraud_score,
                                gpg45_verification_score,
                                extensions_age,
                                extensions_isukissued,
                                month,
                                day,
                                processed_date,
                                validityscore,
                                activityhistoryscore,
                                strengthscore,
                                extensions_evidence,
                                extensions_iss,
                                --extensions_experianiiqresponse,
                                json_serialize(checkdetails) checkdetails_final,
                                json_serialize(failedcheckdetails) failedcheckdetails_final,
                                type,
                                extensions_reproveidentity,
                                event_timestamp_ms,
                                event_timestamp_ms_formatted,
                                extensions_mitigationtype,
                                case when json_serialize(mitigations_extension_data) != '[]' 
                                then trim(json_serialize(mitigations_extension_data),'"')  
                                else NULL
                                end mitigations_extension_data,
                                case when json_serialize(extensions_returncodes_code)='[]' then null else extensions_returncodes_code end extensions_returncodes_code,
                                case when json_serialize(extensions_returncodes_issuer)='[]' then null else extensions_returncodes_issuer end extensions_returncodes_issuer,
                                extensions_journeytype
                            FROM
                                base_data
                            where
                                json_serialize(failedcheckdetails) != ''
                        ),
                        level_2_data as (
                            select
                                event_id,
                                event_name,
                                client_id,
                                component_id,
                                "timestamp",
                                timestamp_formatted,
                                user_govuk_signin_journey_id,
                                user_user_id,
                                extensions_successful,
                                year,
                                month,
                                day,
                                processed_date,
                                validityscore,
                                identityfraudscore,
                                activityhistoryscore,
                                strengthscore,
                                REJECTION_REASON,
                                REASON,
                                HAS_mitigations,
                                LEVEL_OF_CONFIDENCE,
                                CI_FAIL,
                                gpg45_strength_score,
                                gpg45_validity_score,
                                gpg45_activity_score,
                                gpg45_fraud_score,
                                gpg45_verification_score,
                                extensions_age,
                                extensions_isukissued,
                                verificationscore,
                                extensions_evidence,
                                extensions_iss,
                                --extensions_experianiiqresponse,
                                type,
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
                                else null end as valid_json_checkdetails_data,
                                extensions_reproveidentity,
                                event_timestamp_ms,
                                event_timestamp_ms_formatted,
                                extensions_mitigationtype,
                            case mitigations_extension_data != ''
                                and is_valid_json_array(mitigations_extension_data)
                                when true then json_parse(
                                    json_extract_array_element_text(mitigations_extension_data, 0)
                                )
                                else null end as mitigations_extension_data,
                                json_serialize(extensions_returncodes_code) as extensions_returncodes_code,
                                json_serialize(extensions_returncodes_issuer) as extensions_returncodes_issuer,
                                extensions_journeytype                         
                            from
                                level_1_data
                        )
                        select
                            event_id,
                            event_name,
                            client_id,
                            component_id,
                            "timestamp",
                            timestamp_formatted,
                            user_govuk_signin_journey_id,
                            extensions_iss,
                            verificationscore,
                            extensions_successful,
                            --extensions_experianiiqresponse,
                            user_user_id,
                            year,
                            month,
                            day,
                            processed_date,
                            validityscore,
                            identityfraudscore,
                            activityhistoryscore,
                            strengthscore,
                            REJECTION_REASON,
                            REASON,
                            HAS_mitigations,
                            LEVEL_OF_CONFIDENCE,
                            CI_FAIL,
                            gpg45_strength_score,
                            gpg45_validity_score,
                            gpg45_activity_score,
                            gpg45_fraud_score,
                            gpg45_verification_score,
                            extensions_age,
                            extensions_isukissued,
                            type,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.biometricverificationprocesslevel,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_biometricverificationprocesslevel,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.biometricverificationprocesslevel,
                                valid_json_checkdetails_data
                            ) AS checkdetails_biometricverificationprocesslevel,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.checkmethod,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_checkmethod,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.kbvresponsemode,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_kbvresponsemode,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.checkmethod,
                                valid_json_checkdetails_data
                            ) AS checkdetails_checkmethod,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.kbvquality,
                                valid_json_checkdetails_data
                            ) AS checkdetails_kbvquality,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.kbvresponsemode,
                                valid_json_checkdetails_data
                            ) AS checkdetails_kbvresponsemode,
                            extensions_reproveidentity,
                            event_timestamp_ms,
                            event_timestamp_ms_formatted,
                            extensions_mitigationtype,
                            mitigations_extension_data,
                            nvl2(
                                    mitigations_extension_data,
                                    mitigations_extension_data.code,
                                    mitigations_extension_data
                                ) AS code_derived,
                                SUBSTRING(json_serialize(mitigations_extension_data), POSITION(',' IN json_serialize(mitigations_extension_data)) + 1) AS after_first_comma,
                                replace(replace(replace(replace(replace(extensions_returncodes_code,'"',''),']',''),'}',''),'\\',''),'[','') as extensions_returncodes_code,
                                replace(replace(replace(replace(replace(extensions_returncodes_issuer,'"',''),']',''),'}',''),'\\',''),'[','') as extensions_returncodes_issuer,
                                extensions_journeytype
                        from
                            level_2_data
)
            )
        where
            row_num = 1
) Auth
   join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
   and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
   join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    ---

Create
/*
 Name       Date         Notes
 P Sodhi    08/12/2023   Added processed date to row_number logic to avoid issue with migrated data.
 */
or replace view conformed.v_stg_ipv_cri_kbv AS
select
    DISTINCT Auth.product_family,
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
    null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    null strength_score,
    Null ADDRESSES_ENTERED,
    null ACTIVITY_HISTORY_SCORE,
    Null IDENTITY_FRAUD_SCORE,
    Null DECISION_SCORE,
    failedcheckdetails_kbvresponsemode FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    failedcheckdetails_checkmethod FAILED_CHECK_DETAILS_CHECK_METHOD,
    checkdetails_kbvresponsemode CHECK_DETAILS_KBV_RESPONSE_MODEL,
    checkdetails_kbvquality CHECK_DETAILS_KBV_QUALITY,
    verificationscore VERIFICATION_SCORE,
    checkdetails_checkmethod CHECK_DETAILS_CHECK_METHOD,
    extensions_iss Iss,
    null experianiiqresponse,
    null VALIDITY_SCORE,
    type "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain,
    experian_total_questions_correct,
    experian_totalquestionsasked,
    experian_totalquestionsansweredincorrect,
    experian_outcome    
from
    (
        select
            *
        from
            (
                SELECT
                    'ipv_cri_kbv' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,
                            cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    (
                        with base_data as (
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
                                extensions_experianiiqresponse,
                                extensions_iss,
                                extensions_evidence,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.checkdetails,
                                    valid_json_data
                                ) AS checkdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.failedcheckdetails,
                                    valid_json_data
                                ) AS failedcheckdetails,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.verificationscore,
                                    valid_json_data
                                ) AS verificationscore,
                                json_extract_path_text(valid_json_data_extensions_experian, 'totalquestionsansweredcorrect') AS experian_total_questions_correct,
                                json_extract_path_text(valid_json_data_extensions_experian, 'totalquestionsasked') AS experian_totalquestionsasked,
                                json_extract_path_text(valid_json_data_extensions_experian, 'totalquestionsansweredincorrect') AS experian_totalquestionsansweredincorrect,
                                json_extract_path_text(valid_json_data_extensions_experian, 'outcome') AS experian_outcome
                            FROM
                                (
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
                                        extensions_iss,
                                        processed_date,
                                        extensions_evidence,
                                        extensions_experianiiqresponse,
                                        case extensions_evidence != ''
                                        and is_valid_json_array(extensions_evidence)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_evidence, 0)
                                        )
                                        else null end as valid_json_data,
                                        case extensions_experianiiqresponse != ''
                                        and IS_VALID_JSON(extensions_experianiiqresponse)
                                        when true then extensions_experianiiqresponse
                                        else null 
                                        end as valid_json_data_extensions_experian 
                                    FROM
                                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_kbv" 
                                )
                        ),
                        level_1_data as (
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
                                verificationscore,
                                month,
                                day,
                                processed_date,
                                extensions_evidence,
                                extensions_iss,
                                extensions_experianiiqresponse,
                                json_serialize(checkdetails) checkdetails_final,
                                json_serialize(failedcheckdetails) failedcheckdetails_final,
                                type,
                                case when experian_total_questions_correct='' or experian_total_questions_correct='null'
                                     then NULL 
                                     else experian_total_questions_correct 
                                end AS experian_total_questions_correct,
                                case when experian_totalquestionsasked='' or experian_totalquestionsasked='null'
                                     then NULL 
                                     else experian_totalquestionsasked 
                                end AS experian_totalquestionsasked,
                                case when experian_totalquestionsansweredincorrect='' or experian_totalquestionsansweredincorrect='null'
                                     then NULL 
                                     else experian_totalquestionsansweredincorrect 
                                end AS experian_totalquestionsansweredincorrect,
                                case when experian_outcome='' or experian_outcome='null'
                                     then NULL 
                                     else experian_outcome 
                                end AS experian_outcome
                            FROM
                                base_data
                            where
                                json_serialize(failedcheckdetails) != ''
                        ),
                        level_2_data as (
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
                                verificationscore,
                                extensions_evidence,
                                extensions_iss,
                                extensions_experianiiqresponse,
                                type,
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
                                else null end as valid_json_checkdetails_data,
                                experian_total_questions_correct,
                                experian_totalquestionsasked,
                                experian_totalquestionsansweredincorrect,
                                experian_outcome
                            from
                                level_1_data
                        )
                        select
                            event_id,
                            event_name,
                            client_id,
                            component_id,
                            "timestamp",
                            timestamp_formatted,
                            user_govuk_signin_journey_id,
                            extensions_iss,
                            verificationscore,
                            extensions_experianiiqresponse,
                            user_user_id,
                            year,
                            month,
                            day,
                            processed_date,
                            type,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.checkmethod,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_checkmethod,
                            nvl2(
                                valid_json_failedcheckdetails_data,
                                valid_json_failedcheckdetails_data.kbvresponsemode,
                                valid_json_failedcheckdetails_data
                            ) AS failedcheckdetails_kbvresponsemode,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.checkmethod,
                                valid_json_checkdetails_data
                            ) AS checkdetails_checkmethod,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.kbvquality,
                                valid_json_checkdetails_data
                            ) AS checkdetails_kbvquality,
                            nvl2(
                                valid_json_checkdetails_data,
                                valid_json_checkdetails_data.kbvresponsemode,
                                valid_json_checkdetails_data
                            ) AS checkdetails_kbvresponsemode,
                                experian_total_questions_correct,
                                experian_totalquestionsasked,
                                experian_totalquestionsansweredincorrect,
                                experian_outcome                            
                        from
                            level_2_data
                    )
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name  with no schema binding;

    ---

Create
/*
Name       Date         Notes
P Sodhi    08/12/2023   Added processed date to row_number logic to avoid issue with migrated data.
*/
or replace view conformed.v_stg_ipv_cri_passport AS
select
    DISTINCT Auth.product_family,
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
    null FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    null CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
    strengthscore strength_score,
    Null ADDRESSES_ENTERED,
    activityhistoryscore ACTIVITY_HISTORY_SCORE,
    identityfraudscore IDENTITY_FRAUD_SCORE,
    decisionscore DECISION_SCORE,
    Null FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,
    null FAILED_CHECK_DETAILS_CHECK_METHOD,
    Null CHECK_DETAILS_KBV_RESPONSE_MODEL,
    Null CHECK_DETAILS_KBV_QUALITY,
    Null VERIFICATION_SCORE,
    null CHECK_DETAILS_CHECK_METHOD,
    iss Iss,
    validityscore VALIDITY_SCORE,
    type "TYPE",
    BatC.product_family batch_product_family,
    BatC.maxrundate,
    ref.product_family ref_product_family,
    ref.domain,
    ref.sub_domain,
    ref.other_sub_domain
from
    (
        select
            *
        from
            (
                SELECT
                    'ipv_cri_passport' Product_family,
                    row_number() over (
                        partition by event_id,
                        timestamp_formatted
                        order by
                            processed_date desc,cast (day as integer) desc
                    ) as row_num,
                    *
                FROM
                    (
                        with base_data as (
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
                                iss,
                                processed_date,
                                extensions_evidence,
                            null AS
                                activityhistoryscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.strengthscore,
                                    valid_json_data
                                ) AS strengthscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.validityscore,
                                    valid_json_data
                                ) AS validityscore,
                            null AS
                                decisionscore,
                            null AS
                                identityfraudscore,
                                nvl2(
                                    valid_json_data,
                                    valid_json_data.type,
                                    valid_json_data
                                ) AS type
                            FROM
                                (
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
                                        extensions_iss as iss,
                                        case extensions_evidence != ''
                                        and is_valid_json_array(extensions_evidence)
                                        when true then json_parse(
                                            json_extract_array_element_text(extensions_evidence, 0)
                                        )
                                        else null end as valid_json_data
                                    FROM
                                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_passport"
                                )
                        )
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
                            iss,
                            processed_date,
                            activityhistoryscore,
                            strengthscore,
                            identityfraudscore,
                            decisionscore,
                            type,
                            validityscore
                        FROM
                            base_data
                    )
            )
        where
            row_num = 1
    ) Auth
    join conformed.BatchControl BatC On Auth.Product_family = BatC.Product_family
    and to_date(processed_date, 'YYYYMMDD') > NVL(MaxRunDate, null)
    join conformed.REF_EVENTS ref on Auth.EVENT_NAME = ref.event_name with no schema binding;

    raise info 'Setup of conformed layer ran successfully';

    ---

EXCEPTION WHEN OTHERS THEN 
    RAISE EXCEPTION'[Error while setting up conformed layer] Exception: %',sqlerrm;

END;

$$ language plpgsql;