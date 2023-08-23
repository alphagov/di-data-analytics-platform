CREATE OR replace PROCEDURE conformed.sp_ipv_cri_driving_license ()
AS $$
BEGIN

	
	UPDATE conformed.DIM_EVENT
    SET 
      EVENT_NAME = st.EVENT_NAME,
      EVENT_DESCRIPTION = st.EVENT_NAME,
      PRODUCT_FAMILY=REF_PRODUCT_FAMILY,
      EVENT_JOURNEY_TYPE = st.domain,
      SERVICE_NAME = st.sub_domain,
      MODIFIED_BY=current_user,
      MODIFIED_DATE=CURRENT_DATE,
      BATCH_ID=0000
    FROM (
      SELECT *
      FROM conformed.v_stg_ipv_cri_driving_license
      WHERE EVENT_NAME IN (
        SELECT EVENT_NAME
        FROM conformed.DIM_EVENT
      )
    ) AS st
    WHERE DIM_EVENT.EVENT_NAME = st.event_name;
    
    
    INSERT INTO conformed.DIM_EVENT ( EVENT_NAME, EVENT_DESCRIPTION, PRODUCT_FAMILY ,EVENT_JOURNEY_TYPE, SERVICE_NAME, CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE,BATCH_ID)
    SELECT DISTINCT EVENT_NAME, EVENT_NAME, REF_PRODUCT_FAMILY ,domain, sub_domain,current_user,CURRENT_DATE,current_user, CURRENT_DATE,9999
    FROM conformed.v_stg_ipv_cri_driving_license
    WHERE EVENT_NAME NOT IN (SELECT EVENT_NAME FROM conformed.DIM_EVENT);
    
    
    
    ----DIM_JOURNEY_CHANNEL insert/update
    
    UPDATE conformed.DIM_JOURNEY_CHANNEL
    SET 
      CHANNEL_NAME = CASE 
        WHEN EVENT_NAME LIKE '%IPV%' THEN 'Web'
        WHEN EVENT_NAME LIKE '%DCMAW%' THEN 'App'
        ELSE 'General'
      END,
      CHANNEL_DESCRIPTION = CASE 
        WHEN EVENT_NAME LIKE '%IPV%' THEN 'Event has taken place via Web channel'
        WHEN EVENT_NAME LIKE '%DCMAW%' THEN 'Event has taken place via App channel'
        ELSE 'General - This is the default channel'
      END,
      MODIFIED_BY=current_user,
      MODIFIED_DATE=CURRENT_DATE,
      BATCH_ID=0000
    FROM (
      SELECT DISTINCT EVENT_NAME
      FROM conformed.v_stg_ipv_cri_driving_license
    ) AS st
    WHERE (
      CASE 
        WHEN st.EVENT_NAME LIKE '%IPV%' THEN 'Web'
        WHEN st.EVENT_NAME LIKE '%DCMAW%' THEN 'App'
        ELSE 'General'
      END
    ) = conformed.DIM_JOURNEY_CHANNEL.CHANNEL_NAME
    AND (
      CASE 
        WHEN st.EVENT_NAME LIKE '%IPV%' THEN 'Web'
        WHEN st.EVENT_NAME LIKE '%DCMAW%' THEN 'App'
        ELSE 'General'
      END
    ) IN (
      SELECT CHANNEL_NAME
      FROM conformed.DIM_JOURNEY_CHANNEL
    );
    
    
    INSERT INTO conformed.DIM_JOURNEY_CHANNEL (CHANNEL_NAME, CHANNEL_DESCRIPTION, CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE, BATCH_ID)
    SELECT DISTINCT CASE 
            WHEN EVENT_NAME LIKE '%IPV%' THEN 'Web'
            WHEN EVENT_NAME LIKE '%DCMAW%' THEN 'App'
            ELSE 'General'
        END,
        CASE 
            WHEN EVENT_NAME LIKE '%IPV%' THEN 'Event has taken place via Web channel'
            WHEN EVENT_NAME LIKE '%DCMAW%' THEN 'Event has taken place via App channel'
            ELSE 'General - This is the default channel'
        END,
        current_user,
        CURRENT_DATE,
        current_user,
        CURRENT_DATE,
        9999
    FROM conformed.v_stg_ipv_cri_driving_license AS st
    WHERE (CASE 
            WHEN st.EVENT_NAME LIKE '%IPV%' THEN 'Web'
            WHEN st.EVENT_NAME LIKE '%DCMAW%' THEN 'App'
            ELSE 'General'
        END) NOT IN (
            SELECT CHANNEL_NAME
            FROM conformed.DIM_JOURNEY_CHANNEL
        );
    
    
    ----Insert and update for dim_relying_party 
    
    
    UPDATE conformed.DIM_RELYING_PARTY
    SET
      CLIENT_ID = NVL(st.CLIENT_ID,'-1'),
      RELYING_PARTY_NAME = st.CLIENT_NAME,
      RELYING_PARTY_DESCRIPTION = st.CLIENT_NAME,
        MODIFIED_BY= current_user,
        MODIFIED_DATE=CURRENT_DATE,
        BATCH_ID=0000
    FROM (
            select DISTINCT
            mn.CLIENT_ID,
            ref.CLIENT_NAME,
            current_user,
            CURRENT_DATE,
            current_user,
            CURRENT_DATE,
            9999
            FROM conformed.v_stg_ipv_cri_driving_license mn
            left join  "dap_txma_reporting_db"."conformed"."ref_relying_parties" ref
            on mn.CLIENT_ID=ref.CLIENT_ID
    ) AS st
    WHERE  st.CLIENT_NAME=conformed.DIM_RELYING_PARTY.RELYING_PARTY_NAME
    and  st.CLIENT_ID IN (
      SELECT CLIENT_ID
      FROM conformed.DIM_RELYING_PARTY
    );



    INSERT INTO  conformed.DIM_RELYING_PARTY (CLIENT_ID, RELYING_PARTY_NAME, RELYING_PARTY_DESCRIPTION, CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE, BATCH_ID)
    SELECT
        NVL(st.CLIENT_ID,'-1')  ,
        st.CLIENT_NAME,
        st.CLIENT_NAME,
        current_user,
        CURRENT_DATE,
        current_user,
        CURRENT_DATE,
        9999
    FROM ( select DISTINCT
            mn.CLIENT_ID,
            ref.CLIENT_NAME,
            current_user,
            CURRENT_DATE,
            current_user,
            CURRENT_DATE,
            9999
            FROM conformed.v_stg_ipv_cri_driving_license mn
            left join  "dap_txma_reporting_db"."conformed"."ref_relying_parties" ref
            on mn.CLIENT_ID=ref.CLIENT_ID) AS st
    WHERE   st.CLIENT_ID NOT IN (
            SELECT CLIENT_ID
            FROM conformed.DIM_RELYING_PARTY
        );




    UPDATE conformed.DIM_VERIFICATION_ROUTE
    SET
        VERIFICATION_ROUTE_NAME = st.sub_domain,
        VERIFICATION_SHORT_NAME = st.sub_domain,
        ROUTE_DESCRIPTION = st.DOMAIN,
        MODIFIED_BY= current_user,
        MODIFIED_DATE=CURRENT_DATE,
        BATCH_ID=0000
    FROM (
        SELECT DISTINCT DOMAIN, sub_domain
        FROM conformed.v_stg_ipv_cri_driving_license
        WHERE sub_domain IN (
            SELECT VERIFICATION_ROUTE_NAME
            FROM conformed.DIM_VERIFICATION_ROUTE
        )
    ) AS st
    WHERE st.sub_domain = conformed.DIM_VERIFICATION_ROUTE.VERIFICATION_ROUTE_NAME;


    INSERT INTO conformed.DIM_VERIFICATION_ROUTE ( VERIFICATION_ROUTE_NAME, VERIFICATION_SHORT_NAME, ROUTE_DESCRIPTION, CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE,BATCH_ID)
    SELECT DISTINCT sub_domain, sub_domain, domain,current_user,CURRENT_DATE,current_user, CURRENT_DATE,9999
    FROM conformed.v_stg_ipv_cri_driving_license
    WHERE sub_domain NOT IN (SELECT VERIFICATION_ROUTE_NAME  FROM conformed.DIM_VERIFICATION_ROUTE);


    
    UPDATE "dap_txma_reporting_db"."conformed"."fact_user_journey_event"
    SET 
      REJECTION_REASON=st.REJECTION_REASON
      ,REASON=st.REASON
      ,NOTIFICATION_TYPE=st.NOTIFICATION_TYPE
      ,MFA_TYPE=st.MFA_TYPE
      ,ACCOUNT_RECOVERY=st.ACCOUNT_RECOVERY
      ,FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL=JSON_SERIALIZE(st.FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL)
      ,CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL=JSON_SERIALIZE(st.CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL)
      ,ADDRESSES_ENTERED=st.ADDRESSES_ENTERED
      ,ACTIVITY_HISTORY_SCORE=JSON_SERIALIZE(st.ACTIVITY_HISTORY_SCORE)
      ,IDENTITY_FRAUD_SCORE=st.IDENTITY_FRAUD_SCORE
      ,DECISION_SCORE=st.DECISION_SCORE
      ,FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE=st.FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE
      ,FAILED_CHECK_DETAILS_CHECK_METHOD=JSON_SERIALIZE(st.FAILED_CHECK_DETAILS_CHECK_METHOD)
      ,CHECK_DETAILS_KBV_RESPONSE_MODEL=st.CHECK_DETAILS_KBV_RESPONSE_MODEL
      ,CHECK_DETAILS_KBV_QUALITY=st.CHECK_DETAILS_KBV_QUALITY
      ,VERIFICATION_SCORE=st.VERIFICATION_SCORE
      ,CHECK_DETAILS_CHECK_METHOD=JSON_SERIALIZE(st.CHECK_DETAILS_CHECK_METHOD)
      ,Iss=st.Iss
      ,VALIDITY_SCORE=JSON_SERIALIZE(st.VALIDITY_SCORE)
      ,STRENGTH_SCORE=JSON_SERIALIZE(st.STRENGTH_SCORE)
      ,"TYPE"=JSON_SERIALIZE(st."TYPE")
      ,PROCESSED_DATE=st.PROCESSED_DATE
      ,MODIFIED_BY=current_user
      ,MODIFIED_DATE=CURRENT_DATE
      ,BATCH_ID=0000
    FROM (SELECT *
      FROM conformed.v_stg_ipv_cri_driving_license
      WHERE EVENT_ID IN (
        SELECT EVENT_ID
        FROM "dap_txma_reporting_db"."conformed"."fact_user_journey_event"
    ) )AS st
    WHERE fact_user_journey_event.EVENT_ID = st.EVENT_ID;
    
    
    
    
    INSERT INTO conformed.FACT_USER_JOURNEY_EVENT (EVENT_KEY,DATE_KEY,verification_route_key,journey_channel_key,relying_party_key,USER_ID,
                            EVENT_ID,EVENT_TIME,JOURNEY_ID,COMPONENT_ID,EVENT_COUNT,
                            REJECTION_REASON,REASON,NOTIFICATION_TYPE,MFA_TYPE,ACCOUNT_RECOVERY,FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,
                            CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL,ADDRESSES_ENTERED,ACTIVITY_HISTORY_SCORE,IDENTITY_FRAUD_SCORE,DECISION_SCORE,
                            FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE,FAILED_CHECK_DETAILS_CHECK_METHOD,CHECK_DETAILS_KBV_RESPONSE_MODEL,CHECK_DETAILS_KBV_QUALITY,
                            VERIFICATION_SCORE,CHECK_DETAILS_CHECK_METHOD,Iss,VALIDITY_SCORE,STRENGTH_SCORE,"TYPE", PROCESSED_DATE,
                            CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE, BATCH_ID)
    SELECT NVL(DE.event_key,-1) AS event_key
          ,dd.date_key
          ,NVL(dvr.verification_route_key,-1) AS verification_route_key
          , NVL(djc.journey_channel_key,-1) AS journey_channel_key
          , NVL(drp.relying_party_key,-1) AS relying_party_key
          ,user_user_id AS USER_ID
          ,event_id AS EVENT_ID
          --,cnf.event_name
          --,cnf.timestamp AS EVENT_TIME
          ,cnf.timestamp_formatted as EVENT_TIME
          ,cnf.user_govuk_signin_journey_id AS JOURNEY_ID
          ,cnf.component_id AS COMPONENT_ID
          ,EVENT_COUNT
           ,REJECTION_REASON
           ,REASON
           ,NOTIFICATION_TYPE
           ,MFA_TYPE,ACCOUNT_RECOVERY
           ,JSON_SERIALIZE(FAILED_CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL)
           ,JSON_SERIALIZE(CHECK_DETAILS_BIOMETRIC_VERIFICATION_PROCESS_LEVEL)
           ,ADDRESSES_ENTERED
           ,JSON_SERIALIZE(ACTIVITY_HISTORY_SCORE)
           ,IDENTITY_FRAUD_SCORE
           ,DECISION_SCORE
           ,FAILED_CHECK_DETAILS_KBV_RESPONSE_MODE
           ,JSON_SERIALIZE(FAILED_CHECK_DETAILS_CHECK_METHOD)
           ,CHECK_DETAILS_KBV_RESPONSE_MODEL
           ,CHECK_DETAILS_KBV_QUALITY
           ,VERIFICATION_SCORE
           ,JSON_SERIALIZE(CHECK_DETAILS_CHECK_METHOD)
           ,Iss
           ,JSON_SERIALIZE(VALIDITY_SCORE)
           ,JSON_SERIALIZE(STRENGTH_SCORE)
           ,JSON_SERIALIZE("TYPE")
        ,PROCESSED_DATE
           ,current_user
           , CURRENT_DATE
           ,current_user
           , CURRENT_DATE
           , 9999
    FROM (SELECT *
      FROM conformed.v_stg_ipv_cri_driving_license
      WHERE EVENT_ID NOT IN (
        SELECT EVENT_ID
        FROM conformed.FACT_USER_JOURNEY_EVENT))cnf
    JOIN conformed.dim_date dd ON date(cnf.timestamp_formatted)= dd.date
    LEFT JOIN conformed.DIM_EVENT DE ON cnf.event_name = DE.EVENT_NAME
    LEFT JOIN conformed.dim_journey_channel djc ON 
        (CASE 
            WHEN cnf.EVENT_NAME LIKE '%IPV%' THEN 'Web'
            WHEN cnf.EVENT_NAME LIKE '%DCMAW%' THEN 'App'
            ELSE 'General'
        END) = djc.channel_name
    LEFT JOIN conformed.dim_relying_party drp ON 
    cnf.CLIENT_ID = drp.CLIENT_ID
    LEFT JOIN conformed.dim_verification_route dvr 
         ON  cnf.sub_domain = dvr.verification_route_name;
         

    INSERT into audit.err_duplicate_event_id_ipv_cri_driving_license_8 (total_duplicate_event_count_minus_one
    ,product_family,event_name,event_id,timestamp_formatted,created_by,created_datetime)
    SELECT event_count,Product_family,event_name,event_id,timestamp_formatted,current_user,GETDATE() as Current_date
    FROM
        (
            SELECT COUNT(*) AS event_count,event_name,auth.Product_family,event_id,timestamp_formatted
            FROM
                (
                    SELECT
                        'ipv_cri_driving_license' AS Product_family,
                        ROW_NUMBER() OVER (PARTITION BY event_id, timestamp_formatted ORDER BY timestamp_formatted) AS row_num,
                        *
                    FROM
                        "dap_txma_reporting_db"."dap_txma_stage"."ipv_cri_driving_license" 
                        --where event_id='5c94f844-f05d-4c32-87fe-e3b6b265223f'
                ) auth
            JOIN "dap_txma_reporting_db"."conformed"."batchcontrol" batc ON auth.Product_family = batc.product_family
                AND auth.processed_date > batc.maxrundate
            WHERE row_num <> 1
            AND (auth.product_family,event_name, event_id) NOT IN (SELECT product_family ,event_name, event_id 
                                                                        FROM audit.err_duplicate_event_id_ipv_cri_driving_license_8)
            GROUP BY
                auth.Product_family,
                event_name,
                event_id,
                timestamp_formatted            
        ) subquery;

    --update config table
    
    UPDATE conformed.BatchControl 
    SET MaxRunDate = CAST(subquery.updated_value AS DATE)
    FROM (
      SELECT PRODUCT_FAMILY, MAX(PROCESSED_DATE) updated_value
      FROM conformed.v_stg_ipv_cri_driving_license
      GROUP BY PRODUCT_FAMILY
    ) AS subquery
    WHERE conformed.BatchControl.Product_family =subquery.PRODUCT_FAMILY;

	raise info 'processing of product family: ipv_cri_driving_license ran successfully';

	EXCEPTION WHEN OTHERS THEN 
        RAISE EXCEPTION '[error while processing product family: ipv_cri_driving_license] exception: %',sqlerrm;

END;

$$ LANGUAGE plpgsql;
