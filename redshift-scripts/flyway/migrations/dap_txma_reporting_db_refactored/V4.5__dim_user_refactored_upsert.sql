CREATE OR REPLACE PROCEDURE conformed_refactored.dim_user_refactored_upsert()
 LANGUAGE plpgsql
AS $$
DECLARE
BEGIN
        delete from  audit_refactored.audit_procedure_status
        where Procedure_name='dim_user_refactored_upsert';
        
        insert into audit_refactored.audit_procedure_status
        (step_number,Procedure_name,running_status,procedure_start_date)
        values(3,'dim_user_refactored_upsert','started',sysdate);

          UPDATE conformed_refactored.dim_user_refactored
            SET
              user_id = st.user_user_id,
              MODIFIED_BY=current_user,
              MODIFIED_DATE=CURRENT_DATE 
            FROM (
              SELECT distinct user_user_id
              FROM  dap_txma_stage.txma_stage_layer stg
              JOIN conformed_refactored.batch_events_refactored bth
              ON  stg.partition_event_name=bth.event_name
              WHERE user_user_id IN (
                SELECT user_id
                FROM conformed_refactored.dim_user_refactored
              )
              AND TO_TIMESTAMP(
                        LEFT(processed_dt, 4) || '-' || 
                        SUBSTRING(processed_dt, 5, 2) || '-' || 
                        RIGHT(processed_dt, 2) || ' ' || 
                        LPAD(LEFT(LPAD(processed_time, 6, '0'), 2), 2, '0') || ':' || 
                        LPAD(SUBSTRING(LPAD(processed_time, 6, '0'), 3, 2), 2, '0') || ':' || 
                        LPAD(RIGHT(LPAD(processed_time, 6, '0'), 2), 2, '0'),
                        'YYYY-MM-DD HH24:MI:SS'  -- Format string
                    )   >= (SELECT NVL(MIN((max_run_date )),'1999-01-01 00:00:00')
                                                     FROM conformed_refactored.batch_events_refactored)
              AND bth.event_active =1                                       
            ) AS st
            WHERE dim_user_refactored.user_id = st.user_user_id;

            INSERT INTO  conformed_refactored.dim_user_refactored 
                        ( user_id,CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE)
            SELECT DISTINCT
                stg.user_user_id,
                CURRENT_USER,
                CURRENT_DATE,
                CURRENT_USER,
                CURRENT_DATE
            FROM
                dap_txma_stage.txma_stage_layer stg
            JOIN conformed_refactored.batch_events_refactored bth ON stg.partition_event_name = bth.event_name
            LEFT JOIN conformed_refactored.dim_user_refactored dim ON stg.user_user_id = dim.user_id
            WHERE TO_TIMESTAMP(
                        LEFT(processed_dt, 4) || '-' || 
                        SUBSTRING(processed_dt, 5, 2) || '-' || 
                        RIGHT(processed_dt, 2) || ' ' || 
                        LPAD(LEFT(LPAD(processed_time, 6, '0'), 2), 2, '0') || ':' || 
                        LPAD(SUBSTRING(LPAD(processed_time, 6, '0'), 3, 2), 2, '0') || ':' || 
                        LPAD(RIGHT(LPAD(processed_time, 6, '0'), 2), 2, '0'),
                        'YYYY-MM-DD HH24:MI:SS'  -- Format string
                    )   >=
                  ( SELECT NVL(MIN((max_run_date )),'1999-01-01 00:00:00')
                    FROM conformed_refactored.batch_events_refactored
                )
            AND bth.event_active = 1
            AND  dim.user_id is null
            AND stg.user_user_id is not null;  

        update audit_refactored.audit_procedure_status
        set running_status='Complete'
        ,procedure_end_date=sysdate
        where Procedure_name='dim_user_refactored_upsert';                                     


END;
$$
