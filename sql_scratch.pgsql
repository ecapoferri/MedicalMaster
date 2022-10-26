SELECT *
FROM
        att_data AS p
    FULL OUTER JOIN
        af_message_data AS m
    ON p.number_orig = m.callerid
WHERE
        p.connected >= '2022-10-20'
    AND
        p.connected < '2022-10-21'
    AND
        m.connected >= '2022-10-20'
    AND
        m.connected < '2022-10-21'
LIMIT 100;

WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    af_callers as (SELECT DISTINCT(recording_id) as message_id, callerid FROM m)
SELECT callerid, ARRAY_AGG(af_callers.message_id) as message_id, ARRAY_AGG(p.id) as att_id FROM af_callers LEFT JOIN p ON af_callers.callerid = p.number_orig
GROUP BY af_callers.callerid
;


WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    joined AS (
        SELECT
            m.callerid, ARRAY_AGG(p.id) AS att_data_id, ARRAY_AGG(m.recording_id) AS messages
        FROM
            m
            LEFT JOIN
            p
            ON m.callerid = p.number_orig
        GROUP BY m.callerid
    )
SELECT * from p WHERE id NOT IN (SELECT UNNEST(att_data_id) FROM joined)
; -- ATT CALLS NOT IN ANSWER FIRST CALLER ID


WITH
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    )
SELECT
    acct,
    callerid,
    COUNT(connected) AS calls,
    ARRAY_AGG(phone) AS phone,
    ARRAY_AGG(caller_name) AS caller_name,
    ARRAY_AGG(reference) AS reference
FROM m
GROUP BY callerid, acct
; -- ANSWER FIRST MESSAGES


SELECT table_name, column_name
FROM information_schema.columns
WHERE table_name LIKE 'af\_%'
;

WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    )
SELECT
    p.created,
    p.connected,
    p.duration,
    p.number_orig,
    p.number_dial,
    p.number_term,
    p.id,
    p.zip,
    p.state,
    p.disconnect,
    m.recording_id,
    m.callerid,
    m.acct,
    m.ext,
    m.call_type,
    m.practice_id,
    m.zipcode,
    m.connected,
    m.statecheck,
    m.addr_city,
    m.majorcity,
    m.besttime,
    m.sent_emails_to,
    m.city_id,
    m.caller_name,
    m.phone,
    m.email,
    m.addr_state,
    m.reference
FROM
    p FULL OUTER JOIN m
    on m.callerid =  p.number_orig
;



-- Master Join to view all ATT and AF data joined together by caller id. Group by should isolate unite caller id/toll destination combinations, aka: origin/termination combinations. Other fields are aggregated together into arrays of unique values or all values for duplicates within the GROUP BY combinations.
-- CREATE OR REPLACE VIEW master_join AS
WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-10-24'
            AND
            connected >= '2022-10-21'
    )
SELECT
    -- COUNT(p.created) AS mms_createds,
   p.number_orig AS att_callerid,
   m.callerid AS af_callerid,
   p.number_dial AS toll,
   m.acct AS af_acct,
   m.practice_id AS practice_id,
   p.number_term AS att_fwd,
    COUNT(p.connected) AS att_connections,
    CASE WHEN COUNT(m.connected)::INTEGER = 0
        THEN NULL
        ELSE COUNT(m.connected)::INTEGER
        END
        AS af_connections,
    ARRAY_AGG(p.duration) AS att_durations,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(p.id)), NULL)
        AS att_ids,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.recording_id)), NULL)
        AS af_ids,
    CASE WHEN m.callerid IS NOT NULL
        THEN ARRAY_REPLACE(ARRAY_AGG(m.call_type), NULL, FALSE)::BOOLEAN[]
        ELSE ARRAY_REMOVE(ARRAY_AGG(m.call_type), NULL)::BOOLEAN[]
        END
        AS af_typed,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.besttime)), NULL)
        AS af_msg_besttimes,
    ARRAY_REMOVE(ARRAY_AGG(m.sent_emails_to), NULL)
        AS af_msg_emailed,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.caller_name)), NULL)
        AS af_msg_caller_name,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.phone)), NULL)
        AS af_msg_given_phones,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.email)), NULL)
        AS af_msg_given_emails,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT(m.addr_state)), NULL)
        AS af_msg_given_addr_state,
    ARRAY_REMOVE(ARRAY_AGG(m.reference), NULL)
        AS af_msg_bodies
FROM
    p FULL OUTER JOIN m
    on m.callerid =  p.number_orig
GROUP BY
    att_callerid,
    af_callerid,
    toll,
    af_acct,
    practice_id,
    att_fwd
;
