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
    -- these 6 are the identifying fields, the rest are aggregated
   --
   p.number_orig AS att_callerid,
   m.callerid AS af_callerid,
   p.number_dial AS toll,
   p.number_term AS att_fwd,
   --
    -- this could be useful, some ATT records have a zero duration,
    -- summing this agg may be useful for filtering those out
    -- count att connection timestamps as calls, shouldn't have nulls
    COUNT(p.connected) AS att_connections,
    array_agg(p.duration) AS att_durations,
    array_agg(DISTINCT(p.id)) AS att_ids,
    -- att joined calls will have an AF connection ts,
    -- but counts of nulls will resolve to zero,
    -- this will resolve zero counts to null
   --
   m.acct AS af_acct,
   m.recording_id AS af_ids,
   --
    NULLIF(array_remove(array_agg(DISTINCT(m.practice_id)), NULL), '{}')
        practice_id,
    NULLIF(COUNT(m.connected), 0)
        af_connections,
    -- COALESCES TO TRUE IF ANY WERE TRUE
    CASE WHEN NULLIF(COUNT(m.connected), 0) IS NOT NULL
        THEN ARRAY_REPLACE(array_agg(m.call_for_ad), NULL, FALSE) @> ARRAY[TRUE]
        ELSE NULL
        END
    call_for_ad,
    -- 
    -- the rest are just lists of existing results
    NULLIF(array_remove(array_agg(DISTINCT(m.besttime)), NULL), '{}')
        af_msg_besttimes,
    NULLIF(array_remove(array_agg(m.sent_emails_to), NULL), '{}')
        af_msg_emailed,
    NULLIF(array_remove(array_agg(DISTINCT(m.caller_name)), NULL), '{}')
        af_msg_caller_name,
    NULLIF(array_remove(array_agg(DISTINCT(m.phone)), NULL), '{}')
        af_msg_given_phones,
    NULLIF(array_remove(array_agg(DISTINCT(m.email)), NULL), '{}')
        af_msg_given_emails,
    NULLIF(array_remove(array_agg(DISTINCT(m.addr_state)), NULL), '{}')
        af_msg_given_addr_state,
    NULLIF(array_remove(array_agg(DISTINCT(m.addr_state)), NULL), '{}')
        af_msg_given_addr_stateDA,
    NULLIF(array_remove(array_agg(m.reference), NULL), '{}')
        af_msg_bodies
FROM
    p FULL OUTER JOIN m
    on m.callerid =  p.number_orig
GROUP BY
    att_callerid,
    af_callerid,
    af_ids,
    toll,
    af_acct,
    att_fwd
;