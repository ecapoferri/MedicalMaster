
WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-11-09'
            AND
            connected >= '2022-11-04'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-11-09'
            AND
            connected >= '2022-11-04'
    )
SELECT
   m.callerid af_callerid,
   m.acct af_acct,
   m.connected::DATE af_date,
   --
    NULLIF(array_remove(array_agg(DISTINCT(m.recording_id)), NULL), '{}')
        af_ids,
    NULLIF(array_remove(array_agg(DISTINCT(m.practice_id)), NULL), '{}')
        practice_id,
    NULLIF(COUNT(m.connected), 0)
        af_connections,
    -- COALESCES TO TRUE IF ANY WERE TRUE
    CASE WHEN NULLIF(COUNT(m.connected), 0) IS NOT NULL
        THEN ARRAY_REPLACE(array_agg(m.call_for_ad), NULL, FALSE) @> ARRAY[TRUE]
        ELSE NULL
        END
        AS call_for_ad,
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
    NULLIF(array_remove(array_agg(m.reference), NULL), '{}')
        af_msg_bodies,
    NULLIF(array_remove(array_agg(m.dispo), NULL), '{}')
        af_dispos,
    NULLIF(array_remove(array_agg(m.history), NULL), '{}')
        af_hists,

   m.client af_client


FROM m
GROUP BY
    af_callerid,
    af_client,
    af_acct,
    af_date
;
