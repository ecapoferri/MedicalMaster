CREATE OR REPLACE VIEW med_master_join AS
SELECT
   p.number_orig att_callerid,
   m.callerid af_callerid,

   p.number_dial att_toll,
   p.acct_af att_acct_af,
   m.acct af_acct,

    NULLIF(array_remove(array_agg(DISTINCT(m.recording_id)), NULL), '{}')
        af_ids,
    -- COALESCES TO TRUE IF ANY WERE TRUE
    CASE WHEN NULLIF(COUNT(m.connected), 0) IS NOT NULL
        THEN ARRAY_REPLACE(array_agg(m.call_for_ad), NULL, FALSE) @> ARRAY[TRUE]
        ELSE NULL
        END
        AS call_for_ad,

   m.practice_id af_practice_id,
   m.client af_client,
    NULLIF(array_remove(array_agg(m.dispo), NULL), '{}')
        af_dispos,
    NULLIF(array_remove(array_agg(m.reference), NULL), '{}')
        af_msg_bodies,
    NULLIF(array_remove(array_agg(m.history), NULL), '{}')
        af_hists,

    NULLIF(array_remove(array_agg(DISTINCT(p.state)), NULL), '{}')
        att_state_agg,
    NULLIF(array_remove(array_agg(DISTINCT(m.addr_state)), NULL), '{}')
        af_msg_given_addr_state,

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


    SUM(duration) durations_sum,
   p.connected::DATE att_date,
    MIN(p.connected) att_connected,
    -- NULLIF(array_remove(array_agg(DISTINCT(p.connected)), NULL), '{}')
    --     att_connected,
   m.connected::DATE af_date,
    MIN(m.connected) af_connected



FROM att_data p FULL OUTER JOIN af_message_data m
    -- JOIN BY COMMON caller id, destination acct, date of call (CDT/CST CONVERTED FROM UTC)
    ON
    p.number_orig = m.callerid
    AND
    p.acct_af = m.acct
    AND
    p.connected::DATE = m.connected::DATE

WHERE
    m.connected::DATE < 'today'
    AND m.connected::DATE >= '2022-11-04'
    AND P.connected::DATE >= '2022-11-04'
GROUP BY
    att_callerid, af_callerid,
    att_acct_af, af_acct,
    att_toll,
    af_practice_id,
    af_client,
    att_date, af_date

ORDER BY af_date DESC, af_callerid ASC
;