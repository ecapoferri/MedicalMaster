
CREATE OR REPLACE VIEW join_billables AS
SELECT
    a.call_date call_date,
    a.connected AT TIME ZONE 'US/Central' att_connected,

    a.number_orig att_callerid,
    a.number_dial toll_af_acct,

    a.acct_af att_af_acct,
    a.state att_caller_state,

    b.practice_id,
    p.af_practice,

    b.lead_delivery_date,
    b.caller_phone,
    b.caller_name,
    b.delivery_code,

    b.processed,
    b.af_logs

FROM
    att_data a
    FULL OUTER JOIN gather_billables b
        ON b.callerid = a.number_orig
        AND b.af_acct::INTEGER = a.acct_af
    LEFT JOIN d_practice p ON b.practice_id = p.practice_id

WHERE a.connected > '2022-12-4'
ORDER BY att_connected
;