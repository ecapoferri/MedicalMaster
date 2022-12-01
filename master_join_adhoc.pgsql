SELECT
    p.number_orig att_callerid,
    p.number_dial att_toll,
    p.acct_af att_acct_af,
    p.state att_state,
    p.duration att_duration,
    p.connected att_connected,
    p.dispo_code att_dispo,

    m.acct af_acct,
    m.callerid af_callerid,

    m.practice_id af_practice_id,
    m.client af_client,

    m.connected af_connected


FROM att_data p LEFT JOIN af_message_data m
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
    AND p.connected::DATE >= '2022-11-04'

ORDER BY att_connected DESC
;