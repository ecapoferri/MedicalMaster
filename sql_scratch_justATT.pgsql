
WITH
    p AS (
        SELECT *
        FROM att_data
        WHERE
            connected < '2022-12-07'
            AND
            connected >= '2022-12-06'
    ),
    m AS (
        SELECT *
        FROM af_message_data
        WHERE
            connected < '2022-12-07'
            AND
            connected >= '2022-12-06'
    )
SELECT
   p.connected::DATE att_date,
   p.number_orig att_callerid,

   p.number_dial toll,
    SUM(duration) durations_sum,
    COUNT(p.connected) att_connections,
    array_agg(DISTINCT(p.state)) att_state_agg,
   p.acct_af acct_af

FROM p
GROUP BY att_callerid, toll, acct_af, att_date
;
