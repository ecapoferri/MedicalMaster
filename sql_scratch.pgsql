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