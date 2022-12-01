DELETE FROM
    billables a
        USING billables b
WHERE
    a.id < b.id
    AND a.toll_dialed = b.toll_dialed
    AND a.client_callerid = b.client_callerid
    AND a.lead_delivery_date = b.lead_delivery_date
;

DROP TABLE billables;


SELECT * FROM med_master_join
WHERE att_connected < '@today'
AND att_connected >= '2022-11-20'
;