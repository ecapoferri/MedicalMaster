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

-- select unique records from billables table
WITH a AS(
    SELECT
        id,
        client_callerid,
        toll_dialed,
        practice_id,
        campaign_id,
        UNNEST(names) client_name,
        lead_delivery_date
    FROM
        billables
)
SELECT
    client_callerid,
    practice_id,
    array_agg(DISTINCT(campaign_id)) campaign_ids,
    array_agg(DISTINCT(toll_dialed)) tolls_dialed,
    array_agg(DISTINCT(client_name)) names,
    array_agg(DISTINCT(lead_delivery_date)) dates,
    COUNT(id) delivered_n_times
FROM
    a
GROUP BY
    client_callerid,
    practice_id
;

SELECT * FROM med_master_join
WHERE att_connected < '@today'
AND att_connected >= '2022-11-20'
;