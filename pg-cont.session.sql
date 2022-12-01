-- Unique billables fully unnested into all dups
SELECT
    client_callerid,
    campaign_id,
    practice_id,
    toll,
    name_,
    date_,
    code

FROM (
    SELECT
        *,
        UNNEST(tolls_dialed) toll,
        UNNEST(names) name_,
        UNNEST(delivered_dates) date_,
        UNNEST(delivery_codes) code
    FROM export_unique_billables
) a
WHERE a.date_ > '2022-11-27'
;