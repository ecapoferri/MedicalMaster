-- select unique records from billables table
-- DROP VIEW IF EXISTS export_unique_billables;
CREATE OR REPLACE VIEW export_unique_billables AS
    WITH a AS (
        SELECT
            id,
            client_callerid,
            toll_dialed,
            practice_id,
            campaign_id,
            UNNEST(names) client_name,
            -- multiple names per script run for the
            -- same potential lead are in an array
            -- unnested in this subquery in order to
            -- avoid having nested arrays in the final query
            -- which should only return one line for a unique lead per practice
            lead_delivery_date,
            lead_delivery_code
        FROM billables
        ORDER BY lead_delivery_date ASC
    ),
    b AS (
        SELECT
            client_callerid,
            campaign_id,
            practice_id,
            array_agg(DISTINCT(toll_dialed)) tolls_dialed,
            array_agg(DISTINCT(client_name)) names,
            array_agg(DISTINCT(lead_delivery_date)) delivered_dates,
            array_agg(DISTINCT(lead_delivery_code)) delivery_codes,
            COUNT(id) n_lines_in_table
        FROM
            a
        GROUP BY
            practice_id,
            client_callerid,
            campaign_id
        ORDER BY
            campaign_id,
            client_callerid,
            delivered_dates,
            practice_id
    )
    SELECT *, delivery_codes[1] first_delivery_code FROM b
;
