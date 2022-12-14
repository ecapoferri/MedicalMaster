DROP VIEW IF EXISTS gather_billables CASCADE;
-- Gather billables from AF and email data.
CREATE OR REPLACE VIEW gather_billables AS
    SELECT
        -- Answer First data
        acct af_acct,
        CASE WHEN practice_id IS NOT NULL THEN call_date ELSE NULL END
            lead_delivery_date,
        practice_id,
        callerid,
        caller_name,
        phone caller_phone,
        connected processed,

        history af_logs,
        dispo af_dispo,

        'AF' delivery_code
    FROM
        af_message_data
    WHERE
        practice_id IS NOT NULL
        AND practice_id != 0
        AND callerid IS NOT NULL
UNION ALL
    -- Email form data
    SELECT
        af_acct,
        lead_delivery_date,
        practice_id,
        af_msg_id callerid,
        lead_name caller_name,
        lead_phone caller_phone,
        submitted processed,

        NULL af_logs,
        NULL af_dispo,

        'IH' delivery_code

    FROM f_lead_email_form
    WHERE
        practice_id IS NOT NULL
        AND lead_name IS NOT NULL
        AND af_acct IS NOT NULL
        AND af_msg_id IS NOT NULL
;

DROP VIEW IF EXISTS join_billables CASCADE;
-- Outer join of att and potentially billable lead deliveries
--      An outer join, it will contain duplications of both sets of data.
--      This can be grouped for billable, unique leads in unique_billables
--      and, to some extent, medical_master.
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
    p.lead_ref_id client_lead_id,
    p.lead_company client_comp_name,

    b.lead_delivery_date,
    b.caller_phone,
    b.caller_name,
    b.delivery_code,

    b.processed,
    b.af_dispo,
    b.af_logs

FROM
    att_data a
    -- Not using gather_billables_all because this is just to join AF to ATT
    FULL OUTER JOIN gather_billables b
        ON b.callerid = a.number_orig
        AND b.af_acct::INTEGER = a.acct_af
    LEFT JOIN d_practice p ON b.practice_id = p.practice_id

ORDER BY att_connected
;

DROP VIEW IF EXISTS medical_master CASCADE;
-- Mimics the medical master, groups by att ids.
CREATE OR REPLACE VIEW medical_master AS
SELECT
    att_af_acct "AF Group",
    call_date "Connect Date",
    att_connected::TIME "Connect Time",
    att_callerid "Caller Number",
    toll_af_acct "Toll Dialed",
    att_caller_state,

    NULLIF(array_remove(array_agg(DISTINCT(delivery_code)), NULL), '{}')
        "Deliveries",
    NULLIF(array_remove(array_agg(DISTINCT(practice_id)), NULL), '{}')
        practice_ids,
    NULLIF(array_remove(array_agg(DISTINCT(af_practice)), NULL), '{}')
        "Clients",
    NULLIF(array_remove(array_agg(DISTINCT(client_lead_id)), NULL), '{}')
        client_lead_ids,
    NULLIF(array_remove(array_agg(DISTINCT(client_comp_name)), NULL), '{}')
        client_comp_name,
    NULLIF(array_remove(array_agg(DISTINCT(caller_phone)), NULL), '{}')
        "Caller Phone Number (Given)",
    NULLIF(array_remove(array_agg(DISTINCT(caller_name)), NULL), '{}')
        "Caller Name",
    NULLIF(array_remove(array_agg(DISTINCT(lead_delivery_date)), NULL), '{}')
        lead_delivery_dates,

    NULLIF(array_remove(array_agg(DISTINCT(processed)), NULL), '{}')
        processed,
    NULLIF(array_remove(array_agg(DISTINCT(af_dispo)), NULL), '{}')
        af_dispos,
    NULLIF(array_remove(array_agg(DISTINCT(af_logs)), NULL), '{}')
        af_logs_
FROM
    join_billables
GROUP BY
    call_date,
    att_connected,
    att_af_acct,
    att_callerid,
    toll_af_acct,
    att_caller_state
;

DROP VIEW IF EXISTS gather_billables_all CASCADE;
-- Shows all billables, including those imported from the old medical master.
CREATE OR REPLACE VIEW gather_billables_all AS
    SELECT
        af_acct,
        lead_delivery_date,
        af_prac_id practice_id,
        callerid,
        caller_name,
        processed,
        delivery_code,
        NULL af_dispo,
        NULL af_logs,
        NULL caller_phone
    FROM prior_billables
    WHERE lead_delivery_date < '2022-11-4'
UNION ALL
    SELECT
        af_acct,
        lead_delivery_date,
        practice_id,
        callerid,
        caller_name,
        processed,
        delivery_code,
        af_dispo,
        af_logs,
        caller_phone
    FROM gather_billables
    WHERE lead_delivery_date >= '2022-11-4'
ORDER BY lead_delivery_date
;

DROP VIEW IF EXISTS unique_billables CASCADE;
-- Compiles billables, group by will collect duplicate callers to the
--      same practice into one record to prevent double billing
--      for the same person
CREATE OR REPLACE VIEW unique_billables AS
SELECT
    MIN(b.lead_delivery_date)
        first_lead_delivery_date,
    b.af_acct,
    b.callerid,
    NULLIF(array_remove(array_agg(DISTINCT(b.caller_name)), NULL), '{}')
        caller_names,
    b.practice_id,
    p.af_practice,
    NULLIF(array_remove(array_agg(DISTINCT(b.caller_phone)), NULL), '{}')
        caller_phones,
    NULLIF(array_remove(array_agg(DISTINCT(b.lead_delivery_date)), NULL), '{}')
        lead_delivery_dates,
    NULLIF(array_remove(array_agg(DISTINCT(b.delivery_code)), NULL), '{}')
        delivery_codes,
    NULLIF(array_remove(array_agg(DISTINCT(b.processed)), NULL), '{}')
        processed,
    NULLIF(array_remove(array_agg(DISTINCT(b.af_dispo)), NULL), '{}')
        af_dispos,
    NULLIF(array_remove(array_agg(DISTINCT(b.af_logs)), NULL), '{}')
        af_logs
    
FROM gather_billables_all b
    LEFT JOIN d_practice p ON b.practice_id = p.practice_id
GROUP BY b.af_acct, b.callerid, b.practice_id, p.af_practice
ORDER BY first_lead_delivery_date DESC
;
