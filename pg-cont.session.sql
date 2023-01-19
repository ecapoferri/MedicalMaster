SELECT
    p.lead_ref_id client_lead_id,
    p.lead_company client_company,
    b.*
FROM unique_billables b, d_practice p
WHERE p.practice_id = b.practice_id
;