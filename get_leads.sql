

-- select * from lead where  like 'Core Integrative Health';

-- SELECT * FROM lead_field WHERE name like '%Company%';

SELECT l.id, l.client_org, d.value, l.partner_rep, concat(e.name_first, ' ', e.name_last) name, max(l.status_id), s.name
FROM lead l LEFT JOIN lead_data d ON l.id = d.lead_id
LEFT JOIN member_employee e ON l.partner_rep = e.member_id
LEFT JOIN status s ON l.status_id = s.id
WHERE
    -- d.value IN ('Manhattan Medical Associates', 'Dynamic Health and Pain Management', 'Core Integrative Health', 'Danville Neuropathy Center', 'Advanced Integrated Medical', 'Raleigh Spine Clinic', 'Disc Centers of America', 'WELLNESS 1ST INTEGRATIVE MEDICAL CENTER', 'Dees Integrative Health', 'NSI Stem Cell', 'Joseph Health Group', 'Secoya Health', 'Advanced Regen Medical', 'Apple Medical Center', 'Revolution Wellness', 'Spokane Spine & Disc Chiropractic & Massage Therapy', 'Discovery Medical Center', 'Arctic Medical Center', 'Granite Bay Regenerative Medicine', 'Pain Relief Institute', 'Vitality Ageless Center', 'Genesis Lifestyle Medicine', 'Advanced Integrated Medical', 'Ohio Integrated Pain & Wellness Centers', 'Dr. John A Kotis - Chicago Plastic Surgeon', 'California Medical Group', 'New Hope Wellness Advisors', 'Nava Health & Vitality Center', 'Little Mountain Chiropractic & Wellness', 'Reflex Knees', 'West LA Neuro Kinesiology', 'California Med Group')
    -- l.id IN (150744, 138287, 138163, 140627, 154443, 149711, 140998, 135702, 153731, 140612, 136602, 143329, 141078, 135709, 154193, 131847, 136608, 152769, 135895, 101138, 144681, 142500, 152611, 145731, 140321, 135725, 140599, 146065, 135911, 142781)
    -- d.value IN ('Indy Regenerative Medicine', 'Northwell Health Imaging', 'Neuropathy Treatment Clinics of Texas', 'Brodwyn and Associates')
    AND d.lead_field_id IN (28, 44) -- company, company in ad
    -- AND l.status_id IN (48)
    AND l.status_id IN (45, 48)
    -- AND l.status_id NOT IN (96, 100, 144, 82)
ORDER BY value ASC, id DESC
LIMIT 1000;