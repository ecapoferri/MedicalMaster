-- CREATE TYPE lead_delivery_code_enum AS ENUM ('BB', 'AF', 'DT');
CREATE TABLE IF NOT EXISTS billables (
    client_callerid BIGINT NOT NULL,
    toll_dialed BIGINT NOT NULL,
    campaign_id INT NOT NULL,
    practice_id INT NOT NULL,
    names VARCHAR[],
    lead_delivery_date TIMESTAMP NOT NULL,
    lead_delivery_code lead_delivery_code_enum NOT NULL,
    id SERIAL PRIMARY KEY
)
;
