drop table if exists congress_member CASCADE;
CREATE TABLE IF NOT EXISTS congress_member
(
    geoid       TEXT PRIMARY KEY,
    state       TEXT  NOT NULL,
    district    INT   NOT NULL,
    name        TEXT  NOT NULL,
    party       TEXT  NOT NULL,
    bioguide_id TEXT  NOT NULL,
    data        JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_congress_member_state ON congress_member (state);
CREATE INDEX IF NOT EXISTS ix_congress_member_district ON congress_member (district);
CREATE INDEX IF NOT EXISTS ix_congress_member_name ON congress_member (name);
CREATE INDEX IF NOT EXISTS ix_congress_member_bioguide_id ON congress_member (bioguide_id);
CREATE INDEX IF NOT EXISTS ix_congress_member_data ON congress_member USING GIN (data);

ALTER TABLE congress_member
    ADD CONSTRAINT fk_congress_member_state_state
        FOREIGN KEY (state) REFERENCES state (state)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
