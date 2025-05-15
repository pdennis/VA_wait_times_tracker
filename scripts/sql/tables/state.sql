--
-- The State table maps the numeric statefp value used in the congressional district
-- shape files to the regular 2-character postal abbreviation more familiarly used.
--

drop table if exists state cascade;
CREATE TABLE state
(
    statefp   CHARACTER VARYING(2) NOT NULL,
    state     CHARACTER VARYING(2) NOT NULL,
    full_name TEXT                 NOT NULL,
    PRIMARY KEY (statefp),
    CONSTRAINT ic_state_state_unique UNIQUE (state),
    CONSTRAINT ic_state_full_name_unique UNIQUE (full_name)
);

drop index if exists ix_state_full_name;
CREATE UNIQUE INDEX ix_state_full_name ON state (full_name);

drop index if exists ix_state_state;
CREATE UNIQUE INDEX ix_state_state ON state (state);

--
-- DDL statements
--
INSERT INTO state (statefp, state, full_name)
VALUES ('01', 'AL', 'Alabama');
INSERT INTO state (statefp, state, full_name)
VALUES ('02', 'AK', 'Alaska');
INSERT INTO state (statefp, state, full_name)
VALUES ('04', 'AZ', 'Arizona');
INSERT INTO state (statefp, state, full_name)
VALUES ('05', 'AR', 'Arkansas');
INSERT INTO state (statefp, state, full_name)
VALUES ('06', 'CA', 'California');
INSERT INTO state (statefp, state, full_name)
VALUES ('08', 'CO', 'Colorado');
INSERT INTO state (statefp, state, full_name)
VALUES ('09', 'CT', 'Connecticut');
INSERT INTO state (statefp, state, full_name)
VALUES ('10', 'DE', 'Delaware');
INSERT INTO state (statefp, state, full_name)
VALUES ('11', 'DC', 'District of Columbia');
INSERT INTO state (statefp, state, full_name)
VALUES ('12', 'FL', 'Florida');
INSERT INTO state (statefp, state, full_name)
VALUES ('13', 'GA', 'Georgia');
INSERT INTO state (statefp, state, full_name)
VALUES ('15', 'HI', 'Hawaii');
INSERT INTO state (statefp, state, full_name)
VALUES ('16', 'ID', 'Idaho');
INSERT INTO state (statefp, state, full_name)
VALUES ('17', 'IL', 'Illinois');
INSERT INTO state (statefp, state, full_name)
VALUES ('18', 'IN', 'Indiana');
INSERT INTO state (statefp, state, full_name)
VALUES ('19', 'IA', 'Iowa');
INSERT INTO state (statefp, state, full_name)
VALUES ('20', 'KS', 'Kansas');
INSERT INTO state (statefp, state, full_name)
VALUES ('21', 'KY', 'Kentucky');
INSERT INTO state (statefp, state, full_name)
VALUES ('22', 'LA', 'Louisiana');
INSERT INTO state (statefp, state, full_name)
VALUES ('23', 'ME', 'Maine');
INSERT INTO state (statefp, state, full_name)
VALUES ('24', 'MD', 'Maryland');
INSERT INTO state (statefp, state, full_name)
VALUES ('25', 'MA', 'Massachusetts');
INSERT INTO state (statefp, state, full_name)
VALUES ('26', 'MI', 'Michigan');
INSERT INTO state (statefp, state, full_name)
VALUES ('27', 'MN', 'Minnesota');
INSERT INTO state (statefp, state, full_name)
VALUES ('28', 'MS', 'Mississippi');
INSERT INTO state (statefp, state, full_name)
VALUES ('29', 'MO', 'Missouri');
INSERT INTO state (statefp, state, full_name)
VALUES ('30', 'MT', 'Montana');
INSERT INTO state (statefp, state, full_name)
VALUES ('31', 'NE', 'Nebraska');
INSERT INTO state (statefp, state, full_name)
VALUES ('32', 'NV', 'Nevada');
INSERT INTO state (statefp, state, full_name)
VALUES ('33', 'NH', 'New Hampshire');
INSERT INTO state (statefp, state, full_name)
VALUES ('34', 'NJ', 'New Jersey');
INSERT INTO state (statefp, state, full_name)
VALUES ('35', 'NM', 'New Mexico');
INSERT INTO state (statefp, state, full_name)
VALUES ('36', 'NY', 'New York');
INSERT INTO state (statefp, state, full_name)
VALUES ('37', 'NC', 'North Carolina');
INSERT INTO state (statefp, state, full_name)
VALUES ('38', 'ND', 'North Dakota');
INSERT INTO state (statefp, state, full_name)
VALUES ('39', 'OH', 'Ohio');
INSERT INTO state (statefp, state, full_name)
VALUES ('40', 'OK', 'Oklahoma');
INSERT INTO state (statefp, state, full_name)
VALUES ('41', 'OR', 'Oregon');
INSERT INTO state (statefp, state, full_name)
VALUES ('42', 'PA', 'Pennsylvania');
INSERT INTO state (statefp, state, full_name)
VALUES ('44', 'RI', 'Rhode Island');
INSERT INTO state (statefp, state, full_name)
VALUES ('45', 'SC', 'South Carolina');
INSERT INTO state (statefp, state, full_name)
VALUES ('46', 'SD', 'South Dakota');
INSERT INTO state (statefp, state, full_name)
VALUES ('47', 'TN', 'Tennessee');
INSERT INTO state (statefp, state, full_name)
VALUES ('48', 'TX', 'Texas');
INSERT INTO state (statefp, state, full_name)
VALUES ('49', 'UT', 'Utah');
INSERT INTO state (statefp, state, full_name)
VALUES ('50', 'VT', 'Vermont');
INSERT INTO state (statefp, state, full_name)
VALUES ('51', 'VA', 'Virginia');
INSERT INTO state (statefp, state, full_name)
VALUES ('53', 'WA', 'Washington');
INSERT INTO state (statefp, state, full_name)
VALUES ('54', 'WV', 'West Virginia');
INSERT INTO state (statefp, state, full_name)
VALUES ('55', 'WI', 'Wisconsin');
INSERT INTO state (statefp, state, full_name)
VALUES ('56', 'WY', 'Wyoming');
INSERT INTO state (statefp, state, full_name)
VALUES ('60', 'AS', 'American Samoa');
INSERT INTO state (statefp, state, full_name)
VALUES ('66', 'GU', 'Guam');
INSERT INTO state (statefp, state, full_name)
VALUES ('69', 'MP', 'Northern Mariana Islands');
INSERT INTO state (statefp, state, full_name)
VALUES ('72', 'PR', 'Puerto Rico');
INSERT INTO state (statefp, state, full_name)
VALUES ('78', 'VI', 'Virgin Islands');



