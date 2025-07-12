--1. dialogs
-- STV202506137__DWH.h_dialogs определение

CREATE TABLE STV202506137__DWH.h_dialogs
(
    hk_message_id int NOT NULL,
    message_id int NOT NULL,
    message_ts timestamp,
    load_dt timestamp NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_message_id) DISABLED
)
PARTITION BY ((h_dialogs.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_dialogs.load_dt)::date))::date WHEN ("datediff"('month', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_dialogs.load_dt)::date))::date ELSE (h_dialogs.load_dt)::date END);


CREATE PROJECTION STV202506137__DWH.h_dialogs /*+createtype(P)*/ 
(
 hk_message_id,
 message_id,
 message_ts,
 load_dt,
 load_src
)
AS
 SELECT h_dialogs.hk_message_id,
        h_dialogs.message_id,
        h_dialogs.message_ts,
        h_dialogs.load_dt,
        h_dialogs.load_src
 FROM STV202506137__DWH.h_dialogs
 ORDER BY h_dialogs.load_dt
SEGMENTED BY h_dialogs.hk_message_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--2. groups
-- STV202506137__DWH.h_groups определение

CREATE TABLE STV202506137__DWH.h_groups
(
    hk_group_id int NOT NULL,
    group_id int NOT NULL,
    registration_dt timestamp,
    load_dt timestamp NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_group_id) DISABLED
)
PARTITION BY ((h_groups.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_groups.load_dt)::date))::date WHEN ("datediff"('month', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_groups.load_dt)::date))::date ELSE (h_groups.load_dt)::date END);


CREATE PROJECTION STV202506137__DWH.h_groups /*+createtype(P)*/ 
(
 hk_group_id,
 group_id,
 registration_dt,
 load_dt,
 load_src
)
AS
 SELECT h_groups.hk_group_id,
        h_groups.group_id,
        h_groups.registration_dt,
        h_groups.load_dt,
        h_groups.load_src
 FROM STV202506137__DWH.h_groups
 ORDER BY h_groups.load_dt
SEGMENTED BY h_groups.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--3. users
-- STV202506137__DWH.h_users определение

CREATE TABLE STV202506137__DWH.h_users
(
    hk_user_id int NOT NULL,
    user_id int NOT NULL,
    registration_dt timestamp,
    load_dt timestamp NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_user_id) DISABLED
)
PARTITION BY ((h_users.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_users.load_dt)::date))::date WHEN ("datediff"('month', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_users.load_dt)::date))::date ELSE (h_users.load_dt)::date END);


CREATE PROJECTION STV202506137__DWH.h_users /*+createtype(P)*/ 
(
 hk_user_id,
 user_id,
 registration_dt,
 load_dt,
 load_src
)
AS
 SELECT h_users.hk_user_id,
        h_users.user_id,
        h_users.registration_dt,
        h_users.load_dt,
        h_users.load_src
 FROM STV202506137__DWH.h_users
 ORDER BY h_users.load_dt
SEGMENTED BY h_users.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);