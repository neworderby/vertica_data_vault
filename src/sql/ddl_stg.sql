--1. dialogs
-- STV202506137__STAGING.dialogs определение

CREATE TABLE STV202506137__STAGING.dialogs
(
    message_id int NOT NULL,
    message_ts timestamp NOT NULL,
    message_from int NOT NULL,
    message_to int NOT NULL,
    message varchar(1000),
    message_group int,
    CONSTRAINT pk_dialogs PRIMARY KEY (message_id) DISABLED
)
PARTITION BY ((dialogs.message_ts)::date) GROUP BY (CASE WHEN ("datediff"('year', (dialogs.message_ts)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (dialogs.message_ts)::date))::date WHEN ("datediff"('month', (dialogs.message_ts)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (dialogs.message_ts)::date))::date ELSE (dialogs.message_ts)::date END);


ALTER TABLE STV202506137__STAGING.dialogs ADD CONSTRAINT fk_dialogs_from FOREIGN KEY (message_from) references STV202506137__STAGING.users (id);
ALTER TABLE STV202506137__STAGING.dialogs ADD CONSTRAINT fk_dialogs_to FOREIGN KEY (message_to) references STV202506137__STAGING.users (id);
ALTER TABLE STV202506137__STAGING.dialogs ADD CONSTRAINT fk_dialogs_group FOREIGN KEY (message_group) references STV202506137__STAGING.groups (id);

CREATE PROJECTION STV202506137__STAGING.dialogs /*+createtype(P)*/ 
(
 message_id,
 message_ts,
 message_from,
 message_to,
 message,
 message_group
)
AS
 SELECT dialogs.message_id,
        dialogs.message_ts,
        dialogs.message_from,
        dialogs.message_to,
        dialogs.message,
        dialogs.message_group
 FROM STV202506137__STAGING.dialogs
 ORDER BY dialogs.message_id
SEGMENTED BY hash(dialogs.message_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--2. users
-- STV202506137__STAGING.users определение

CREATE TABLE STV202506137__STAGING.users
(
    id int NOT NULL,
    chat_name varchar(200) NOT NULL,
    registration_dt timestamp,
    country varchar(200),
    age int,
    CONSTRAINT pk_users PRIMARY KEY (id) DISABLED
);


CREATE PROJECTION STV202506137__STAGING.users /*+createtype(P)*/ 
(
 id,
 chat_name,
 registration_dt,
 country,
 age
)
AS
 SELECT users.id,
        users.chat_name,
        users.registration_dt,
        users.country,
        users.age
 FROM STV202506137__STAGING.users
 ORDER BY users.id
SEGMENTED BY hash(users.id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--3. groups
-- STV202506137__STAGING.groups определение

CREATE TABLE STV202506137__STAGING.groups
(
    id int NOT NULL,
    admin_id int NOT NULL,
    group_name varchar(100) NOT NULL,
    registration_dt timestamp NOT NULL,
    is_private boolean,
    CONSTRAINT pk_groups PRIMARY KEY (id) DISABLED
)
PARTITION BY ((groups.registration_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (groups.registration_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (groups.registration_dt)::date))::date WHEN ("datediff"('month', (groups.registration_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (groups.registration_dt)::date))::date ELSE (groups.registration_dt)::date END);


ALTER TABLE STV202506137__STAGING.groups ADD CONSTRAINT fk_groups_admin FOREIGN KEY (admin_id) references STV202506137__STAGING.users (id);

CREATE PROJECTION STV202506137__STAGING.groups /*+createtype(P)*/ 
(
 id,
 admin_id,
 group_name,
 registration_dt,
 is_private
)
AS
 SELECT groups.id,
        groups.admin_id,
        groups.group_name,
        groups.registration_dt,
        groups.is_private
 FROM STV202506137__STAGING.groups
 ORDER BY groups.id,
          groups.admin_id
SEGMENTED BY hash(groups.id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--4. group_log
-- STV202506137__STAGING.group_log определение

CREATE TABLE STV202506137__STAGING.group_log
(
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(10),
    event_datetime timestamp
)
PARTITION BY ((group_log.event_datetime)::date) GROUP BY (CASE WHEN ("datediff"('year', (group_log.event_datetime)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (group_log.event_datetime)::date))::date WHEN ("datediff"('month', (group_log.event_datetime)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (group_log.event_datetime)::date))::date ELSE (group_log.event_datetime)::date END);


ALTER TABLE STV202506137__STAGING.group_log ADD CONSTRAINT fk_group_log_groups_group_id FOREIGN KEY (group_id) references STV202506137__STAGING.groups (id);
ALTER TABLE STV202506137__STAGING.group_log ADD CONSTRAINT fk_group_log_users_user_id_from FOREIGN KEY (user_id) references STV202506137__STAGING.users (id);
ALTER TABLE STV202506137__STAGING.group_log ADD CONSTRAINT fk_group_log_users_user_id FOREIGN KEY (user_id_from) references STV202506137__STAGING.users (id);

CREATE PROJECTION STV202506137__STAGING.group_log /*+createtype(P)*/ 
(
 group_id,
 user_id,
 user_id_from,
 event,
 event_datetime
)
AS
 SELECT group_log.group_id,
        group_log.user_id,
        group_log.user_id_from,
        group_log.event,
        group_log.event_datetime
 FROM STV202506137__STAGING.group_log
 ORDER BY group_log.event_datetime
SEGMENTED BY hash(group_log.group_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);