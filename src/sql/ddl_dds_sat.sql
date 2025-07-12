--1. s_admins
-- STV202506137__DWH.s_admins определение

CREATE TABLE STV202506137__DWH.s_admins
(
    hk_admin_id int NOT NULL,
    is_admin boolean,
    admin_from timestamp,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_admins.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_admins.load_dt)::date))::date WHEN ("datediff"('month', (s_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_admins.load_dt)::date))::date ELSE (s_admins.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_admins ADD CONSTRAINT fk_s_admins_l_admins FOREIGN KEY (hk_admin_id) references STV202506137__DWH.l_admins (hk_l_admin_id);

CREATE PROJECTION STV202506137__DWH.s_admins /*+createtype(P)*/ 
(
 hk_admin_id,
 is_admin,
 admin_from,
 load_dt,
 load_src
)
AS
 SELECT s_admins.hk_admin_id,
        s_admins.is_admin,
        s_admins.admin_from,
        s_admins.load_dt,
        s_admins.load_src
 FROM STV202506137__DWH.s_admins
 ORDER BY s_admins.load_dt
SEGMENTED BY s_admins.hk_admin_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--2. s_auth_history
-- STV202506137__DWH.s_auth_history определение

CREATE TABLE STV202506137__DWH.s_auth_history
(
    hk_l_user_group_activity int NOT NULL,
    user_id_from int,
    event varchar(10),
    event_dt timestamp,
    load_dt timestamp NOT NULL,
    load_src varchar(20)
);


ALTER TABLE STV202506137__DWH.s_auth_history ADD CONSTRAINT fk_s_auth_history_l_user_group_activity FOREIGN KEY (hk_l_user_group_activity) references STV202506137__DWH.l_user_group_activity (hk_l_user_group_activity);

CREATE PROJECTION STV202506137__DWH.s_auth_history /*+createtype(L)*/ 
(
 hk_l_user_group_activity,
 user_id_from,
 event,
 event_dt,
 load_dt,
 load_src
)
AS
 SELECT s_auth_history.hk_l_user_group_activity,
        s_auth_history.user_id_from,
        s_auth_history.event,
        s_auth_history.event_dt,
        s_auth_history.load_dt,
        s_auth_history.load_src
 FROM STV202506137__DWH.s_auth_history
 ORDER BY s_auth_history.hk_l_user_group_activity,
          s_auth_history.user_id_from,
          s_auth_history.event,
          s_auth_history.event_dt
SEGMENTED BY hash(s_auth_history.hk_l_user_group_activity, s_auth_history.user_id_from, s_auth_history.event_dt, s_auth_history.load_dt, s_auth_history.event, s_auth_history.load_src) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--3. s_dialog_info
-- STV202506137__DWH.s_dialog_info определение

CREATE TABLE STV202506137__DWH.s_dialog_info
(
    hk_message_id int NOT NULL,
    message varchar(1000),
    message_from int,
    message_to int,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_dialog_info.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_dialog_info.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_dialog_info.load_dt)::date))::date WHEN ("datediff"('month', (s_dialog_info.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_dialog_info.load_dt)::date))::date ELSE (s_dialog_info.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_dialog_info ADD CONSTRAINT fk_s_dialog_info_h_dialogs FOREIGN KEY (hk_message_id) references STV202506137__DWH.h_dialogs (hk_message_id);

CREATE PROJECTION STV202506137__DWH.s_dialog_info /*+createtype(P)*/ 
(
 hk_message_id,
 message,
 message_from,
 message_to,
 load_dt,
 load_src
)
AS
 SELECT s_dialog_info.hk_message_id,
        s_dialog_info.message,
        s_dialog_info.message_from,
        s_dialog_info.message_to,
        s_dialog_info.load_dt,
        s_dialog_info.load_src
 FROM STV202506137__DWH.s_dialog_info
 ORDER BY s_dialog_info.load_dt
SEGMENTED BY s_dialog_info.hk_message_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--4. s_group_name
-- STV202506137__DWH.s_group_name определение

CREATE TABLE STV202506137__DWH.s_group_name
(
    hk_group_id int NOT NULL,
    group_name varchar(100),
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_group_name.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_group_name.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_group_name.load_dt)::date))::date WHEN ("datediff"('month', (s_group_name.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_group_name.load_dt)::date))::date ELSE (s_group_name.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_group_name ADD CONSTRAINT fk_s_group_name_h_groups FOREIGN KEY (hk_group_id) references STV202506137__DWH.h_groups (hk_group_id);

CREATE PROJECTION STV202506137__DWH.s_group_name /*+createtype(P)*/ 
(
 hk_group_id,
 group_name,
 load_dt,
 load_src
)
AS
 SELECT s_group_name.hk_group_id,
        s_group_name.group_name,
        s_group_name.load_dt,
        s_group_name.load_src
 FROM STV202506137__DWH.s_group_name
 ORDER BY s_group_name.load_dt
SEGMENTED BY s_group_name.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--5. s_group_private_status
-- STV202506137__DWH.s_group_private_status определение

CREATE TABLE STV202506137__DWH.s_group_private_status
(
    hk_group_id int NOT NULL,
    is_private boolean,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_group_private_status.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_group_private_status.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_group_private_status.load_dt)::date))::date WHEN ("datediff"('month', (s_group_private_status.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_group_private_status.load_dt)::date))::date ELSE (s_group_private_status.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_group_private_status ADD CONSTRAINT fk_s_group_private_status_h_groups FOREIGN KEY (hk_group_id) references STV202506137__DWH.h_groups (hk_group_id);

CREATE PROJECTION STV202506137__DWH.s_group_private_status /*+createtype(P)*/ 
(
 hk_group_id,
 is_private,
 load_dt,
 load_src
)
AS
 SELECT s_group_private_status.hk_group_id,
        s_group_private_status.is_private,
        s_group_private_status.load_dt,
        s_group_private_status.load_src
 FROM STV202506137__DWH.s_group_private_status
 ORDER BY s_group_private_status.load_dt
SEGMENTED BY s_group_private_status.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--6. s_user_chatinfo
-- STV202506137__DWH.s_user_chatinfo определение

CREATE TABLE STV202506137__DWH.s_user_chatinfo
(
    hk_user_id int NOT NULL,
    chat_name varchar(200),
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_user_chatinfo.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_user_chatinfo.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_user_chatinfo.load_dt)::date))::date WHEN ("datediff"('month', (s_user_chatinfo.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_user_chatinfo.load_dt)::date))::date ELSE (s_user_chatinfo.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_user_chatinfo ADD CONSTRAINT fk_s_user_chatinfo_h_users FOREIGN KEY (hk_user_id) references STV202506137__DWH.h_users (hk_user_id);

CREATE PROJECTION STV202506137__DWH.s_user_chatinfo /*+createtype(P)*/ 
(
 hk_user_id,
 chat_name,
 load_dt,
 load_src
)
AS
 SELECT s_user_chatinfo.hk_user_id,
        s_user_chatinfo.chat_name,
        s_user_chatinfo.load_dt,
        s_user_chatinfo.load_src
 FROM STV202506137__DWH.s_user_chatinfo
 ORDER BY s_user_chatinfo.load_dt
SEGMENTED BY s_user_chatinfo.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--7. s_user_socdem
-- STV202506137__DWH.s_user_socdem определение

CREATE TABLE STV202506137__DWH.s_user_socdem
(
    hk_user_id int NOT NULL,
    country varchar(200),
    age int,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_user_socdem.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_user_socdem.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_user_socdem.load_dt)::date))::date WHEN ("datediff"('month', (s_user_socdem.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_user_socdem.load_dt)::date))::date ELSE (s_user_socdem.load_dt)::date END);


ALTER TABLE STV202506137__DWH.s_user_socdem ADD CONSTRAINT fk_s_user_socdem_h_users FOREIGN KEY (hk_user_id) references STV202506137__DWH.h_users (hk_user_id);

CREATE PROJECTION STV202506137__DWH.s_user_socdem /*+createtype(P)*/ 
(
 hk_user_id,
 country,
 age,
 load_dt,
 load_src
)
AS
 SELECT s_user_socdem.hk_user_id,
        s_user_socdem.country,
        s_user_socdem.age,
        s_user_socdem.load_dt,
        s_user_socdem.load_src
 FROM STV202506137__DWH.s_user_socdem
 ORDER BY s_user_socdem.load_dt
SEGMENTED BY s_user_socdem.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);