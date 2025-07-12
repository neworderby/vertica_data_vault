WITH
--Активные группы
user_group_messages AS (
SELECT
t.hk_group_id,
COUNT(DISTINCT t.hk_user_id) AS cnt_users_in_group_with_messages
FROM (
SELECT
lgd.hk_group_id,
msg.hk_user_id,
lgd.hk_message_id
FROM STV202506137__DWH.l_groups_dialogs AS lgd
LEFT JOIN STV202506137__DWH.l_user_message AS msg ON lgd.hk_message_id = msg.hk_message_id
) AS t
GROUP BY hk_group_id
),
--10 самых ранних групп с кол-вом пользователей
user_group_log AS (
SELECT
act.hk_group_id,
COUNT(DISTINCT act.hk_user_id) AS cnt_added_users
FROM STV202506137__DWH.l_user_group_activity AS act
WHERE act.hk_l_user_group_activity IN (
SELECT hk_l_user_group_activity
FROM STV202506137__DWH.s_auth_history
WHERE event = 'add')
AND act.hk_group_id IN (
SELECT hk_group_id
FROM STV202506137__DWH.h_groups
ORDER BY registration_dt
LIMIT 10)
GROUP BY act.hk_group_id
)
--Итоговый запрос с расчетом конверсии
SELECT 
log.hk_group_id,
log.cnt_added_users,
msg.cnt_users_in_group_with_messages,
ROUND(msg.cnt_users_in_group_with_messages / log.cnt_added_users * 100, 2) AS group_conversion
FROM user_group_log AS log
LEFT JOIN user_group_messages AS msg ON log.hk_group_id = msg.hk_group_id
ORDER BY msg.cnt_users_in_group_with_messages / log.cnt_added_users DESC;