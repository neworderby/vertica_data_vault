### Описание
Data Vault Social Media

### Инструменты
- Vertica
- Airflow

### Структура репозитория
- `/src/dags` - импорт из S3, загрузка данных в STG и DDS
- `/src/sql` - DDL таблиц (хабы, линки, сателлиты, CTE с расчетом конверсии групп)
