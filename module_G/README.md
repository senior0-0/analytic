# Установить необходимые пакеты
pip3 install pandas numpy psycopg2-binary sqlalchemy apache-airflow
# Для работы с большими данными
pip3 install pyspark

# Создание базы данных
sudo -u postgres psql << SQL
CREATE DATABASE transport_dwh;
CREATE USER user WITH PASSWORD '0000';
GRANT ALL PRIVILEGES ON DATABASE transport_dwh TO user;
SQL

# Создание таблиц
psql -d transport_dwh -U user -f sql/create_tables.sql