CREATE DATABASE airflow;
CREATE USER 'airflow'@'%';
GRANT ALL PRIVILEGES ON airflow.* To 'airflow'@'%' IDENTIFIED BY 'somepassword';