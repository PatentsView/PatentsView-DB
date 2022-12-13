CREATE DATABASE airflow CHARACTER SET UTF8mb3 COLLATE utf8_general_ci;
CREATE USER 'airflow'@'%' IDENTIFIED BY '{somepassword}';
GRANT ALL PRIVILEGES ON airflow.* To 'airflow'@'%';