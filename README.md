# Airflow ETL Basics

### Learning how to configure ELT pipelines using Apache Airflow

This DAG is composed of three operators:

1. PythonOperator to grab and wrangle data from Twitter

2. PythonOperator creates a DataFrame and stores it in a local MySQL database

3. BashOperator just echoes a completion notification
  
Scratching the surface of this workflow, so much to learn.
