# CSV to PostgreSQL ETL Pipeline 
This project implements Python and Pandas to extract raw information from a .csv file and clean it. After that the data is staged in a PostgreSQL table, then it is merged into a production table.
The set of steps is orchestrated using Apache Airflow and executed in a containerized environment using Docker and Docker Compose.
## Arquitecture
<img width="1309" height="319" alt="image" src="https://github.com/user-attachments/assets/dc6d3bce-7d51-4ece-b2da-8ae86dcc3f28" />

## Technology Stack
* Python
* Pandas
* PostgreSQL
* Apache Airflow
* Docker
* Docker Compose
* Parquet
