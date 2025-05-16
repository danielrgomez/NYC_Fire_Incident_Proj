# ETL Process Configuration Document

## Overview
This project implements a scalable **batch-processing ETL pipeline** to process **Fire Incident Dispatch Data** and **Automated Traffic Volume Counts** sourced from the NYC Open Data Platform. The pipeline efficiently **ingests, transforms, and stores data** across **Postgres** (transactional storage) and **Amazon Redshift** (analytical storage), enabling **data visualization in Power BI** for actionable insights.

## Architecture Diagram
![alt text](https://github.com/danielrgomez/NYC_Fire_Incident_Proj/blob/main/documentation/ArchitectureDiagram.png)

## ETL Process Overview

### 1. Data Extraction & Ingestion (Automated Airflow DAGs in Docker)
- Two **automated Airflow DAGs**, running in a **Docker container**, extract **monthly Fire Incident Dispatch Data** and **Traffic Volume Counts** from the NYC Open Data Platform.
- The extracted data undergoes transformations using **PySpark** before being loaded into a **Postgres database**, serving as the transactional storage layer.

### 2. Data Transfer & Monitoring (Airflow DAG with External Task Sensor)
- A third **Airflow DAG**, equipped with an **External Task Sensor**, monitors the completion of the first two DAGs.
- Once both DAGs finish processing, the third DAG **extracts the processed data from Postgres**, **converts it into CSV files**, and **uploads them to AWS S3**.

### 3. Data Processing in AWS (Glue Jobs & Redshift Integration)
- **AWS Glue Jobs** are triggered automatically upon file arrival in S3:
  - The first two jobs **standardize the individual datasets** and **load them into Amazon Redshift**.
  - The third Glue Job executes **SQL-based transformations**, **joins both datasets** based on **Borough and derived date fields**, and **applies schema consistency** before loading the final table into Redshift.

### 4. Data Analysis & Visualization (Power BI)
- **Power BI** connects to **Amazon Redshift**, generating **interactive dashboards** to analyze **incident dispatch trends and traffic volumes**.
- The visualizations help stakeholders monitor **monthly insights** and make **data-driven decisions**.

## Data Breakdown
- **Fire Incident Dispatch Data:** Captured from the **Starfire Computer-Aided Dispatch System**, tracking **incidents from creation to resolution**. Provides insight into **response times, resource allocation, and emergency patterns**.
- **Automated Traffic Volume Counts:** NYC DOT collects **vehicle volume data** via **Automated Traffic Recorders (ATR)** at key crossings and roadways, assisting in **congestion analysis**.

## Stakeholder Benefits – Power BI Dashboard
- **City Planners & Policy Makers** – Optimize **resource allocation** and **traffic infrastructure planning**.
- **Public Safety Officials & First Responders** – Improve **emergency response strategies**.
- **Transportation Authorities** – Identify **traffic bottlenecks** affecting **emergency routes**.
- **Urban Data Analysts & Researchers** – Gain insights into **fire dispatch trends** and **traffic density** for policy recommendations.
- **Local Communities & Residents** – Understand **city-wide emergency and traffic patterns** for safer mobility.

This structured pipeline enhances **data-driven decision-making**, providing a **real-time, automated workflow** that bridges **transactional and analytical databases** for **urban insights**.

## Power BI Dashboard 
- The Power BI dashboard consists of three tabs, each pulling data from AWS Redshift for structured analysis.
- The tabs provide insights into NYC Fire Incidents, NYC Traffic Data, and a merged dataset, combining both sources for comprehensive comparisons.

![alt text](https://github.com/danielrgomez/NYC_Fire_Incident_Proj/blob/main/documentation/ArchitectureDiagram.png)


**For additional configuration details, refer to the Configuration Document.**


