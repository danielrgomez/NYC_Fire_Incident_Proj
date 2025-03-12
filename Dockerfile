# Use the official Jupyter base image. This was used when testing in Jupyter Notebook:
#FROM jupyter/base-notebook
#RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary pyarrow argparse sodapy retry
##datetime timedelta apache-airflow
#__________________________________________________________________________________________

#FROM python:3.9
#
##Install additional packages
#RUN pip install pandas sqlalchemy psycopg2 pyarrow argparse sodapy retry tenacity 
#WORKDIR /app
#COPY ./etl/pull_fire_incidents.py pull_fire_incidents.py
#
#ENTRYPOINT ["python","pull_fire_incidents.py"]


# Stage 1: Build dependencies
FROM python:3.9

# Install additional packages
RUN pip install pandas sqlalchemy psycopg2 pyarrow argparse sodapy retry tenacity apache-airflow

# Copy application code
WORKDIR /app
COPY ./dags/pull_fire_incidents.py pull_fire_incidents.py

# Add user airflow_user with UID 50000 and add it to the existing group with GID 0
RUN useradd -u 50000 -g 0 -m airflow_user && \
    chown airflow_user:root pull_fire_incidents.py && \
    chmod +x pull_fire_incidents.py

# Switch to the user with UID 50000
USER 50000:0

# Install any other dependencies
#RUN pip install -r /opt/airflow/dags/requirements.txt

# Define entrypoint
ENTRYPOINT ["python", "pull_fire_incidents.py"]