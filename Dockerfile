# Use the latest Jupyter Notebook image
FROM jupyter/base-notebook:latest
#
## Install Java (required for PySpark)
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Install PySpark
RUN pip install pyspark findspark pandas sqlalchemy psycopg2-binary pyarrow argparse sodapy retry tenacity


# Set working directory inside the container
WORKDIR /home/jovyan/work
COPY ./Jupyter_Notebook /home/jovyan/work

## Expose Jupyter Notebook port
EXPOSE 8888




# Use the official Jupyter base image. This was used when testing in Jupyter Notebook:
#FROM jupyter/base-notebook
#WORKDIR /home/jovyan/work
#COPY ./Jupyter_Notebook /home/jovyan/work
#
##dgomezpe/My_Projects/DE_Zoomcamp/NYC_Fire_Incident_Proj/
##RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary pyarrow argparse sodapy retry
## Install Jupyter and other dependencies
##RUN pip install --no-cache-dir jupyter notebook pandas sqlalchemy psycopg2-binary pyarrow argparse sodapy retry
#
#EXPOSE 8888
#
#CMD ["start-notebook.sh"]
## Expose the default Jupyter port
#EXPOSE 8888

# Run the Jupyter Notebook server and set the directory
#CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--notebook-dir=/."]

#datetime timedelta apache-airflow
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
#FROM python:3.9
#
## Install additional packages
#RUN pip install pandas sqlalchemy psycopg2 pyarrow argparse sodapy retry tenacity apache-airflow
#
## Copy application code
#WORKDIR /app
#COPY ./dags/pull_fire_incidents.py pull_fire_incidents.py
#COPY ./dags/transformations_pyspark.py transformations_pyspark.py
#
## Add user airflow_user with UID 50000 and add it to the existing group with GID 0
#RUN useradd -u 50000 -g 0 -m airflow_user && \
#    chown airflow_user:root pull_fire_incidents.py && \
#    chmod +x pull_fire_incidents.py
#
## Switch to the user with UID 50000
#USER 50000:0
#
## Install any other dependencies
##RUN pip install -r /opt/airflow/dags/requirements.txt
#
## Define entrypoint
#ENTRYPOINT ["python", "pull_fire_incidents.py"]