# Use the latest Jupyter Notebook image
FROM jupyter/base-notebook:latest
#
## Install Java (required for PySpark)
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Install PySpark
RUN pip install pyspark findspark pandas sqlalchemy psycopg2-binary pyarrow argparse sodapy retry tenacity boto3


# Set working directory inside the container
WORKDIR /home/jovyan/work
COPY ./Jupyter_Notebook /home/jovyan/work

## Expose Jupyter Notebook port
EXPOSE 8888


