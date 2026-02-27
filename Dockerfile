FROM apache/airflow:2.9.3-python3.10

USER root

# Java (bắt buộc cho Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python libraries
RUN pip install --no-cache-dir -r /requirements.txt
