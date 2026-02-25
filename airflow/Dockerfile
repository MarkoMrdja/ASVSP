FROM apache/airflow:2.7.2-python3.11
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt
