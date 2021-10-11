FROM datamechanics/spark:3.1.1-hadoop-3.2.0-java-11-scala-2.12-python-3.8-dm14
WORKDIR ...
COPY Spark-Mongo-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/
USER root
