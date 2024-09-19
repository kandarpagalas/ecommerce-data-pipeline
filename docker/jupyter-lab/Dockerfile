# FROM jupyter/all-spark-notebook:latest
FROM jupyter/all-spark-notebook:spark-3.3.2

USER root

ENV PYSPARK_SUBMIT_ARGS '--packages org.postgresql:postgresql:42.7.4,com.amazonaws:aws-java-sdk:1.11.950,org.apache.hadoop:hadoop-aws:3.2.0,net.java.dev.jets3t:jets3t:0.9.4 pyspark-shell'


## Download missing jars

# Get hadoop-aws 3.3.2 JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar)

# Get aws-java-sdk-bundle JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar)

# Get hadoop-common 3.3.2  JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.2/hadoop-common-3.3.2.jar)

# Get jets3t JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar)

# Get postgresql-42.7.4 JAR
RUN (cd /usr/local/spark/jars && curl -O https://jdbc.postgresql.org/download/postgresql-42.7.4.jar)

USER $NB_UID

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt