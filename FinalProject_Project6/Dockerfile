FROM python:3.9-slim

# 1. Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    default-jdk \
    build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# 2. Install Spark 3.4.1 (Hadoop 3)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt && rm spark.tgz

ENV SPARK_HOME=/opt/spark-3.4.1-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# 3. Clean out Jetty 9+ (optional, if you keep Jetty dependencies)
RUN rm -f $SPARK_HOME/jars/jetty-*.jar $SPARK_HOME/jars/jetty-util-*.jar

# 4. Remove Azure + Jetty jars step because we're using mounted Azure File Share
# (No need to download Azure blob storage jars)

# 5. Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 6. Jupyter setup (optional)
RUN pip install notebook
EXPOSE 8888

# 7. Copy app files and entrypoint script
COPY ./app /app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app

# 8. Note: you will mount the Azure File Share at runtime using a Docker volume mount,
#     e.g., docker run -v /mnt/azurefiles:/mnt/azurefiles ...
#     So your app should access files under /mnt/azurefiles inside container

ENTRYPOINT ["/entrypoint.sh"]
