FROM python:3.9-slim

# Install dependencies including build tools for compiling packages and Java
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    software-properties-common \
    default-jdk \
    build-essential \
    gcc \
    g++ \
    make \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Symlink python3 and pip3 to python and pip
RUN ln -s /usr/local/bin/python3 /usr/bin/python && \
    ln -s /usr/local/bin/pip3 /usr/bin/pip

# Install Spark 3.4.1 (Hadoop 3 version)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark-3.4.1-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy requirements file
COPY requirements.txt /tmp/

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Verify Jupyter installation and create symlinks if needed
RUN which jupyter || (echo "Jupyter not found, installing directly..." && pip install jupyter notebook)
RUN ln -sf /usr/local/bin/jupyter /usr/bin/jupyter || true

# Copy your application code
COPY ./app /app

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set working directory
WORKDIR /app

# Expose Jupyter port
EXPOSE 8888

# Use entrypoint script to set JAVA_HOME dynamically and start app
ENTRYPOINT ["/entrypoint.sh"]
