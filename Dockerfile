FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
ENV HADOOP_USER_NAME=root

# Install OpenJDK 11 and dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark 4.0.1
RUN curl -s https://archive.apache.org/dist/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz | tar xz -C /opt \
    && mv /opt/spark-4.0.1-bin-hadoop3 /opt/spark \
    && ln -s /opt/spark /usr/local/spark

# Install project dependencies
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen --no-dev

# Copy project files
COPY . .

# Set working directory
WORKDIR /app

# Default command
CMD ["python", "main.py", "--remote"]