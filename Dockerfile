# Start from the official Apache Airflow image
FROM apache/airflow:3.1.1

# Copy requirements file
COPY requirements.txt /

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Add custom Python path so Airflow can find local modules
ENV PYTHONPATH="/root/datascience:${PYTHONPATH}"
