FROM python:3.11-slim

WORKDIR /app

# Add these lines for Cloud SDK authentication
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Remove EXPOSE as it's not needed for batch jobs
# Remove CMD and use ENTRYPOINT for better parameter handling
ENTRYPOINT ["python", "-m", "app.main"]