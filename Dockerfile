FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer ./producer
COPY consumer ./consumer
COPY dashboard ./dashboard
ENV PYTHONUNBUFFERED=1