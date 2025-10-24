FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    redis \
    jsonpickle \
    minio \
    demucs

COPY worker-server.py .

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV MINIO_HOST=minio-service.minio-ns:9000
ENV MINIO_USER=rootuser
ENV MINIO_PASS=rootpass123

CMD ["python", "worker-server.py"]
