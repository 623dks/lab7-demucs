FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir flask redis jsonpickle

COPY rest-server.py .

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

EXPOSE 5000

CMD ["python", "rest-server.py"]
