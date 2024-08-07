FROM python:3.11-slim

# Install dependencies
# kafka: librdkafka-dev gcc 
# psycopg: libpq-dev python3-dev
RUN apt-get update && apt-get install -y librdkafka-dev gcc libpq-dev python3-dev pkg-config
RUN pip install --upgrade pip
COPY cluo/ cluo/
COPY examples/ examples/
COPY pyproject.toml pyproject.toml
COPY README.md README.md
EXPOSE 9090
RUN pip install .