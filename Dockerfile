# syntax=docker/dockerfile:1
FROM python:3.12-slim

LABEL maintainer="Astronomer <humans@astronomer.io>"
LABEL org.opencontainers.image.description="Orbiter can land legacy workloads safely down in a new home on Apache Airflow!"

WORKDIR /app

COPY astronomer_orbiter-*-py3-none-any.whl .
RUN pip install --no-cache-dir ./astronomer_orbiter-*-py3-none-any.whl

ENTRYPOINT ["orbiter"]
CMD ["--help"]
