# syntax=docker/dockerfile:1
FROM python:3.12-slim

LABEL maintainer="Astronomer <humans@astronomer.io>"
LABEL org.opencontainers.image.description="Orbiter can land legacy workloads safely down in a new home on Apache Airflow!"

WORKDIR /app

COPY astronomer_orbiter-*.whl astronomer_orbiter.whl
RUN pip install ./astronomer_orbiter.whl

ENTRYPOINT ["orbiter"]
CMD ["--help"]
