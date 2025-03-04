# syntax=docker/dockerfile:1
ARG ORBITER_WHEEL=dist/astronomer_orbiter-*-py3-none-any.whl

FROM python:3.12-slim

LABEL maintainer="Astronomer <humans@astronomer.io>"
LABEL org.opencontainers.image.description="Orbiter can land legacy workloads safely down in a new home on Apache Airflow!"

WORKDIR /app

COPY ${ORBITER_WHEEL} .
RUN pip install ./astronomer_orbiter-*.whl

ENTRYPOINT ["orbiter"]
CMD ["--help"]
