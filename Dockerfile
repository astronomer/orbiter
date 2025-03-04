FROM debian:stable-slim
LABEL maintainer="Astronomer <humans@astronomer.io>"
LABEL org.opencontainers.image.description="Orbiter can land legacy workloads safely down in a new home on Apache Airflow!"

ARG ORBITER_BINARY=./orbiter-linux-x86_64

WORKDIR /app

COPY ${ORBITER_BINARY_PATH} orbiter
RUN chmod +x ./orbiter
ENTRYPOINT ["./orbiter"]
CMD ["--help"]
