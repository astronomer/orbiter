FROM debian:stable-slim
LABEL maintainer="Astronomer <humans@astronomer.io>"
LABEL org.opencontainers.image.source="https://github.com/astronomer/orbiter"
LABEL org.opencontainers.image.description="Orbiter can land legacy workloads safely down in a new home on Apache Airflow!"
LABEL org.opencontainers.image.license="Apache-2.0"
LABEL org.opencontainers.image.title="Orbiter"

WORKDIR /app

COPY ./orbiter-linux-x86_64 orbiter
RUN chmod +x ./orbiter
ENTRYPOINT ["./orbiter"]
CMD ["--help"]
