# syntax=docker/dockerfile:1
FROM python:3.9-slim-buster as base

FROM base as builder

WORKDIR /install
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y curl make automake gcc g++ subversion python3-dev && rm -rf /var/lib/apt/lists/*
COPY scripts/service_requirements.txt requirements.txt
RUN pip3 install --prefix=/install -r requirements.txt

ARG MAXMIND_LICENCE_KEY
RUN mkdir maxmind
COPY scripts/maxmind/download.sh maxmind/download.sh
WORKDIR maxmind
RUN chmod +x download.sh && ./download.sh ${MAXMIND_LICENCE_KEY}

FROM base
ENV FLASK_APP=parser_service.py
ENV FLASK_RUN_HOST=0.0.0.0

COPY --from=builder /install /usr/local
WORKDIR /app

COPY --from=builder /install/maxmind maxmind
COPY scripts/parsing parsing
COPY scripts/parser_service.py .


CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=9000"]