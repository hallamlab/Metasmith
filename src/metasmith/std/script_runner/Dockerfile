FROM python:3.13

RUN apt-get update
RUN apt-get install -y \
    curl

WORKDIR /runner
RUN python -m venv venv/
RUN venv/bin/pip install polars plotly

ENV PATH="/runner/venv/bin:$PATH"

RUN python --version
RUN pip list
