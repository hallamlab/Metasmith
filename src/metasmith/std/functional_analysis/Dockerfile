FROM alpine:3.22

RUN apk add --no-cache \
    uv \
    curl \
    alpine-sdk

WORKDIR /runner
RUN uv init --python 3.13
RUN uv add \
    polars \
    plotly
