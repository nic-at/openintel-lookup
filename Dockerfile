FROM tiangolo/uvicorn-gunicorn:python3.8-alpine3.10

RUN apk --no-cache add --virtual \
      build-dependencies \
      build-base \
      curl \
      yarn

## Setup python packages
COPY ./app/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

## Setup js packages
COPY ./app/package.json /app/package.json
COPY ./app/yarn.lock /app/yarn.lock
RUN cd /app && yarn install --modules-folder ./node_modules

COPY ./app/ /app/

HEALTHCHECK --interval=30s --timeout=3s  CMD curl -f http://localhost:$PORT/test/ping || exit 1
