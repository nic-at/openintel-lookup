# openintel-lookup

## Overview
openintel-lookup provides a user interface and an API that allows querying 
[OpenINTEL](https://openintel.nl) data.

OpenINTEL is a project by the [University of Twente](https://www.utwente.nl). The project's
goal is to actively query all DNS records within a number of DNS zones in order to build a
high quality data series that captures the state of the DNS system over time. An
[in depth description of the measurements taken](https://openintel.nl/background/) and
[the data that is available](https://openintel.nl/data-access/) can be found on the OpenINTEL
homepage.

In addition the following paper provides details on the measurement methodology:

> R. van Rijswijk-Deij, M. Jonker, A. Sperotto and A. Pras, "A High-Performance, Scalable Infrastructure for
> Large-Scale Active DNS Measurements," in IEEE Journal on Selected Areas in Communications, vol. 34, no. 6,
> pp. 1877-1888, June 2016, doi: [10.1109/JSAC.2016.2558918](https://dx.doi.org/10.1109/JSAC.2016.2558918).

openintel-lookup provides three basic queries:
+ Domains --> IPs: find the IP addresses associated with a domain name
+ IPs --> Domains: find the domain names associated with an IP address
+ MX SQL LIKE pattern: find IP addresses and domain names associated with an MX record pattern.

## Requirements
In order to use openintel-lookup you require
+ an [Apache hadoop](https://hadoop.apache.org/) database that contains the dataset you want to query (no data is provided with this repository),
+ [Apache impala](https://impala.apache.org/) to execute the queries,
+ and a way to build and run [docker](https://www.docker.com/) containers.

The python and JavaScript dependencies can be found in [app/requirements.txt](app/requirements.txt)
and [app/package.json](app/package.json), respectively. The openintel-lookup docker container is build on
top of the [tiangolo/uvicorn-gunicorn](https://hub.docker.com/r/tiangolo/uvicorn-gunicorn) container.

No data is provided with this repository. The required data must be obtained from the OpenINTEL project directly.

## Setup
OpenINTEL uses [Apache impala](https://impala.apache.org/) to query the dataset and expects that the OpenINTEL table is partitioned by the measurement's day, month, and year. The create table statement must look like this:
``` sql
default> show create table openintel.measurements;
CREATE EXTERNAL TABLE openintel.measurements (
   query_type STRING,
   query_name STRING,
   -- ... other openintel columns
 )
 PARTITIONED BY (
   year STRING,
   month STRING,
   day STRING
 )
 -- ... other table properties
```

The [docker-compose.yml] file  makes building and deploying the service on your local docker installation easy.

+ First create a `.env` file:
    ```
    cp env.example .env
    ```

+ Open it with your favorite editor and adjust the settings as required. Then build the service with
    ```
    docker-compose build
    ```
    and start it
    ```
    docker-compose up -d
    ```

By default openintel-lookup is now available under http://localhost:8888 or the equivalent address if you are not running the service on your local machine.

The API documentation is available under /docs.

## Load testing
The [benchmark](benchmark/) folder contains a configuration for use with [artillery](https://artillery.io), a
load testing tool.

## License
openintel-lookup is published under the MIT License. Please see the enclosed
[LICENSE](LICENSE) for further information.


## Funded by
This project was partially funded by the CEF framework

![CEF logo](en_horizontal_cef_logo_2.png)
