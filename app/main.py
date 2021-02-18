#!/usr/bin/env python3

import time
import json
import logging
import ipaddress
import datetime
import io
import csv

from typing import Optional, List
from pydantic import IPvAnyAddress, BaseModel

from fastapi import FastAPI, Query, HTTPException, Request, Form
from fastapi.exceptions import ValidationError
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import config

from db import HadoopDBConnection
from openintel_sql_calls import (
    openintel_select_domains_by_ips,
    openintel_select_ips_by_domains,
    openintel_select_ips_by_mx_records,
    openintel_select_measurements_by_name_and_ip_or_type
)


class CSVModel(BaseModel):
    data: str


###############################################################################
# Global vars

# Impyla
DBConnection = None
cursor = None

# Fast API
version = config['version']
description = """
## A RESTful API for OpenINTEL
"""
baseurl = config['baseurl']
app = FastAPI(title="Open INTEL API",
              docs_url="/docs",
              version=version,
              description=description)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Set up packages installed by yarn
app.mount("/vendor/bootstrap", StaticFiles(directory="node_modules/bootstrap/dist"), name="bootstrap")
app.mount("/vendor/jquery", StaticFiles(directory="node_modules/jquery/dist"), name="jquery")
app.mount("/vendor/datatables", StaticFiles(directory="node_modules/datatables/media"), name="datatables")
app.mount("/vendor/datatables-buttons", StaticFiles(directory="node_modules/datatables.net-buttons/js"), name="datatables-buttons")
app.mount("/vendor/datatables-buttons-dt/js", StaticFiles(directory="node_modules/datatables.net-buttons-dt/js"), name="datatables-buttons-dt-js")
app.mount("/vendor/datatables-buttons-dt/css", StaticFiles(directory="node_modules/datatables.net-buttons-dt/css"), name="datatables-buttons-dt-css")
app.mount("/vendor/popper", StaticFiles(directory="node_modules/@popperjs/core/dist/umd"), name="popper")

HELPSTR = """
# About

This app provides a simple RESTful API for OpenINTEL data
Please see %s

""" % (baseurl,)

# SETUP LOGGING
logger = logging.getLogger('simple_example')
logger.setLevel(config["loglevel"])
# create console handler and set level to debug
logger_sh = logging.StreamHandler()
logger_sh.setLevel(config["loglevel"])
formatter = logging.Formatter(
    fmt='[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %z'
)
logger_sh.setFormatter(formatter)
logger.addHandler(logger_sh)

#############
# DB specific functions


@app.on_event('startup')
def startup():
    global DBConnection
    logger.info("starting up")
    DBConnection = HadoopDBConnection(logger, config)
    return


@app.on_event('shutdown')
def shutdown():
    global DBConnection
    # do shutdown stuff like closing a DB connection
    logger.info('shutting down....')


###############################################################################

@app.get("/api/v1/about", response_class=HTMLResponse)
async def getIndex(request: Request):
    return templates.TemplateResponse("about_en.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
async def showDomain2IP(request: Request, queryType: str = "ips_by_domains"):
    if queryType == "ips_by_domains":
        columns = select_ips_by_domains_get_columns()
        inputTextLabel = "Domains"
        headline = "Domains --> IPs"
        inputplaceholder = "nic.at \ncert.at"
        inputfieldtype = "textarea"
    elif queryType == "domains_by_ips":
        columns = select_domains_by_ips_get_columns()
        inputTextLabel = "IPs"
        headline = "IPs --> Domains"
        inputplaceholder = "1.2.3.4 \n2001:67c:10b8::98"
        inputfieldtype = "textarea"
    elif queryType == "ips_by_mx_pattern":
        columns = select_ips_by_mx_pattern_get_columns()
        inputTextLabel = "MX SQL LIKE Pattern"
        headline = "MX SQL LIKE Pattern"
        inputplaceholder = "%.mailservice.example"
        inputfieldtype = "input"
    else:
        raise HTTPException("unknown query type")
    return templates.TemplateResponse(
        "openintellookup.html",
        {
            "request": request,
            "columns": columns,
            "queryType": queryType,
            "inputTextLabel": inputTextLabel,
            "headline": headline,
            "inputplaceholder": inputplaceholder,
            "inputfieldtype": inputfieldtype,
            "todayString": datetime.date.today(),
            "startDate": datetime.date.today() - datetime.timedelta(days=1),
            "endDate": datetime.date.today() - datetime.timedelta(days=1)
        }
    )

# API endpoint functions


@app.get('/help')
@app.get('/api/v1')
async def help():
    return {'help': HELPSTR}


@app.get("/meta/version", tags=["Meta"])
async def meta_version():
    return {"version": "%s" % config['version']}


@app.get("/test/ping",
         name="Ping test",
         summary="Run a ping test, to check if the service is running",
         tags=["Tests"])
async def ping():
    """ this is a simple liveliness check """
    return {"message": "Pong!"}


@app.get("/test/self-test",
         name="Self-test",
         summary="Run a self-test",
         tags=["Tests"])
async def selftest():
    """ here you should trigger a unit test or similar """
    logger.debug("performing self test")
    start = time.time()
    try:
        results = await DBConnection.execute_query_async('SELECT * FROM %s LIMIT 1' % config["DB"])
    except Exception as e:
        logger.error("error while executing DB query - %s" % str(e))
        return {"message": "FAIL"}
    dt = time.time() - start
    if len(results['rows']) == 1:
        return {"message": "OK", "query_time": dt}
    else:
        return {"message": "FAIL"}


@app.get(
    "/api/v1/domains_by_ip/{ip}",
    name="Find domains pointing to IP",
    summary="Finds the domains that point to {ip}",
    tags=["Find domains"]
)
async def select_domains_by_ip(
    ip: IPvAnyAddress,
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100
):
    start = time.time()
    results = await openintel_select_domains_by_ips(
        DBConnection,
        logger,
        [ip, ],
        date_from,
        date_to,
        limit
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/domains_by_ip/{ip} completed in %f s" % dt)
    return results


@app.post(
    "/api/v1/domains_by_ips/",
    name="Find domains pointing to a list of IPs",
    summary="Finds the domains that point to the posted IPs",
    tags=["Find domains"]
)
async def select_domains_by_ips(
    ips: List[IPvAnyAddress],
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100
):
    start = time.time()
    results = await openintel_select_domains_by_ips(
        DBConnection,
        logger,
        ips,
        date_from,
        date_to,
        limit
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/domains_by_ips/ completed lookup if %i domains in %f s" % (len(ips), dt))
    return results


def select_domains_by_ips_get_columns():
    return [
        "ip_address",
        "asn",
        "pointing_query_name",
        "pointing_response_name",
        "query_name",
        "response_name",
        "pointing_record_type",
        "pointing_record_field",
        "first_ts",
        "last_ts"
    ]


@app.get(
    "/api/v1/ips_by_domain/{domain}",
    name="Find IPs listed under domain",
    summary="Finds the IPS that are pointed to by domain {domain}.",
    tags=["Find IPs"]
)
async def select_ips_by_domain(
    domain: str,
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100
):
    start = time.time()
    results = await openintel_select_ips_by_domains(
        DBConnection,
        logger,
        [domain, ],
        date_from,
        date_to,
        limit
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/domains_by_ip/{ip} completed in %f s" % dt)
    return results


@app.post(
    "/api/v1/ips_by_domains/",
    name="Find IPs listed under domain",
    summary="Finds the IPS that are pointed to by domain a list of domains.",
    tags=["Find IPs"]
)
async def select_ips_by_domains(
    domains: List[str],
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100
):
    start = time.time()
    results = await openintel_select_ips_by_domains(
        DBConnection,
        logger,
        domains,
        date_from,
        date_to,
        limit
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/domains_by_ip/{ip} completed lookup of %i domains in %f s" % (len(domains), dt))
    return results


def select_ips_by_domains_get_columns():
    return [
        "domain_name",
        "record_type",
        "record_names",
        "ip_address",
        "asn",
        "country",
        "first_ts",
        "last_ts"
    ]


@app.post(
    "/api/v1/ips_by_mx_pattern/",
    name="Find MX records and associated IDs by MX pattern",
    summary="Finds MX records and associated IDs by MX pattern",
    tags=["Find IPs"]
)
async def select_ips_by_mx_pattern(
    pattern:  List[str],
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100
):
    start = time.time()
    results = await openintel_select_ips_by_mx_records(
        DBConnection,
        logger,
        pattern[0],
        date_from,
        date_to,
        limit
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/ips_by_mx_pattern/ completed in %f s" % dt)
    return results


def select_ips_by_mx_pattern_get_columns():
    return [
        "query_name",
        "response_name",
        "mx_address",
        "query_type",
        "ip_address",
        "first_ts",
        "last_ts"
    ]


@app.get(
    "/api/v1/measurements_by_domain/{domain}",
    name="Find all measurements per domain",
    summary="Find all measurements per domain over a timeframe, optionally filtered by IP or record_type",
    tags=["Measurement Histories"]
)
async def select_measurements_by_domain(
    domain: str,
    ip: Optional[IPvAnyAddress] = None,
    type: Optional[str] = None,
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100,
    full: Optional[bool] = False
):
    start = time.time()
    results = await openintel_select_measurements_by_name_and_ip_or_type(
        DBConnection,
        logger,
        domain,
        date_from,
        date_to,
        limit,
        ip=ip,
        type=type,
        full=full
    )
    results["data"] = results["rows"]
    del(results["rows"])
    dt = time.time() - start
    logger.info("/api/v1/measurements_by_domain_ip/{domain} completed in %f s" % dt)
    return results


@app.get(
    "/api/v1/measurements_by_domain/{domain}/csv",
    name="Find all measurements per domain",
    summary="Find all measurements per domain over a timeframe, optionally filtered by IP or record_type as a csv file",
    tags=["Measurement Histories"],
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Measurement history of domain {domain}",
            "content": {
                "text/csv": {}
            },
        },
    }
)
async def select_measurements_by_domain_csv(
    domain: str,
    ip: Optional[IPvAnyAddress] = None,
    type: Optional[str] = None,
    date_from: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    date_to: Optional[datetime.date] = datetime.date.today() - datetime.timedelta(days=1),
    limit: int = 100,
    full: Optional[bool] = False
):
    logger.debug(domain)
    results = await select_measurements_by_domain(
        domain,
        ip,
        type,
        date_from,
        date_to,
        limit,
        full
    )

    stream = io.StringIO()

    csv_writer = csv.writer(
        stream,
        delimiter=';',
        quotechar='"'
    )

    for i, line in enumerate(results["data"]):
        if i == 0:
            fields = line.keys()
            csv_writer.writerow(fields)
        values = [line[key] for key in fields]
        csv_writer.writerow(values)

    stream.seek(0)

    response = StreamingResponse(
        stream,
        media_type="text/csv"
    )

    csv_filename = "openintel_msm_history_%s" % domain

    if ip is not None:
        csv_filename += "_ip_%s" % (ip)

    if type is not None:
        csv_filename += "_type_%s" % (type)

    csv_filename += "_%s_%s" % (date_from.strftime("%Y-%m-%d"), date_from.strftime("%Y-%m-%d"))
    csv_filename += ".csv"

    response.headers["Content-Disposition"] = "attachment; filename=%s" % csv_filename

    print(csv_filename)

    return response
