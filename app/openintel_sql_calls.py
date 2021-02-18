import time
import ipaddress
import datetime
from config import config
from collections import defaultdict

next_level = defaultdict(lambda: None)
next_level['year'] = 'month'
next_level['month'] = 'day'


def get_field():
    return "CAST(%(prefix)s.%(field)s AS integer)"


def get_value(prefix):
    return "%%%%(%s_%%(field)s)s" % prefix


def _get_date_where_sub_clauses(prefix, field, from_, to, range_='full', level=0):
    if to < from_:
        return "FALSE"
    spec = {
        "prefix": prefix,
        "field": field
    }
    nl = next_level[field]
    if range_ == 'full' and (getattr(to, field) - getattr(from_, field) <= 0):
        # if the value of the field is the same we can bypass the more complicated logic below
        where_clause = "(%s = %s)" % (get_field(), get_value("from"))
        if nl is not None:
            where_clause += " AND %s" % _get_date_where_sub_clauses(prefix, nl, from_, to, 'full', level=level+1)
    else:
        # put together the statement for this line
        # three cases:
        #   first period - need to include where clause of next level
        #   periods in between - take alle entries
        #   last period - need to include where clause of next level
        # if the range_ is open to one side, only include the side opposite to the open side and skip the other one

        _cases = []

        # Left/Right end period - here we have to consider the levels below, i.e. if this is month
        # then a condition on the days must be included
        if range_ == 'open-right' or range_ == 'full':
            _clauses = ["(%s = %s)" % (get_field(), get_value("from")), ]
            if nl is not None:
                _clauses.append(_get_date_where_sub_clauses(prefix, nl, from_, to, 'open-right', level=level+1))
            _cases.append(" AND ".join(_clauses))

        if range_ == 'open-left' or range_ == 'full':
            _clauses = ["(%s = %s)" % (get_field(), get_value("to")), ]
            if nl is not None:
                _clauses.append(_get_date_where_sub_clauses(prefix, nl, from_, to, 'open-left', level=level+1))
            _cases.append(" AND ".join(_clauses))

        # in-between periods - in these periods we take the whole periods so no additional conditions
        # are required
        if range_ == 'open-right':
            _cases.append("(%s > %s)" % (get_field(), get_value("from")))
        elif range_ == 'open-left':
            _cases.append("(%s < %s)" % (get_field(), get_value("to")))
        elif range_ == 'full':
            _cases.append("((%s > %s) AND (%s < %s))" % (get_field(), get_value("from"), get_field(), get_value("to")))
        else:
            raise ValueError("unknown range_ '%s'" % range_)

        # A date of interest can be in either of the above cases so we join them with OR
        where_clause = ' OR '.join(map(lambda s: "(%s)" % s, _cases))

    if level == 0:
        return where_clause % spec
    else:
        return "(%s)" % ((where_clause % spec).replace('%', '%%'))


def get_date_where_clause(prefix, from_, to):
    if not isinstance(from_, datetime.date):
        raise ValueError("from_ must be a datetime.date")
    if not isinstance(to, datetime.date):
        raise ValueError("to must be a datetime.date")
    return _get_date_where_sub_clauses(prefix, "year", from_, to)


def get_spec_for_clause(spec):
    _res = dict(spec)
    keys = list(_res.keys())
    for key in keys:
        if isinstance(_res[key], datetime.date) or isinstance(_res[key], datetime.datetime):
            _res[key + "_day"] = _res[key].day
            _res[key + "_month"] = _res[key].month
            _res[key + "_year"] = _res[key].year
    return _res


async def openintel_select_domains_by_ips(
    DBConnection,
    logger,
    ip_list,
    date_from,
    date_to,
    limit
):
    ip4_list = []
    ip6_list = []
    for _ip in ip_list:
        try:
            ip_address = ipaddress.ip_address(_ip)
        except ValueError:
            raise ValueError("'%s' is not an IP address" % _ip)
        if ip_address.version == 4:
            ip4_list.append(ip_address.compressed)
        elif ip_address.version == 6:
            ip6_list.append(ip_address.compressed)
        else:
            raise RuntimeError("unexpected error: ip_address version != 4 or 6 found")
    if len(ip4_list) > 0:
        logger.debug("fetching domains for IPv4 addresses: %s" % ', '.join(map(str, ip4_list)))
    if len(ip6_list) > 0:
        logger.debug("fetching domains for IPv6 addresses: %s" % ', '.join(map(str, ip6_list)))

    result_dict = {
        'queried_ipv4': ip4_list,
        'queried_ipv6': ip6_list,
        'queried_interval': [date_from, date_to]
    }

    if len(ip4_list) == 0 and len(ip6_list) == 0:
        logger.debug("empty IP list - skipping DB call")
        result_dict['names'] = []
        return result_dict

    ip_where_clause = []
    if len(ip4_list) > 0:
        _clause = "(l2.ip4_address IN (%s))" % ','.join(
            ["%%(__o%i)s" % i for i in range(len(ip4_list))]
        )
        ip_where_clause.append(_clause)

    if len(ip6_list) > 0:
        _clause = "(l2.ip6_address IN (%s))" % ','.join(
            ["%%(__n%i)s" % i for i in range(len(ip6_list))]
        )
        ip_where_clause.append(_clause)

    ip_where_clause = ' OR '.join(ip_where_clause)

    params = {
        'from': date_from,
        'to': date_to
    }

    date_where_clause = get_date_where_clause("l1", params["from"], params["to"])
    params = get_spec_for_clause(params)

    query = """
        SELECT
            IF(nonnullvalue(l2.ip4_address), l2.ip4_address, l2.ip6_address) AS ip_address,
            l2.asn,
            SUBSTR(l1.query_name, 1, LENGTH(l1.query_name) - 1) AS pointing_query_name,
            SUBSTR(l1.response_name, 1, LENGTH(l1.response_name) - 1) AS pointing_response_name,
            SUBSTR(l2.query_name, 1, LENGTH(l2.query_name) - 1) AS query_name,
            SUBSTR(l2.response_name, 1, LENGTH(l2.response_name) - 1) AS response_name,
            l1.query_type AS pointing_record_type,
            l1.field AS pointing_record_field,
            to_timestamp(CAST(MIN(l2.time)/1000 AS BIGINT)) AS first_ts,
            to_timestamp(CAST(MAX(l2.time)/1000 AS BIGINT)) AS last_ts
        FROM %(db)s AS l2
        JOIN (
                      SELECT query_type, query_name, response_name, query_name AS ref_query_name, year, day, month, time, 'query_name' AS field FROM %(db)s WHERE (ip4_address IS NOT NULL) OR (ip6_address IS NOT NULL)
            UNION ALL SELECT query_type, query_name, response_name, mx_address AS ref_query_name, year, day, month, time, 'mx_address' AS field FROM %(db)s WHERE mx_address IS NOT NULL
            UNION ALL SELECT query_type, query_name, response_name, ns_address AS ref_query_name, year, day, month, time, 'ns_address' AS field FROM %(db)s WHERE ns_address IS NOT NULL
            UNION ALL SELECT query_type, query_name, response_name, cname_name AS ref_query_name, year, day, month, time, 'cname_name' AS field FROM %(db)s WHERE cname_name IS NOT NULL
            UNION ALL SELECT query_type, query_name, response_name, dname_name AS ref_query_name, year, day, month, time, 'dname_name' AS field FROM %(db)s WHERE dname_name IS NOT NULL
            UNION ALL SELECT query_type, query_name, response_name, soa_mname  AS ref_query_name, year, day, month, time, 'soa_mname'  AS field FROM %(db)s WHERE soa_mname  IS NOT NULL
        ) AS l1 ON (l2.query_name = l1.ref_query_name)
        WHERE
            (
                %(ip_where_clause)s
            )
            AND
            (
                %(date_where_clause)s
            )
            AND
            (
                l2.year = l1.year
                AND l2.month = l1.month
                AND l2.day = l1.day
            )
        GROUP BY l2.ip4_address, l2.ip6_address, l2.query_name, l2.response_name, l2.asn, l1.query_type, l1.query_name, l1.response_name, l1.field
        ORDER BY first_ts
    """ % {
        'db': config["DB"],
        'ip_where_clause': ip_where_clause,
        'date_where_clause': date_where_clause
    }

    params.update({
        '__o%i' % i: ip4
        for i, ip4 in enumerate(ip4_list)
    })
    params.update({
        '__n%i' % i: ip6
        for i, ip6 in enumerate(ip6_list)
    })

    if limit > 0:
        query += "\nLIMIT %(limit)s"
        params["limit"] = limit

    results = await DBConnection.execute_query_async(
        query,
        params,
        {'paramstyle': 'format'},
        query_name="openintel_domains_by_ips"
    )

    result_dict.update(results)

    if len(result_dict["rows"]) > 0:
        result_dict['columns'] = list(result_dict['rows'][0].keys())

    return result_dict


async def openintel_select_ips_by_domains(
    DBConnection,
    logger,
    domains,
    date_from,
    date_to,
    limit
):
    domains = list(map(str, domains))
    result_dict = {
        'queried_domains': domains,
        'queried_interval': [date_from, date_to]
    }
    if len(domains) == 0:
        logger.debug("empty domain list - skipping DB call")
        result_dict["ips"] = []
        return result_dict

    params = {
        'from': date_from,
        'to': date_to
    }

    date_clause = get_date_where_clause("l1", params["from"], params["to"])
    params = get_spec_for_clause(params)

    query = """
        SELECT
            SUBSTR(l1.query_name, 1, LENGTH(l1.query_name) - 1) AS domain_name,
            l1.query_type AS record_type,
            GROUP_CONCAT(DISTINCT SUBSTR(l2.query_name, 1, LENGTH(l2.query_name) - 1), ',') AS record_names,
            IF(nonnullvalue(l2.ip4_address), l2.ip4_address, l2.ip6_address) AS ip_address,
            l2.asn,
            l2.country,
            to_timestamp(CAST(MIN(l1.time)/1000 AS BIGINT)) AS first_ts,
            to_timestamp(CAST(MAX(l1.time)/1000 AS BIGINT)) AS last_ts
        FROM %(db)s AS l2
        JOIN (
            -- do one self join to find entries that are not referenced from another MX/NS/CNAME/DNAME/SOA record
                      SELECT query_type, query_name AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE (ip4_address IS NOT NULL) OR (ip6_address IS NOT NULL)
            UNION ALL SELECT query_type, mx_address AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE mx_address IS NOT NULL
            UNION ALL SELECT query_type, ns_address AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE ns_address IS NOT NULL
            UNION ALL SELECT query_type, cname_name AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE cname_name IS NOT NULL
            UNION ALL SELECT query_type, dname_name AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE dname_name IS NOT NULL
            UNION ALL SELECT query_type, soa_mname  AS ref_query_name, year, day, month, query_name, time FROM %(db)s WHERE soa_mname  IS NOT NULL
        ) AS l1 ON (l2.query_name = l1.ref_query_name)
        WHERE
            (l1.query_name IN (%(domain_names)s))
            AND (
                (l2.ip4_address IS NOT NULL)
                OR (l2.ip6_address IS NOT NULL)
            )
            AND
            (
                %(date_clause)s
            )
            AND
            (
                l2.year = l1.year
                AND l2.month = l1.month
                AND l2.day = l1.day
            )
        GROUP BY l1.query_name, l1.query_type, l2.ip4_address, l2.ip6_address, l2.asn, l2.country
        ORDER BY l1.query_name
    """ % {
        'db': config["DB"],
        'date_clause': date_clause,
        'domain_names': ','.join(
            ['%%(__domain_name%i)s' % i for i in range(len(domains))]
        )
    }

    params.update({
        '__domain_name%i' % i: _name + "."
        for i, _name in enumerate(domains)
    })

    if limit > 0:
        query += "\nLIMIT %(limit)s"
        params["limit"] = limit

    results = await DBConnection.execute_query_async(
        query,
        params,
        {'paramstyle': 'format'},
        query_name="openintel_ips_by_domains"
    )

    result_dict.update(results)

    if len(result_dict["rows"]) > 0:
        result_dict['columns'] = list(result_dict['rows'][0].keys())

    return result_dict


async def openintel_select_ips_by_mx_records(
    DBConnection,
    logger,
    pattern,
    date_from,
    date_to,
    limit
):
    pattern = str(pattern) + "."
    result_dict = {
        'queried_pattern': pattern,
        'queried_interval': [date_from, date_to]
    }

    params = {
        'pattern': pattern,
        'from': date_from,
        'to': date_to
    }

    date_clause = get_date_where_clause("l2", params["from"], params["to"])
    params = get_spec_for_clause(params)

    query = """
    SELECT
        SUBSTR(l2.query_name, 1, LENGTH(l2.query_name) - 1) AS query_name,
        SUBSTR(l2.response_name, 1, LENGTH(l2.response_name) - 1) AS response_name,
        SUBSTR(l2.mx_address, 1, LENGTH(l2.mx_address) - 1) AS mx_address,
        l3.query_type,
        IF(nonnullvalue(l3.ip4_address), l3.ip4_address, l3.ip6_address) AS ip_address,
        to_timestamp(CAST(MIN(l2.time)/1000 AS BIGINT)) AS first_ts,
        to_timestamp(CAST(MAX(l2.time)/1000 AS BIGINT)) AS last_ts
    FROM %(db)s AS l2
    LEFT JOIN %(db)s AS l3 ON (l2.mx_address = l3.query_name)
    WHERE
        (l2.mx_address LIKE %%(pattern)s)
        AND
        (
            %(date_clause)s
        )
        AND
        (
            l2.year = l3.year
            AND l2.month = l3.month
            AND l2.day = l3.day
        )
        AND (
            (NOT l3.ip4_address IS NULL)
            OR (NOT l3.ip6_address IS NULL)
        )
    GROUP BY
        l2.query_name,
        l2.response_name,
        l2.mx_address,
        l3.query_name,
        l3.query_type,
        l3.ip4_address,
        l3.ip6_address
    """ % {
        'db': config["DB"],
        'date_clause': date_clause
    }

    if limit > 0:
        query += "\nLIMIT %(limit)s"
        params['limit'] = limit

    results = await DBConnection.execute_query_async(
        query,
        params,
        query_name='openintel_ips_by_mx_pattern'
    )

    result_dict.update(results)

    if len(result_dict["rows"]) > 0:
        result_dict['columns'] = list(result_dict['rows'][0].keys())

    return result_dict


async def openintel_select_measurements_by_name_and_ip_or_type(
    DBConnection,
    logger,
    name,
    date_from,
    date_to,
    limit,
    ip=None,
    type=None,
    full=False
):
    result_dict = {
        'queried_domain_name': name,
        'queried_ip': ip,
        'queried_interval': [date_from, date_to],
        'queried_record_type':  type,
        'full': full,
        'limit': limit
    }

    params = {
        'query_name': str(name) + ".",
        'from': date_from,
        'to': date_to
    }

    if full:
        sql_fields = """to_timestamp(CAST(time/1000 AS BIGINT)) AS ts, *"""
    else:
        sql_fields = """SUBSTR(query_name, 1, LENGTH(query_name) - 1),
                        SUBSTR(response_name, 1, LENGTH(response_name) - 1),
                        query_type,
                        response_type,
                        ip4_address,
                        ip6_address,
                        to_timestamp(CAST(time/1000 AS BIGINT)) AS ts"""

    if ip is None:
        ip_clause = "True"
    else:
        try:
            ip_address = ipaddress.ip_address(ip)
        except ipaddress.AddressValueError:
            raise ValueError("not a valid IP address")
        if ip_address.version == 4:
            ip_clause = "(ip4_address = %(ip_address)s)"
        elif ip_address.version == 6:
            ip_clause = "(ip6_address = %(ip_address)s)"
        else:
            raise RuntimeError("unknown IP protocol version")
        params['ip_address'] = ip_address.compressed

    if type is None:
        type_clause = "True"
    else:
        type_clause = "((query_type = %(query_type)s) OR (response_type = %(query_type)s))"
        params['query_type'] = str(type)

    date_clause = get_date_where_clause("l1", params["from"], params["to"])
    params = get_spec_for_clause(params)

    query = """
        SELECT
            %(sql_fields)s
        FROM %(db)s AS l1
        WHERE
            (
                (query_name = %%(query_name)s)
                OR (response_name = %%(query_name)s)
            )
            AND
            (
                %(ip_clause)s
            )
            AND
            (
                %(type_clause)s
            )
            AND
            (
                %(date_clause)s
            )
        ORDER BY ts
    """ % {
        'db': config["DB"],
        'sql_fields': sql_fields,
        'ip_clause': ip_clause,
        'type_clause': type_clause,
        'date_clause': date_clause
    }

    if limit > 0:
        query += "\nLIMIT %(limit)s"
        params['limit'] = limit

    logger.debug(query % params)

    results = await DBConnection.execute_query_async(
        query,
        params,
        query_name='openintel_measurements_by_name_and_ip_pair'
    )

    result_dict.update(results)

    if len(result_dict["rows"]) > 0:
        result_dict['columns'] = list(result_dict['rows'][0].keys())

    return result_dict


async def openintel_select_records_summary(
    DBConnection,
    logger,
    date_from,
    date_to,
    limit,
    name=None,
    ip=None,
    type=None
):
    result_dict = {
        'queried_domain_name': name,
        'queried_ip': ip,
        'queried_interval': [date_from, date_to],
        'queried_record_type':  type,
        'limit': limit
    }

    params = {
        'from': date_from,
        'to': date_to
    }

    if name is None:
        name_clause = "True"
    else:
        name_clause = "(query_name = %(query_name)s) OR (response_name = %(query_name)s)"
        params['query_name'] = str(name) + "."

    if ip is None:
        ip_clause = "True"
    else:
        try:
            ip_address = ipaddress.ip_address(ip)
        except ipaddress.AddressValueError:
            raise ValueError("not a valid IP address")
        if ip_address.version == 4:
            ip_clause = "(ip4_address = %(ip_address)s)"
        elif ip_address.version == 6:
            ip_clause = "(ip6_address = %(ip_address)s)"
        else:
            raise RuntimeError("unknown IP protocol version")
        params['ip_address'] = ip_address.compressed

    if type is None:
        type_clause = "True"
    else:
        type_clause = "((query_type = %(query_type)s) OR (response_type = %(query_type)s))"
        params['query_type'] = str(type)

    date_clause = get_date_where_clause("l1", params["from"], params["to"])
    params = get_spec_for_clause(params)

    sql_fields = """"""

    query = """
        SELECT
            SUBSTR(query_name, 1, LENGTH(query_name) - 1),
                    SUBSTR(response_name, 1, LENGTH(response_name) - 1),
                    query_type,
                    response_type,
                    ip4_address,
                    ip6_address,
                    to_timestamp(CAST(time/1000 AS BIGINT)) AS ts
        FROM %(db)s AS l1
        WHERE
            (
                %(name_clause)s
            )
            AND
            (
                %(ip_clause)s
            )
            AND
            (
                %(type_clause)s
            )
            AND
            (
                %(date_clause)s
            )
        ORDER BY ts
        GROUP BY query_name, response_name, query_type, response_type, ip4_address, ip6_address
    """ % {
        'db': config["DB"],
        'name_clause': name_clause,
        'ip_clause': ip_clause,
        'type_clause': type_clause,
        'date_clause': date_clause
    }

    if limit > 0:
        query += "\nLIMIT %(limit)s"
        params['limit'] = limit

    logger.debug(query % params)

    results = await DBConnection.execute_query_async(
        query,
        params,
        query_name='openintel_select_records_summary'
    )

    result_dict.update(results)

    if len(result_dict["rows"]) > 0:
        result_dict['columns'] = list(result_dict['rows'][0].keys())

    return result_dict
