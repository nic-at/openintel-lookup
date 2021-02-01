import time
import asyncio

from impala.dbapi import connect, InterfaceError

class HadoopDBConnection(object):
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.conn = None

        self.n_reconnect_tries = int(self.config["N_RECONNECT_TRIES"])
        self.reconnect_delay = float(self.config["RECONNECT_DELAY"]) # seconds

    def connect_to_db(self):
        self.logger.info(
            "connecting to database %s:%s"%(
                self.config["DBHOST"],
                self.config["DBPORT"]
            )
        )
        self.conn = connect(
            host=str(self.config["DBHOST"]),
            port=int(self.config["DBPORT"])
        )

    def reconnect_to_db(self):
        if self.conn is None:
            raise RuntimeError("no connection established")
        if self.conn is None:
            raise RuntimeError("no connection has previously been established")
        else:
            self.conn.reconnect()

    def disconnect_from_db(self):
        if self.conn is None:
            raise RuntimeError("no connection established")
        self.conn.close()
        self.conn = None

    def execute_query(self, *args, **kwargs):
        if self.conn is None:
            raise RuntimeError("no connection established")
        if 'query_name' in kwargs:
            query_name = kwargs.pop('query_name')
        else:
            query_name = None

        self.logger.debug("executing query '%s'" % query_name)
        if 'operation' in kwargs:
            self.logger.debug(kwargs['operation'])
        else:
            self.logger.debug(args[0])

        if 'parameters' in kwargs:
            self.logger.debug(kwargs['parameters'])
        elif len(args) >= 2:
            self.logger.debug(args[1])

        for _ in range(self.n_reconnect_tries):
            try:
                return self._do_query(query_name, *args, **kwargs)
            except InterfaceError:
                self.logger.debug("trying to reconnect to hadoop")
                time.sleep(self.reconnect_delay)
                self.reconnect_to_db()
        raise RuntimeError("could not connect to DB")

    def _do_query(self, query_name, *args, **kwargs):
        self.connect_to_db()
        if self.conn is None:
            raise RuntimeError("no connection established")
        cursor = self.conn.cursor(dictify=True)
        query_time_start = time.time()
        cursor.execute(*args, **kwargs)
        dt_query = time.time() - query_time_start
        if query_name is None:
            self.logger.debug("query time: %f" % dt_query)
        else:
            self.logger.debug("query time (%s): %f" % (query_name, dt_query))
        fetch_time_start = time.time()
        results = cursor.fetchall()
        dt_fetch = time.time() - fetch_time_start
        if query_name is None:
            self.logger.debug("fetch time: %f" % dt_fetch)
        else:
            self.logger.debug("fetch time (%s): %f" % (query_name, dt_fetch))
        cursor.close()
        self.disconnect_from_db()
        return {
            'rows': results,
            'query_time': dt_query,
            'fetch_time': dt_fetch
        }

    async def execute_query_async(self, *args, **kwargs):
        self.connect_to_db()
        if self.conn is None:
            raise RuntimeError("no connection established")
        if 'query_name' in kwargs:
            query_name = kwargs.pop('query_name')
        else:
            query_name = None

        self.logger.debug("executing query (async) '%s'" % query_name)
        if 'operation' in kwargs:
            self.logger.debug(kwargs['operation'])
        else:
            self.logger.debug(args[0])

        if 'parameters' in kwargs:
            self.logger.debug(kwargs['parameters'])
        elif len(args) >= 2:
            self.logger.debug(args[1])

        for _ in range(self.n_reconnect_tries):
            try:
                return await self._do_query_async(query_name, *args, **kwargs)
            except InterfaceError:
                self.logger.debug("trying to reconnect to hadoop")
                time.sleep(self.reconnect_delay)
                self.reconnect_to_db()
        raise RuntimeError("could not connect to DB")
        self.disconnect_from_db()

    async def _do_query_async(self, query_name, *args, **kwargs):
        if self.conn is None:
            raise RuntimeError("no connection established")
        self.logger.debug("fetching cursor")
        cursor = self.conn.cursor(dictify=True)
        self.logger.debug("executing query")
        query_time_start = time.time()
        cursor.execute_async(*args, **kwargs)

        counter = 0
        while cursor.is_executing():
            if (counter % 60) == 0:
                self.logger.debug("waiting for result...")
            await asyncio.sleep(1.0)
            counter += 1

        # Populate the fields in cursor (hack to make dict-cursor work with async)
        # see https://github.com/cloudera/impyla/issues/292
        if cursor.description is not None:
            cursor.fields = [d[0] for d in cursor.description]
        else:
            cursor.fields = None

        dt_query = time.time() - query_time_start
        if query_name is None:
            self.logger.debug("query time: %f" % dt_query)
        else:
            self.logger.debug("query time (%s): %f" % (query_name, dt_query))
        fetch_time_start = time.time()
        results = cursor.fetchall()
        dt_fetch = time.time() - fetch_time_start
        if query_name is None:
            self.logger.debug("fetch time: %f" % dt_fetch)
        else:
            self.logger.debug("fetch time (%s): %f" % (query_name, dt_fetch))
        cursor.close()
        return {
            'rows': results,
            'query_time': dt_query,
            'fetch_time': dt_fetch
        }