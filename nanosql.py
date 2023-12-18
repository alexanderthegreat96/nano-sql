#!/usr/bin/env python
# vim: fileencoding=utf-8: noexpandtab

"""
    A very simple wrapper for mysql (mysql-connector)

    Methods:
        getOne() - get a single row
        getAll() - get all rows
        lastId() - get the last insert id
        lastQuery() - get the last executed query
        insert() - insert a row
        insertBatch() - Batch Insert
        insertOrUpdate() - insert a row or update it if it exists
        update() - update rows
        delete() - delete rows
        query()  - run a raw sql query
        leftJoin() - do an inner left join query and get results

    License: GNU GPLv2

    Kailash Nadh, http://nadh.in
    May 2013

    Updated by: Milosh Bolic
    June 2019

    Modified and updated by Alexandru Lupaescu
    December 2023
"""

import mysql.connector as mysql
from mysql.connector import pooling, Error
from collections import namedtuple
from itertools import repeat

class NanoSql:
    _pool = None
    conn = None
    cur = None
    conf = None

    def __init__(self, **kwargs):
        self.conf = kwargs
        self.conf["charset"] = kwargs.get("charset", "utf8")
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 3306)
        self.conf["ssl"] = kwargs.get("ssl", None)

        
        if NanoSql._pool is None:
            pool_config = {
                "pool_size": kwargs.get("pool_size", 8),
                "pool_reset_session":  kwargs.get("pool_reset_session", True),
                "host": self.conf['host'],
                "port": self.conf['port'],
                "user": self.conf['user'],
                "passwd": self.conf['passwd'],
                "db": self.conf['db'],
                "charset": self.conf['charset'],
            }
            if self.conf["ssl"]:
                pool_config["ssl"] = self.conf["ssl"]

            try:
                NanoSql._pool = pooling.MySQLConnectionPool(**pool_config)
            except Error as e:
                print(f"Error creating MySQL connection pool: {e}")
                raise
            
        self.connect()

    def connect(self):
        """Connect to the MySQL server"""

        try:
            self.conn = NanoSql._pool.get_connection()
            if self.conn:
                self.conn.autocommit = self.conf.get("autocommit", False)  # Set autocommit on the connection
                self.cur = self.conn.cursor()
        except Error as e:
            print(f"MySQL connection failed: {e}")
            raise

    def release_connection(self):
        """Release the connection back to the pool"""
        try:
            if self.conn:
                self.conn.close()
        except Error as e:
            print(f"Failed to release connection: {e}")

    def get_connection(self):
        """Get the connection from the pool"""
        try:
            # Get a connection from the pool
            self.conn = NanoSql._pool.get_connection()
            self.conn.autocommit = self.conf.get("autocommit", False)
            return self.conn
        except Error as e:
            print(f"MySQL connection failed: {e}")
            raise

    def __exit__(self, type, value, traceback):
        self.release_connection()

    def getOne(self, table=None, fields='*', where=None, order=None, limit=(0, 1)):
        """Get a single result

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [from, to]
        """

        cur = self._select(table, fields, where, order, limit)

        result = cur.fetchone()

        row = {}
        if result:
            fields = [f[0] for f in cur.description]
            row = zip(fields, result)

        return dict(row)

    def getAll(self, table=None, fields='*', where=None, order=None, limit=None):
        """Get all results

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [from, to]
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchall()

        rows = {}
        if result:
            fields = [f[0] for f in cur.description]
            rows = [dict(zip(fields, r)) for r in result]

        return rows

    def lastId(self):
        """Get the last insert id"""
        return self.cur.lastrowid

    def lastQuery(self):
        """Get the last executed query"""
        try:
            return self.cur.statement
        except AttributeError:
            return self.cur._last_executed

    def leftJoin(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query

            tables = (table1, table2)
            fields = ([fields from table1], [fields from table 2])  # fields to select
            join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """

        cur = self._select_join(tables, fields, join_fields, where, order, limit)
        result = cur.fetchall()

        #rows = None
        rows = {}
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def insert(self, table, data):
        """Insert a record"""
        query = self._serialize_insert(data)

        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, query[0], query[1])

        return self.query(sql, tuple(data.values())).rowcount

    def insertBatch(self, table, data):
        """Insert multiple record"""

        query = self._serialize_batch_insert(data)
        sql = "INSERT INTO %s (%s) VALUES %s" % (table, query[0], query[1])

        flattened_values = [v for sublist in data for k, v in iter(sublist.items())]

        return self.query(sql, flattened_values).rowcount

    def update(self, table, data, where=None):
        """Insert a record"""

        query = self._serialize_update(data)

        sql = "UPDATE %s SET %s" % (table, query)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        values = tuple(data.values())

        return self.query(
            sql, values + where[1] if where and len(where) > 1 else values
        ).rowcount

    def insertOrUpdate(self, table, data, keys):
        insert_data = data.copy()

        data = {k: data[k] for k in data if k not in keys}

        insert = self._serialize_insert(insert_data)
        update = self._serialize_update(data)

        sql = "INSERT INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (table, insert[0], insert[1], update)

        return self.query(sql, tuple(insert_data.values()) + tuple(data.values())).rowcount

    def delete(self, table, where=None):
        """Delete rows based on a where condition"""

        sql = "DELETE FROM %s" % table

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        return self.query(sql, where[1] if where and len(where) > 1 else None).rowcount

    def addIndex(self, table, index_name, fields=[]):
        sanitized_fields = ','.join(fields)
        sql = 'ALTER TABLE %s ADD INDEX %s (%s)' % (table, index_name, sanitized_fields)

        return self.query(sql)

    def dropIndex(self, table_name, index_name):
        sql = 'ALTER TABLE %s DROP INDEX %s' % (table_name, index_name)

        return self.query(sql)

    def query(self, sql, params=None):
        """Run a raw query"""
        try:
            connection = self.get_connection()
            cursor = connection.cursor(buffered=True)

            try:
                cursor.execute(sql, params)
            except Error as e:
                # mysql timed out or connection fails. reconnect and retry once
                if e.errno == 2006 or e.errno == 2013:
                    self.connect()
                    cursor.execute(sql, params)
                else:
                    raise

        except Error as e:
            print(f"Failed to execute query: {e}")
            raise
        finally:
            if connection:
                self.release_connection()

        return cursor

    def commit(self):
        """Commit a transaction (transactional engines like InnoDB require this)"""
        return self.conn.commit()

    def is_open(self):
        """Check if the connection is open"""
        return self.conn.open

    def end(self):
        """Kill the connection"""
        self.cur.close()
        self.conn.close()

        # ===

    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])

        return [keys, vals]

    def _serialize_batch_insert(self, data):
        """Format insert dict values into strings"""

        keys = ",".join(data[0].keys())
        v = "(%s)" % ",".join(tuple("%s".rstrip(',') for v in range(len(data[0]))))
        l = ','.join(list(repeat(v, len(data))))

        return [keys, l]

    def _serialize_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _select(self, table=None, fields=(), where=None, order=None, limit=None):
        """Run a select query"""

        sql = "SELECT %s FROM `%s`" % (",".join(fields), table)

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " %s" % order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit[0]

            if len(limit) > 1:
                sql += ", %s" % limit[1]


        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def _select_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query"""

        fields = [tables[0] + "." + f for f in fields[0]] + \
                 [tables[1] + "." + f for f in fields[1]]

        sql = "SELECT %s FROM %s LEFT JOIN %s ON (%s = %s)" % \
              (",".join(fields),
               tables[0],
               tables[1],
               tables[0] + "." + join_fields[0],
               tables[1] + "." + join_fields[1]
               )

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " " + order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit[0]

            if len(limit) > 1:
                sql += ", %s" % limit[1]

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.end()
