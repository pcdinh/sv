import asyncpg
import logging
import math
from . import Null, is_placeholder, is_null
from asyncpg import utils

logger = logging.getLogger("app.postgresql")


def pyformat_to_native(query: str, params: dict):
    """Convert SQL query formatted in pyformat to PostgreSQL native format
    E.x: SELECT * FROM users WHERE user_id = %(user_id)s AND status = %(status)s AND country = %(country)s
         will be converted to
         SELECT * FROM users WHERE user_id = $1 AND status = $2 AND country = $3
    :param query: str
    :param: params: dict
            A mapping between field name and its value. E.x: {"user_id": 1, "status": 3, "country": "US"}
    """
    field_values = []
    counter = 1
    new_query = query
    for field_name, value in params.items():
        new_query = new_query.replace("".join(["%(", field_name, ")s"]), f"${counter}")
        field_values.append(value)
        counter += 1
    return new_query, field_values


class SessionManager:
    """Provides manageability for a database session"""
    def __init__(self, pg_pool: asyncpg.pool.Pool, timeout=None):
        self.pool = pg_pool
        self.connection: asyncpg.connection.Connection = None
        self.transaction: asyncpg.connection.transaction.Transaction = None
        self.timeout = timeout
        self.isolation = "read_committed"
        self.readonly = False
        self.deferrable = False

    async def start(self, transactional, isolation, readonly, deferrable) -> asyncpg.connection.Connection:
        """Initialize a database session
        :param transactional: bool
        :param isolation: str
        :param readonly: bool
        :param deferrable: bool
        :return asyncpg.connection.Connection
        """
        if self.connection:
            raise UserWarning("The use of initialize() caused leaked connection")
        self.connection = await self.pool.acquire(timeout=self.timeout)
        self.isolation = isolation
        self.readonly = readonly
        self.deferrable = deferrable
        return self.connection

    async def start_transaction(self):
        """Start a transaction"""
        self.transaction = await self.connection.transaction()
        await self.transaction.start()

    async def close(self, release=True):
        """Close database session and release the current connection to the pool
        :param release: bool
        """
        if release is True:
            await self.pool.release(self.connection)
        self.connection = None
        self.transaction = None

    async def fetch_one(self, query, params=None):
        """Retrieve a single row
        :param query: str
        :param params: dict
        :return: dict
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self.connection.fetchrow(query, *params)
        else:
            ret = await self.connection.fetchrow(query)
        if ret is None:
            return {}
        return dict(ret)

    async def fetch_column(self, query, params=None):
        """Fetch all possible values of the first column of rows, returning a list
        :param str query:
        :param dict params:
        :return: list
                 An empty list is returned if there is no match rows
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self.connection.fetch(query, *params)
        else:
            ret = await self.connection.fetch(query)
        return [row[0] for row in ret]

    async def fetch_value(self, query, params=None):
        """Retrieve the value of the first column on the first row
        :param str query:
        :param dict params:
        :return: Null if there is no matching row,
                 int|str|None if there is a matching row
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self.connection.fetchval(query, *params)
        else:
            ret = await self.connection.fetchval(query)
        if ret is None:
            return Null()
        return ret

    async def fetch_all(self, query, params=None):
        """Fetches all (remaining) rows of a query result, returning a list
        :param str query:
        :param dict params:
        :return: list
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self.connection.fetch(query, *params)
        else:
            ret = await self.connection.fetch(query)
        return [dict(row) for row in ret]

    async def fetch_by_page(self, query, page, rows_per_page, params=None):
        """Fetch all (remaining) rows of a query result, returning a tuple (rows, total)
        :param str query:
        :param int page:
        :param int rows_per_page:
        :param dict params:
        :raise UserWarning:
        :return: a tuple (list of rows in the page, row_count)
        """
        try:
            q = 'SELECT COUNT(1) AS row_count ' + query[query.index('FROM'):]
        except ValueError:
            raise UserWarning('Missing FROM in the provided query')
        import sys
        if params:
            ret = await self.execute_and_fetch(q, params)
        else:
            ret = await self.execute_and_fetch(q, None)
        row_count = ret[0]['row_count']
        if row_count == 0:
            return [], 0
        max_pages = math.ceil(float(row_count) / rows_per_page)
        # Check the page value just in case someone is trying to input an arbitrary value
        if page > max_pages or page <= 0:
            page = 1
        if params is None:
            params = {}
        # Calculate offset
        offset = rows_per_page * (page - 1)
        query = query + ' LIMIT %(rows_per_page)s OFFSET %(offset)s'
        if params:
            params.update({'rows_per_page': rows_per_page, 'offset': offset})
            ret = await self.execute_and_fetch(query, params)
        else:
            ret = await self.execute_and_fetch(query, None)
        return ret, row_count

    async def execute(self, query: str, params: dict=None, timeout: float=None) -> str:
        """Execute a query
        :param str query:
        :param dict params:
        :param float timeout:
        :return: The number of affected rows
        """
        self.connection._check_open()
        if not params:
            # status can be: SELECT 0
            #                INSERT 0 1
            status = await self.connection._protocol.query(query, timeout)
            return int(status.split()[-1])
        query, params = pyformat_to_native(query, params)
        _, status, _ = await self.connection._execute(query, params, 0, timeout, True)
        return int(status.split()[-1])

    async def execute_and_fetch(self, query, params=None, timeout=None, return_status=False):
        """Execute a query and get returned data
        :param str query:
        :param dict params:
        :param int timeout:
        :param bool return_status:
        :return: a list of dictionaries
        """
        if params:
            query, params = pyformat_to_native(query, params)
        with self.connection._stmt_exclusive_section:
            executor = lambda stmt, timeout: self.connection._protocol.bind_execute(
                stmt, params, '', 0, return_status, timeout
            )
        timeout = self.connection._protocol._get_timeout(timeout)
        # type : result: list(asyncpg.Record)
        # type : _stmt: asyncpg.protocol.protocol.PreparedStatementState
        result, _stmt = await self.connection._do_execute(query, executor, timeout)
        return [dict(item) for item in result]

    async def insert(self, table, values, return_fields=None, check_placeholder=False):
        """Insert a row into a table
        :param table: Table name
        :param values: A dict of field name and its value
        :param return_fields: Field name to return
        :param check_placeholder: bool
        :return: a tuple (return_dict, affected_rows)
        """
        self.connection._check_open()
        fields = values.keys()
        if check_placeholder is False:
            value_placeholders = ['%%(%s)s' % field for field in fields]  # Created %(field_name)s
        else:
            value_placeholders = []
            for field in fields:
                # values = {'coordinates': Placeholder("ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)", bind_values) }
                if is_placeholder(values[field]):
                    value_placeholders.append(values[field].placeholder)  # Assigned to a position: (%(field_1)s, place_holder_here, %(field_2)s)
                    values.update(values[field].bind_values)
                else:
                    value_placeholders.append('%%(%s)s' % field)  # Created %(field_name)s
        if not return_fields:
            q = "INSERT INTO %s (%s) VALUES (%s)" % (
                table, ','.join(fields), ','.join(value_placeholders)
            )
        else:
            q = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s" % (
                table, ','.join(fields), ','.join(value_placeholders), return_fields
            )
        query, params = pyformat_to_native(q, values)
        if return_fields:
            ret = await self.connection.fetchrow(query, *params)
            if not ret:
                return {}, 0
            return dict(ret), 1
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return {}, int(status.split()[-1])

    def insert_many(self, table, values, return_id=None, autocommit=True, check_placeholder=False):
        self.connect()
        q = self._generate_bulk_insert_query(table, values, return_id, check_placeholder)
        execute_many = False if return_id or check_placeholder is True else True
        return self._execute(q, values, autocommit, execute_many=execute_many)

    def _generate_bulk_insert_query(self, table, values, return_id, check_placeholder=False):
        if return_id or check_placeholder is True:
            return self._multi_insert_sql(table, values, return_id, check_placeholder)
        return self._bind_bulk_insert_sql(table, values, check_placeholder=False)

    def _bind_bulk_insert_sql(self, table, values, check_placeholder=True):
        """
        @see psycopg2:executemany()
        """
        fields = values[0].keys()
        all_fields = []  # field placeholders
        if check_placeholder is False:
            row_data = []
            for field in fields:
                row_data.append('%%(%s)s' % field)  # Created %(field_name)s: %% is converted to %
            all_fields = ','.join(row_data)
        else:
            row_data = []
            for field in fields:
                if is_placeholder(values[0][field]):
                    row_data.append(values[0][field].placeholder)  # Assigned by position, not creating a %(field_name)s
                else:
                    row_data.append('%%(%s)s' % field)  # Created %(field_name)s
            all_fields = ','.join(row_data)

            for row in values:
                for field in fields:
                    # values = {'coordinates': Placeholder("ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)", bind_values) }
                    if is_placeholder(row[field]):
                        row_data.append(row[field].placeholder)  # Assigned by position, not creating a %(field_name)s
                        row.update(row[field].bind_values)  # Bind values used in placeholder string
        return "INSERT INTO %s (%s) VALUES (%s)" % (table, ','.join(fields), all_fields)

    def _multi_insert_sql(self, table, values, return_id, check_placeholder):
        """
        @see http://www.postgresql.org/docs/9.2/static/sql-insert.html (look for "To insert multiple rows using the multirow VALUES syntax:")
        """
        fields = values[0].keys()
        all_rows = []
        if check_placeholder is False:
            for row in values:
                field_values = []
                for field in fields:
                    if row[field]:
                        # @see http://michael.otacoo.com/postgresql-2/manipulating-arrays-in-postgresql/
                        if isinstance(row[field], list):
                            v = "'{%s}'" % ','.join(utils._quote_literal(str(x)) for x in row[field])
                        else:
                            v = utils._quote_ident(row[field])
                    else:
                        v = 'NULL'
                    field_values.append(v)
                all_rows.append('(%s)' % ','.join(str(value) for value in field_values))
        else:
            for row in values:
                field_values = []
                for field in fields:
                    if row[field]:
                        if is_placeholder(row[field]):
                            if row[field].bind_values:
                                v = row[field].placeholder % row[field].bind_values
                            else:
                                v = row[field].placeholder
                        # @see http://michael.otacoo.com/postgresql-2/manipulating-arrays-in-postgresql/
                        elif isinstance(row[field], list):
                            v = "'{%s}'" % ','.join(utils._quote_literal(str(x)) for x in row[field])
                        else:
                            v = utils._quote_literal(row[field])
                    else:
                        v = 'NULL'
                    field_values.append(v)
                all_rows.append('(%s)' % ','.join(str(value) for value in field_values))
        if not return_id:
            return "INSERT INTO %s (%s) VALUES %s" % (table, ','.join(fields), ','.join(all_rows))
        return "INSERT INTO %s (%s) VALUES %s RETURNING %s" % (table, ','.join(fields), ','.join(all_rows), return_id)

    def update_all(self, table, values, autocommit=True):
        self.connect()
        fields = values.keys()
        update_fields = ', '.join(['%s = %%(%s)s'] % ((field, field) for field in fields))
        q = "UPDATE %s SET %s" % (table, update_fields)
        return self._execute(q, values, autocommit)

    def update(self, table, values, where, autocommit=True):
        """Update certain rows in a table
        :param table: Table name
        :param values: A dict (field_name: value)
        :param where: A dict (field_name: value)
        :return The number of affected rows
        """
        fields = values.keys()
        # field_name = %(field_name_v)s (avoid conflicts with WHERE values)
        update_fields = ', '.join(['%s = %%(%s_v)s' % (field, field) for field in fields])
        # Dangerous action
        if not where:
            raise UserWarning('Database update() without WHERE clause. Use update_all() instead')
        # 'field_name': (1, 2, 3, 4) => field_name IN (1, 2, 3, 4)
        # Psycopg2 syntax: field_name IN %(field_name)
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "UPDATE %s SET %s WHERE %s" % (table, update_fields, where_clause)
        values = dict([(k + '_v', v) for k, v in values.items()])
        values.update(where)
        self._execute(q, values, autocommit)
        return self.cursor.rowcount

    def delete_all(self, table, autocommit=True):
        """Delete all rows from a table
        :return The number of deleted rows
        """
        self.connect()
        query = "DELETE FROM %s" % table
        self._execute(query, None, autocommit)
        return self.cursor.rowcount

    def delete(self, table, where):
        """Deletes all rows that match the provided condition
        :param str table: Table name
        :param dict where: A dict (field_name: value) indicates equality clause (=), (field_name: tuple) indicates IN clause
        :return The number of deleted rows
        """
        if not where:
            raise UserWarning('Database delete() without WHERE clause. Use delete_all() instead')
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "DELETE FROM %s WHERE %s" % (table, where_clause)
        self._execute(q, where, autocommit)
        return self.cursor.rowcount

    def delete_and_return(self, table, where, return_field='*', autocommit=True):
        """Delete and return deleted rows
        """
        if not where:
            raise UserWarning('Method delete_and_return() without WHERE clause. Use delete_all() instead')
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "DELETE FROM %s WHERE %s RETURNING %s" % (table, where_clause, return_field)
        self._execute(q, where, autocommit)
        return self.cursor.fetchall()

    def get_columns(self, table):
        """Return all columns as a list
        """
        self.connect()
        self.cursor.execute('SELECT * FROM %s WHERE 1=0' % table)
        return [rec[0] for rec in self.cursor.description]

    def get_last_query(self):
        self.connect()
        return self.cursor.query

    def generate_query(self, query, params):
        return utils._mogrify(self.connection, query, params)
