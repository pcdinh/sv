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
        """Fetches all (remaining) rows of a query result, returning a list
        @raise InvalidArgumentException:
        :return: a tuple (row_count, list of rows in the page)
        """
        try:
            q = 'SELECT COUNT(1) AS row_count ' + query[query.index('FROM'):]
        except ValueError:
            raise UserWarning('Missing FROM in the provided query')
        self.connect()
        self.cursor.execute(q, params)
        row = self.cursor.fetchone()
        if self.row_type == 'dict':
            row_count = row['row_count']
        else:
            row_count = row[0]
        if row_count == 0:
            return 0, []
        max_pages = math.ceil(float(row_count) / rows_per_page)
        # Check the page value just in case someone is trying to input an arbitrary value
        if page > max_pages or page <= 0:
            page = 1
        if params is None:
            params = {}
        # Calculate offset
        offset = rows_per_page * (page - 1)
        params.update({'rows_per_page': rows_per_page, 'offset': offset})
        self.cursor.execute(query + ' LIMIT %(rows_per_page)s OFFSET %(offset)s', params)
        return row_count, self.cursor.fetchall()

    def execute(self, query, params=None):
        """
        Executes the query
        :return None
        """
        self.connect()
        try:
            if self.debug_queries:
                logger.debug(self.generate_query(query, params))
            return self.cursor.execute(query, params)
        except BaseException as e:
            if self.is_write_query(query):
                # @todo: support SAVEPOINT
                self.end_transaction(commit=False)  # roll back the current transaction
            raise e

    def get_last_insert_values(self):
        self.connect()
        try:
            return self.cursor.fetchone()
        except BaseException as e:
            raise e

    def get_last_insert_id(self):
        """Gets the inserted ID caused by the previous INSERT ... RETURNING ID statements
        You must invoke execute() first"""
        self.connect()
        try:
            value = self.cursor.fetchone()
            if value:
                return value[0]
            return None
        except BaseException as e:
            raise e

    def get_last_insert_ids(self):
        """Gets all the inserted IDs caused by the previous multiple INSERT ... RETURNING ID statements
        You must invoke execute() first"""
        self.connect()
        try:
            return self.cursor.fetchall()
        except BaseException as e:
            raise e

    def affected_rows(self):
        """Get number of affected rows
        """
        return self.cursor.rowcount

    def insert(self, table, values, return_id=None, autocommit=True, check_placeholder=False):
        """
        :param table: Table name
        :param values: A dict of field name and its value
        :param function_aware: To check if there is any column that is function aware
        :param return_id: Field name to return
        :param autocommit: bool
        :param check_placeholder: bool
        :return Number of affected rows
        """
        self.connect()
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
        if not return_id:
            q = "INSERT INTO %s (%s) VALUES (%s)" % (table, ','.join(fields), ','.join(value_placeholders))
        else:
            q = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s" % (table, ','.join(fields), ','.join(value_placeholders), return_id)
        return self._execute(q, values, autocommit)

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
        '''
        The method returns number of updated rows
        :param table: Table name
        :param values: A dict (field_name: value)
        :param where: A dict (field_name: value)
        '''
        fields = values.keys()
        # field_name = %(field_name_v)s (avoid conflicts with WHERE values)
        update_fields = ', '.join(['%s = %%(%s_v)s' % (field, field) for field in fields])
        # Dangerous action
        if not where:
            raise Exception('Database update() without WHERE clause. Use update_all() instead')
        # 'field_name': (1, 2, 3, 4) => field_name IN (1, 2, 3, 4)
        # Psycopg2 syntax: field_name IN %(field_name)
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "UPDATE %s SET %s WHERE %s" % (table, update_fields, where_clause)
        values = dict([(k + '_v', v) for k, v in values.items()])
        values.update(where)
        self._execute(q, values, autocommit)
        return self.cursor.rowcount

    def delete_all(self, table, autocommit=True):
        """Deletes all rows from a table
        :return The number of deleted rows
        """
        self.connect()
        query = "DELETE FROM %s" % table
        self._execute(query, None, autocommit)
        return self.cursor.rowcount

    def delete(self, table, where, autocommit=True):
        """Deletes all rows that match the provided condition
        :param table: Table name
        :param where: A dict (field_name: value) indicates equality clause (=), (field_name: tuple) indicates IN clause
        :return The number of deleted rows
        """
        self.connect()
        # Dangerous action
        if not where:
            raise BaseException('Database delete() without WHERE clause. Use delete_all() instead')
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "DELETE FROM %s WHERE %s" % (table, where_clause)
        self._execute(q, where, autocommit)
        return self.cursor.rowcount

    def delete_and_return(self, table, where, return_field='*', autocommit=True):
        '''
        Deletes and return deleted rows
        '''
        if not where:
            raise BaseException('Method delete_and_return() without WHERE clause. Use delete_all() instead')
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field) for field, v in where.items()])
        q = "DELETE FROM %s WHERE %s RETURNING %s" % (table, where_clause, return_field)
        self._execute(q, where, autocommit)
        return self.cursor.fetchall()

    def _execute(self, query, params, autocommit, execute_many=False):
        """
        :return Affected row count, return_id
        """
        if autocommit is False:
            self.begin_transaction()
            executed = False
            try:
                if self.debug_queries:
                    logger.debug(self.generate_query(query, params))
                if execute_many:
                    self.cursor.executemany(query, params)
                else:
                    self.cursor.execute(query, params)
                executed = True
                return self.cursor.rowcount
            finally:
                if executed is False:
                    self.end_transaction(commit=False)  # roll back the current transaction
        else:
            self.connect()
            if self.debug_queries:
                logger.debug(self.cursor.mogrify(query, params))
            if execute_many:
                self.cursor.executemany(query, params)
            else:
                self.cursor.execute(query, params)
            if self.conn.autocommit is False or self.conn.isolation_level != extensions.ISOLATION_LEVEL_AUTOCOMMIT:
                self.commit()
            return self.cursor.rowcount

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
