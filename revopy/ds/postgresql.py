import asyncpg
import logging
import math
from typing import Dict, List, Tuple, Union
from . import Null, is_placeholder, Placeholder
from asyncpg import utils

logger = logging.getLogger("app.postgresql")


def pyformat_to_native(query: str, params: Dict) -> Tuple[str, List]:
    """Rewrite SQL query formatted in pyformat to PostgreSQL native format
    E.x: SELECT * FROM users WHERE user_id = %(user_id)s AND status = %(status)s AND country = %(country)s
         will be converted to
         SELECT * FROM users WHERE user_id = $1 AND status = $2 AND country = $3
    :param str query:
    :param dict params:
            A mapping between field name and its value. E.x: {"user_id": 1, "status": 3, "country": "US"}
    """
    field_values = []
    counter = 1
    new_query = query
    for field_name, value in params.items():
        new_query = new_query.replace("".join(["%(", field_name, ")s"]), "$%s" % counter)
        field_values.append(value)
        counter += 1
    return new_query, field_values


def pyformat_in_list_to_native(query: str, params: List[Dict]) -> Tuple[str, List[List]]:
    """Rewrite SQL query formatted in pyformat to PostgreSQL native format
    E.x: INSERT INTO users (user_id, first_name) VALUES (%(user_id)s, %(first_name)s)
         [
           {'user_id': 1, 'first_name': 'A1'}, {'user_id': 2, 'first_name': 'A2'}
         ]
         will be converted to
         INSERT INTO users (user_id, first_name) VALUES ($1, $2)
         [
           (1, 'A1'), (2, 'A2')
         ]
    :param str query:
    :param list params:
            A list of mapping between field name and its value. E.x: [{"user_id": 1, "status": 3, "country": "US"}]
    :return: a tuple (str, list[dict])
    """
    field_values = []
    counter = 1
    new_query = query
    data = params[:]  # copy it to avoid pass-by-reference
    first_data_item = data.pop(0)
    field_value_per_set = []
    for field_name, value in first_data_item.items():
        new_query = new_query.replace("".join(["%(", field_name, ")s"]), "$%s" % counter)
        field_value_per_set.append(value)
        counter += 1
    field_values.append(field_value_per_set)
    for item in data:
        field_value_per_set = []
        for field_name, value in item.items():
            field_value_per_set.append(value)
        field_values.append(field_value_per_set)
    return new_query, field_values


def quote(field_value) -> str:
    """Escape a value to be able to insert into PostgreSQL

    :param field_value:
    :return:
    """
    if isinstance(field_value, str):
        return utils._quote_literal(field_value)
    if field_value is None:
        return 'NULL'
    if isinstance(field_value, (int, float, complex)):
        return str(field_value)
    # Applicable for date, time, text, varchar
    return utils._quote_literal(str(field_value))


def quote_array(values) -> str:
    """Convert Python value into string that is compatible with PostgreSQL's query
    first_name = "Name 01" => 'Name 01'
    this_date = datetime.datetime.now() => '2018-07-30 11:54:25.161946'
    this_date = datetime.datetime.now(datetime.timezone.utc) => '2018-07-30 05:12:30.279286+00:00'
    See: https://paquier.xyz/postgresql-2/manipulating-arrays-in-postgresql/

    :param values:
    :return:
    """
    ret = "'{%s}'"
    quoted_values = []
    for v in values:
        quoted_values.append(quote(v))
    return ret % ",".join(quoted_values)


def quote_placeholder(placeholder: Placeholder):
    # Noted that we may have pyformat replacement inside
    # E.x: "ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)"
    if placeholder.bind_values:
        bind_values = placeholder.bind_values.copy()  # copy it
        for bind_key, bind_val in bind_values.items():
            if isinstance(bind_val, (list, tuple)):
                bind_values[bind_key] = quote_array(bind_val)
            else:
                # Nested placeholder is not accepted
                bind_values[bind_key] = quote(bind_val)
        return placeholder.placeholder % bind_values
    return placeholder.placeholder


def generate_bulk_insert_query(table: str, rows: List[Dict]) -> str:
    """Generate bulk insert query

    :param str table:
    :param dict rows:
    :return: str
            A query looks like:
            INSERT INTO table_name (user_id, first_name, last_name) VALUES
            (1, 'D1', 'D2'), (2, 'A1', 'A2');
    """
    fields = rows[0].keys()
    row_values = []
    for row in rows:
        new_row = []
        for field in fields:
            value = row[field]
            if value is None or isinstance(value, (int, str, bytes)):
                new_row.append(quote(value))
            elif isinstance(value, (list, tuple)):
                new_row.append(quote_array(value))
            elif is_placeholder(value):
                new_row.append(quote_placeholder(value))
            else:
                new_row.append(quote(value))
        row_values.append(",".join(new_row))
    return "INSERT INTO %s (%s) VALUES (%s)" % (table, ','.join(fields), '),('.join(row_values))


def generate_native_insert_query(table: str, row: Dict) -> Tuple[str, List]:
    """Create an INSERT query using PostgreSQL's native bind format: $n

    :param str table:
    :param dict row:
    :return: a tuple (str, dict)
    """
    fields = row.keys()
    placeholders = []
    counter = 1
    params = []
    for field in fields:
        value = row[field]
        if is_placeholder(value):
            placeholders.append(quote_placeholder(value))
        else:
            placeholders.append("$%s" % counter)
            params.append(value)
            counter += 1
    return "INSERT INTO %s (%s) VALUES (%s)" % (table, ','.join(fields), ','.join(placeholders)), params


class SessionManager:
    """Provides manageability for a database session"""

    def __init__(self, pg_pool: 'asyncio.Future[asyncpg.pool.Pool]', timeout=None):
        """Create an instance of SessionManager

        :rtype: SessionManager
        :param asyncio.Future pg_pool:
        :param int timeout:
        """
        self.pool: 'asyncio.Future[asyncpg.pool.Pool]' = pg_pool
        self.connection: asyncpg.connection.Connection = None
        self.transaction: asyncpg.connection.transaction.Transaction = None
        self.timeout = timeout
        self.isolation = "read_committed"
        self.readonly = False
        self.deferrable = False

    async def start(self, isolation: str,
                    readonly: bool, deferrable: bool) -> asyncpg.connection.Connection:
        """Initialize a database session

        :param str isolation:
        :param bool readonly:
        :param bool deferrable:
        :return an asyncpg's Connection
        :rtype: asyncpg.connection.Connection
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

    async def close(self, release: bool=True):
        """Close database session and release the current connection to the pool

        :param bool release:
        """
        if release is True:
            await self.pool.release(self.connection)
        self.connection = None
        self.transaction = None

    async def fetch_one(self, query: str, params: Dict=None) -> Dict:
        """Retrieve a single row

        :param str query:
        :param dict params:
        :return: dict
        :rtype: dict
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self._execute_and_fetch(query, params, 1, timeout=self.timeout)
        else:
            ret = await self._execute_and_fetch(query, None, 1, timeout=self.timeout)
        if not ret:
            return {}
        return dict(ret[0])

    async def fetch_column(self, query: str, params: Dict=None) -> List:
        """Fetch all possible values of the first column of rows, returning a list

        :param str query:
        :param dict params:
        :return: list
                 An empty list is returned if there is no match rows
        :rtype: list
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self._execute_and_fetch(query, params, 1, timeout=self.timeout)
        else:
            ret = await self._execute_and_fetch(query, None, 1, timeout=self.timeout)
        return [row[0] for row in ret]

    async def fetch_value(self, query: str, params: Dict=None) -> Union[Null, None, str, int]:
        """Retrieve the value of the first column on the first row

        :param str query:
        :param dict params:
        :return: Null if there is no matching row,
                 int|str|None if there is a matching row
        :rtype: Null|None|str|int
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self._execute_and_fetch(query, params, 1, timeout=self.timeout)
        else:
            ret = await self._execute_and_fetch(query, None, 1, timeout=self.timeout)
        if ret is None:
            return Null()
        return ret[0][0]

    async def fetch_all(self, query: str, params: Dict=None) -> List[Dict]:
        """Fetch all (remaining) rows of a query result, returning a list

        :param str query:
        :param dict params:
        :return: a list of dictionaries
        :rtype: list
        """
        if params:
            query, params = pyformat_to_native(query, params)
            ret = await self._execute_and_fetch(query, params, 0, timeout=self.timeout)
        else:
            ret = await self._execute_and_fetch(query, None, 0, timeout=self.timeout)
        return [dict(row) for row in ret]

    async def fetch_by_page(self, query: str, page: int, rows_per_page: int, params: Dict=None) -> Tuple[List, int]:
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

    async def execute(self, query: str, params: Dict=None, timeout: float=None) -> int:
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
        else:
            query, params = pyformat_to_native(query, params)
            _, status, _ = await self.connection._execute(query, params, 0, timeout, True)
        parts = status.split()
        if parts[0] in ("DELETE", "INSERT", "UPDATE"):
            return int(parts[-1])
        # CREATE SEQUENCE, TRUNCATE TABLE
        return 0

    async def execute_many(self, query: str, params: List, timeout: float=None) -> int:
        """Sequentially perform a query against a list of data
        :param str query:
        :param dict params:
        :param float timeout:
        :return: The number of affected rows
        """
        if not params or not isinstance(params, list):
            raise UserWarning('execute_many() requires a list of data')
        self.connection._check_open()
        query, params = pyformat_in_list_to_native(query, params)
        return await self.connection._executemany(query, params, timeout)

    async def execute_and_fetch(self, query: str, params: Dict=None, limit:int=0,
                                timeout: int=None, return_status: bool=False) -> List[Dict]:
        """Execute a query and get returned data
        :param str query:
        :param dict params:
        :param int limit: Can be no limit (0) or limit to 1 row (1)
        :param int timeout:
        :param bool return_status:
        :return: a list of dictionaries
        """
        self.connection._check_open()
        if params:
            query, params = pyformat_to_native(query, params)
        result = await self._execute_and_fetch(query, params, limit, timeout=timeout, return_status=return_status)
        return [dict(item) for item in result]

    async def insert(self, table: str, row_values: Dict, return_fields: str=None) -> Tuple[Dict, int]:
        """Insert a row into a table
        :param table: Table name
        :param row_values: A dict of field name and its value
        :param return_fields: Field name to return
        :return: a tuple (return_dict, affected_rows)
        """
        query, params = generate_native_insert_query(table, row_values)
        if return_fields:
            query = query + " RETURNING %s" % return_fields
        if return_fields:
            ret = await self.connection.fetchrow(query, *params)
            if not ret:
                return {}, 0
            return dict(ret), 1
        self.connection._check_open()
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return {}, int(status.split()[-1])

    async def bulk_insert(self, table: str, row_values: List[Dict], timeout: int=None) -> int:
        """Insert many rows into a table using a single query
        :param str table:
        :param dict row_values:
        :param int timeout:
        :return: a tuple (return_dict, affected_rows)
        """
        self.connection._check_open()
        query = generate_bulk_insert_query(table, row_values)
        status = await self.connection._protocol.query(query, timeout)
        return int(status.split()[-1])

    async def bulk_insert_and_fetch(self, table: str, row_values: List[Dict], return_fields: str,
                                    timeout: int=None) -> List[Dict]:
        """Insert many rows into a table using a single query and return specific fields of newly inserted rows
        The method can be used to retrieve automatically generated field values such as primary keys
        :param str table:
        :param dict row_values:
        :param str return_fields:
        :param int timeout:
        :return: a tuple (return_dict, affected_rows)
        """
        query = generate_bulk_insert_query(table, row_values)
        q = query + " RETURNING %s" % return_fields
        return await self.execute_and_fetch(q, None, timeout=timeout)

    async def update_all(self, table: str, values: Dict) -> int:
        """Update all rows in a table
        :param table: Table name
        :param values: A dict (field_name: value)
        :return: The number of affected rows
        """
        self.connection._check_open()
        fields = values.keys()
        update_fields = ', '.join([f'{field} = %({field})s' for field in fields])
        q = f"UPDATE {table} SET {update_fields}"
        query, params = pyformat_to_native(q, values)
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return int(status.split()[-1])

    async def update(self, table: str, values: Dict, where: Dict) -> int:
        """Update certain rows in a table
        :param table: Table name
        :param values: A dict (field_name: value)
        :param where: A dict (field_name: value)
        :return: The number of affected rows
        """
        if not where:
            raise UserWarning('Inappropriate use of update() without WHERE clause. Use update_all() instead')
        self.connection._check_open()
        fields = values.keys()
        # field_name = %(field_name_v)s (avoid conflicts with WHERE values)
        update_fields = ', '.join(['%s = %%(%s_v)s' % (field, field) for field in fields])
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field)
                                     for field, v in where.items()])
        q = "UPDATE %s SET %s WHERE %s" % (table, update_fields, where_clause)
        values = dict([(k + '_v', v) for k, v in values.items()])
        values.update(where)
        query, params = pyformat_to_native(q, values)
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return int(status.split()[-1])

    async def delete_all(self, table: str) -> int:
        """Delete all rows from a table
        :param str table: Table name
        :return The number of deleted rows
        """
        self.connection._check_open()
        query = "DELETE FROM {}".format(table)
        status = await self.connection._protocol.query(query, None)
        return int(status.split()[-1])

    async def delete(self, table: str, where: Dict) -> int:
        """Delete all rows that match the provided condition
        :param str table: Table name
        :param dict where: A dict (field_name: value) indicates equality clause (=),
                          (field_name: tuple) indicates IN clause
        :return The number of deleted rows
        """
        if not where:
            raise UserWarning('Inappropriate use of delete() without WHERE clause. Use delete_all() instead')
        self.connection._check_open()
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field)
                                     for field, v in where.items()])
        query = "DELETE FROM %s WHERE %s" % (table, where_clause)
        query, params = pyformat_to_native(query, where)
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return int(status.split()[-1])

    async def delete_and_fetch(self, table: str, where: Dict, return_field='*') -> List[Dict]:
        """Delete and return deleted rows
        :param str table: Table name
        :param dict where:
        :param str return_field:
        :return List of deleted rows
        """
        if not where:
            raise UserWarning('Inappropriate use of delete_and_fetch() without WHERE clause. Use delete_all() instead')
        where_clause = " AND ".join(['%s %s %%(%s)s' % (field, ' IN ' if isinstance(v, tuple) else '=', field)
                                     for field, v in where.items()])
        q = "DELETE FROM %s WHERE %s RETURNING %s" % (table, where_clause, return_field)
        return await self.execute_and_fetch(q, where)

    async def _execute_and_fetch(self, query: str, args: Union[List, None],
                                 limit: int, timeout: int, return_status: bool=False) -> List:
        """Execute a query and fetch effected rows

        :param str query:
        :param list args:
        :param int limit:
        :param int timeout:
        :param bool return_status:
        :return: a list of asyncpg.Record
        """
        with self.connection._stmt_exclusive_section:
            def bind_execute(stmt, timeout_):
                return self.connection._protocol.bind_execute(
                    stmt, args or [], '', limit, return_status, timeout_
                )
            timeout = self.connection._protocol._get_timeout(timeout)
            # type : result: list(asyncpg.Record)
            # type : _stmt: asyncpg.protocol.protocol.PreparedStatementState
            result, _stmt = await self.connection._do_execute(query, bind_execute, timeout)
        return result
