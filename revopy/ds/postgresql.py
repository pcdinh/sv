# -*- coding: utf-8 -*-

import asyncpg
import logging
import math
from typing import Dict, List, Tuple, Union

from . import Null, is_placeholder, Placeholder, WHERE_NOT_IN, WHERE_IN, WHERE_BETWEEN
from asyncpg import utils

logger = logging.getLogger("revopy.ds.postgresql")


def pyformat_query_to_native(query: str, params: Dict) -> Tuple[str, List]:
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


def quote_array(values, wrap=True) -> str:
    """Convert Python value into string that is compatible with PostgreSQL's query
    first_name = "Name 01" => 'Name 01'
    this_date = datetime.datetime.now() => '2018-07-30 11:54:25.161946'
    this_date = datetime.datetime.now(datetime.timezone.utc) => '2018-07-30 05:12:30.279286+00:00'
    See: https://paquier.xyz/postgresql-2/manipulating-arrays-in-postgresql/

    :param list values:
    :param bool wrap:
    :return:
    """
    if wrap:
        ret = "'{%s}'"
    else:
        ret = "%s"
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


def generate_native_update_query(table: str, row: Dict, where_clause: Dict) -> Tuple[str, List]:
    """Create an UPDATE query using PostgreSQL's native bind format: $n

    :param str table:
    :param dict row:
    :param dict where_clause:
    :return: a tuple (str, dict)
    """
    field_names, placeholders, params, next_position = quote_fields(row)
    set_phrases = []
    for idx, field_name in enumerate(field_names):
        set_phrases.append("%s = %s" % (field_name, placeholders[idx]))
    if where_clause:
        where_, where_params = _generate_where_clause(where_clause, next_position)
        params.extend(where_params)
        return "UPDATE %s SET %s %s" % (
            table, ', '.join(set_phrases), where_
        ), params
    return "UPDATE %s SET %s" % (table, ','.join(field_names), ','.join(placeholders)), params


def _generate_where_clause(condition: Dict, start_counter=1) -> Tuple[str, List]:
    """

    :param dict condition:
    :param int start_counter:
    :return:
    """
    field_names, placeholders, params, next_position = quote_fields(condition, start_counter)
    where_clause = []
    for idx, field_name in enumerate(field_names):
        where_clause.append(
            "%s %s" % (
                field_name,
                "= %s" % placeholders[idx]
                if not isinstance(placeholders[idx], list) else "= ANY(%s)" % placeholders[idx]
            )
        )
    return "WHERE " + " AND ".join(where_clause), params


def _generate_filter(field: str, value: Union[str, Tuple], op: Union[str, None]):
    """

    :param field:
    :param value:
    :param op:
    :return:
    """

    and_list = []
    op = op or "="  # op can be None (default value)
    simple_ops = {
        "=": 1,
        ">": 2,
        ">=": 3,
        "<": 4,
        "<=": 5,
        "<>": 6,
        "!=": 7,
        "in": 8,
        "not in": 9,
        "between": 10,
        "contain": 11,
        "not contain": 12,
        "overlap": 13,
        "not overlap": 14,
        "like": 15
    }
    try:
        op_position = simple_ops[op]
    except IndexError:
        raise UserWarning("Bad operation: %s", op)
    if op_position < 8:
        return u"%s %s %s" % (field, op, quote(value))
    if op_position < 9:
        return u"%s = ANY(%s)" % (field, quote_array(value))
    if op_position < 10:
        return u"%s != ALL(%s)" % (field, quote_array(value))
    if op_position < 11:
        return u"%s BETWEEN %s AND %s" % (field, quote(value[0]), quote(value[1]))
    if op_position < 12:
        # applicable for range field
        return u"%s @> %s" % (field, quote(value))
    if op_position < 13:
        # applicable for range field
        return u"NOT (%s @> %s)" % (field, quote(value))
    if op_position < 14:
        # applicable for range field
        # value must be a function: int4range, int8range, tsrange ...
        # https://www.postgresql.org/docs/10/static/rangetypes.html
        if not isinstance(value, str):
            raise UserWarning("Bad value compared against the field %s: string is required", field)
        if not "range(" in value:
            raise UserWarning("Bad value compared against the field %s: range function is required", field)
        return u"%s && %s" % (field, value.replace("'", ""))
    if op_position < 15:
        if not isinstance(value, str):
            raise UserWarning("Bad value compared against the field %s: string is required", field)
        if not "range(" in value:
            raise UserWarning("Bad value compared against the field %s: range function is required", field)
        return u"NOT (%s && %s)" % (field, value.replace("'", ""))
    else:
        # LIKE
        return u"%s LIKE %s" % (field, quote(value))


class JoinedTable:
    """Utility class that helps building JOIN query in `generate_select`
    :code
         select(
           JoinedTable(
               "table1", "table2", "primary_key1", "foreign_key2", join_type=JoinedTable.INNER_JOIN
           ).combine("table1", "table3", "primary_key1", "foreign_key3")
         )
    """

    INNER_JOIN = 1
    LEFT_JOIN = 2
    RIGHT_JOIN = 3

    def __init__(self, left_table, right_table, left_table_pk, right_table_fk, join_type=1):
        self.steps = [(left_table, right_table, left_table_pk, right_table_fk, join_type)]

    def combine(self, left_table, right_table, left_table_pk, right_table_fk, join_type=1):
        self.steps.append((left_table, right_table, left_table_pk, right_table_fk, join_type))
        return self

    @staticmethod
    def _build_join(left_table, right_table, left_table_pk, right_table_fk, join_sql):
        """
        Return SQL phrase:
            INNER JOIN right_table
            ON left_table.left_table_pk = right_table.right_table_fk
        :param left_table:
        :param right_table:
        :param left_table_pk:
        :param right_table_fk:
        :param join_sql:
        :return:
        """
        return "%s %s ON %s.%s = %s.%s" % (
            join_sql, right_table,
            left_table, left_table_pk, right_table, right_table_fk
        )

    def to_sql(self):
        """
        Return SQL phrase:
            table1 INNER JOIN table2
            ON table1.left_table_pk = table2.right_table_fk
            INNER JOIN table3
            ON table1.left_table_pk = table3.right_table_fk
        :return:
        """
        ret = ["%s" % self.steps[0][0]]  # first table
        for step in self.steps:
            # Check join_type
            join_sql = "INNER JOIN"
            if step[4] == 2:
                join_sql = "LEFT JOIN"
            elif step[4] == 3:
                join_sql = "RIGHT JOIN"
            ret.append(
                JoinedTable._build_join(
                    step[0], step[1], step[2], step[3], join_sql
                )
            )
        return " ".join(ret)


def generate_select(table: Union[str, JoinedTable], columns: Tuple[str], where: Union[Tuple[Tuple], None],
                    group_by: Union[Tuple[str], None], group_filter: Union[Dict, None],
                    order_by: Union[Dict, None], offset: int=None, limit: int=None):
    """Generate dynamic SELECT query

    :param str|JoinedTable table:
           `table` can be a `JoinedTable:
           JoinedTable("table1", "table2", "primary-key1", "foreign-key2").combine(
             "table1", "table3", "primary-key1", "foreign-key3"
           ).combine(
             "table3", "table4", "primary-key3", "foreign-key4"
           )
    :param tuple columns:
    :param tuple where:
           A list of conditions. E.x:
           (
              -- field_name = <value>
             ("field_name", <value>),
              -- field_name = <value>
             ("field_name", <value>, "="),
              -- field_name1 = <value1> OR field_name2 = <value2>
             OR(("field_name1", <value>), ("field_name2", <value2>)),
              -- field_name > <value>
             ("field_name", <value>, ">"),
             ("field_name", <value>, "<="),
              -- field_name >< <value>
             ("field_name", <value>, "<>"),
              -- field_name != <value>
             ("field_name", <value>, "!="),
              -- field_name = ANY(<value1>, <value2>)
             ("field_name", (<value1>, <value2>), "in"),
              -- field_name != ALL(<value>)
             ("field_name", <value>, "not in"),
              -- field_name BETWEEN <value1> AND <value2>
             ("field_name", (<value1>, <value2>), "bw"),
           )
    :param tuple group_by:
           List of fields to group. E.x: ["<field_name1>", "<field_name2>"]
    :param tuple group_filter:
           Filter after grouping
    :param dict order_by:
    :param int offset:
    :param int limit:
    :return:
    """
    table_prefix = False
    if isinstance(table, str):
        query = ["SELECT", ", ".join(columns), "FROM", table]
    else:  # JoinedTable
        table_prefix = True
        query = ["SELECT", ", ".join(columns), "FROM", table.to_sql()]

    if where:
        where_clause = []
        make_filter = _generate_filter  # avoid lookup
        for cond in where:
            try:
                and_filter = True
                op = cond[2]
            except IndexError:
                op = None
            except TypeError:
                and_filter = False
            if and_filter is False:
                where_clause.append(cond.to_sql())
            else:
                where_clause.append(make_filter(cond[0], cond[1], op))
        query.extend(("WHERE", " AND ".join(where_clause)))
    if group_by:
        query.append("GROUP BY %s" % ", ".join(group_by))
    if group_filter:
        group_conditions = []
        make_filter = _generate_filter  # avoid lookup
        for cond in group_filter:
            try:
                and_filter = True
                op = cond[2]
            except IndexError:
                op = None
            except TypeError:
                and_filter = False
            if and_filter is False:
                group_conditions.append(cond.to_sql())
            else:
                group_conditions.append(make_filter(cond[0], cond[1], op))
        query.extend(("HAVING", " AND ".join(group_conditions)))
    if order_by:
        query.append("ORDER BY %s" % ", ".join(["%s %s" % (field_info[0], field_info[1]) for field_info in order_by]))
    if offset and limit:
        query.append("OFFSET %s LIMIT %s" % (offset, limit))
    return " ".join(query)


def quote_fields(fields: Dict, start_counter=1) -> Tuple[List, List, List, int]:
    """Given a mapping between field names and their values, return a tuple of field names, list of $n placeholders
    and list of field values

    :param dict fields:
    :param int start_counter:
    :return:
    :rtype: tuple
    """
    field_names = fields.keys()  # list of field names
    placeholders = []  # list of $n placeholders: $1, $2, $3
    next_position = start_counter  # set up internal counter for positional parameters: $n
    params = []  # List of field values
    for field_name, value in fields.items():
        if is_placeholder(value):
            placeholders.append(quote_placeholder(value))
        else:
            placeholders.append("$%s" % next_position)
            params.append(value)
            next_position += 1
    return list(field_names), placeholders, params, next_position


class Or:
    """SQL builder for the conditional clause OR
    :code

    Or(
        Match("last_name", "to_tsquery('F5')", query_type=Match.FT_CUSTOM),
        Match("first_name", "Định F5"),
    )
    """

    def __init__(self, *conditions):
        self.conditions = conditions

    def to_sql(self):
        ret = []
        make_filter = _generate_filter  # avoid lookup
        for cond in self.conditions:
            try:
                complex_filter = False
                op = cond[2]
            except IndexError:
                op = None
            except TypeError:
                complex_filter = True
            if complex_filter is True:
                ret.append(cond.to_sql())
            else:
                ret.append(make_filter(cond[0], cond[1], op))
        try:
            ret[1]  # 2+ filters?
            return "(%s)" % " OR ".join(ret)
        except IndexError:
            return " OR ".join(ret)

    def __str__(self):
        return self.to_sql()


class Match:
    """SQL builder for the full-text matching clause
    See: https://www.postgresql.org/docs/10/static/textsearch-controls.html
    """
    FT_PLAIN = 1
    # Term search: & or | or <-> or ! or <N>
    # Pre-defined operator
    FT_USER_QUERY = 2
    FT_ALL_TERM = 3  # Term search: &
    FT_ANY_TERM = 4  # Term search: |
    FT_PHRASE = 5  # Phrase search: phraseto_tsquery()
    FT_PHRASE_DISTANCE = 6
    # Prefix search
    FT_PREFIX = 7
    # Used when developer wants to specify his own query. E.x: to_tsquery('fat') <-> to_tsquery('cat | rat')
    # which results in: 'fat' <-> 'cat' | 'fat' <-> 'rat'
    # reg_config will not be applied
    # See: https://www.postgresql.org/docs/10/static/textsearch-features.html
    FT_CUSTOM = 8

    def __init__(self, field: str, terms: Union[str, Tuple], phrase_distance=2, query_type: int=1,
                 reg_config: str="english"):
        """

        :param str field:
               Field name or a function applied to a field name. E.x: title or to_tsvector(title).
               It can be a concatenation of several field names. E.x: title || '. ' || content
        :param str|tuple terms:
               User provided keyword to search for. E.x: guitar, guitar | piano
        :param int phrase_distance:
               Find matching rows that have words word1 and word2 separated by at most <phrase_distance> other word.
        :param int query_type:
        :param str reg_config:
        """
        self.field = field
        self.terms = terms
        self.phrase_distance = phrase_distance  # applied for FT_PHRASE_DISTANCE
        self.query_type = query_type
        self.reg_config = reg_config

    def to_sql(self):
        if self.query_type == 1:
            # The operator AND (&) will be used
            return "%s @@ plainto_tsquery('%s', %s)" % (self.field, self.reg_config, quote(self.terms))
        if self.query_type == 2:
            # The operator AND (&) or OR (|) or FOLLOWED_by (<->) or DISTANCE (<N>) must be prepared by developer
            # E.x: learning & mathematics
            #  Single-quoted phrases are accepted. E.x: ''supernovae stars'' & !crab
            return "%s @@ to_tsquery('%s', %s)" % (self.field, self.reg_config, quote(self.terms))
        if self.query_type == 3:
            # All terms must be found in matching documents
            if not isinstance(self.terms, tuple):
                raise UserWarning("Tuple is required in a query FT_ALL_TERM")
            return "%s @@ to_tsquery('%s', %s)" % (self.field, self.reg_config, quote(" & ".join(self.terms)))
        if self.query_type == 4:
            # At least one of terms must be found in matching documents
            if not isinstance(self.terms, tuple):
                raise UserWarning("Tuple is required in a query FT_ANY_TERM")
            return "%s @@ to_tsquery('%s', %s)" % (self.field, self.reg_config, quote(" | ".join(self.terms)))
        if self.query_type == 5:
            # The operator FOLLOWED_BY (<->) will be used
            return "%s @@ phraseto_tsquery('%s', %s)" % (self.field, self.reg_config, quote(self.terms))
        if self.query_type == 6:
            # The operator DISTANCE (<N>: <2>, <3> ...) will be used
            # Phrase "like mathematics" will be converted to "like <2> mathematics"
            return "%s @@ to_tsquery('%s', %s)" % (
                self.field, self.reg_config, quote(self.terms.replace(" ", " <%s> " % self.phrase_distance))
            )
        if self.query_type == 7:
            # Prefix search
            if not isinstance(self.terms, str):
                raise UserWarning("String is required in a query FT_PREFIX")
            if " " in self.terms:
                raise UserWarning("Single term, not phrase, is required in a query FT_PREFIX")
            return "%s @@ to_tsquery('%s', %s)" % (self.field, self.reg_config, quote(self.terms + ":*"))
        # Custom query specified by developer
        return "%s @@ (%s)" % (self.field, self.terms)

    def __str__(self):
        return self.to_sql()


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
            query, params = pyformat_query_to_native(query, params)
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
            query, params = pyformat_query_to_native(query, params)
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
            query, params = pyformat_query_to_native(query, params)
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
            query, params = pyformat_query_to_native(query, params)
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
            query, params = pyformat_query_to_native(query, params)
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
            query, params = pyformat_query_to_native(query, params)
        result = await self._execute_and_fetch(query, params, limit, timeout=timeout, return_status=return_status)
        return [dict(item) for item in result]

    async def insert(self, table: str, row_values: Dict) -> int:
        """Insert a row into a table

        :param str table: Table name
        :param dict row_values: A dict of field name and its value
        :return: a number of affected rows
        """
        query, params = generate_native_insert_query(table, row_values)
        self.connection._check_open()
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return int(status.split()[-1])

    async def insert_and_fetch(self, table: str, row_values: Dict, return_fields: str) -> Dict:
        """Insert a row into a table and retrieve specific fields of affected rows

        :param str table: Table name
        :param dict row_values: A dict of field name and its value
        :param str return_fields: Field name to return
        :return: a dictionary of field names and field values
        """
        query, params = generate_native_insert_query(table, row_values)
        if return_fields:
            query = query + " RETURNING %s" % return_fields
        ret = await self._execute_and_fetch(query, params, 1, self.timeout)
        if not ret:
            return {}
        return dict(ret[0])

    async def bulk_insert(self, table: str, row_values: List[Dict], timeout: int=None) -> int:
        """Insert many rows into a table using a single query

        :param str table:
        :param dict row_values:
        :param int timeout:
        :return: a number of affected_rows
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
        :return: a list of specific fields of affected rows
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
        query, params = pyformat_query_to_native(q, values)
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
        update_query, params = generate_native_update_query(table, values, where)
        _, status, _ = await self.connection._execute(update_query, params, 0, None, True)
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
        where_clause, params = _generate_where_clause(where)
        query = "DELETE FROM %s %s" % (table, where_clause)
        _, status, _ = await self.connection._execute(query, params, 0, None, True)
        return int(status.split()[-1])

    async def delete_and_fetch(self, table: str, where: Dict, return_field: str='*') -> List[Dict]:
        """Delete and return deleted rows

        :param str table: Table name
        :param dict where:
        :param str return_field:
        :return List of deleted rows
        """
        if not where:
            raise UserWarning('Inappropriate use of delete_and_fetch() without WHERE clause. Use delete_all() instead')
        where_clause, params = _generate_where_clause(where)
        query = "DELETE FROM %s %s RETURNING %s" % (table, where_clause, return_field)
        result = await self._execute_and_fetch(query, params, 0, self.timeout, return_status=False)
        return [dict(item) for item in result]

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

    async def find(self, table: str, columns: List[str], where: Union[List, None],
                   group_by: Union[List[str], None], group_filter: Union[Dict, None], order_by: Union[Dict, None],
                   offset=0, limit=1):
        """Fetch all rows of a query result, returning a list

        :param str table:
        :param list columns:
        :param dict where:
        :param list group_by:
        :param dict group_filter:
        :param dict order_by:
        :param int offset:
        :param int limit:
        :return: a list of dictionaries
        :rtype: list
        """
        query = generate_select(table, columns, where, group_by, group_filter, order_by, offset, limit)
        ret = await self._execute_and_fetch(query, None, 0, timeout=self.timeout)
        return [dict(row) for row in ret]
