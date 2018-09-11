# -*- coding: utf-8 -*-

import logging
from vibora import Route
from vibora import Request, Response

from start_app import app
import revopy
from revopy.ds.postgresql import SessionManager
from revopy.helpers.debug_utils import get_exception_details
from revopy import Config
from revopy.ds.manager import managed
from revopy.helpers.response_utils import JsonResponse, WebResponse
from revopy.ds import is_null, Placeholder

logger = logging.getLogger("app.home")


@app.route('/')
async def home(request: Request, config: Config):
    config = request.app.components.get(Config)

    return JsonResponse(
        {
            'hello': 'world',
            'config': config.POSTGRESQL_DSN,
            'pg_pool': hasattr(request.app, "pg")
        }
    )


@app.route('/home')
async def home(request: Request):
    try:
        request.app.components.get(Route)
        return Response(b'Second')
    except Exception as error:
        return Response(str(error).encode(), status_code=500)


@app.route('/c')
async def test_connection(request: Request):
    try:
        # Take a connection from the pool.
        ''':type : asyncpg.connection.Connection'''
        async with request.app.pool.acquire() as connection:
            # Open a transaction.
            async with connection.transaction():
                # Run the query
                result = await connection.fetchval('SELECT 2 ^ $1', 2)
                config = request.app.components.get(Config)
                return WebResponse('Result {}. DEBUG: {}'.format(result, config.DEBUG))
    except Exception as error:
        return WebResponse(str(error), status_code=500)


@app.route('/c1', methods=[b"GET", b"POST"])
async def test_connection(request: Request):
    try:
        import datetime
        # Take a connection from the pool.
        ''':type : asyncpg.connection.Connection'''
        async with request.app.pg.pool.acquire() as connection:
            # Open a transaction.
            async with connection.transaction():
                # Run the query
                await connection.execute(
                    '''INSERT INTO users(user_id, first_name, last_name, source, status, weight, created_time) 
                       VALUES($1, $2, $3, $4, $5, $6)''',
                    1, "Lionen", "Messi", 1, 1, datetime.datetime.utcnow()
                )
                result_01 = await connection.fetchrow('SELECT user_id, first_name FROM users')
                result_02 = await connection.fetchrow('SELECT user_id, first_name FROM users WHERE user_id = 10')
                result_03 = await connection.fetch('SELECT user_id, first_name FROM users WHERE user_id = 10')
                await connection.execute('''DELETE FROM users WHERE user_id = $1''', 1)
                config = request.app.components.get(Config)
                return WebResponse(
                    'Result {}. Keys: {}, Values: {}, Empty: {}, DEBUG: {}'.format(result_01, result_01.keys(),
                                                                                   result_01.values(), result_03,
                                                                                   config.DEBUG))
    except Exception as error:
        return WebResponse(str(error), status_code=500)


@app.route('/c2', methods=[b"GET", b"POST"])
async def test_connection(request: Request):
    import traceback
    import sys
    try:
        import datetime
        connection: revopy.ds.postgresql.SessionManager = None
        async with managed(request.app.pg) as connection:
            ''':type : revopy.ds.postgresql.SessionManager'''
            await connection.execute(
                "INSERT INTO users(user_id, first_name, last_name, source, status, created_time) \
                 VALUES(%(user_id)s, %(first_name)s, %(last_name)s, %(source)s, %(status)s, %(created_time)s)",
                {
                    "user_id": 1,
                    "first_name": "L1000",
                    "last_name": "M1000",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs1 = await connection.fetch_one("SELECT user_id, first_name FROM users")
            rs2 = await connection.fetch_one(
                "SELECT user_id, first_name FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs3 = await connection.fetch_value(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs4 = await connection.fetch_column(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs5 = await connection.fetch_all(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs6 = await connection.execute(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs7 = await connection.execute(
                "SELECT user_id FROM users"
            )
            rs8 = await connection.execute(
                "INSERT INTO users(user_id, first_name, last_name, source, status, created_time) \
                 VALUES(%(user_id)s, %(first_name)s, %(last_name)s, %(source)s, %(status)s, %(created_time)s)",
                {
                    "user_id": 1,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs9 = await connection.execute(
                "INSERT INTO users(user_id, first_name, last_name, source, status, created_time) \
                 VALUES(%(user_id)s, %(first_name)s, %(last_name)s, %(source)s, %(status)s, %(created_time)s) RETURNING user_id",
                {
                    "user_id": 2,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs10 = await connection.insert(
                "users",
                {
                    "user_id": 3,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs11 = await connection.insert_and_fetch(
                "users",
                {
                    "user_id": 3,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                },
                "user_id, first_name"
            )
            rs12 = await connection.insert(
                "users",
                {
                    "user_id": 5,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs13 = await connection.execute_and_fetch(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {
                    "user_id": 5
                }
            )
            rs14 = await connection.execute_and_fetch(
                "INSERT INTO users(user_id, first_name, last_name, source, status, created_time) \
                 VALUES(%(user_id)s, %(first_name)s, %(last_name)s, %(source)s, %(status)s, %(created_time)s) RETURNING user_id",
                {
                    "user_id": 6,
                    "first_name": "Lionen6",
                    "last_name": "Messi6",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs15 = await connection.fetch_by_page(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                1, 2,
                params={
                    "user_id": 5
                }
            )
            rs16 = await connection.update(
                "users",
                {
                    "first_name": "UpdatedName",
                    "last_name": Placeholder("CAST('UpdatedNameLast2' AS VARCHAR)")
                },
                {
                    "user_id": 5
                }
            )
            rs17 = await connection.update_all(
                "users",
                {
                    "first_name": "UpdatedName2"
                }
            )
            rs18 = await connection.fetch_all(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 1}
            )
            rs19 = await connection.delete_all("users")
            rs20 = await connection.insert(
                "users",
                {
                    "user_id": 1,
                    "first_name": "F1",
                    "last_name": "L1",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs21 = await connection.delete_all("users")
            rs22 = await connection.insert(
                "users",
                {
                    "user_id": 2,
                    "first_name": "F1",
                    "last_name": "L1",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs23 = await connection.delete(
                "users",
                {
                    "user_id": 2
                }
            )
            rs24 = await connection.insert(
                "users",
                {
                    "user_id": 3,
                    "first_name": "F3",
                    "last_name": "L3",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                }
            )
            rs25 = await connection.delete_and_fetch(
                "users",
                {
                    "user_id": 3
                }
            )
            rs26 = await connection.fetch_all(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {"user_id": 3}
            )
            rs27 = await connection.execute_many(
                "INSERT INTO users(user_id, first_name, last_name, source, status, created_time) \
                 VALUES(%(user_id)s, %(first_name)s, %(last_name)s, %(source)s, %(status)s, %(created_time)s)",
                [
                    {
                        "user_id": 3,
                        "first_name": "F3",
                        "last_name": "L3",
                        "source": 1,
                        "status": 1,
                        "created_time": datetime.datetime.utcnow()
                    },
                    {
                        "user_id": 4,
                        "first_name": "F4",
                        "last_name": "L4",
                        "source": 1,
                        "status": 1,
                        "created_time": datetime.datetime.utcnow()
                    }
                ]
            )
            rs28 = await connection.fetch_all(
                "SELECT user_id FROM users WHERE user_id = ANY(%(user_id)s)",
                {"user_id": [3, 4]}
            )
            rs29 = await connection.fetch_all(
                "SELECT user_id FROM users WHERE first_name = ANY(%(first_name)s)",
                {"first_name": ["F4", "F5"]}
            )
            await connection.delete_all("users")
            rs30 = await connection.bulk_insert(
                "users",
                [
                    {
                        "user_id": 5,
                        "first_name": "F5",
                        "last_name": Placeholder("UPPER(%(last_name)s)", {"last_name": "Last5"}),
                        "source": 1,
                        "status": 1,
                        "access_token": None,
                        "created_time": datetime.datetime.utcnow()
                    },
                    {
                        "user_id": 6,
                        "first_name": "F6",
                        "last_name": Placeholder("UPPER('Last6')"),
                        "source": 1,
                        "status": 1,
                        "access_token": None,
                        "created_time": datetime.datetime.utcnow()
                    }
                ]
            )
            rs31 = await connection.fetch_all("SELECT user_id, first_name, last_name FROM users")

            await connection.delete_all("users")
            rs32 = await connection.execute("CREATE SEQUENCE IF NOT EXISTS users_user_id;")
            rs33 = await connection.bulk_insert_and_fetch(
                "users",
                [
                    {
                        "user_id": Placeholder("NEXTVAL('users_user_id')"),
                        "first_name": "F5",
                        "last_name": Placeholder("UPPER(%(last_name)s)", {"last_name": "Last5"}),
                        "source": 1,
                        "status": 1,
                        "weight": Placeholder("int4range(50, 70)"),
                        "access_token": None,
                        "created_time": datetime.datetime.utcnow()
                    },
                    {
                        "user_id": Placeholder("NEXTVAL('users_user_id')"),
                        "first_name": "F6",
                        "last_name": Placeholder("UPPER('Last6')"),
                        "source": 1,
                        "status": 1,
                        "weight": Placeholder("int4range(80, 90)"),
                        "access_token": None,
                        "created_time": datetime.datetime.utcnow()
                    }
                ],
                return_fields="user_id"
            )
            from datetime import timedelta
            from revopy.ds.postgresql import generate_select, Or, Match
            from revopy.ds import SORT_ASC, SORT_DESC
            rs34 = generate_select(
                "users",
                ("user_id", "first_name", "last_name", "status"),
                (
                    ("status", 1),
                    Or(
                        ("status", 1),
                        ("status", 2)
                    ),
                    ("first_name", "Định")
                ),
                ("last_name", ),
                None,
                (("user_id", SORT_ASC), ("first_name", SORT_DESC))
            )
            now = datetime.datetime.utcnow()
            two_days_before = now - timedelta(days=2)
            rs35 = generate_select(
                # table
                "users",
                # columns
                ("user_id", ),
                # where
                (
                    ("first_name", "Định"),
                    ("last_name", "Định", "<>"),
                    ("status", (0, 1), "not in"),
                    ("status", (3, 4), "in"),
                    ("created_time", (now, two_days_before), "between"),
                    ("weight", 40, "contain"),
                    Or(
                        ("weight", 40, "contain"),
                        ("weight", 50, "contain")
                    ),
                    ("weight", 8, "not contain"),
                    ("weight", "int4range(80, 90)", "overlap"),
                    ("weight", "int4range(50, 60)", "not overlap"),
                    ("last_name", "%nh", "like"),
                ),
                # group by
                ("user_id", "first_name"),
                # group filter
                None,
                # order by
                (("user_id", SORT_ASC), ("first_name", SORT_DESC))
            )
            rs36 = await connection.find(
                # table
                "users",
                # columns
                ("user_id", ),
                # where
                (
                    ("first_name", "Định"),
                    ("last_name", "Định", "<>"),
                    ("status", (0, 1), "not in"),
                    ("status", (3, 4), "in"),
                    ("created_time", (now, two_days_before), "between"),
                    ("weight", 4, "contain"),
                    Or(
                        ("status", 4),
                        ("status", 6)
                    ),
                    ("weight", 8, "not contain"),
                    ("weight", "int4range(5, 6)", "overlap"),
                    ("weight", "int4range(50, 60)", "not overlap"),
                ),
                # group by
                ("user_id", "first_name"),
                # group filter
                None,
                # order by
                (("user_id", SORT_ASC), ("first_name", SORT_DESC))
            )
            rs37 = generate_select(
                # table
                "users",
                # columns
                ("user_id", "first_name"),
                # where
                (
                    Match("first_name", "Định F5"),
                    Match("last_name", "Định | F5", query_type=Match.FT_USER_QUERY),
                    Match("last_name", ('Định', 'F5'), query_type=Match.FT_ALL_TERM),
                    Match("last_name", ('Định', 'F5'), query_type=Match.FT_ANY_TERM),
                    Match("last_name", 'Định F5', query_type=Match.FT_PHRASE),
                    Match("last_name", 'Định F5', query_type=Match.FT_PHRASE_DISTANCE, phrase_distance=3),
                    Match("last_name", 'F', query_type=Match.FT_PREFIX),
                    Match("last_name", "to_tsquery('F5') <-> to_tsquery('Định | Phạm')", query_type=Match.FT_CUSTOM),
                    Match("last_name", "to_tsquery('F5')", query_type=Match.FT_CUSTOM),
                    Or(
                        Match("last_name", "to_tsquery('F10')", query_type=Match.FT_CUSTOM),
                        Match("first_name", "Định F10"),
                    ),
                ),
                # group by
                ("user_id", "first_name"),
                # group filter
                None,
                # order by
                (("user_id", SORT_ASC), ("first_name", SORT_DESC))
            )
            rs38 = await connection.find(
                # table
                "users",
                # columns
                ("user_id", "first_name"),
                # where
                (
                    Match("first_name", "Định F5"),
                    Match("last_name", "Định | F5", query_type=Match.FT_USER_QUERY),
                    Match("last_name", ('Định', 'F5'), query_type=Match.FT_ALL_TERM),
                    Match("last_name", ('Định', 'F5'), query_type=Match.FT_ANY_TERM),
                    Match("last_name", 'Định F5', query_type=Match.FT_PHRASE),
                    Match("last_name", 'Định F5', query_type=Match.FT_PHRASE_DISTANCE, phrase_distance=3),
                    Match("last_name", 'F', query_type=Match.FT_PREFIX),
                    Match("last_name", "to_tsquery('F5') <-> to_tsquery('Định | Phạm')", query_type=Match.FT_CUSTOM),
                    Match("last_name", "to_tsquery('F5')", query_type=Match.FT_CUSTOM),
                    Or(
                        Match("last_name", "to_tsquery('F5')", query_type=Match.FT_CUSTOM),
                        Match("first_name", "Định F5"),
                    ),
                ),
                # group by
                ("user_id", "first_name"),
                # group filter
                None,
                # order by
                (("user_id", SORT_ASC), ("first_name", SORT_DESC))
            )
            return JsonResponse(
                {
                    "rs1": rs1,
                    "rs2": rs2,
                    "rs3": str(rs3) if is_null(rs3) else rs3,
                    "rs4": rs4,
                    "rs5": rs5,
                    "rs6": rs6,
                    "rs7": rs7,
                    "rs8": rs8,
                    "rs9": rs9,
                    "rs10": rs10,
                    "rs11": rs11,
                    "rs12": {"rs": rs12},
                    "rs13": {"rs": rs13},
                    "rs14": {"rs": rs14},
                    "rs15": {"rs": rs15},
                    "rs16": {"rs": rs16},
                    "rs17": {"rs": rs17},
                    "rs18": {"rs": rs18},
                    "rs19": {"rs": rs19},
                    "rs20": rs20,
                    "rs21": rs21,
                    "rs22": rs22,
                    "rs23": rs23,
                    "rs24": rs24,
                    "rs25": rs25,
                    "rs26": rs26,
                    "rs27": rs27,
                    "rs28": rs28,
                    "rs29": rs29,
                    "rs30": rs30,
                    "rs31": rs31,
                    "rs32": rs32,
                    "rs33": rs33,
                    "rs34": rs34,
                    "rs35": rs35,
                    "rs36": rs36,
                    "rs37": rs37,
                    "rs38": rs38
                }
            )
    except Exception as error:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tbe = traceback.TracebackException(
            exc_type, exc_value, exc_tb,
        )
        e1 = ''.join(tbe.format())
        e2 = ''.join(tbe.format_exception_only())
        try:
            e3 = get_exception_details()
        except BaseException as e:
            return WebResponse(str(e), status_code=500)
        return WebResponse(
            str(error) + ":" + type(error).__name__ + ">> \n" + str(e1) + "\n\n" + str(e2) + "\n\n" + e3,
            status_code=500
        )


@app.route('/product/<product_id>')
async def show_product(product_id: int, request: Request):
    logger.info("Testing REST route")
    config = request.app.components.get(Config)
    debug_mode = config.DEBUG
    return WebResponse(f'Chosen product: {product_id}. Debug: {debug_mode}')
