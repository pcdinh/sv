from vibora import Route
from vibora import Request, Response

from revopy.ds.postgresql import SessionManager
from revopy.helpers.debug_utils import get_exception_details
from start_app import app, managed, Config
from revopy.helpers.response_utils import JsonResponse, WebResponse
from revopy.ds import is_null


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
                    '''INSERT INTO users(user_id, first_name, last_name, source, status, created_time) 
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
        async with managed(request.app.pg) as connection:
            ''':type : revopy.ds.postgresql.SessionManager'''
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
            rs10, count10 = await connection.insert(
                "users",
                {
                    "user_id": 3,
                    "first_name": "Lionen",
                    "last_name": "Messi",
                    "source": 1,
                    "status": 1,
                    "created_time": datetime.datetime.utcnow()
                },
                return_fields="user_id, first_name"
            )
            rs11, count11 = await connection.insert(
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
            rs12 = await connection.execute_and_fetch(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                {
                    "user_id": 5
                }
            )
            rs13 = await connection.execute_and_fetch(
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
            rs14 = await connection.fetch_by_page(
                "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                1, 2,
                params={
                    "user_id": 5
                }
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
                    "rs10": {"rs": rs10, "count": count10},
                    "rs11": {"rs": rs11, "count": count11},
                    "rs12": {"rs": rs12},
                    "rs13": {"rs": rs13},
                    "rs14": {"rs": rs14}
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
